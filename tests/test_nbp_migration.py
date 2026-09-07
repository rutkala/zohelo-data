import importlib.util
import json
from hashlib import sha256
from pathlib import Path
import sys
import tempfile
import unittest

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("check_nbp_migration", ROOT / "scripts" / "check_nbp_migration.py")
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)


class MemoryStore:
    def __init__(self):
        self.files = {}

    def put(self, file_id, data):
        self.files[file_id] = data

    def read(self, file_id):
        return self.files[file_id]


class MigrationCoverageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = MemoryStore()
        self.number = 0

    def _parquet(self, dataset_id, rows, *, gold=False):
        self.number += 1
        path = self.root / f"{self.number}-{dataset_id}.parquet"
        connection = duckdb.connect()
        try:
            if gold:
                connection.execute("create table source(effectiveDate date, price double)")
                connection.executemany("insert into source values (?, ?)", rows)
            else:
                connection.execute("create table source(effectiveDate date, code varchar, value double)")
                connection.executemany("insert into source values (?, ?, ?)", rows)
            connection.execute(f"copy source to '{str(path)}' (format parquet)")
        finally:
            connection.close()
        data = path.read_bytes()
        file_id = f"file{self.number}"
        self.store.put(file_id, data)
        dates = [row[0] for row in rows]
        return {
            "dataset_id": dataset_id,
            "min_date": min(dates),
            "max_date": max(dates),
            "files": [{"id": file_id, "size": len(data), "sha256": sha256(data).hexdigest()}],
        }

    def _manifest(self, version, datasets):
        return {
            "format_version": version,
            "release_scope": "nbp_silver" if version == 1 else "nbp_platform",
            "datasets": datasets,
        }

    def _pair(self, *, omit_a_key=False, regress_a_date=False):
        old, new = [], []
        for dataset_id, old_rows, new_rows, gold in (
            ("nbp_exchange_rates_table_a", [("2026-09-03", "USD", 4.0), ("2026-09-04", "EUR", 4.2)], [("2026-09-03", "USD", 9.0), ("2026-09-04", "EUR", 4.3), ("2026-09-06", "GBP", 5.0)], False),
            ("nbp_exchange_rates_table_b", [("2026-09-04", "USD", 4.1)], [("2026-09-04", "USD", 4.9), ("2026-09-06", "EUR", 4.2)], False),
            ("nbp_exchange_rates_table_c", [("2026-09-04", "USD", 4.1)], [("2026-09-04", "USD", 4.8), ("2026-09-06", "EUR", 4.2)], False),
            ("nbp_gold_prices", [("2026-09-04", 401.0)], [("2026-09-04", 499.0), ("2026-09-06", 500.0)], True),
        ):
            old.append(self._parquet(dataset_id, old_rows, gold=gold))
            if omit_a_key and dataset_id == "nbp_exchange_rates_table_a":
                new_rows = [row for row in new_rows if row[1] != "EUR"]
            if regress_a_date and dataset_id == "nbp_exchange_rates_table_a":
                new_rows = [("2026-09-03", "USD", 9.0)]
            new.append(self._parquet(dataset_id, new_rows, gold=gold))
        return self._manifest(1, old), self._manifest(2, new)

    def test_added_rows_and_changed_values_keep_all_baseline_keys(self):
        baseline, current = self._pair()
        report = module.compare_silver_key_coverage(self.store, baseline, current, self.root / "compare")
        self.assertEqual(report["downloaded_bytes"], sum(len(value) for value in self.store.files.values()))
        self.assertEqual([item["missing_key_count"] for item in report["datasets"]], [0, 0, 0, 0])
        self.assertEqual(report["datasets"][0]["baseline_distinct_keys"], 2)
        self.assertGreater(report["datasets"][0]["current_distinct_keys_through_baseline_cutoff"], 1)

    def test_omitted_baseline_key_returns_complete_bounded_failure_report(self):
        baseline, current = self._pair(omit_a_key=True)
        with self.assertRaises(module.MigrationCoverageError) as raised:
            module.compare_silver_key_coverage(self.store, baseline, current, self.root / "compare")
        comparison = raised.exception.comparison
        self.assertEqual(len(comparison["datasets"]), 4)
        self.assertEqual(comparison["missing_key_count"], 1)
        first = comparison["datasets"][0]
        self.assertEqual(first["missing_key_samples"], [["2026-09-04", "EUR"]])
        self.assertTrue(first["date_bounds_metadata_match"])
        self.assertIsInstance(json.dumps(comparison, sort_keys=True), str)

    def test_current_max_date_regression_returns_failure_report(self):
        baseline, current = self._pair(regress_a_date=True)
        with self.assertRaises(module.MigrationCoverageError) as raised:
            module.compare_silver_key_coverage(self.store, baseline, current, self.root / "compare")
        first = raised.exception.comparison["datasets"][0]
        self.assertTrue(first["max_date_regressed"])
        self.assertIn({"dataset_id": "nbp_exchange_rates_table_a", "reason": "max_date_regressed"}, raised.exception.comparison["violations"])

    def test_ambiguous_baseline_release_folder_is_rejected(self):
        class AmbiguousStore:
            def find(self, name, parent_id):
                return ["one", "two"] if name == "releases" else []

        with self.assertRaisesRegex(module.MigrationAuditError, "releases folder is missing or ambiguous"):
            module.load_baseline_release(AmbiguousStore(), "root", "1ab2f2f0-4325-42fc-bc92-cf3d9e9d9eea", "474bbb61a2bb9d88266808e872f8a7613aca23d6")

    def test_wrong_baseline_or_current_format_is_rejected(self):
        baseline, current = self._pair()
        baseline["format_version"] = 2
        with self.assertRaisesRegex(module.MigrationAuditError, "baseline release must be v1"):
            module.compare_silver_key_coverage(self.store, baseline, current, self.root / "compare")
        baseline["format_version"] = 1
        current["format_version"] = 1
        with self.assertRaisesRegex(module.MigrationAuditError, "current release must be v2"):
            module.compare_silver_key_coverage(self.store, baseline, current, self.root / "compare")


if __name__ == "__main__":
    unittest.main()
