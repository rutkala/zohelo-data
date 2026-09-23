"""Credential-free regressions for retained DBW Actions release handoff."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import prepare_retained_dbw_release as prepare
import verify_retained_dbw_release as verify


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "fresh"
        self.raw, self.baseline = prepare.reviewed_report()

    def run_preparation(self, *, mismatch=None, changed_inventory=False, validator_error=False):
        report = dict(self.baseline)
        report["run_id"] = "fresh-independent-run"
        if mismatch:
            report[mismatch] = "changed"
        def audit(storage, package, **kwargs):
            package.mkdir(parents=True)
            (package / "audit-report.json").write_text(json.dumps(report))
            (package / "run-status.json").write_text(json.dumps({"run_id": report["run_id"]}))
            return report
        inventory = {
            "inventory_sha256": "0" * 64 if changed_inventory else self.baseline["inventory_sha256"],
            "object_count": self.baseline["remote_object_count"],
            "total_bytes": self.baseline["remote_total_bytes"],
        }
        with patch.object(prepare.audit, "discover", return_value={}), \
             patch.object(prepare.audit, "validate_inventory", return_value=({}, set())), \
             patch.object(prepare.audit, "inventory_document", return_value=inventory), \
             patch.object(prepare.audit, "audit_retained_dbw", side_effect=audit) as restore, \
             patch.object(prepare, "validate_audit", side_effect=ValueError("invalid package") if validator_error else None) as validate:
            if changed_inventory:
                with self.assertRaisesRegex(prepare.PreparationError, "inventory differs"):
                    prepare.prepare(object(), self.output)
                restore.assert_not_called()
                return
            result = prepare.prepare(object(), self.output)
            validate.assert_called_once_with(self.output / "audit", require_reviewed_snapshot=True)
            return result

    def test_historic_report_is_byte_identical_and_fresh_identity_is_separate(self):
        result = self.run_preparation()
        self.assertEqual(result["status"], "verified")
        self.assertFalse(result["publication_performed"])
        self.assertTrue(result["read_only"])
        self.assertEqual((self.output / "audit/audit-report.json").read_bytes(), self.raw)
        fresh = json.loads((self.output / "fresh-audit-report.json").read_bytes())
        self.assertEqual(fresh["run_id"], "fresh-independent-run")
        status = json.loads((self.output / "audit/run-status.json").read_bytes())
        self.assertEqual(status["run_id"], self.baseline["run_id"])
        self.assertEqual(status["restore_run_id"], fresh["run_id"])

    def test_inventory_change_stops_before_any_payload_restore(self):
        self.run_preparation(changed_inventory=True)
        self.assertEqual(json.loads((self.output / "restore-evidence.json").read_bytes())["status"], "failed")

    def test_changed_source_evidence_invalidates_package(self):
        with self.assertRaisesRegex(prepare.PreparationError, "Restored evidence differs"):
            self.run_preparation(mismatch="measured_parquet_rows")
        self.assertEqual(json.loads((self.output / "audit/run-status.json").read_bytes())["status"], "failed")

    def test_failed_publisher_validation_invalidates_compatibility_envelope(self):
        with self.assertRaisesRegex(ValueError, "invalid package"):
            self.run_preparation(validator_error=True)
        self.assertEqual(json.loads((self.output / "audit/run-status.json").read_bytes())["status"], "failed")

    def test_existing_directory_is_not_reused(self):
        self.output.mkdir()
        with self.assertRaisesRegex(prepare.PreparationError, "new output directory"):
            prepare.prepare(object(), self.output)

    def test_modified_baseline_is_rejected(self):
        path = Path(self.temp.name) / "changed.json"
        path.write_bytes(self.raw + b" ")
        with self.assertRaisesRegex(prepare.PreparationError, "SHA-256"):
            prepare.reviewed_report(path)


class ConsumerTests(unittest.TestCase):
    def test_actual_parquet_schema_rows_and_indicator_binding(self):
        with tempfile.TemporaryDirectory() as tmp, duckdb.connect() as connection:
            path = Path(tmp) / "part.parquet"
            connection.execute("COPY (SELECT 7::BIGINT AS indicator_id FROM range(3)) TO ? (FORMAT PARQUET)", [str(path)])
            columns = [{"name": "indicator_id", "type": "BIGINT"}]
            verify.check_parquet(connection, path, columns, 3, 7)
            with self.assertRaisesRegex(ValueError, "identity or row count"):
                verify.check_parquet(connection, path, columns, 3, 8)
            with self.assertRaisesRegex(ValueError, "row count"):
                verify.check_parquet(connection, path, columns, 4)
            with self.assertRaisesRegex(ValueError, "schema"):
                verify.check_parquet(connection, path, [{"name": "wrong", "type": "BIGINT"}], 3)

    def test_null_indicator_identity_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, duckdb.connect() as connection:
            path = Path(tmp) / "part.parquet"
            connection.execute("COPY (SELECT NULL::BIGINT AS indicator_id) TO ? (FORMAT PARQUET)", [str(path)])
            with self.assertRaisesRegex(ValueError, "identity or row count"):
                verify.check_parquet(connection, path, [{"name": "indicator_id", "type": "BIGINT"}], 1, 7)

    def fixture(self):
        from retained_dbw_publication import REVIEWED_AUDIT_REPORT_SHA256
        schema = [{"name": "indicator_id", "type": "BIGINT"}]
        baseline = {"run_id": "historic", "inventory_sha256": "a" * 64,
                    "schemas": {k: schema for k in verify.TABLES},
                    "measured_parquet_rows": {k: 1550 if k in ("observations", "taxonomy") else 1 for k in verify.TABLES}}
        pointer = {"format_version": 1, "source_id": verify.SOURCE, "snapshot_id": "snapshot", "manifest_file_id": "manifest"}
        def part(identity, rows=1, number=None):
            value = {"id": identity, "name": identity + ".parquet", "size": 100,
                     "sha256": hashlib.sha256(identity.encode()).hexdigest(), "row_count": rows}
            if number is not None:
                value["part"] = number
            return value
        manifest = {"format_version": 1, "kind": "retained_bronze_snapshot", "source_id": verify.SOURCE,
                    "snapshot_id": "snapshot", "code_sha": "b" * 40, "status": "validated", "layer": "02_bronze",
                    "coverage_status": "incomplete_retained_inventory", "lineage_status": "unresolved_native_to_bronze",
                    "inventory_sha256": baseline["inventory_sha256"], "audit_run_id": "historic",
                    "audit_report_sha256": REVIEWED_AUDIT_REPORT_SHA256, "indicator_count": 1550,
                    "published_indicator_count": 1550, "pending_indicator_count": 0,
                    "tests": {"passed": True, "rows_and_schemas_preserved": True}, "indicator_index": {"id": "index"},
                    "observation_schema": schema,
                    "datasets": [{"name": k, "table_name": v, "columns": schema,
                                  "row_count": baseline["measured_parquet_rows"][k],
                                  "files": [] if k == "observations" else [part(k, baseline["measured_parquet_rows"][k])]}
                                 for k, v in verify.TABLES.items()]}
        index = {"format_version": 1, "kind": "retained_bronze_indicator_index",
                 **{k: manifest[k] for k in ("source_id", "inventory_sha256", "indicator_count", "published_indicator_count", "pending_indicator_count")},
                 "indicators": [{"indicator_id": i, "status": "published", "row_count": 1,
                                 "parts": [part(f"observation-{i}", number=1)]} for i in range(1, 1551)]}
        return pointer, manifest, index, baseline

    def test_complete_fixture_and_rejected_partial_duplicate_or_schema_change(self):
        values = self.fixture()
        verify.validate_contract(*values, "snapshot", "b" * 40)
        mutations = [
            lambda p, m, i, b: m.update(pending_indicator_count=1),
            lambda p, m, i, b: i["indicators"][1].update(indicator_id=1),
            lambda p, m, i, b: i["indicators"][1]["parts"][0].update(id="observation-1"),
            lambda p, m, i, b: m.update(observation_schema=[]),
            lambda p, m, i, b: i["indicators"][0]["parts"][0].update(size=verify.MAX_FILE_BYTES + 1),
            lambda p, m, i, b: m.update(lineage_status="complete"),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                changed = deepcopy(values)
                mutate(*changed)
                with self.assertRaises(ValueError):
                    verify.validate_contract(*changed, "snapshot", "b" * 40)

    def test_boolean_and_noninteger_sizes_are_not_counts(self):
        for value in (True, False, 1.5, "10", -1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                verify.integer(value)


if __name__ == "__main__":
    unittest.main()
