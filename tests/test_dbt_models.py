"""Execute real dbt models against synthetic, local NBP inputs."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "nbp"


class NbpDbtFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="zohelo-fixture-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.workspace = Path(cls.temp.name)
        cls.data_root = cls.workspace / "data"
        cls.database = cls.workspace / "fixture.duckdb"
        cls.target = cls.workspace / "target"
        cls.dbt = Path(sys.executable).parent / "dbt"
        if not cls.dbt.is_file():
            raise RuntimeError("dbt is missing from this Python environment; install requirements.txt")

        con = duckdb.connect()
        try:
            for table in ("a", "b", "c"):
                destination = cls.data_root / "02_bronze" / f"nbp_exchange_rates_table_{table}"
                destination.mkdir(parents=True)
                output = destination / "batch-001.parquet"
                con.read_json(str(FIXTURES / f"table_{table}.json")).write_parquet(str(output))
                shutil.copyfile(output, destination / "batch-002.parquet")
            destination = cls.data_root / "02_bronze" / "nbp_gold_prices"
            destination.mkdir(parents=True)
            output = destination / "batch-001.parquet"
            con.read_json(str(FIXTURES / "gold_prices.json")).write_parquet(str(output))
            shutil.copyfile(output, destination / "batch-002.parquet")
        finally:
            con.close()

        build = cls.run_dbt(
            "build",
            "--select",
            "stg_nbp_table_a",
            "stg_nbp_table_b",
            "stg_nbp_table_c",
            "stg_nbp_gold_prices",
            "mart_exchange_rates_daily",
        )
        if build.returncode:
            raise AssertionError(f"Fixture dbt build failed:\n{build.stdout}")

    @classmethod
    def run_dbt(cls, *arguments, data_root=None, database=None, target=None):
        env = dict(os.environ)
        env.update(
            ZOHELO_DATA_ROOT=str(data_root or cls.data_root),
            ZOHELO_DUCKDB_PATH=str(database or cls.database),
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            DO_NOT_TRACK="1",
        )
        return subprocess.run(
            [str(cls.dbt), *arguments, "--profiles-dir", str(REPO_ROOT),
             "--target-path", str(target or cls.target), "--log-path", str(cls.workspace / "logs"),
             "--no-partial-parse"],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=120,
            check=False,
        )

    def test_expected_rates_survive_flattening_and_identical_replay(self):
        cases = {
            "stg_nbp_table_a": ("mid", [
                ("2002-01-02", "EUR", 5.0),
                ("2002-01-02", "USD", 4.0),
                ("2002-01-03", "USD", 4.2),
            ]),
            "stg_nbp_table_b": ("mid", [
                ("2002-01-02", "AFN", 0.08),
                ("2002-01-02", "ALL", 0.03),
            ]),
            "stg_nbp_table_c": ("bid, ask", [
                ("2002-01-02", "EUR", 4.9, 5.1),
                ("2002-01-02", "USD", 3.9, 4.1),
            ]),
            "stg_nbp_gold_prices": ("price_pln_per_gram", [
                ("2013-01-02", Decimal("165.83")),
                ("2013-01-03", Decimal("166.12")),
            ]),
            "mart_exchange_rates_daily": ("mid", [
                ("2002-01-02", "EUR", 5.0),
                ("2002-01-02", "USD", 4.0),
                ("2002-01-03", "USD", 4.2),
            ]),
        }
        # Reopen after dbt has exited, as a separate SQL consumer would.
        con = duckdb.connect(str(self.database), read_only=True)
        try:
            for model, (rates, expected) in cases.items():
                with self.subTest(model=model):
                    key = "effectiveDate"
                    dimensions = "" if model == "stg_nbp_gold_prices" else ", code"
                    actual = con.execute(
                        f"SELECT cast({key} AS varchar){dimensions}, {rates} "
                        f"FROM {model} ORDER BY effectiveDate"
                        f"{', code' if dimensions else ''}"
                    ).fetchall()
                    self.assertEqual(actual, expected)
                    columns = con.execute(f"DESCRIBE {model}").fetchall()
                    self.assertEqual(next(row[1] for row in columns if row[0] == "effectiveDate"), "DATE")
        finally:
            con.close()

    def test_gold_price_identical_replay_is_one_row_per_publication_date(self):
        con = duckdb.connect(str(self.database), read_only=True)
        try:
            columns = {
                row[0]: row[1]
                for row in con.execute("DESCRIBE stg_nbp_gold_prices").fetchall()
            }
            self.assertEqual(columns["effectiveDate"], "DATE")
            self.assertEqual(columns["price_pln_per_gram"], "DECIMAL(18,2)")
            actual = con.execute(
                "SELECT cast(effectiveDate AS varchar), price_pln_per_gram, count(*) "
                "FROM stg_nbp_gold_prices GROUP BY effectiveDate, price_pln_per_gram "
                "ORDER BY effectiveDate"
            ).fetchall()
            self.assertEqual(actual, [
                ("2013-01-02", Decimal("165.83"), 1),
                ("2013-01-03", Decimal("166.12"), 1),
            ])
        finally:
            con.close()

    def test_docs_generation_matches_portal_artifact_contract(self):
        result = self.run_dbt("docs", "generate")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertTrue((self.target / "index.html").is_file())
        manifest = json.loads((self.target / "manifest.json").read_text())
        catalog = json.loads((self.target / "catalog.json").read_text())
        model_id = "model.zohelo_data.stg_nbp_table_a"
        self.assertIn(model_id, manifest["nodes"])
        self.assertIn(model_id, catalog["nodes"])
        self.assertIn("model.zohelo_data.stg_nbp_table_a", manifest["nodes"]["model.zohelo_data.mart_exchange_rates_daily"]["depends_on"]["nodes"])

    def test_missing_bronze_input_fails_instead_of_passing_an_empty_build(self):
        empty_root = self.workspace / "missing-input"
        empty_root.mkdir()
        result = self.run_dbt(
            "build", "--select", "stg_nbp_table_c",
            data_root=empty_root,
            database=self.workspace / "missing.duckdb",
            target=self.workspace / "missing-target",
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("nbp_exchange_rates_table_c", result.stdout)

    def test_missing_gold_input_fails_instead_of_passing_an_empty_build(self):
        empty_root = self.workspace / "missing-gold-input"
        empty_root.mkdir()
        result = self.run_dbt(
            "build", "--select", "stg_nbp_gold_prices",
            data_root=empty_root,
            database=self.workspace / "missing-gold.duckdb",
            target=self.workspace / "missing-gold-target",
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("nbp_gold_prices", result.stdout)

    def test_conflicting_gold_values_for_one_date_are_rejected(self):
        conflict_root = self.workspace / "conflicting-gold-input"
        destination = conflict_root / "02_bronze" / "nbp_gold_prices"
        destination.mkdir(parents=True)
        first = destination / "legacy-001.parquet"
        second = destination / "legacy-002.parquet"
        con = duckdb.connect()
        try:
            for source, output in ((
                [{"data": "2013-01-02", "cena": 165.83}], first
            ), (
                [{"data": "2013-01-02", "cena": 165.84}], second
            )):
                source_json = self.workspace / f"{output.stem}.json"
                source_json.write_text(json.dumps(source))
                con.read_json(str(source_json)).write_parquet(str(output))
        finally:
            con.close()

        result = self.run_dbt(
            "build", "--select", "stg_nbp_gold_prices",
            data_root=conflict_root,
            database=self.workspace / "conflicting-gold.duckdb",
            target=self.workspace / "conflicting-gold-target",
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Conflicting NBP gold prices", result.stdout)

    def test_conflicting_exchange_rate_values_are_rejected_independent_of_file_order(self):
        cases = {
            "a": (
                "stg_nbp_table_a",
                "Conflicting NBP table A rates",
                [
                    {"table": "A", "no": "001/A/NBP/2002", "effectiveDate": "2002-01-02",
                     "rates": [{"currency": "synthetic US dollar", "code": "USD", "mid": 4.0}]},
                    {"table": "A", "no": "002/A/NBP/2002", "effectiveDate": "2002-01-02",
                     "rates": [{"currency": "synthetic US dollar", "code": "USD", "mid": 4.1}]},
                ],
            ),
            "b": (
                "stg_nbp_table_b",
                "Conflicting NBP table B rates",
                [
                    {"table": "B", "no": "001/B/NBP/2002", "effectiveDate": "2002-01-02",
                     "rates": [{"currency": "synthetic Albanian lek", "code": "ALL", "mid": 0.03}]},
                    {"table": "B", "no": "002/B/NBP/2002", "effectiveDate": "2002-01-02",
                     "rates": [{"currency": "synthetic Albanian lek", "code": "ALL", "mid": 0.04}]},
                ],
            ),
            "c": (
                "stg_nbp_table_c",
                "Conflicting NBP table C rates",
                [
                    {"table": "C", "no": "001/C/NBP/2002", "tradingDate": "2001-12-31",
                     "effectiveDate": "2002-01-02",
                     "rates": [{"currency": "synthetic US dollar", "code": "USD", "bid": 3.9, "ask": 4.1}]},
                    {"table": "C", "no": "002/C/NBP/2002", "tradingDate": "2001-12-31",
                     "effectiveDate": "2002-01-02",
                     "rates": [{"currency": "synthetic US dollar", "code": "USD", "bid": 3.9, "ask": 4.2}]},
                ],
            ),
        }

        con = duckdb.connect()
        try:
            for table, (model, message, records) in cases.items():
                for order, filenames in enumerate((("a.parquet", "z.parquet"), ("z.parquet", "a.parquet"))):
                    data_root = self.workspace / f"conflicting-{table}-{order}"
                    destination = data_root / "02_bronze" / f"nbp_exchange_rates_table_{table}"
                    destination.mkdir(parents=True)
                    for record, filename in zip(records, filenames):
                        source_json = self.workspace / f"conflicting-{table}-{order}-{filename}.json"
                        source_json.write_text(json.dumps([record]))
                        con.read_json(str(source_json)).write_parquet(str(destination / filename))

                    result = self.run_dbt(
                        "build", "--select", model,
                        data_root=data_root,
                        database=self.workspace / f"conflicting-{table}-{order}.duckdb",
                        target=self.workspace / f"conflicting-{table}-{order}-target",
                    )
                    with self.subTest(table=table, order=order):
                        self.assertNotEqual(result.returncode, 0, result.stdout)
                        self.assertIn(message, result.stdout)
        finally:
            con.close()

    def test_metadata_only_exchange_rate_replay_prefers_non_null_currency(self):
        records = [
            {"table": "A", "no": "001/A/NBP/2002", "effectiveDate": "2002-01-02",
             "rates": [{"currency": None, "code": "USD", "mid": 4.0}]},
            {"table": "A", "no": "002/A/NBP/2002", "effectiveDate": "2002-01-02",
             "rates": [{"currency": "synthetic US dollar", "code": "USD", "mid": 4.0}]},
        ]
        for order, filenames in enumerate((("z.parquet", "a.parquet"), ("a.parquet", "z.parquet"))):
            data_root = self.workspace / f"metadata-only-replay-{order}"
            destination = data_root / "02_bronze" / "nbp_exchange_rates_table_a"
            destination.mkdir(parents=True)
            con = duckdb.connect()
            try:
                for record, filename in zip(records, filenames):
                    source_json = self.workspace / f"metadata-only-{order}-{filename}.json"
                    source_json.write_text(json.dumps([record]))
                    con.read_json(str(source_json)).write_parquet(str(destination / filename))
            finally:
                con.close()

            result = self.run_dbt(
                "build", "--select", "stg_nbp_table_a",
                data_root=data_root,
                database=self.workspace / f"metadata-only-replay-{order}.duckdb",
                target=self.workspace / f"metadata-only-replay-{order}-target",
            )
            with self.subTest(order=order):
                self.assertEqual(result.returncode, 0, result.stdout)
            con = duckdb.connect(
                str(self.workspace / f"metadata-only-replay-{order}.duckdb"),
                read_only=True,
            )
            try:
                self.assertEqual(
                    con.execute(
                        "SELECT currency, mid FROM stg_nbp_table_a "
                        "WHERE effectiveDate = DATE '2002-01-02' AND code = 'USD'"
                    ).fetchall(),
                    [("synthetic US dollar", 4.0)],
                )
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
