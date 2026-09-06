"""Execute real dbt models against synthetic, local NBP inputs."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
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
        finally:
            con.close()

        build = cls.run_dbt("build", "--select", "stg_nbp_table_a", "stg_nbp_table_b", "stg_nbp_table_c", "mart_exchange_rates_daily")
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
                    actual = con.execute(
                        f"SELECT cast(effectiveDate AS varchar), code, {rates} "
                        f"FROM {model} ORDER BY effectiveDate, code"
                    ).fetchall()
                    self.assertEqual(actual, expected)
                    columns = con.execute(f"DESCRIBE {model}").fetchall()
                    self.assertEqual(next(row[1] for row in columns if row[0] == "effectiveDate"), "DATE")
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


if __name__ == "__main__":
    unittest.main()
