"""Execute the production semantic definitions against independent gold values."""
import csv
from decimal import Decimal
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from semantic_query import FX_GRAIN, METRICS, query_metric, validate_metric_grain, validate_release_metrics


class NbpSemanticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temporary_root = ROOT / ".local/test-tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(prefix="nbp-semantics-", dir=temporary_root)
        cls.addClassCleanup(cls.temp.cleanup)
        cls.workspace = Path(cls.temp.name)
        # Parsing uses one database name; the actual restored file has another.
        # The original semantic manifest must remain usable without rewriting it.
        cls.database = cls.workspace / "restored_release.duckdb"
        cls.manifest = cls.workspace / "target/semantic_manifest.json"
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("GOOGLE_", "GCP_", "AWS_", "AZURE_"))}
        env.update(ZOHELO_DUCKDB_PATH=str(cls.workspace / "original_build.duckdb"),
                   DBT_SEND_ANONYMOUS_USAGE_STATS="false", DO_NOT_TRACK="1")
        result = subprocess.run(
            [sys.executable, str(ROOT / "src/offline_process.py"), sys.executable,
             "-c", "from dbt.cli.main import cli; cli()", "parse", "--profiles-dir", str(ROOT),
             "--target-path", str(cls.workspace / "target"), "--log-path", str(cls.workspace / "logs"),
             "--no-partial-parse"], cwd=ROOT, env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120, check=False,
        )
        if result.returncode:
            raise AssertionError(result.stdout)
        with duckdb.connect(str(cls.database)) as con:
            con.execute('create schema "04_gold"')
            con.execute('''create table "04_gold".fact_fx_quotes (
                effective_date date, trading_date date, source_table_key varchar,
                currency_key varchar, quote_currency_key varchar, mid double, bid double, ask double
            )''')
            con.execute('''insert into "04_gold".fact_fx_quotes values
                ('2020-01-01', null, 'A', 'USD', 'PLN', 3.8, null, null),
                ('2020-01-01', null, 'A', 'EUR', 'PLN', 4.2, null, null),
                ('2020-01-02', null, 'A', 'USD', 'PLN', 3.9, null, null),
                ('2020-01-01', null, 'B', 'USD', 'PLN', 3.81, null, null),
                ('2020-01-01', null, 'B', 'ZWR', 'PLN', 0.0, null, null),
                ('2020-01-02', null, 'B', 'USD', 'PLN', 3.82, null, null),
                ('2020-01-01', '2019-12-31', 'C', 'USD', 'PLN', null, 3.7, 3.95),
                ('2020-01-02', '2020-01-01', 'C', 'USD', 'PLN', null, 3.75, 4.0)
            ''')
            con.execute('''create table "04_gold".fact_gold_prices (
                effective_date date, commodity_key varchar, quote_currency_key varchar,
                price_pln_per_gram_1000 double
            )''')
            con.execute('''insert into "04_gold".fact_gold_prices values
                ('2020-01-01', 'nbp_gold_1000_gram', 'PLN', 200.11),
                ('2020-01-02', 'nbp_gold_1000_gram', 'PLN', 210.12)
            ''')
            con.execute('''create table "04_gold".dim_date as
                select value::date date_key from generate_series(
                    '2019-12-31'::date, '2020-01-05'::date, interval 1 day) as dates(value)''')

    def test_all_five_native_queries_match_source_values_at_required_daily_grain(self):
        manifest_before = self.manifest.read_bytes()
        database_before = self.database.read_bytes()
        report = validate_release_metrics(self.database, self.manifest, self.workspace / "results")
        self.assertEqual(report["status"], "verified")
        self.assertEqual({row["name"] for row in report["metrics"]}, set(METRICS))
        self.assertTrue(all(row["matches_gold"] for row in report["metrics"]))
        self.assertEqual(self.manifest.read_bytes(), manifest_before)
        self.assertEqual(self.database.read_bytes(), database_before)
        with (self.workspace / "results/nbp_table_b_mid.csv").open() as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual({row["fx_quote__source_table"] for row in rows}, {"B"})
        zero = [row for row in rows if row["fx_quote__currency"] == "ZWR"]
        self.assertEqual(len(zero), 1)
        self.assertEqual(Decimal(zero[0]["nbp_table_b_mid"]), Decimal("0"))
        self.assertEqual(len(rows), 3)

    def test_publication_date_filter_does_not_use_trading_date(self):
        output = self.workspace / "filtered.csv"
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/query_metrics.py"),
             "--database", str(self.database), "--semantic-manifest", str(self.manifest),
             "--metric", "nbp_table_c_bid", "--start-date", "2020-01-01",
             "--end-date", "2020-01-01", "--output", str(output)],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=180, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(result.stdout)["status"], "queried")
        with output.open() as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric_time__day"].split(" ")[0], "2020-01-01")
        self.assertEqual(Decimal(rows[0]["nbp_table_c_bid"]), Decimal("3.7"))

    def test_missing_publication_is_not_filled_from_a_prior_date(self):
        rows = query_metric(self.database, self.manifest, "nbp_gold_price_pln_per_gram_1000",
                            "2020-01-03", "2020-01-03", self.workspace / "no_publication.csv")
        self.assertEqual(rows, [])

    def test_changed_metric_artifact_cannot_change_the_supported_definition(self):
        changed = self.workspace / "changed_semantic_manifest.json"
        document = json.loads(self.manifest.read_text())
        metric = next(item for item in document["metrics"] if item["name"] == "nbp_table_a_mid")
        metric["filter"] = None
        changed.write_text(json.dumps(document))
        with self.assertRaisesRegex(ValueError, "Unsupported executable definition"):
            query_metric(self.database, changed, "nbp_table_a_mid", "2020-01-01", "2020-01-02",
                         self.workspace / "invalid.csv")

    def test_unsupported_grain_metric_and_date_requests_fail_before_execution(self):
        for grain in ((), ("metric_time",), ("metric_time__month", *FX_GRAIN[1:]),
                      ("metric_time", "fx_quote__currency")):
            with self.subTest(grain=grain), self.assertRaisesRegex(ValueError, "requires the exact daily grain"):
                query_metric(self.database, self.manifest, "nbp_table_a_mid", "2020-01-01", "2020-01-02",
                             self.workspace / "invalid.csv", group_by=grain)
        with self.assertRaisesRegex(ValueError, "Unsupported metric"):
            query_metric(self.database, self.manifest, "sum_fx", "2020-01-01", "2020-01-02",
                         self.workspace / "invalid.csv")
        with self.assertRaisesRegex(ValueError, "end date"):
            query_metric(self.database, self.manifest, "nbp_table_a_mid", "2020-01-02", "2020-01-01",
                         self.workspace / "invalid.csv")

    def test_duplicate_grain_is_rejected_instead_of_hidden_by_max(self):
        copied = self.workspace / "duplicate.duckdb"
        shutil.copyfile(self.database, copied)
        with duckdb.connect(str(copied)) as con:
            con.execute('''insert into "04_gold".fact_fx_quotes select *
                           from "04_gold".fact_fx_quotes limit 1''')
        with self.assertRaisesRegex(ValueError, "Duplicate FX daily metric grain"):
            validate_metric_grain(copied)

    def test_required_source_values_cannot_be_null(self):
        copied = self.workspace / "null_value.duckdb"
        shutil.copyfile(self.database, copied)
        with duckdb.connect(str(copied)) as con:
            con.execute('''update "04_gold".fact_gold_prices set price_pln_per_gram_1000 = null''')
        with self.assertRaisesRegex(ValueError, "Invalid gold metric grain"):
            validate_metric_grain(copied)

    def test_real_manifest_contains_production_lineage_and_no_synthetic_metric(self):
        manifest = json.loads(self.manifest.read_text())
        self.assertEqual({item["name"] for item in manifest["metrics"]}, set(METRICS))
        self.assertEqual({item["name"] for item in manifest["semantic_models"]},
                         {"nbp_fx_quotes", "nbp_gold_prices"})
        self.assertNotIn("fixture_value_total", self.manifest.read_text())
        for metric in manifest["metrics"]:
            self.assertEqual(metric["config"]["meta"]["definition_status"], "source_defined")
            self.assertEqual(metric["config"]["meta"]["aggregation_policy"], "identity_at_daily_source_grain")


if __name__ == "__main__":
    unittest.main()
