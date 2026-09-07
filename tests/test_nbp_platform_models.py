"""Exercise the verified NBP raw-to-gold dbt graph without network access."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_CLI = "from dbt.cli.main import cli; cli()"
OFFLINE = REPO_ROOT / "tests" / "helpers" / "run_offline.py"


def envelope(source_id, sequence, body, *, suffix="", batch_id=None, requested_date="2020-01-01"):
    return {
        "source_id": source_id,
        "batch_id": batch_id or f"{source_id}-{sequence}{suffix}",
        "ingestion_sequence": sequence,
        "requested_start_date": requested_date,
        "requested_end_date": requested_date,
        "retrieved_at_utc": f"2020-01-{sequence + 1:02d}T00:00:00Z",
        "response_sha256": f"sha-{source_id}-{sequence}{suffix}",
        "raw_file_id": f"raw-{source_id}-{sequence}{suffix}",
        "body_json": json.dumps(body, separators=(",", ":")),
    }


class NbpPlatformModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_root = REPO_ROOT / ".local" / "test-tmp"
        cls.temp_root.mkdir(parents=True, exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(prefix="zohelo-nbp-platform-", dir=cls.temp_root)
        cls.addClassCleanup(cls.temp.cleanup)
        cls.workspace = Path(cls.temp.name)
        cls.database = cls.workspace / "platform.duckdb"
        cls.batch_path = cls.workspace / "batches.jsonl"
        cls._write_batches(cls.batch_path)
        result = cls.run_dbt(cls.batch_path, cls.database, cls.workspace / "target")
        if result.returncode:
            raise AssertionError(f"verified raw dbt build failed:\n{result.stdout}")

    @staticmethod
    def _write_batches(path):
        a_initial = [{
            "table": "A", "no": "001/A/NBP/2020", "effectiveDate": "2020-01-01",
            "rates": [
                {"currency": "US dollar", "code": "USD", "mid": 3.80},
                {"country": "Austria", "code": "EUR", "mid": 4.20},
                {"currency": "Euro", "country": "Belgium", "symbol": "978", "code": "EUR", "mid": 4.20},
            ],
        }]
        a_changed = [{
            "table": "A", "no": "002/A/NBP/2020", "effectiveDate": "2020-01-01",
            "rates": [{"currency": "US dollar", "code": "USD", "mid": 3.90}],
        }]
        a_metadata_only = [{
            "table": "A", "no": "003/A/NBP/2020", "effectiveDate": "2020-01-01",
            "rates": [{"currency": "US dollar revised label", "code": "USD", "mid": 3.90}],
        }]
        a_reverted = [{
            "table": "A", "no": "004/A/NBP/2020", "effectiveDate": "2020-01-01",
            "rates": [{"currency": "US dollar revised label", "code": "USD", "mid": 3.80}],
        }]
        b = [{
            "table": "B", "no": "006/B/NBP/2009", "effectiveDate": "2009-02-11",
            "rates": [
                {"currency": "Zimbabwe dollar", "code": "ZWR", "mid": 0.0},
                {"currency": "US dollar", "code": "USD", "mid": 3.81},
            ],
        }]
        c = [{
            "table": "C", "no": None, "tradingDate": "2019-12-31", "effectiveDate": "2020-01-01",
            "rates": [{"currency": "US dollar", "code": "USD", "bid": 3.70, "ask": 3.95}],
        }]
        gold = [{"data": "2020-01-01", "cena": 200.11}]
        rows = [
            envelope("nbp_exchange_rates_table_a", 1, a_initial),
            envelope("nbp_exchange_rates_table_a", 2, a_initial, suffix="-replay"),
            envelope("nbp_exchange_rates_table_a", 3, a_changed),
            envelope("nbp_exchange_rates_table_a", 4, a_metadata_only),
            envelope("nbp_exchange_rates_table_a", 5, a_reverted),
            envelope("nbp_exchange_rates_table_b", 1, b, requested_date="2009-02-11"),
            envelope("nbp_exchange_rates_table_c", 1, c),
            envelope("nbp_gold_prices", 1, gold),
            envelope("nbp_gold_prices", 2, gold, suffix="-replay"),
        ]
        path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")

    @classmethod
    def run_dbt(cls, batches, database, target):
        env = dict(os.environ)
        for name in tuple(env):
            if name.startswith(("GOOGLE_", "GCP_", "AWS_", "AZURE_")) or name in {"CLOUDSDK_CONFIG", "ZOHELO_DRIVE_ROOT_ID", "ZOHELO_DRIVE_ROOT_NAME"}:
                env.pop(name, None)
        env.update(
            ZOHELO_NBP_BATCHES_PATH=str(batches),
            ZOHELO_DUCKDB_PATH=str(database),
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            DO_NOT_TRACK="1",
        )
        return subprocess.run(
            [sys.executable, str(OFFLINE), sys.executable, "-c", DBT_CLI, "build", "--profiles-dir", str(REPO_ROOT),
             "--target-path", str(target), "--log-path", str(target / "logs"), "--threads", "1", "--no-partial-parse",
             "--vars", "{nbp_verified_batches: true}"],
            cwd=REPO_ROOT, env=env, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=120, check=False,
        )

    def connection(self):
        return duckdb.connect(str(self.database), read_only=True)

    def test_replay_reversion_and_missing_rows_preserve_current_values(self):
        with self.connection() as con:
            # EUR is absent from the changed and later observations but remains current from its unchanged replay.
            self.assertEqual(con.execute(
                'select code, mid, ingestion_sequence from "03_silver"."nbp_exchange_rates_table_a" order by code'
            ).fetchall(), [("EUR", 4.2, 2), ("USD", 3.8, 5)])
            self.assertEqual(con.execute(
                'select count(*) from "02_bronze"."nbp_exchange_rates_table_a" where code = \'USD\''
            ).fetchone()[0], 5)
            # NBP can repeat one code for multiple countries in a single publication.
            # Bronze retains both source rows; current silver collapses identical values deterministically.
            self.assertEqual(con.execute(
                'select count(*) from "02_bronze"."nbp_exchange_rates_table_a" where code = \'EUR\''
            ).fetchone()[0], 4)
            self.assertEqual(con.execute(
                'select currency from "03_silver"."nbp_exchange_rates_table_a" where code = \'EUR\''
            ).fetchone()[0], "Euro")
            self.assertEqual(con.execute(
                'select count(*) from "03_silver"."nbp_change_events" where code = \'EUR\''
            ).fetchone()[0], 0)
            # Raw response bytes remain immutable landing objects addressed by hash/file id;
            # they are not repeated once per flattened currency observation.
            bronze_columns = {row[0] for row in con.execute('describe "02_bronze"."nbp_exchange_rates_table_a"').fetchall()}
            self.assertNotIn("body_json", bronze_columns)
            self.assertTrue({"raw_file_id", "response_sha256"}.issubset(bronze_columns))
            self.assertEqual(con.execute(
                'select mid from "02_bronze"."nbp_exchange_rates_table_b" where code = \'ZWR\''
            ).fetchone()[0], 0.0)
            self.assertEqual(con.execute(
                'select code, mid, has_zero_source_quote from "03_silver"."nbp_exchange_rates_table_b" order by code'
            ).fetchall(), [("USD", 3.81, False), ("ZWR", 0.0, True)])
            self.assertEqual(con.execute(
                'select currency_key, mid, has_zero_source_quote from "04_gold"."fact_fx_quotes" '
                "where source_table_key = 'B' order by currency_key"
            ).fetchall(), [("USD", 3.81, False), ("ZWR", 0.0, True)])
            self.assertEqual(con.execute(
                'select event_type, ingestion_sequence from "03_silver"."nbp_change_events" '
                "where source_id = 'nbp_exchange_rates_table_a' order by ingestion_sequence"
            ).fetchall(), [
                ("source_value_changed", 3),
                ("source_metadata_changed", 4),
                ("source_value_changed", 5),
            ])

    def test_gold_replay_emits_no_change_event_and_zero_event_relation_is_valid(self):
        with self.connection() as con:
            self.assertEqual(con.execute(
                'select count(*) from "03_silver"."nbp_change_events" where source_id = \'nbp_gold_prices\''
            ).fetchone()[0], 0)

        zero_batches = self.workspace / "zero-events.jsonl"
        zero_database = self.workspace / "zero-events.duckdb"
        rows = [
            envelope("nbp_exchange_rates_table_a", 1, [{
                "table": "A", "effectiveDate": "2020-01-01",
                "rates": [{"currency": "US dollar", "code": "USD", "mid": 3.8}],
            }]),
            envelope("nbp_exchange_rates_table_b", 1, [{
                "table": "B", "effectiveDate": "2020-01-01",
                "rates": [{"currency": "US dollar", "code": "USD", "mid": 3.81}],
            }]),
            envelope("nbp_exchange_rates_table_c", 1, [{
                "table": "C", "effectiveDate": "2020-01-01",
                "rates": [{"currency": "US dollar", "code": "USD", "bid": 3.7, "ask": 3.95}],
            }]),
            envelope("nbp_gold_prices", 1, [{"data": "2020-01-01", "cena": 200.11}]),
        ]
        zero_batches.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
        result = self.run_dbt(zero_batches, zero_database, self.workspace / "zero-target")
        self.assertEqual(result.returncode, 0, result.stdout)
        with duckdb.connect(str(zero_database), read_only=True) as con:
            self.assertEqual(con.execute('select count(*) from "03_silver"."nbp_change_events"').fetchone()[0], 0)

    def test_gold_grains_dimensions_and_unmodified_api_numbers(self):
        with self.connection() as con:
            self.assertEqual(con.execute(
                'select source_table_key, currency_key, mid, bid, ask, has_zero_source_quote from "04_gold"."fact_fx_quotes" '
                "order by source_table_key, currency_key"
            ).fetchall(), [
                ("A", "EUR", 4.2, None, None, False),
                ("A", "USD", 3.8, None, None, False),
                ("B", "USD", 3.81, None, None, False),
                ("B", "ZWR", 0.0, None, None, True),
                ("C", "USD", None, 3.7, 3.95, False),
            ])
            self.assertEqual(con.execute(
                "select cast(effective_date as varchar), commodity_key, price_pln_per_gram_1000, raw_cena "
                'from "04_gold"."fact_gold_prices"'
            ).fetchall(), [("2020-01-01", "nbp_gold_1000_gram", Decimal("200.11"), Decimal("200.11"))])
            self.assertEqual(con.execute("""
                select count(*) from "04_gold"."fact_fx_quotes" f
                left join "04_gold"."dim_date" d on f.effective_date = d.date_key
                left join "04_gold"."dim_currency" c on f.currency_key = c.currency_key
                left join "04_gold"."dim_source_table" s on f.source_table_key = s.source_table_key
                where d.date_key is null or c.currency_key is null or s.source_table_key is null
            """).fetchone()[0], 0)
            date_keys = {row[0] for row in con.execute(
                'select cast(date_key as varchar) from "04_gold"."dim_date"'
            ).fetchall()}
            self.assertTrue({"2009-02-11", "2019-12-31", "2020-01-01"}.issubset(date_keys))
            self.assertEqual((min(date_keys), max(date_keys)), ("2009-02-11", "2020-01-01"))
            self.assertEqual(len(date_keys), (date(2020, 1, 1) - date(2009, 2, 11)).days + 1)
            self.assertEqual(con.execute(
                "select calendar_year, calendar_month, calendar_day, iso_weekday "
                'from "04_gold"."dim_date" where date_key = date \'2019-12-31\''
            ).fetchone(), (2019, 12, 31, 2))
            self.assertEqual(con.execute("""
                select count(*) from "04_gold"."fact_gold_prices" f
                left join "04_gold"."dim_date" d on f.effective_date = d.date_key
                left join "04_gold"."dim_commodity" c on f.commodity_key = c.commodity_key
                where d.date_key is null or c.commodity_key is null
            """).fetchone()[0], 0)

    def test_manifest_keeps_raw_to_gold_lineage(self):
        manifest = json.loads((self.workspace / "target" / "manifest.json").read_text())
        fact = manifest["nodes"]["model.zohelo_data.fact_fx_quotes"]
        staging = manifest["nodes"]["model.zohelo_data.stg_nbp_table_a"]
        bronze = manifest["nodes"]["model.zohelo_data.br_nbp_table_a"]
        self.assertIn("model.zohelo_data.stg_nbp_table_a", fact["depends_on"]["nodes"])
        self.assertIn("model.zohelo_data.br_nbp_table_a", staging["depends_on"]["nodes"])
        self.assertIn("source.zohelo_data.landing.nbp_batches", bronze["depends_on"]["nodes"])
        self.assertNotIn("source.zohelo_data.bronze.nbp_exchange_rates_table_a", manifest["sources"])
        self.assertEqual(bronze["schema"], "02_bronze")
        self.assertEqual(staging["schema"], "03_silver")
        self.assertEqual(fact["schema"], "04_gold")
        self.assertEqual(
            manifest["sources"]["source.zohelo_data.landing.nbp_batches"]["schema"],
            "01_landing",
        )


if __name__ == "__main__":
    unittest.main()
