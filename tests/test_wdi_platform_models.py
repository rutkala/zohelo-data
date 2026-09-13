"""Exercise complete-archive WDI dbt models and native semantic coverage offline."""
import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from wdi_platform_contract import WDI_ARCHIVE_MEMBERS, WDI_PLATFORM_MODEL_NAMES  # noqa: E402
from wdi_semantic import METRICS, validate_release_metrics  # noqa: E402


DBT_CLI = "from dbt.cli.main import cli; cli()"
OFFLINE = REPO_ROOT / "tests" / "helpers" / "run_offline.py"


class WdiPlatformModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        local = REPO_ROOT / ".local" / "test-tmp"
        local.mkdir(parents=True, exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(prefix="zohelo-wdi-models-", dir=local)
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.members = cls.root / "members"
        cls.members.mkdir()
        cls._write_members()
        cls.evidence = cls.root / "archive-evidence.json"
        cls.evidence.write_text(json.dumps({
            "source_id": "world_bank_wdi",
            "retrieved_at_utc": "2026-09-13T12:00:00Z",
            "raw_sha256": "a" * 64,
            "raw_size_bytes": 1234,
            "coverage_status": "complete_current_catalogue",
            "current_distribution_total": 1,
            "member_total": 6,
        }), encoding="utf-8")
        cls.database = cls.root / "wdi.duckdb"
        cls.target = cls.root / "target"
        result = cls._dbt("build")
        if result.returncode:
            raise AssertionError(f"WDI dbt build failed:\n{result.stdout}")
        run_results = (cls.target / "run_results.json").read_bytes()
        docs = cls._dbt("docs")
        if docs.returncode:
            raise AssertionError(f"WDI docs generation failed:\n{docs.stdout}")
        (cls.target / "run_results.json").write_bytes(run_results)

    @classmethod
    def _write_csv(cls, name, headings, rows):
        path = cls.members / name
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(headings)
            writer.writerows(rows)

    @classmethod
    def _write_members(cls):
        cls._write_csv("WDICountry.csv", [
            "Country Code", "Short Name", "Table Name", "Long Name", "2-alpha code",
            "Currency Unit", "Region", "Income Group", "Lending category", "Special Notes",
        ], [
            ["POL", "Poland", "Poland", "Republic of Poland", "PL", "Polish zloty", "Europe & Central Asia", "High income", "", ""],
            ["WLD", "World", "World", "World", "1W", "", "Aggregates", "", "", "World aggregate"],
        ])
        cls._write_csv("WDICountry-Series.csv", ["CountryCode", "SeriesCode", "DESCRIPTION"], [
            ["POL", "SP.POP.TOTL", "National source note"],
        ])
        cls._write_csv("WDIData.csv", [
            "Country Name", "Country Code", "Indicator Name", "Indicator Code", "2020", "2021", "",
        ], [
            ["Poland", "POL", "Population, total", "SP.POP.TOTL", "38000000", "37900000", ""],
            ["World", "WLD", "Population, total", "SP.POP.TOTL", "7800000000", "7880000000", ""],
            ["Poland", "POL", "GDP growth (annual %)", "NY.GDP.MKTP.KD.ZG", "-2.0", "6.9", ""],
        ])
        cls._write_csv("WDIFootNote.csv", ["CountryCode", "SeriesCode", "Year", "DESCRIPTION"], [
            ["POL", "SP.POP.TOTL", "2021", "Estimate"],
        ])
        cls._write_csv("WDISeries.csv", [
            "Series Code", "Topic", "Indicator Name", "Short definition", "Long definition",
            "Unit of measure", "Periodicity", "Aggregation method", "Source", "License Type",
            "Limitations and exceptions",
        ], [
            ["SP.POP.TOTL", "Population", "Population, total", "Population", "Total population", "people", "Annual", "Sum", "World Bank", "CC BY-4.0", ""],
            ["NY.GDP.MKTP.KD.ZG", "Economy", "GDP growth (annual %)", "Growth", "Annual GDP growth", "%", "Annual", "Weighted average", "World Bank", "CC BY-4.0", ""],
        ])
        cls._write_csv("WDISeries-Time.csv", ["SeriesCode", "Year", "DESCRIPTION"], [
            ["SP.POP.TOTL", "2021", "Series note"],
        ])

    @classmethod
    def _environment(cls):
        env = dict(os.environ)
        for name, variable in WDI_ARCHIVE_MEMBERS.items():
            env[variable] = str(cls.members / name)
        env.update(
            ZOHELO_WDI_ARCHIVE_EVIDENCE=str(cls.evidence),
            ZOHELO_DUCKDB_PATH=str(cls.database), DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            DO_NOT_TRACK="1",
        )
        return env

    @classmethod
    def _dbt(cls, command):
        dbt_command = [command] if command == "build" else ["docs", "generate"]
        args = [
            sys.executable, str(OFFLINE), sys.executable, "-c", DBT_CLI, *dbt_command,
            "--profiles-dir", str(REPO_ROOT), "--target-path", str(cls.target),
            "--log-path", str(cls.root / "logs"), "--threads", "1", "--no-partial-parse",
            "--vars", "{enable_wdi: true}",
        ]
        if command == "build":
            args.extend(["--select", *WDI_PLATFORM_MODEL_NAMES.values()])
        else:
            args.append("--no-compile")
        return subprocess.run(
            args, cwd=REPO_ROOT, env=cls._environment(), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300, check=False,
        )

    def test_complete_value_cells_reach_gold_at_exact_grain(self):
        with duckdb.connect(str(self.database), read_only=True) as connection:
            self.assertEqual(connection.execute('select count(*) from "02_bronze"."wdi_data"').fetchone()[0], 6)
            self.assertEqual(connection.execute('select count(*) from "04_gold"."fact_wdi_observations"').fetchone()[0], 6)
            self.assertEqual(connection.execute('select count(distinct indicator_key) from "04_gold"."fact_wdi_observations"').fetchone()[0], 2)
            aggregate = connection.execute(
                'select geography_type from "04_gold"."dim_wdi_geography" where geography_key = ?', ["WLD"]
            ).fetchone()[0]
            self.assertEqual(aggregate, "source_published_aggregate")

    def test_coverage_proves_every_populated_archive_value_is_modeled(self):
        with duckdb.connect(str(self.database), read_only=True) as connection:
            row = connection.execute(
                'select current_archive_total, archive_member_total, source_value_total, modeled_observation_total, modeled_value_coverage_ratio from "04_gold"."mart_wdi_coverage"'
            ).fetchone()
        self.assertEqual(row, (1, 6, 6, 6, 1.0))

    def test_every_coverage_metric_executes_offline_and_matches_gold(self):
        report = validate_release_metrics(
            self.database, self.target / "semantic_manifest.json", self.root / "semantic-validation",
        )
        self.assertEqual(report["status"], "verified")
        self.assertEqual({item["name"] for item in report["metrics"]}, set(METRICS))
        self.assertTrue(all(item["matches_gold"] for item in report["metrics"]))


if __name__ == "__main__":
    unittest.main()
