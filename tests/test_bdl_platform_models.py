"""Exercise the BDL modeled dbt graph and semantic coverage checks without network access."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from bdl_platform_contract import BDL_PLATFORM_MODEL_NAMES  # noqa: E402
from bdl_semantic import METRICS, validate_release_metrics  # noqa: E402


DBT_CLI = "from dbt.cli.main import cli; cli()"
OFFLINE = REPO_ROOT / "tests" / "helpers" / "run_offline.py"


def landing_row(task_id, lane, task_kind, retrieved_at, request, payload, *, record_count):
    payload_utf8 = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    raw = payload_utf8.encode("utf-8")
    return (
        "gus_bdl",
        task_id,
        lane,
        task_kind,
        datetime.fromisoformat(retrieved_at.replace("Z", "+00:00")).astimezone(timezone.utc),
        record_count,
        hashlib.sha256(raw).hexdigest(),
        len(raw),
        json.dumps(request, separators=(",", ":"), ensure_ascii=False),
        "{}",
        payload_utf8,
        "application/json",
    )


class BdlPlatformModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_root = REPO_ROOT / ".local" / "test-tmp"
        cls.temp_root.mkdir(parents=True, exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(prefix="zohelo-bdl-platform-", dir=cls.temp_root)
        cls.addClassCleanup(cls.temp.cleanup)
        cls.workspace = Path(cls.temp.name)
        cls.database = cls.workspace / "platform.duckdb"
        cls.landing_path = cls.workspace / "gus_bdl_responses.parquet"
        cls._write_landing(cls.landing_path)
        result = cls.run_dbt(cls.landing_path, cls.database, cls.workspace / "target")
        if result.returncode:
            raise AssertionError(f"verified BDL dbt build failed:\n{result.stdout}")
        run_results = (cls.workspace / "target" / "run_results.json").read_bytes()
        docs = cls.run_docs(cls.landing_path, cls.database, cls.workspace / "target")
        if docs.returncode:
            raise AssertionError(f"BDL docs generation failed:\n{docs.stdout}")
        (cls.workspace / "target" / "run_results.json").write_bytes(run_results)

    @staticmethod
    def _write_landing(path):
        rows = [
            landing_row(
                "discovery:variables:pl:p000000",
                "discovery",
                "variables",
                "2026-09-10T00:00:00Z",
                {"params": {"lang": "pl", "page": 0}},
                {
                    "totalRecords": 2,
                    "page": 0,
                    "pageSize": 20,
                    "results": [
                        {"id": 101, "subjectId": "S1", "n1": "Ludność", "n2": "ogółem", "level": 0, "measureUnitId": 1, "measureUnitName": "osoba"},
                        {"id": 102, "subjectId": "S2", "n1": "Bezrobocie", "n2": "stopa", "level": 0, "measureUnitId": 2, "measureUnitName": "procent"},
                    ],
                },
                record_count=2,
            ),
            landing_row(
                "discovery:variables:en:p000000",
                "discovery",
                "variables",
                "2026-09-10T00:01:00Z",
                {"params": {"lang": "en", "page": 0}},
                {
                    "totalRecords": 2,
                    "page": 0,
                    "pageSize": 20,
                    "results": [
                        {"id": 101, "subjectId": "S1", "n1": "Population", "n2": "total", "level": 0, "measureUnitId": 1, "measureUnitName": "person"},
                        {"id": 102, "subjectId": "S2", "n1": "Unemployment", "n2": "rate", "level": 0, "measureUnitId": 2, "measureUnitName": "percent"},
                    ],
                },
                record_count=2,
            ),
            landing_row(
                "discovery:subjects:pl:root:p000000",
                "discovery",
                "subjects",
                "2026-09-10T00:02:00Z",
                {"params": {"lang": "pl", "page": 0}},
                {
                    "totalRecords": 2,
                    "page": 0,
                    "pageSize": 20,
                    "results": [
                        {"id": "S1", "parentId": None, "name": "Demografia", "hasVariables": True, "children": [], "levels": [0]},
                        {"id": "S2", "parentId": None, "name": "Rynek pracy", "hasVariables": True, "children": [], "levels": [0]},
                    ],
                },
                record_count=2,
            ),
            landing_row(
                "discovery:subjects:en:root:p000000",
                "discovery",
                "subjects",
                "2026-09-10T00:03:00Z",
                {"params": {"lang": "en", "page": 0}},
                {
                    "totalRecords": 2,
                    "page": 0,
                    "pageSize": 20,
                    "results": [
                        {"id": "S1", "parentId": None, "name": "Demography", "hasVariables": True, "children": [], "levels": [0]},
                        {"id": "S2", "parentId": None, "name": "Labour market", "hasVariables": True, "children": [], "levels": [0]},
                    ],
                },
                record_count=2,
            ),
            landing_row(
                "discovery:units:pl:root:p000000",
                "discovery",
                "units",
                "2026-09-10T00:04:00Z",
                {"params": {"lang": "pl", "page": 0}},
                {
                    "totalRecords": 1,
                    "page": 0,
                    "pageSize": 20,
                    "results": [
                        {"id": "000000000001", "parentId": None, "name": "Polska", "level": 0, "kind": "country", "hasDescription": True, "description": "Państwo"},
                    ],
                },
                record_count=1,
            ),
            landing_row(
                "discovery:units:en:root:p000000",
                "discovery",
                "units",
                "2026-09-10T00:05:00Z",
                {"params": {"lang": "en", "page": 0}},
                {
                    "totalRecords": 1,
                    "page": 0,
                    "pageSize": 20,
                    "results": [
                        {"id": "000000000001", "parentId": None, "name": "Poland", "level": 0, "kind": "country", "hasDescription": True, "description": "Country"},
                    ],
                },
                record_count=1,
            ),
            landing_row(
                "discovery:years",
                "discovery",
                "years",
                "2026-09-10T00:06:00Z",
                {"params": {}},
                {"totalRecords": 2, "results": [{"id": 2024}, {"id": 2025}]},
                record_count=2,
            ),
            landing_row(
                "discovery:dictionary:aggregates:pl:p000000",
                "discovery",
                "dictionary",
                "2026-09-10T00:07:00Z",
                {"params": {"lang": "pl"}},
                {"totalRecords": 1, "results": [{"id": "5", "name": "Ogółem"}]},
                record_count=1,
            ),
            landing_row(
                "discovery:dictionary:aggregates:en:p000000",
                "discovery",
                "dictionary",
                "2026-09-10T00:08:00Z",
                {"params": {"lang": "en"}},
                {"totalRecords": 1, "results": [{"id": "5", "name": "Total"}]},
                record_count=1,
            ),
            landing_row(
                "discovery:dictionary:attributes:pl:p000000",
                "discovery",
                "dictionary",
                "2026-09-10T00:09:00Z",
                {"params": {"lang": "pl"}},
                {"totalRecords": 1, "results": [{"id": "1", "name": "szacunek"}]},
                record_count=1,
            ),
            landing_row(
                "discovery:dictionary:attributes:en:p000000",
                "discovery",
                "dictionary",
                "2026-09-10T00:10:00Z",
                {"params": {"lang": "en"}},
                {"totalRecords": 1, "results": [{"id": "1", "name": "estimate"}]},
                record_count=1,
            ),
            landing_row(
                "recent:data_by_variable:101:2024",
                "recent",
                "data_by_variable",
                "2026-09-10T00:11:00Z",
                {"params": {"page": 0}},
                {
                    "totalRecords": 1,
                    "page": 0,
                    "pageSize": 20,
                    "variableId": 101,
                    "measureUnitId": 1,
                    "aggregateId": 5,
                    "lastUpdate": "2026-09-10T00:00:00",
                    "results": [
                        {"id": "000000000001", "name": "Polska", "values": [{"year": "2024", "val": "10", "precision": "0", "attrId": "1"}]},
                    ],
                },
                record_count=1,
            ),
            landing_row(
                "recent:data_by_variable:101:2024:replay",
                "recent",
                "data_by_variable",
                "2026-09-10T00:12:00Z",
                {"params": {"page": 0}},
                {
                    "totalRecords": 1,
                    "page": 0,
                    "pageSize": 20,
                    "variableId": 101,
                    "measureUnitId": 1,
                    "aggregateId": 5,
                    "lastUpdate": "2026-09-10T00:00:00",
                    "results": [
                        {"id": "000000000001", "name": "Polska", "values": [{"year": "2024", "val": "10", "precision": "0", "attrId": "1"}]},
                    ],
                },
                record_count=1,
            ),
            landing_row(
                "recent:data_by_variable:101:2024:changed",
                "recent",
                "data_by_variable",
                "2026-09-10T00:13:00Z",
                {"params": {"page": 0}},
                {
                    "totalRecords": 1,
                    "page": 0,
                    "pageSize": 20,
                    "variableId": 101,
                    "measureUnitId": 1,
                    "aggregateId": 5,
                    "lastUpdate": "2026-09-11T00:00:00",
                    "results": [
                        {"id": "000000000001", "name": "Polska", "values": [{"year": "2024", "val": "11", "precision": "0", "attrId": "1"}]},
                    ],
                },
                record_count=1,
            ),
            landing_row(
                "recent:data_by_variable:102:2025Q1",
                "recent",
                "data_by_variable",
                "2026-09-10T00:14:00Z",
                {"params": {"page": 0}},
                {
                    "totalRecords": 1,
                    "page": 0,
                    "pageSize": 20,
                    "variableId": 102,
                    "measureUnitId": 2,
                    "aggregateId": 5,
                    "lastUpdate": "2026-09-10T00:00:00",
                    "results": [
                        {"id": "000000000001", "name": "Poland", "values": [{"year": "2025-Q1", "value": "7.5", "precision": "1"}]},
                    ],
                },
                record_count=1,
            ),
        ]
        with duckdb.connect() as connection:
            parquet_path = str(path).replace("'", "''")
            connection.execute(
                """
                create table landing (
                    source_id varchar,
                    task_id varchar,
                    lane varchar,
                    task_kind varchar,
                    retrieved_at_utc timestamp,
                    record_count bigint,
                    raw_sha256 varchar,
                    raw_size_bytes bigint,
                    request_json varchar,
                    metadata_json varchar,
                    payload_utf8 varchar,
                    content_type varchar
                )
                """
            )
            connection.executemany("insert into landing values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
            connection.execute(f"COPY landing TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    @classmethod
    def run_dbt(cls, landing_path, database, target):
        env = dict(os.environ)
        for name in tuple(env):
            if name.startswith(("GOOGLE_", "GCP_", "AWS_", "AZURE_")) or name in {"CLOUDSDK_CONFIG", "ZOHELO_DRIVE_ROOT_ID", "ZOHELO_DRIVE_ROOT_NAME"}:
                env.pop(name, None)
        env.update(
            ZOHELO_GUS_BDL_RESPONSES_PATH=str(landing_path),
            ZOHELO_DUCKDB_PATH=str(database),
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            DO_NOT_TRACK="1",
        )
        return subprocess.run(
            [
                sys.executable,
                str(OFFLINE),
                sys.executable,
                "-c",
                DBT_CLI,
                "build",
                "--profiles-dir",
                str(REPO_ROOT),
                "--target-path",
                str(target),
                "--log-path",
                str(target / "logs"),
                "--threads",
                "1",
                "--no-partial-parse",
                "--select",
                *BDL_PLATFORM_MODEL_NAMES.values(),
            ],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=180,
            check=False,
        )

    @classmethod
    def run_docs(cls, landing_path, database, target):
        env = dict(os.environ)
        for name in tuple(env):
            if name.startswith(("GOOGLE_", "GCP_", "AWS_", "AZURE_")) or name in {"CLOUDSDK_CONFIG", "ZOHELO_DRIVE_ROOT_ID", "ZOHELO_DRIVE_ROOT_NAME"}:
                env.pop(name, None)
        env.update(
            ZOHELO_GUS_BDL_RESPONSES_PATH=str(landing_path),
            ZOHELO_DUCKDB_PATH=str(database),
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            DO_NOT_TRACK="1",
        )
        return subprocess.run(
            [
                sys.executable,
                str(OFFLINE),
                sys.executable,
                "-c",
                DBT_CLI,
                "docs",
                "generate",
                "--no-compile",
                "--profiles-dir",
                str(REPO_ROOT),
                "--target-path",
                str(target),
                "--log-path",
                str(target / "logs"),
                "--threads",
                "1",
                "--no-partial-parse",
            ],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=180,
            check=False,
        )

    def connection(self):
        return duckdb.connect(str(self.database), read_only=True)

    def test_revision_history_and_current_fact_rows_are_modeled(self):
        with self.connection() as connection:
            self.assertEqual(
                connection.execute(
                    'select variable_id, unit_id, period_key, revision_number, replayed_response_count, is_current, value_numeric '
                    'from "03_silver"."bdl_observation_revisions" order by variable_id, revision_number'
                ).fetchall(),
                [
                    (101, "000000000001", "2024", 1, 2, False, 10.0),
                    (101, "000000000001", "2024", 2, 1, True, 11.0),
                    (102, "000000000001", "2025-Q1", 1, 1, True, 7.5),
                ],
            )
            self.assertEqual(
                connection.execute(
                    'select variable_key, period_key, attribute_key, value_numeric, current_revision_event_type '
                    'from "04_gold"."fact_bdl_observations" order by variable_key'
                ).fetchall(),
                [
                    (101, "2024", "1", 11.0, "observed_value_changed"),
                    (102, "2025-Q1", "reported", 7.5, "first_observed"),
                ],
            )

    def test_strings_periods_and_coverage_counts_follow_the_source_contract(self):
        with self.connection() as connection:
            self.assertEqual(
                connection.execute(
                    'select distinct unit_id from "02_bronze"."bdl_units"'
                ).fetchall(),
                [("000000000001",)],
            )
            self.assertEqual(
                connection.execute(
                    'select period_key, period_granularity, period_start_date, period_end_date '
                    'from "04_gold"."dim_bdl_period" where period_key = \'2025-Q1\''
                ).fetchone(),
                ("2025-Q1", "quarter", datetime(2025, 1, 1).date(), datetime(2025, 3, 31).date()),
            )
            self.assertEqual(
                connection.execute(
                    'select source_universe_total, discovered_total, landed_accepted_total, modeled_total, modeled_observation_total, '
                    'round(discovery_coverage_ratio, 6), round(modeled_coverage_ratio, 6), round(landed_responses_per_discovered_variable_ratio, 6) '
                    'from "04_gold"."mart_bdl_coverage"'
                ).fetchone(),
                (2, 2, 15, 2, 2, 1.0, 1.0, 7.5),
            )

    def test_release_bound_semantic_metrics_match_gold_coverage_values(self):
        report = validate_release_metrics(
            self.database,
            self.workspace / "target" / "semantic_manifest.json",
            self.workspace / "metric-results",
        )
        self.assertEqual(report["status"], "verified")
        self.assertEqual({row["name"] for row in report["metrics"]}, set(METRICS))
        self.assertTrue(all(row["matches_gold"] for row in report["metrics"]))


if __name__ == "__main__":
    unittest.main()
