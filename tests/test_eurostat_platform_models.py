"""Exercise Eurostat Landing decoding and the medallion graph offline."""
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

from eurostat_decode import decode_landing_files  # noqa: E402
from eurostat_platform_contract import EUROSTAT_PLATFORM_MODEL_NAMES  # noqa: E402
from eurostat_semantic import METRICS, validate_release_metrics  # noqa: E402


DBT_CLI = "from dbt.cli.main import cli; cli()"
OFFLINE = REPO_ROOT / "tests" / "helpers" / "run_offline.py"


class EurostatPlatformModelTests(unittest.TestCase):
    def test_complete_native_keys_missing_cells_and_revisions_reach_gold(self):
        with tempfile.TemporaryDirectory(prefix="zohelo-eurostat-model-") as temporary:
            root = Path(temporary)
            landing = root / "landing.parquet"
            decoded = root / "decoded.parquet"
            database = root / "platform.duckdb"
            target = root / "target"
            coverage_path = root / "full-source-coverage.json"
            rows = [
                self._row("history-1", "history", "2026-09-10T00:00:00Z", "demo_pjan_pl_2023_2025.json"),
                self._row("recent-1", "recent", "2026-09-11T00:00:00Z", "prc_hicp_minr_pl_2025_2026.json"),
                self._row("recent-2", "recent", "2026-09-12T00:00:00Z", "nama_10_gdp_pl_2023_2025.json"),
                self._row("history-2", "history", "2026-09-13T00:00:00Z", "demo_pjan_pl_2023_2025.json", changed_first_value=True),
                self._row("history-3", "history", "2026-09-14T00:00:00Z", "demo_pjan_pl_2023_2025.json"),
            ]
            self._write_landing(landing, rows)
            report = decode_landing_files([landing], decoded)
            self.assertEqual(report["decoded_response_count"], 5)
            self.assertGreater(report["decoded_cell_count"], 20)
            coverage_path.write_text(json.dumps({
                "catalogue_distributions": 21247,
                "validated_current_distributions": 3517,
                "pending_tasks": 19185,
                "failed_pending_tasks": 0,
                "accepted_distributions": 4103,
                "received_raw_bytes": 13305378130,
                "inventories_current": True,
                "catalogue_checked_on": "2026-09-14",
                "coverage_status": "incomplete",
            }), encoding="utf-8")

            env = dict(os.environ)
            env.update(
                ZOHELO_EUROSTAT_DECODED_PATH=str(decoded),
                ZOHELO_EUROSTAT_FULL_COVERAGE_PATH=str(coverage_path),
                ZOHELO_DUCKDB_PATH=str(database),
                DBT_SEND_ANONYMOUS_USAGE_STATS="false",
                DO_NOT_TRACK="1",
            )
            result = subprocess.run(
                [sys.executable, str(OFFLINE), sys.executable, "-c", DBT_CLI,
                 "build", "--select", *EUROSTAT_PLATFORM_MODEL_NAMES.values(),
                 "--profiles-dir", str(REPO_ROOT), "--target-path", str(target),
                 "--log-path", str(root / "logs"), "--threads", "1", "--no-partial-parse",
                 "--vars", "{enable_eurostat: true}"],
                cwd=REPO_ROOT, env=env, text=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, timeout=180,
            )
            self.assertEqual(result.returncode, 0, result.stdout)
            semantic_report = validate_release_metrics(
                database, target / "semantic_manifest.json", root / "metric-validation"
            )
            self.assertEqual(semantic_report["status"], "verified")
            self.assertEqual(
                {item["name"] for item in semantic_report["metrics"]}, set(METRICS)
            )
            with duckdb.connect(str(database), read_only=True) as connection:
                coverage = connection.execute(
                    'select admitted_dataset_total, admitted_series_total, modeled_dataset_total, '
                    'modeled_series_total, catalogue_distributions, validated_current_distributions, '
                    'latest_observation_date, raw_catalogue_complete, complete_official_catalogue '
                    'from "04_gold"."mart_eurostat_coverage"'
                ).fetchone()
                self.assertEqual(coverage[:6], (3, 81, 3, 3, 21247, 3517))
                self.assertEqual(coverage[6].isoformat(), "2026-08-31")
                self.assertEqual(coverage[7:], (False, False))
                missing = connection.execute(
                    'select count(*) from "04_gold"."fact_eurostat_observations" where is_missing'
                ).fetchone()[0]
                self.assertEqual(missing, 1)
                complete_keys = connection.execute(
                    'select count(*) from "04_gold"."fact_eurostat_observations" '
                    "where json_extract_string(dimension_key_json, '$.geo') = 'PL' "
                    "and json_extract_string(dimension_key_json, '$.time') is not null"
                ).fetchone()[0]
                decoded_keys = connection.execute(
                    "select count(*) from (select distinct dataset_id, dimension_key_sha256 "
                    "from read_parquet(?))",
                    [str(decoded)],
                ).fetchone()[0]
                self.assertEqual(complete_keys, decoded_keys)
                reverted = connection.execute(
                    'select revision_count, current_revision_event_type '
                    'from "04_gold"."fact_eurostat_observations" '
                    "where dataset_key = 'demo_pjan' and period_key = '2023'"
                ).fetchone()
                self.assertEqual(reverted, (3, "observed_value_changed"))
                reverted_history = connection.execute(
                    'select value_json, revision_event_type from "03_silver"."eurostat_observation_revisions" '
                    "where dataset_id = 'demo_pjan' and period_key = '2023' order by revision_number"
                ).fetchall()
                self.assertEqual(len(reverted_history), 3)
                self.assertEqual(reverted_history[0][0], reverted_history[2][0])

    @staticmethod
    def _row(task_id, lane, retrieved_at, fixture_name, changed_first_value=False):
        payload = (REPO_ROOT / "tests/fixtures/sources/eurostat" / fixture_name).read_text(encoding="utf-8")
        document = json.loads(payload)
        if changed_first_value:
            first_key = sorted(document["value"], key=int)[0]
            document["value"][first_key] += 1
            payload = json.dumps(document, ensure_ascii=False, separators=(",", ":"))
        dataset = document["extension"]["id"].lower()
        raw = payload.encode("utf-8")
        return (
            "eurostat", task_id, lane, "jsonstat",
            datetime.fromisoformat(retrieved_at.replace("Z", "+00:00")).astimezone(timezone.utc),
            len(document.get("value", {})), hashlib.sha256(raw).hexdigest(), len(raw),
            "{}", json.dumps({"dataset": dataset, "geo": "PL"}), payload, "application/json",
        )

    @staticmethod
    def _write_landing(path, rows):
        with duckdb.connect() as connection:
            connection.execute("""
                create table landing (
                    source_id varchar, task_id varchar, lane varchar, task_kind varchar,
                    retrieved_at_utc timestamptz, record_count bigint, raw_sha256 varchar,
                    raw_size_bytes bigint, request_json varchar, metadata_json varchar,
                    payload_utf8 varchar, content_type varchar
                )
            """)
            connection.executemany("insert into landing values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
            escaped = str(path).replace("'", "''")
            connection.execute(f"copy landing to '{escaped}' (format parquet)")


if __name__ == "__main__":
    unittest.main()
