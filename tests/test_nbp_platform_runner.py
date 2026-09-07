"""Integration boundaries: bounded catch-up, public HTTP and real dbt publication."""
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from business_catalog import build_business_catalog
from ingestion.nbp_http import fetch_response
from ingestion.nbp_state import (
    SourceSpec,
    ingestion_config_from_config,
    load_state,
    source_specs_from_config,
)
from nbp_platform import build_platform, coverage_complete, ingest
from local_release import restore_local_release
from release_protocol import ReleaseProtocolError, publish_release, restore_current_release
from release_validation import validate_staged_release
import test_nbp_platform_models as model_fixtures
from test_release_protocol import MemoryStore


class PlatformRunnerTests(unittest.TestCase):
    def test_bootstrap_then_daily_has_exactly_one_historical_chunk_per_source(self):
        settings = ingestion_config_from_config(ROOT / "config/nbp-platform.yaml")
        specs = settings.source_specs
        specs = {key: SourceSpec(key, date(2020, 1, 1), value.endpoint_template, value.params)
                 for key, value in specs.items()}
        store = MemoryStore()
        storage = MagicMock()
        storage.resolve_root.return_value = "root"
        storage.resolve_zone.return_value = "landing"
        storage.get_or_create_nested_folder.side_effect = lambda parts, root_id: "-".join([root_id, *parts])

        def fetch(plan, **_):
            start = plan.requested_start_date.isoformat()
            if plan.source_id == "nbp_gold_prices":
                body = [{"data": start, "cena": 200.11}]
            else:
                table = plan.source_id[-1].upper()
                values = {"bid": 3.7, "ask": 3.9} if table == "C" else {"mid": 3.8}
                body = [{"table": table, "no": "test", "effectiveDate": start,
                         **({"tradingDate": start} if table == "C" else {}),
                         "rates": [{"code": "USD", "currency": "dollar", **values}]}]
            return 200, json.dumps(body).encode(), 0

        with patch("nbp_platform.DriveStateStore", return_value=store), patch("nbp_platform.fetch_response", side_effect=fetch), patch("nbp_platform.load_state", wraps=load_state) as load:
            _, loaded, first = ingest(
                storage,
                specs=specs,
                settings=settings,
                cutoff=date(2020, 9, 30),
                mode="full",
                code_sha="a" * 40,
            )
            self.assertTrue(coverage_complete(loaded.state, date(2020, 9, 30)))
            self.assertLess(first["requests"], 25)
            self.assertEqual(load.call_count, 1)
            _, loaded, daily = ingest(
                storage,
                specs=specs,
                settings=settings,
                cutoff=date(2020, 10, 1),
                mode="incremental",
                code_sha="a" * 40,
            )
            self.assertEqual(daily["requests"], 8)
            self.assertTrue(coverage_complete(loaded.state, date(2020, 10, 1)))
            self.assertEqual(load.call_count, 2)

    def test_http_retry_budget_and_size_and_origin_boundary(self):
        spec = next(iter(source_specs_from_config(ROOT / "config/nbp-platform.yaml").values()))
        plan = MagicMock(request_url=spec.endpoint_template.format(start_date="2020-01-01", end_date="2020-01-01"), params={"format": "json"})
        session = MagicMock()
        responses = []
        for status in (408, 429, 200):
            response = MagicMock(status_code=status)
            response.__enter__.return_value = response
            response.iter_content.return_value = [b"[]"]
            responses.append(response)
        session.get.side_effect = responses
        self.assertEqual(fetch_response(plan, session=session, sleep=lambda _: None), (200, b"[]", 2))
        self.assertEqual(session.get.call_count, 3)
        response = MagicMock(status_code=200)
        response.__enter__.return_value = response
        response.iter_content.return_value = [b"too large"]
        session.get.side_effect = [response]
        with self.assertRaisesRegex(ValueError, "size limit"):
            fetch_response(plan, session=session, max_bytes=2)
        plan.request_url = "https://example.org/api/"
        with self.assertRaisesRegex(ValueError, "public HTTPS"):
            fetch_response(plan, session=session)

    def test_http_retry_after_is_bounded_and_all_5xx_are_retried(self):
        spec = next(iter(source_specs_from_config(ROOT / "config/nbp-platform.yaml").values()))
        plan = MagicMock(
            request_url=spec.endpoint_template.format(
                start_date="2020-01-01", end_date="2020-01-01"
            ),
            params={"format": "json"},
        )
        session = MagicMock()
        responses = []
        for status, retry_after in ((429, "999"), (520, "not-a-date"), (200, None)):
            response = MagicMock(status_code=status)
            response.__enter__.return_value = response
            response.headers = {} if retry_after is None else {"Retry-After": retry_after}
            response.iter_content.return_value = [b"[]"]
            responses.append(response)
        session.get.side_effect = responses
        sleeps = []
        self.assertEqual(
            fetch_response(plan, session=session, sleep=sleeps.append), (200, b"[]", 2)
        )
        self.assertEqual(sleeps, [8, 2])

        retry_at = MagicMock(status_code=503)
        retry_at.__enter__.return_value = retry_at
        retry_at.headers = {"Retry-After": "Mon, 07 Sep 2026 00:00:20 GMT"}
        retry_at.iter_content.return_value = [b"busy"]
        success = MagicMock(status_code=200)
        success.__enter__.return_value = success
        success.headers = {}
        success.iter_content.return_value = [b"[]"]
        session.get.side_effect = [retry_at, success]
        sleeps = []
        result = fetch_response(
            plan,
            session=session,
            max_retry_delay_seconds=6,
            sleep=sleeps.append,
            now=lambda: datetime(2026, 9, 7, tzinfo=timezone.utc),
        )
        self.assertEqual(result, (200, b"[]", 1))
        self.assertEqual(sleeps, [6])

        exhausted = []
        for _ in range(4):
            response = MagicMock(status_code=520)
            response.__enter__.return_value = response
            response.headers = {}
            response.iter_content.return_value = [b"still unavailable"]
            exhausted.append(response)
        session.get.side_effect = exhausted
        sleeps = []
        self.assertEqual(
            fetch_response(plan, session=session, sleep=sleeps.append),
            (520, b"still unavailable", 3),
        )
        self.assertEqual(sleeps, [1, 2, 4])

    def test_real_dbt_exports_publish_and_restore_as_one_matching_candidate(self):
        temporary_root = ROOT / ".local/test-tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root, prefix="pipeline-") as temporary:
            workspace = Path(temporary)
            batches = workspace / "batches.jsonl"
            model_fixtures.NbpPlatformModelTests._write_batches(batches)
            datasets, artifacts = build_platform(workspace, batches)
            manifest = json.loads((workspace / "target/manifest.json").read_text())
            catalog = json.loads((workspace / "target/catalog.json").read_text())
            self.assertEqual(len(datasets), 15)
            for dataset in datasets:
                with self.subTest(dataset=dataset["dataset_id"]):
                    node = manifest["nodes"][dataset["model_id"]]
                    relation = catalog["nodes"][dataset["model_id"]]["metadata"]
                    self.assertEqual(node["schema"], dataset["layer"])
                    self.assertEqual(node["alias"], dataset["table_name"])
                    self.assertEqual(relation["schema"], dataset["layer"])
                    self.assertEqual(relation["name"], dataset["table_name"])
            configuration = yaml.safe_load((ROOT / "config/sources.yaml").read_text())
            batch_rows = [json.loads(line) for line in batches.read_text().splitlines()]
            state_sources = {}
            inputs = []
            for source_id in configuration["sources"]:
                source_rows = [row for row in batch_rows if row["source_id"] == source_id]
                observed_dates = []
                descriptors = []
                for row in source_rows:
                    size = len(row["body_json"].encode())
                    descriptors.append({
                        **{key: row[key] for key in (
                            "source_id", "batch_id", "ingestion_sequence", "requested_start_date",
                            "requested_end_date", "retrieved_at_utc", "response_sha256", "raw_file_id",
                        )},
                        "size_bytes": size,
                    })
                    inputs.append({
                        "source_id": source_id, "id": row["raw_file_id"], "size": size,
                        "sha256": row["response_sha256"],
                        "ingestion_sequence": row["ingestion_sequence"],
                        "provenance": "verified_exact_nbp_response",
                    })
                    payload = json.loads(row["body_json"])
                    observed_dates.extend(
                        item["data"] if source_id == "nbp_gold_prices" else item["effectiveDate"]
                        for item in payload
                    )
                state_sources[source_id] = {
                    "last_checked_through_date": "2020-01-01",
                    "latest_observation_date": max(observed_dates),
                    "last_successful_ingestion_at_utc": "2020-01-10T00:00:00Z",
                    "last_attempt_at_utc": "2020-01-10T00:00:00Z",
                    "successful_responses": descriptors,
                }
            state = {"format_version": 1, "code_sha": "a" * 40, "cutoff": "2020-01-01",
                     "sources": state_sources}
            normalized_state = {"sources": {source_id: {
                "checked_through": source["last_checked_through_date"],
                "latest_observation_date": source["latest_observation_date"],
                "last_successful_ingestion_at": source["last_successful_ingestion_at_utc"],
                "last_attempt_at": source["last_attempt_at_utc"],
                "coverage_complete": True,
                "raw_response_count": len(source["successful_responses"]),
            } for source_id, source in state_sources.items()}}
            catalogue = build_business_catalog(code_sha="a" * 40, source_config=configuration,
                ingestion_state=normalized_state, dbt_manifest=manifest,
                dataset_metadata=datasets)
            for name, data in (("business-catalog.json", catalogue), ("ingestion-state.json", state)):
                path = workspace / name
                path.write_text(json.dumps(data))
                artifacts.append({"name": name, "path": str(path)})

            mismatched_inputs = [dict(item) for item in inputs]
            mismatched_inputs[0]["id"] = "unbound-raw-object"
            rejected_store = MemoryStore()
            with self.assertRaisesRegex(ReleaseProtocolError, "failed pre-promotion validation"):
                publish_release(
                    rejected_store, "root", datasets=datasets, artifacts=artifacts,
                    inputs=mismatched_inputs, measurements={}, code_sha="a" * 40,
                    release_scope="nbp_platform",
                    pre_promote_validator=validate_staged_release,
                )
            self.assertEqual(rejected_store.find("current-release.json", "root"), [])

            store = MemoryStore()
            result = publish_release(store, "root", datasets=datasets, artifacts=artifacts,
                inputs=inputs, measurements={}, code_sha="a" * 40, release_scope="nbp_platform",
                pre_promote_validator=validate_staged_release)
            restored = restore_current_release(store, "root")
            self.assertEqual(restored["release_id"], result["release_id"])
            self.assertEqual(len(restored["datasets"]), 15)
            self.assertEqual(restored["format_version"], 2)
            self.assertEqual(catalogue["metrics_status"], "source_defined")
            self.assertTrue(catalogue["lineage"]["edges"])
            restored_workspace = workspace / "portable-release"
            local = restore_local_release(store, "root", restored_workspace)
            self.assertEqual(local["tables"], 15)
            with duckdb.connect(local["database"], read_only=True) as connection:
                self.assertEqual(
                    connection.execute('select count(*) from "04_gold"."fact_fx_quotes"').fetchone()[0],
                    next(item["row_count"] for item in datasets if item["dataset_id"] == "fact_fx_quotes"),
                )


if __name__ == "__main__":
    unittest.main()
