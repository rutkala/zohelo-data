"""Integration boundaries: bounded catch-up, public HTTP and real dbt publication."""
from datetime import date
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from business_catalog import build_business_catalog
from ingestion.nbp_http import fetch_response
from ingestion.nbp_state import SourceSpec, source_specs_from_config
from nbp_platform import build_platform, coverage_complete, ingest
from release_protocol import publish_release, restore_current_release
import test_nbp_platform_models as model_fixtures
from test_release_protocol import MemoryStore


class PlatformRunnerTests(unittest.TestCase):
    def test_bootstrap_then_daily_has_exactly_one_historical_chunk_per_source(self):
        specs = source_specs_from_config(ROOT / "config/sources.yaml")
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

        with patch("nbp_platform.DriveStateStore", return_value=store), patch("nbp_platform.fetch_response", side_effect=fetch):
            _, loaded, first = ingest(storage, specs=specs, cutoff=date(2020, 9, 30), mode="full", code_sha="a" * 40)
            self.assertTrue(coverage_complete(loaded.state, date(2020, 9, 30)))
            self.assertLess(first["requests"], 25)
            _, loaded, daily = ingest(storage, specs=specs, cutoff=date(2020, 10, 1), mode="incremental", code_sha="a" * 40)
            self.assertEqual(daily["requests"], 8)
            self.assertTrue(coverage_complete(loaded.state, date(2020, 10, 1)))

    def test_http_retry_budget_and_size_and_origin_boundary(self):
        spec = next(iter(source_specs_from_config(ROOT / "config/sources.yaml").values()))
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

    def test_real_dbt_exports_publish_and_restore_as_one_matching_candidate(self):
        temporary_root = ROOT / ".local/test-tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root, prefix="pipeline-") as temporary:
            workspace = Path(temporary)
            batches = workspace / "batches.jsonl"
            model_fixtures.NbpPlatformModelTests._write_batches(batches)
            datasets, artifacts = build_platform(workspace, batches)
            configuration = yaml.safe_load((ROOT / "config/sources.yaml").read_text())
            state = {"format_version": 1, "sources": {key: {"checked_through": "2020-01-01",
                     "latest_observation_date": "2020-01-01", "raw_response_count": 1}
                     for key in configuration["sources"]}}
            catalogue = build_business_catalog(code_sha="a" * 40, source_config=configuration,
                ingestion_state=state, dbt_manifest=json.loads((workspace / "target/manifest.json").read_text()),
                dataset_metadata=datasets)
            for name, data in (("business-catalog.json", catalogue), ("ingestion-state.json", state)):
                path = workspace / name
                path.write_text(json.dumps(data))
                artifacts.append({"name": name, "path": str(path)})
            store = MemoryStore()
            result = publish_release(store, "root", datasets=datasets, artifacts=artifacts,
                inputs=[], measurements={}, code_sha="a" * 40, release_scope="nbp_platform")
            restored = restore_current_release(store, "root")
            self.assertEqual(restored["release_id"], result["release_id"])
            self.assertEqual(len(restored["datasets"]), 15)
            self.assertEqual(restored["format_version"], 2)
            self.assertEqual(catalogue["metrics_status"], "awaiting_business_approval")
            self.assertTrue(catalogue["lineage"]["edges"])


if __name__ == "__main__":
    unittest.main()
