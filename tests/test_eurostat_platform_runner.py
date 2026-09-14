import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import eurostat_platform  # noqa: E402
from eurostat_platform_contract import (  # noqa: E402
    EUROSTAT_PLATFORM_DATASETS,
    EUROSTAT_PLATFORM_DATE_COLUMNS,
    EUROSTAT_RELEASE_SCOPE,
)
from release_protocol import PLATFORM_RELEASES  # noqa: E402


class _CampaignStore:
    def __init__(self, state, landing_bytes):
        self._state = state
        self._landing_bytes = landing_bytes

    def load(self):
        return self._state

    def read_landing_object(self, _descriptor):
        return self._landing_bytes


class _ReleaseStore:
    root_id = "root"


class EurostatPlatformRunnerTests(unittest.TestCase):
    def test_release_scope_is_registered_with_the_exact_contract(self):
        contract = PLATFORM_RELEASES[EUROSTAT_RELEASE_SCOPE]
        self.assertEqual(contract["datasets"], EUROSTAT_PLATFORM_DATASETS)
        self.assertEqual(contract["date_columns"], EUROSTAT_PLATFORM_DATE_COLUMNS)
        self.assertEqual(contract["allow_zero_rows"], frozenset())

    def test_run_platform_assembles_release_inputs_and_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "fact_eurostat_observations.parquet"
            dataset.write_bytes(b"dataset")
            manifest = {
                "source_id": "eurostat",
                "snapshot_id": "snapshot-1",
                "accepted_response_count": 2,
                "published_response_count": 2,
                "pending_publication_count": 0,
                "coverage_status": "complete_current_catalogue",
                "files": [
                    {"id": "landing-1", "size": 7, "sha256": "a" * 64},
                    {"id": "landing-2", "size": 7, "sha256": "b" * 64},
                ],
            }
            state = {
                "accepted_responses": 2,
                "pending": [{"id": "pending-1"}],
                "catalogue_totals": {"variables": 2},
                "last_success_utc": "2026-09-10T00:00:00Z",
                "last_attempt_utc": "2026-09-10T00:00:00Z",
            }
            release_calls = {}

            def fake_publish_release(store, root_id, **kwargs):
                release_calls["store"] = store
                release_calls["root_id"] = root_id
                release_calls["kwargs"] = kwargs
                release_calls["artifact_payloads"] = {
                    item["name"]: Path(item["path"]).read_text(encoding="utf-8")
                    for item in kwargs.get("artifacts", [])
                }
                return {"release_id": "release-1"}

            def fake_build_platform(workspace):
                target = workspace / "target"
                target.mkdir(parents=True, exist_ok=True)
                for name in ("manifest.json", "catalog.json", "run_results.json", "semantic_manifest.json", "metric-validation.json"):
                    (target / name).write_text("{}", encoding="utf-8")
                return (
                    [{
                        "dataset_id": "fact_eurostat_observations",
                        "table_name": "fact_eurostat_observations",
                        "model_name": "fact_eurostat_observations",
                        "model_id": "model.zohelo_data.fact_eurostat_observations",
                        "layer": "04_gold",
                        "path": str(dataset),
                        "row_count": 1,
                        "date_column": "period_start_date",
                        "min_date": "2026-09-10",
                        "max_date": "2026-09-10",
                        "columns": [{"name": "period_start_date", "type": "date"}],
                    }],
                    [{"name": name, "path": str(target / name)} for name in ("manifest.json", "catalog.json", "run_results.json", "semantic_manifest.json", "metric-validation.json")],
                    {
                        "snapshot_date": "2026-09-10",
                        "latest_retrieved_at_utc": "2026-09-10T00:00:00",
                        "admitted_dataset_total": 3,
                        "admitted_series_total": 81,
                        "modeled_dataset_total": 1,
                        "modeled_series_total": 1,
                        "modeled_response_total": 2,
                        "modeled_cell_total": 1,
                        "modeled_value_total": 1,
                        "admitted_dataset_coverage_ratio": 1 / 3,
                        "admitted_series_coverage_ratio": 1 / 81,
                        "catalogue_distributions": 21247,
                        "validated_current_distributions": 3517,
                        "pending_distribution_tasks": 19185,
                        "failed_pending_distribution_tasks": 0,
                        "accepted_distributions": 4103,
                        "received_raw_bytes": 13305378130,
                        "full_distribution_coverage_ratio": 3517 / 21247,
                        "inventories_current": True,
                        "catalogue_checked_on": "2026-09-10",
                        "complete_official_catalogue": False,
                    },
                )

            with patch.object(eurostat_platform, "_campaign_store", return_value=(None, None, _CampaignStore(state, b"landing!"), _ReleaseStore())), \
                 patch.object(eurostat_platform, "verify_landing", return_value=manifest), \
                 patch.object(eurostat_platform, "decode_landing_files", return_value={"decoded_response_count": 2, "decoded_cell_count": 1}), \
                 patch.object(eurostat_platform, "_load_full_source_coverage", return_value={"coverage_status": "incomplete"}), \
                 patch.object(eurostat_platform, "build_platform", side_effect=fake_build_platform), \
                 patch.object(eurostat_platform, "_code_sha", return_value="a" * 40), \
                 patch.object(eurostat_platform, "resolve_source_release_root", return_value=("releases", False)), \
                 patch.object(eurostat_platform, "publish_release", side_effect=fake_publish_release):
                report = eurostat_platform.run_platform(
                    backend="drive",
                    allow_production_write=True,
                )

        self.assertEqual(report["status"], "eurostat_platform_published")
        self.assertEqual(report["release_id"], "release-1")
        self.assertEqual(release_calls["root_id"], "root")
        self.assertEqual(release_calls["kwargs"]["release_scope"], "eurostat_progressive_api_platform")
        self.assertEqual(release_calls["kwargs"]["inputs"], [
            {"source_id": "eurostat", "id": "landing-1", "size": 7, "sha256": "a" * 64, "ingestion_sequence": 1},
            {"source_id": "eurostat", "id": "landing-2", "size": 7, "sha256": "b" * 64, "ingestion_sequence": 2},
        ])
        artifact_names = {item["name"] for item in release_calls["kwargs"]["artifacts"]}
        self.assertTrue({"business-catalog.json", "ingestion-state.json", "manifest.json", "catalog.json", "run_results.json"}.issubset(artifact_names))
        state_artifact = next(item for item in release_calls["kwargs"]["artifacts"] if item["name"] == "ingestion-state.json")
        state_document = json.loads(release_calls["artifact_payloads"]["ingestion-state.json"])
        self.assertEqual(state_document["source_id"], "eurostat")
        self.assertEqual(state_document["sources"]["eurostat"]["coverage"]["modeled_dataset_total"], 1)
        self.assertFalse(state_document["sources"]["eurostat"]["coverage_complete"])
        business_catalog = next(item for item in release_calls["kwargs"]["artifacts"] if item["name"] == "business-catalog.json")
        self.assertEqual(json.loads(release_calls["artifact_payloads"]["business-catalog.json"])["sources"][0]["source_id"], "eurostat")
        self.assertEqual(release_calls["kwargs"]["measurements"]["landing_fragments"], 2)
        self.assertEqual(release_calls["kwargs"]["measurements"]["decoded_response_count"], 2)

    def test_local_backend_is_rejected_before_publication(self):
        with self.assertRaisesRegex(ValueError, "only the drive backend"):
            eurostat_platform.run_platform(backend="local", local_root=Path("/tmp/eurostat"))


if __name__ == "__main__":
    unittest.main()
