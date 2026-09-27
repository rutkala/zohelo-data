import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import wdi_platform  # noqa: E402
from wdi_platform_contract import WDI_ARCHIVE_MEMBERS  # noqa: E402


class _ReleaseStore:
    root_id = "wdi-root"


class WdiPlatformRunnerTests(unittest.TestCase):
    def test_unchanged_archive_and_code_reuse_current_release(self):
        release_store = _ReleaseStore()
        manifest = {
            "release_id": "release-1",
            "release_scope": "wdi_platform",
            "code_sha": "b" * 40,
            "inputs": [{
                "source_id": "world_bank_wdi",
                "sha256": "a" * 64,
                "size": 123,
            }],
        }
        receipt = {"raw": {"sha256": "a" * 64, "size_bytes": 123}}
        with patch.object(wdi_platform, "read_current_release_manifest", return_value=manifest):
            self.assertIs(
                wdi_platform._unchanged_release(release_store, "b" * 40, receipt),
                manifest,
            )
            self.assertIsNone(wdi_platform._unchanged_release(release_store, "c" * 40, receipt))

    def test_extract_archive_requires_and_streams_all_six_members(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "WDI.zip"
            members = []
            with zipfile.ZipFile(archive, "w") as bundle:
                for name in WDI_ARCHIVE_MEMBERS:
                    data = f"{name}\n".encode()
                    bundle.writestr(f"nested/{name}", data)
                    members.append({"name": f"nested/{name}", "byte_size": len(data)})
            result = wdi_platform._extract_archive(
                archive, {"members": members}, root / "members"
            )
            self.assertEqual(set(result), set(WDI_ARCHIVE_MEMBERS))
            self.assertTrue(all(path.read_bytes() for path in result.values()))

    def test_extract_archive_fails_closed_when_contract_adds_a_member(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "WDI.zip"
            with zipfile.ZipFile(archive, "w") as bundle:
                bundle.writestr("unexpected.csv", b"x")
            with self.assertRaisesRegex(RuntimeError, "member contract changed"):
                wdi_platform._extract_archive(
                    archive, {"members": [{"name": "unexpected.csv", "byte_size": 1}]},
                    root / "members",
                )

    def test_run_platform_binds_complete_archive_and_release_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "fact.parquet"
            dataset.write_bytes(b"dataset")
            receipt = {
                "accepted": True, "source_id": "world_bank_wdi",
                "retrieved_at_utc": "2026-09-13T12:00:00Z",
                "raw": {"id": "raw-1", "sha256": "a" * 64, "size_bytes": 1234},
                "distribution": {"dataset_id": "WDI", "kind": "wdi_zip"},
                "inspection": {"status": "complete", "member_count": 6},
            }
            full_coverage = {
                "coverage_status": "complete_current_catalogue",
                "catalogue_checked_on": "2026-09-13", "validated_current_distributions": 1,
                "accepted_distributions": 6,
            }
            index = {"snapshot_id": "bulk-index-1"}
            modeled = {
                "snapshot_date": "2026-09-13", "latest_retrieved_at_utc": "2026-09-13T12:00:00",
                "current_archive_total": 1, "archive_member_total": 6,
                "source_geography_total": 2, "source_indicator_total": 2,
                "source_value_total": 6, "modeled_geography_total": 2,
                "modeled_indicator_total": 2, "modeled_observation_total": 6,
                "first_observation_date": "2020-01-01", "latest_observation_date": "2021-01-01",
                "modeled_value_coverage_ratio": 1.0,
            }
            captured = {}

            def fake_build(workspace, _members, _evidence):
                target = workspace / "target"
                target.mkdir()
                manifest = {"metadata": {}, "nodes": {}, "metrics": {}, "semantic_models": {}, "sources": {}}
                for name in wdi_platform.ARTIFACT_NAMES:
                    value = manifest if name == "manifest.json" else {}
                    (target / name).write_text(json.dumps(value), encoding="utf-8")
                return ([{
                    "dataset_id": "fact_wdi_observations", "table_name": "fact_wdi_observations",
                    "model_name": "fact_wdi_observations", "model_id": "model.zohelo_data.fact_wdi_observations",
                    "layer": "04_gold", "path": str(dataset), "paths": [str(dataset)],
                    "row_count": 6, "date_column": "observation_date",
                    "min_date": "2020-01-01", "max_date": "2021-01-01",
                    "columns": [{"name": "observation_date", "type": "DATE"}],
                }], [{"name": name, "path": str(target / name)} for name in wdi_platform.ARTIFACT_NAMES], modeled)

            def fake_extract(_archive, _inspection, output):
                output.mkdir()
                result = {}
                for name in WDI_ARCHIVE_MEMBERS:
                    path = output / name
                    path.write_text(name, encoding="utf-8")
                    result[name] = path
                return result

            def fake_publish(_store, root_id, **kwargs):
                captured["root_id"] = root_id
                captured["kwargs"] = kwargs
                captured["artifacts"] = {
                    item["name"]: Path(item["path"]).read_text(encoding="utf-8")
                    for item in kwargs["artifacts"]
                }
                return {"release_id": "release-1"}

            archive = root / "archive.zip"
            archive.write_bytes(b"archive")
            with patch.object(wdi_platform, "_stores", return_value=(object(), object(), _ReleaseStore(), False, "wdi-root", None)), \
                 patch.object(wdi_platform, "_current_archive", return_value=({}, full_coverage, receipt, archive, receipt["inspection"], index)), \
                 patch.object(wdi_platform, "_extract_archive", side_effect=fake_extract), \
                 patch.object(wdi_platform, "build_platform", side_effect=fake_build), \
                 patch.object(wdi_platform, "_code_sha", return_value="b" * 40), \
                 patch.object(wdi_platform, "read_current_release_manifest", side_effect=wdi_platform.ReleaseProtocolError("no current-release pointer exists")), \
                 patch.object(wdi_platform, "publish_release", side_effect=fake_publish):
                report = wdi_platform.run_platform(allow_production_write=True)

        self.assertEqual(report["status"], "wdi_platform_published")
        self.assertEqual(captured["root_id"], "wdi-root")
        self.assertEqual(captured["kwargs"]["release_scope"], "wdi_platform")
        self.assertEqual(captured["kwargs"]["inputs"], [{
            "source_id": "world_bank_wdi", "dataset_id": "WDI", "id": "raw-1",
            "size": 1234, "sha256": "a" * 64, "retrieved_at_utc": "2026-09-13T12:00:00Z",
        }])
        state = json.loads(captured["artifacts"]["ingestion-state.json"])
        self.assertEqual(state["coverage_status"], "complete_current_catalogue")
        self.assertEqual(state["archive_member_total"], 6)
        self.assertEqual(state["coverage"]["modeled_value_coverage_ratio"], 1.0)


if __name__ == "__main__":
    unittest.main()
