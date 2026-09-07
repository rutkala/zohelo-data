import json
import sys
import tempfile
import unittest
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from release_protocol import (  # noqa: E402
    ReleaseProtocolError,
    publish_release,
    promote_retained_release,
    restore_current_release,
    restore_release,
)
from release_validation import validate_staged_release  # noqa: E402


class MemoryStore:
    """Small exact-name Drive-shaped store used to prove publication recovery."""

    def __init__(self):
        self.files = {}
        self.folders = {}
        self.writes = 0
        self.next_id = 1
        self.fail_create_name = None
        self.corrupt_read_name = None
        self.read_counts = {}
        self.raise_after_replace = False
        self.raise_before_replace = False
        self.pointer_reads = 0
        self.drift_bytes = None

    def _id(self):
        value = f"f{self.next_id}"
        self.next_id += 1
        return value

    def find(self, name, parent_id):
        return [file_id for file_id, item in self.files.items() if item["name"] == name and item["parent"] == parent_id] + [
            folder_id for folder_id, item in self.folders.items() if item["name"] == name and item["parent"] == parent_id
        ]

    def create(self, name, data, parent_id):
        self.writes += 1
        if name == self.fail_create_name:
            raise OSError("injected create failure")
        file_id = self._id()
        self.files[file_id] = {"name": name, "data": data, "parent": parent_id}
        return file_id

    def read(self, file_id):
        item = self.files[file_id]
        self.read_counts[item["name"]] = self.read_counts.get(item["name"], 0) + 1
        if item["name"] == "current-release.json":
            self.pointer_reads += 1
            if self.drift_bytes is not None and self.pointer_reads == 2:
                item["data"] = self.drift_bytes
        if item["name"] == self.corrupt_read_name and self.read_counts[item["name"]] > 1:
            return b"corrupt readback"
        return item["data"]

    def replace(self, file_id, data):
        self.writes += 1
        if self.raise_before_replace:
            raise OSError("request failed before Drive update")
        self.files[file_id]["data"] = data
        if self.raise_after_replace:
            raise OSError("reply lost after Drive update")

    def mkdir(self, name, parent_id):
        self.writes += 1
        folder_id = self._id()
        self.folders[folder_id] = {"name": name, "parent": parent_id}
        return folder_id

    def put(self, name, data, parent="root"):
        file_id = self._id()
        self.files[file_id] = {"name": name, "data": data, "parent": parent}
        return file_id


class ReleaseProtocolTests(unittest.TestCase):
    release_id = "1a0b4d59-5475-4d2f-b97d-4854a2176131"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def _file(self, name, content):
        path = self.directory / name
        path.write_bytes(content)
        return str(path)

    def _candidate(self, *, run_results=None):
        datasets = []
        for dataset_id in (
            "nbp_exchange_rates_table_a",
            "nbp_exchange_rates_table_b",
            "nbp_exchange_rates_table_c",
            "nbp_gold_prices",
        ):
            datasets.append(
                {
                    "dataset_id": dataset_id,
                    "layer": "03_silver",
                    "table_name": f"silver_{dataset_id}",
                    "path": self._file(f"{dataset_id}.parquet", (dataset_id + " bytes").encode()),
                    "row_count": 1,
                    "min_date": "2020-01-01",
                    "max_date": "2020-01-01",
                    "columns": [{"name": "effective_date", "type": "date"}],
                }
            )
        if run_results is None:
            run_results = {
                "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json"},
                "results": [
                    {"unique_id": "model.zohelo_data.stg_nbp_table_a", "status": "success"},
                    {"unique_id": "model.zohelo_data.stg_nbp_table_b", "status": "success"},
                    {"unique_id": "model.zohelo_data.stg_nbp_table_c", "status": "success"},
                    {"unique_id": "model.zohelo_data.stg_nbp_gold_prices", "status": "success"},
                    {"unique_id": "test.zohelo_data.not_null_stg_nbp_table_a_code", "status": "pass"},
                ],
            }
        return {
            "datasets": datasets,
            "artifacts": [
                {"name": "manifest.json", "path": self._file("manifest.json", b'{"dbt":true}')},
                {"name": "catalog.json", "path": self._file("catalog.json", b'{"nodes":{}}')},
                {"name": "run_results.json", "path": self._file("run_results.json", json.dumps(run_results).encode())},
            ],
            "inputs": [{"source_batch_id": "batch-1", "source_file_id": "raw-file-1"}],
            "code_sha": "a" * 40,
            "measurements": {"rows": 4},
            "release_id": self.release_id,
        }

    def test_legacy_candidate_cannot_downgrade_a_platform_publication(self):
        store = MemoryStore()
        published = publish_release(
            store, "root", **self._platform_candidate(),
            pre_promote_validator=lambda _store, _pointer: None,
        )
        writes_before = store.writes
        with self.assertRaisesRegex(ReleaseProtocolError, "legacy silver candidate"):
            publish_release(store, "root", **self._candidate())
        self.assertEqual(store.writes, writes_before)
        self.assertEqual(restore_current_release(store, "root")["release_id"], published["release_id"])

    def _platform_candidate(self):
        specifications = {
            "bronze_nbp_exchange_rates_table_a": ("02_bronze", "br_nbp_table_a"),
            "bronze_nbp_exchange_rates_table_b": ("02_bronze", "br_nbp_table_b"),
            "bronze_nbp_exchange_rates_table_c": ("02_bronze", "br_nbp_table_c"),
            "bronze_nbp_gold_prices": ("02_bronze", "br_nbp_gold_prices"),
            "nbp_exchange_rates_table_a": ("03_silver", "stg_nbp_table_a"),
            "nbp_exchange_rates_table_b": ("03_silver", "stg_nbp_table_b"),
            "nbp_exchange_rates_table_c": ("03_silver", "stg_nbp_table_c"),
            "nbp_gold_prices": ("03_silver", "stg_nbp_gold_prices"),
            "nbp_change_events": ("03_silver", "nbp_change_events"),
            "fact_fx_quotes": ("04_gold", "fact_fx_quotes"),
            "fact_gold_prices": ("04_gold", "fact_gold_prices"),
            "dim_date": ("04_gold", "dim_date"),
            "dim_currency": ("04_gold", "dim_currency"),
            "dim_source_table": ("04_gold", "dim_source_table"),
            "dim_commodity": ("04_gold", "dim_commodity"),
        }
        datasets = []
        results = []
        for dataset_id, (layer, model) in specifications.items():
            is_nondate_dimension = dataset_id in {"dim_currency", "dim_source_table", "dim_commodity"}
            is_empty_changes = dataset_id == "nbp_change_events"
            date_column = None if is_nondate_dimension else (
                "date_key" if dataset_id == "dim_date" else
                "effectiveDate" if layer == "03_silver" and dataset_id != "nbp_change_events" else
                "effective_date"
            )
            table_name = dataset_id.removeprefix("bronze_")
            datasets.append({
                "dataset_id": dataset_id, "layer": layer,
                "model_name": model, "model_id": f"model.zohelo_data.{model}", "table_name": table_name,
                "path": self._file(f"platform-{dataset_id}.parquet", dataset_id.encode()),
                "row_count": 0 if is_empty_changes else 1,
                "date_column": date_column,
                "min_date": None if is_nondate_dimension or is_empty_changes else "2020-01-01",
                "max_date": None if is_nondate_dimension or is_empty_changes else "2020-01-01",
                "columns": [{"name": date_column or "value", "type": "date" if date_column else "text"}],
            })
            results.extend([
                {"unique_id": f"model.zohelo_data.{model}", "status": "success"},
                {"unique_id": f"test.zohelo_data.not_null_{model}_value", "status": "pass"},
            ])
        sources = [{
            "source_id": source_id, "name": source_id, "description": "NBP fixture",
            "status": "published_snapshot", "checked_through": None,
            "latest_observation_date": None, "last_successful_ingestion_at": None,
            "last_attempt_at": None, "raw_response_count": 0,
        } for source_id in ("nbp_exchange_rates_table_a", "nbp_exchange_rates_table_b", "nbp_exchange_rates_table_c", "nbp_gold_prices")]
        business_catalog = {
            "format_version": 1, "code_sha": "a" * 40, "sources": sources,
            "lineage": {"nodes": [], "edges": []}, "metrics": [],
            "metrics_status": "awaiting_business_approval",
        }
        return {
            "datasets": datasets,
            "artifacts": [
                {"name": "manifest.json", "path": self._file("platform-manifest.json", b'{"metadata":{},"nodes":{}}')},
                {"name": "catalog.json", "path": self._file("platform-catalog.json", b'{"nodes":{}}')},
                {"name": "run_results.json", "path": self._file("platform-run-results.json", json.dumps({"metadata": {"dbt_schema_version": "v6"}, "results": results}).encode())},
                {"name": "business-catalog.json", "path": self._file("business-catalog.json", json.dumps(business_catalog).encode())},
                {"name": "ingestion-state.json", "path": self._file("ingestion-state.json", b'{"format_version":1,"sources":{}}')},
            ],
            "inputs": [], "code_sha": "a" * 40, "measurements": {},
            "release_id": self.release_id, "release_scope": "nbp_platform",
        }

    def _install_old_pointer(self, store):
        old_release_id = "a557f6b7-689f-49f3-9bca-01fc8de8d767"
        old_datasets = []
        old_file_id = None
        old_data = None
        for dataset_id in (
            "nbp_exchange_rates_table_a",
            "nbp_exchange_rates_table_b",
            "nbp_exchange_rates_table_c",
            "nbp_gold_prices",
        ):
            data = ("old " + dataset_id).encode()
            file_id = store.put(dataset_id + ".parquet", data, "old-release")
            if old_file_id is None:
                old_file_id, old_data = file_id, data
            old_datasets.append({
                "dataset_id": dataset_id,
                "layer": "03_silver",
                "table_name": "old_" + dataset_id,
                "row_count": 1,
                "min_date": "2020-01-01",
                "max_date": "2020-01-01",
                "columns": [{"name": "effective_date", "type": "date"}],
                "files": [{"id": file_id, "name": dataset_id + ".parquet", "size": len(data), "sha256": sha256(data).hexdigest()}],
            })
        old_artifacts = []
        for name in ("manifest.json", "catalog.json", "run_results.json"):
            data = ("old " + name).encode()
            file_id = store.put(name, data, "old-release")
            old_artifacts.append({"id": file_id, "name": name, "size": len(data), "sha256": sha256(data).hexdigest()})
        old_manifest = {
            "format_version": 1,
            "release_id": old_release_id,
            "release_scope": "nbp_silver",
            "status": "validated",
            "created_at_utc": "2026-09-06T00:00:00Z",
            "code_sha": "b" * 40,
            "datasets": old_datasets,
            "artifacts": old_artifacts,
            "inputs": [],
            "measurements": {},
            "tests": {"passed": True},
        }
        old_manifest_bytes = json.dumps(old_manifest, sort_keys=True, separators=(",", ":")).encode()
        old_manifest_id = store.put("release.json", old_manifest_bytes, "old-release")
        pointer = {
            "format_version": 1,
            "release_id": old_release_id,
            "manifest_file_id": old_manifest_id,
            "manifest_sha256": sha256(old_manifest_bytes).hexdigest(),
            "updated_at_utc": "2026-09-06T00:00:00Z",
        }
        pointer_id = store.put("current-release.json", json.dumps(pointer, sort_keys=True, separators=(",", ":")).encode())
        return pointer_id, store.files[pointer_id]["data"], old_file_id, old_data

    def test_failed_artifact_upload_preserves_old_pointer_and_files(self):
        store = MemoryStore()
        pointer_id, old_pointer, old_file_id, old_data = self._install_old_pointer(store)
        store.fail_create_name = "catalog.json"

        with self.assertRaises(OSError):
            publish_release(store, "root", **self._candidate())

        self.assertEqual(store.files[pointer_id]["data"], old_pointer)
        self.assertEqual(store.files[old_file_id]["data"], old_data)

    def test_failed_artifact_readback_preserves_old_pointer_and_files(self):
        store = MemoryStore()
        pointer_id, old_pointer, old_file_id, old_data = self._install_old_pointer(store)
        store.corrupt_read_name = "catalog.json"

        with self.assertRaisesRegex(ReleaseProtocolError, "did not read back"):
            publish_release(store, "root", **self._candidate())

        self.assertEqual(store.files[pointer_id]["data"], old_pointer)
        self.assertEqual(store.files[old_file_id]["data"], old_data)

    def test_incomplete_four_datasets_and_dbt_build_are_rejected_before_writes(self):
        store = MemoryStore()
        candidate = self._candidate()
        candidate["datasets"].pop()
        with self.assertRaisesRegex(ReleaseProtocolError, "all four datasets"):
            publish_release(store, "root", **candidate)
        self.assertEqual(store.writes, 0)

        store = MemoryStore()
        candidate = self._candidate(run_results={"metadata": {"dbt_schema_version": "v6"}, "results": [{"unique_id": "model.zohelo_data.stg_nbp_table_a", "status": "success"}]})
        with self.assertRaisesRegex(ReleaseProtocolError, "missing successful staging models"):
            publish_release(store, "root", **candidate)
        self.assertEqual(store.writes, 0)

    def test_pointer_drift_is_rejected_without_overwriting_competing_pointer(self):
        store = MemoryStore()
        pointer_id, _old_pointer, _old_file_id, _old_data = self._install_old_pointer(store)
        drift = {
            "format_version": 1,
            "release_id": "f0be52f1-0b1d-48c1-8d3e-6c3c7b9865d0",
            "manifest_file_id": "drift-manifest",
            "manifest_sha256": "a" * 64,
            "updated_at_utc": "2026-09-06T00:01:00Z",
        }
        store.drift_bytes = json.dumps(drift, sort_keys=True, separators=(",", ":")).encode()

        with self.assertRaisesRegex(ReleaseProtocolError, "changed during candidate upload"):
            publish_release(store, "root", **self._candidate())

        self.assertEqual(store.files[pointer_id]["data"], store.drift_bytes)

    def test_fresh_reader_restores_and_verifies_release_manifest_and_files(self):
        store = MemoryStore()
        report = publish_release(store, "root", **self._candidate())

        restored = restore_current_release(store, "root")
        self.assertEqual(restored["release_id"], report["release_id"])
        pointer_id = report["pointer_file_id"]
        self.assertEqual(restore_release(store, pointer_id)["release_id"], report["release_id"])

        released_file_id = restored["datasets"][0]["files"][0]["id"]
        store.files[released_file_id]["data"] = b"corrupted"
        with self.assertRaisesRegex(ReleaseProtocolError, "checksum mismatch"):
            restore_current_release(store, "root")

    def test_fresh_reader_rejects_incomplete_manifest_and_missing_pointer_release_id(self):
        store = MemoryStore()
        incomplete_manifest = {
            "format_version": 1,
            "release_id": self.release_id,
            "release_scope": "nbp_silver",
            "status": "validated",
            "code_sha": "a" * 40,
            "datasets": [],
            "artifacts": [],
            "tests": {"passed": True},
        }
        encoded = json.dumps(incomplete_manifest, sort_keys=True, separators=(",", ":")).encode()
        manifest_id = store.put("release.json", encoded)
        pointer = {
            "format_version": 1,
            "release_id": self.release_id,
            "manifest_file_id": manifest_id,
            "manifest_sha256": sha256(encoded).hexdigest(),
            "updated_at_utc": "2026-09-06T00:00:00Z",
        }
        pointer_id = store.put("current-release.json", json.dumps(pointer).encode())
        with self.assertRaisesRegex(ReleaseProtocolError, "exactly four datasets"):
            restore_release(store, pointer_id)

        pointer.pop("release_id")
        bad_pointer_id = store.put("bad-pointer.json", json.dumps(pointer).encode())
        with self.assertRaisesRegex(ReleaseProtocolError, "canonical UUID"):
            restore_release(store, bad_pointer_id)

    def test_uncertain_pointer_replace_accepts_only_exact_readback(self):
        store = MemoryStore()
        self._install_old_pointer(store)
        store.raise_after_replace = True

        report = publish_release(store, "root", **self._candidate())
        self.assertEqual(restore_current_release(store, "root")["release_id"], report["release_id"])

    def test_uncertain_pointer_failure_preserves_old_pointer(self):
        store = MemoryStore()
        pointer_id, old_pointer, _old_file_id, _old_data = self._install_old_pointer(store)
        store.raise_before_replace = True

        with self.assertRaisesRegex(ReleaseProtocolError, "uncertain outcome"):
            publish_release(store, "root", **self._candidate())
        self.assertEqual(store.files[pointer_id]["data"], old_pointer)

    def test_filename_id_and_inputs_validation_is_bounded_before_writes(self):
        store = MemoryStore()
        candidate = self._candidate()
        candidate["artifacts"][0]["name"] = "../manifest.json"
        with self.assertRaisesRegex(ReleaseProtocolError, "basename"):
            publish_release(store, "root", **candidate)
        self.assertEqual(store.writes, 0)

        store = MemoryStore()
        candidate = self._candidate()
        candidate["inputs"] = [{"value": "x" * 1_000_001}]
        with self.assertRaisesRegex(ReleaseProtocolError, "size limit"):
            publish_release(store, "root", **candidate)
        self.assertEqual(store.writes, 0)

        with self.assertRaisesRegex(ReleaseProtocolError, "root_id is invalid"):
            publish_release(MemoryStore(), "../root", **self._candidate())

    def test_platform_release_restores_v2_and_keeps_v1_pointer_contract(self):
        store = MemoryStore()
        _old_pointer_id, old_pointer, _old_file_id, _old_data = self._install_old_pointer(store)
        report = publish_release(
            store, "root", **self._platform_candidate(),
            pre_promote_validator=lambda _store, _pointer: None,
        )

        self.assertEqual(report["manifest"]["format_version"], 2)
        self.assertEqual(report["manifest"]["release_scope"], "nbp_platform")
        pointer = json.loads(store.files[report["pointer_file_id"]]["data"])
        self.assertEqual(pointer["format_version"], 1)
        self.assertEqual(restore_current_release(store, "root")["release_scope"], "nbp_platform")
        self.assertEqual(restore_release(store, json.loads(old_pointer))["release_scope"], "nbp_silver")

        business_catalog = next(item for item in report["manifest"]["artifacts"] if item["name"] == "business-catalog.json")
        store.files[business_catalog["id"]]["data"] = b"{}"
        with self.assertRaisesRegex(ReleaseProtocolError, "checksum mismatch"):
            restore_current_release(store, "root")

    def test_platform_catalogue_code_tampering_and_missing_model_test_are_rejected_before_writes(self):
        store = MemoryStore()
        candidate = self._platform_candidate()
        business = next(item for item in candidate["artifacts"] if item["name"] == "business-catalog.json")
        Path(business["path"]).write_text(json.dumps({"format_version": 1, "code_sha": "b" * 40, "sources": [], "lineage": {"nodes": [], "edges": []}, "metrics": [], "metrics_status": "proposed"}))
        with self.assertRaisesRegex(ReleaseProtocolError, "candidate code_sha"):
            publish_release(store, "root", **candidate)
        self.assertEqual(store.writes, 0)

        store = MemoryStore()
        candidate = self._platform_candidate()
        run_results = next(item for item in candidate["artifacts"] if item["name"] == "run_results.json")
        document = json.loads(Path(run_results["path"]).read_text())
        document["results"] = [result for result in document["results"] if result["unique_id"] != "model.zohelo_data.dim_date"]
        Path(run_results["path"]).write_text(json.dumps(document))
        with self.assertRaisesRegex(ReleaseProtocolError, "missing successful platform models"):
            publish_release(store, "root", **candidate)
        self.assertEqual(store.writes, 0)

    def test_platform_requires_pre_promotion_validator_before_remote_writes(self):
        store = MemoryStore()
        with self.assertRaisesRegex(ReleaseProtocolError, "requires staged content validation"):
            publish_release(store, "root", **self._platform_candidate())
        self.assertEqual(store.writes, 0)

    def test_failed_staged_validation_preserves_current_pointer(self):
        store = MemoryStore()
        pointer_id, old_pointer, _old_file_id, _old_data = self._install_old_pointer(store)

        with self.assertRaisesRegex(ReleaseProtocolError, "failed pre-promotion validation"):
            publish_release(
                store, "root", **self._platform_candidate(),
                pre_promote_validator=validate_staged_release,
            )
        self.assertEqual(store.files[pointer_id]["data"], old_pointer)

    def test_retained_release_promotion_is_pinned_validated_and_audited(self):
        store = MemoryStore()
        current = publish_release(store, "root", **self._candidate())
        target_candidate = self._candidate()
        target_candidate["release_id"] = "9cfa4638-2df2-4c31-936e-8fcaa787289d"
        target = publish_release(store, "root", **target_candidate)

        seen = []
        result = promote_retained_release(
            store,
            "root",
            target_release_id=current["release_id"],
            expected_current_release_id=target["release_id"],
            pre_promote_validator=lambda _store, pointer: seen.append(pointer["release_id"]),
        )
        self.assertEqual(seen, [current["release_id"]])
        self.assertEqual(result["status"], "retained_release_promoted")
        self.assertEqual(restore_current_release(store, "root")["release_id"], current["release_id"])
        pointer = json.loads(store.files[result["pointer_file_id"]]["data"])
        self.assertEqual(pointer["promotion_audit_file_id"], result["audit_file_id"])
        audit = json.loads(store.files[result["audit_file_id"]]["data"])
        self.assertEqual(audit["event_type"], "retained_release_promotion_requested")
        self.assertEqual(audit["expected_current_release_id"], target["release_id"])

    def test_retained_release_promotion_rejects_wrong_current_pin_without_writes(self):
        store = MemoryStore()
        first = publish_release(store, "root", **self._candidate())
        writes_before = store.writes
        with self.assertRaisesRegex(ReleaseProtocolError, "expected-current"):
            promote_retained_release(
                store,
                "root",
                target_release_id="9cfa4638-2df2-4c31-936e-8fcaa787289d",
                expected_current_release_id="cc2022b2-02ac-4748-a62e-7ce867f9c63e",
                pre_promote_validator=lambda _store, _pointer: None,
            )
        self.assertEqual(store.writes, writes_before)
        self.assertEqual(restore_current_release(store, "root")["release_id"], first["release_id"])

    def test_retained_release_promotion_rejects_protocol_downgrade(self):
        store = MemoryStore()
        legacy = publish_release(store, "root", **self._candidate())
        platform_candidate = self._platform_candidate()
        platform_candidate["release_id"] = "9cfa4638-2df2-4c31-936e-8fcaa787289d"
        platform = publish_release(
            store, "root", **platform_candidate,
            pre_promote_validator=lambda _store, _pointer: None,
        )
        writes_before = store.writes
        with self.assertRaisesRegex(ReleaseProtocolError, "protocol differs"):
            promote_retained_release(
                store,
                "root",
                target_release_id=legacy["release_id"],
                expected_current_release_id=platform["release_id"],
                pre_promote_validator=lambda _store, _pointer: None,
            )
        self.assertEqual(store.writes, writes_before)
        self.assertEqual(
            restore_current_release(store, "root")["release_id"], platform["release_id"]
        )

    def test_retained_release_validation_failure_preserves_current_pointer(self):
        store = MemoryStore()
        target = publish_release(store, "root", **self._candidate())
        current_candidate = self._candidate()
        current_candidate["release_id"] = "9cfa4638-2df2-4c31-936e-8fcaa787289d"
        current = publish_release(store, "root", **current_candidate)
        writes_before = store.writes

        def reject(_store, _pointer):
            raise ValueError("invalid SQL contents")

        with self.assertRaisesRegex(ReleaseProtocolError, "failed validation"):
            promote_retained_release(
                store,
                "root",
                target_release_id=target["release_id"],
                expected_current_release_id=current["release_id"],
                pre_promote_validator=reject,
            )
        self.assertEqual(store.writes, writes_before)
        self.assertEqual(
            restore_current_release(store, "root")["release_id"], current["release_id"]
        )


if __name__ == "__main__":
    unittest.main()
