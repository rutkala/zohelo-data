import hashlib
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import dbw_release_validation as validation  # noqa: E402
from dbw_platform_contract import (  # noqa: E402
    DBW_PLATFORM_DATASETS,
    DBW_PLATFORM_DATE_COLUMNS,
)
from layout_resolution import (  # noqa: E402
    RELEASE_SOURCES,
    resolve_source_release_root,
)
from release_validation import ReleaseValidationError  # noqa: E402


class _Store:
    def __init__(self, files=None):
        self.files = dict(files or {})
        self.folders = {}

    def read(self, file_id):
        return self.files[file_id]

    def find(self, name, parent_id):
        return list(self.folders.get((parent_id, name), []))

    def mkdir(self, name, parent_id):
        key = (parent_id, name)
        value = f"{parent_id}-{name}"
        self.folders.setdefault(key, []).append(value)
        return value


def _entry(file_id, raw):
    return {
        "id": file_id,
        "name": f"{file_id}.bin",
        "size": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def _candidate():
    code_sha = "a" * 40
    retained_release = "8c10d951-1b2d-42cd-8378-315cdc14e2fa"
    files = {}
    datasets = []
    catalogue_datasets = []
    manifest_nodes = {}
    catalog_nodes = {}
    observation_rows = 879_999_727
    for index, (dataset_id, (layer, model_id)) in enumerate(
        DBW_PLATFORM_DATASETS.items()
    ):
        raw = f"dataset-{index}".encode()
        file_id = f"dataset-{index}"
        files[file_id] = raw
        table_name = dataset_id.removeprefix("bronze_")
        date_column = DBW_PLATFORM_DATE_COLUMNS[dataset_id]
        columns = [
            {
                "name": date_column or "identity",
                "type": "DATE" if date_column else "BIGINT",
            }
        ]
        rows = (
            observation_rows
            if dataset_id
            in {
                "bronze_dbw_observations",
                "dbw_observations",
                "fact_dbw_observations",
            }
            else 1
        )
        dataset = {
            "dataset_id": dataset_id,
            "table_name": table_name,
            "model_name": model_id.rsplit(".", 1)[-1],
            "model_id": model_id,
            "layer": layer,
            "row_count": rows,
            "date_column": date_column,
            "min_date": "2000-01-01" if date_column else None,
            "max_date": "2025-01-01" if date_column else None,
            "columns": columns,
            "files": [_entry(file_id, raw)],
        }
        datasets.append(dataset)
        catalogue_datasets.append(
            {
                key: dataset[key]
                for key in (
                    "dataset_id",
                    "table_name",
                    "layer",
                    "model_name",
                    "row_count",
                    "min_date",
                    "max_date",
                    "date_column",
                    "columns",
                )
            }
        )
        manifest_nodes[model_id] = {
            "schema": layer,
            "alias": table_name,
        }
        catalog_nodes[model_id] = {
            "metadata": {
                "schema": layer,
                "name": table_name,
            }
        }

    artifacts = {
        "manifest.json": json.dumps({"nodes": manifest_nodes}).encode(),
        "catalog.json": json.dumps({"nodes": catalog_nodes}).encode(),
        "run_results.json": json.dumps({"results": [{"status": "success"}]}).encode(),
        "business-catalog.json": json.dumps(
            {
                "format_version": 1,
                "code_sha": code_sha,
                "sources": [{"source_id": "gus_dbw"}],
                "datasets": catalogue_datasets,
                "metrics": [],
                "metrics_status": "awaiting_business_approval",
                "lineage": {"nodes": [], "edges": []},
            }
        ).encode(),
        "ingestion-state.json": json.dumps(
            {
                "format_version": 1,
                "source_id": "gus_dbw",
                "code_sha": code_sha,
                "release_id": retained_release,
                "status": "published_snapshot",
                "sources": {
                    "gus_dbw": {
                        "status": "published_snapshot",
                        "release_id": retained_release,
                        "coverage_status": "complete_retained_inventory",
                    }
                },
            }
        ).encode(),
    }
    artifact_entries = []
    for index, (name, raw) in enumerate(artifacts.items()):
        file_id = f"artifact-{index}"
        files[file_id] = raw
        value = _entry(file_id, raw)
        value["name"] = name
        artifact_entries.append(value)
    manifest = {
        "format_version": 2,
        "release_id": "modeled-release",
        "release_scope": "dbw_platform",
        "code_sha": code_sha,
        "datasets": datasets,
        "artifacts": artifact_entries,
        "inputs": [
            {
                "source_id": "gus_dbw",
                "release_id": retained_release,
            }
        ],
    }
    return _Store(files), manifest


class DBWReleaseValidationTests(unittest.TestCase):
    def test_complete_multilayer_release_is_read_back_and_validated(self):
        store, manifest = _candidate()
        with patch.object(
            validation, "restore_release", return_value=manifest
        ), patch.object(
            validation,
            "verify_local_dataset",
            side_effect=lambda _connection, dataset, paths: {
                "dataset_id": dataset["dataset_id"],
                "files": len(paths),
            },
        ) as verify:
            report = validation.validate_staged_dbw_release(
                store, {"manifest_file_id": "unused"}
            )

        self.assertEqual(verify.call_count, 11)
        self.assertEqual(report["observation_rows"], 879_999_727)
        self.assertEqual(
            report["retained_release_id"],
            "8c10d951-1b2d-42cd-8378-315cdc14e2fa",
        )
        self.assertEqual(
            {item["dataset_id"] for item in report["datasets"]},
            set(DBW_PLATFORM_DATASETS),
        )

    def test_observation_row_loss_is_rejected(self):
        store, manifest = _candidate()
        next(
            item
            for item in manifest["datasets"]
            if item["dataset_id"] == "fact_dbw_observations"
        )["row_count"] -= 1
        with patch.object(
            validation, "restore_release", return_value=manifest
        ), patch.object(
            validation,
            "verify_local_dataset",
            return_value={"status": "verified"},
        ):
            with self.assertRaisesRegex(
                ReleaseValidationError,
                "Bronze, Silver and Gold observation row counts differ",
            ):
                validation.validate_staged_dbw_release(store, {})

    def test_changed_remote_file_is_rejected_before_sql_acceptance(self):
        store, manifest = _candidate()
        store.files["dataset-0"] = b"changed"
        with patch.object(validation, "restore_release", return_value=manifest):
            with self.assertRaisesRegex(
                ReleaseValidationError, "fingerprint changed"
            ):
                validation.validate_staged_dbw_release(store, {})

    def test_dbw_has_a_canonical_release_root(self):
        self.assertIn("dbw", RELEASE_SOURCES)
        store = _Store()
        release_root, direct = resolve_source_release_root(
            store, "root", "dbw", is_writer=True
        )
        self.assertTrue(direct)
        self.assertEqual(release_root, "root-releases-dbw")
        again, direct_again = resolve_source_release_root(
            store, "root", "dbw", is_writer=True
        )
        self.assertEqual((again, direct_again), (release_root, True))


if __name__ == "__main__":
    unittest.main()
