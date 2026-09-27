"""Content, completeness and provenance validation for staged WDI modeled releases."""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile
from typing import Any

from release_protocol import restore_release
from release_validation import verify_local_dataset, ReleaseValidationError
from wdi_platform_contract import WDI_ARCHIVE_MEMBERS, WDI_PLATFORM_DATASETS, WDI_SOURCE_ID


_CATALOG_FIELDS = (
    "dataset_id", "table_name", "layer", "model_name", "row_count",
    "min_date", "max_date", "date_column", "columns",
)


def validate_staged_wdi_release(store: Any, pointer: dict[str, Any]) -> dict[str, Any]:
    manifest = restore_release(store, pointer)
    if manifest.get("release_scope") != "wdi_platform":
        raise ReleaseValidationError("staged release is not a WDI modeled release")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ReleaseValidationError("staged WDI release has no datasets")

    by_dataset = {
        item["dataset_id"]: item for item in datasets
        if isinstance(item, dict) and isinstance(item.get("dataset_id"), str)
    }
    if len(by_dataset) != len(datasets):
        raise ReleaseValidationError("staged WDI release has duplicate or invalid datasets")
    if set(by_dataset) != set(WDI_PLATFORM_DATASETS):
        raise ReleaseValidationError("staged WDI release does not contain every required modeled dataset")
    reports = []
    coverage_rows = []
    with tempfile.TemporaryDirectory(prefix="zohelo-wdi-release-validation-") as temporary:
        directory = Path(temporary)
        import duckdb
        with duckdb.connect() as connection:
            for dataset_index, dataset in enumerate(datasets):
                entries = dataset.get("files")
                if not isinstance(entries, list) or not entries:
                    raise ReleaseValidationError("staged WDI release has invalid dataset files")
                paths = []
                for file_index, entry in enumerate(entries):
                    raw = store.read(entry["id"])
                    if not isinstance(raw, bytes) or len(raw) != entry["size"] or sha256(raw).hexdigest() != entry["sha256"]:
                        raise ReleaseValidationError("staged WDI dataset fingerprint changed")
                    path = directory / f"dataset-{dataset_index}-{file_index}.parquet"
                    path.write_bytes(raw)
                    paths.append(path)
                reports.append(verify_local_dataset(connection, dataset, paths))
                if dataset["dataset_id"] == "mart_wdi_coverage":
                    coverage_rows = connection.read_parquet([str(path) for path in paths]).fetchall()

    artifacts = _artifact_bytes(store, manifest)
    state = _json_object(artifacts["ingestion-state.json"], "ingestion-state.json")
    catalogue = _json_object(artifacts["business-catalog.json"], "business-catalog.json")
    dbt_manifest = _json_object(artifacts["manifest.json"], "manifest.json")
    dbt_catalog = _json_object(artifacts["catalog.json"], "catalog.json")
    if state.get("source_id") != WDI_SOURCE_ID or state.get("code_sha") != manifest.get("code_sha"):
        raise ReleaseValidationError("WDI ingestion evidence has invalid source identity or code SHA")
    if state.get("coverage_status") != "complete_current_catalogue":
        raise ReleaseValidationError("WDI modeled release is not bound to a complete current archive")
    if state.get("current_distribution_total") != 1 or state.get("archive_member_total") != len(WDI_ARCHIVE_MEMBERS):
        raise ReleaseValidationError("WDI modeled release does not prove the complete six-member archive contract")
    inputs = manifest.get("inputs")
    if not isinstance(inputs, list) or len(inputs) != 1:
        raise ReleaseValidationError("WDI modeled release must bind exactly one current raw archive")
    release_input = inputs[0]
    if (
        release_input.get("source_id") != WDI_SOURCE_ID
        or release_input.get("sha256") != state.get("archive_sha256")
        or release_input.get("size") != state.get("archive_size_bytes")
    ):
        raise ReleaseValidationError("WDI release input differs from archive evidence")

    sources = catalogue.get("sources")
    if not isinstance(sources, list) or len(sources) != 1 or sources[0].get("source_id") != WDI_SOURCE_ID:
        raise ReleaseValidationError("business catalogue must contain exactly one WDI source")
    if catalogue.get("code_sha") != manifest.get("code_sha"):
        raise ReleaseValidationError("business catalogue code SHA differs from release")
    catalogue_datasets = {
        item.get("dataset_id"): item for item in catalogue.get("datasets", []) if isinstance(item, dict)
    }
    if set(catalogue_datasets) != set(by_dataset):
        raise ReleaseValidationError("business catalogue datasets differ from WDI release datasets")
    for dataset_id, dataset in by_dataset.items():
        expected = {field: dataset.get(field) for field in _CATALOG_FIELDS}
        if catalogue_datasets[dataset_id] != expected:
            raise ReleaseValidationError("business catalogue dataset metadata differs from release")
        model_id = dataset["model_id"]
        node = dbt_manifest.get("nodes", {}).get(model_id)
        relation = dbt_catalog.get("nodes", {}).get(model_id)
        metadata = relation.get("metadata") if isinstance(relation, dict) else None
        if not isinstance(node, dict) or not isinstance(relation, dict) or not isinstance(metadata, dict):
            raise ReleaseValidationError(f"dbt artifacts omit released model {model_id}")
        if (
            node.get("schema") != dataset["layer"] or node.get("alias") != dataset["table_name"]
            or metadata.get("schema") != dataset["layer"] or metadata.get("name") != dataset["table_name"]
        ):
            raise ReleaseValidationError(f"dbt artifact relation differs for {model_id}")

    metrics = catalogue.get("metrics", [])
    semantic = _json_object(artifacts["semantic_manifest.json"], "semantic_manifest.json")
    validation = _json_object(artifacts["metric-validation.json"], "metric-validation.json")
    metric_names = {item.get("name") for item in metrics if isinstance(item, dict)}
    semantic_names = {item.get("name") for item in semantic.get("metrics", []) if isinstance(item, dict)}
    validated_names = {item.get("name") for item in validation.get("metrics", []) if isinstance(item, dict)}
    if (
        catalogue.get("metrics_status") != "source_defined" or not metric_names
        or validation.get("status") != "verified" or not metric_names.issubset(semantic_names)
        or metric_names != validated_names
    ):
        raise ReleaseValidationError("WDI semantic artifacts do not verify every coverage metric")

    coverage = state.get("coverage")
    if not isinstance(coverage, dict) or len(coverage_rows) != 1:
        raise ReleaseValidationError("WDI ingestion evidence omits its single coverage row")
    row = coverage_rows[0]
    expected_coverage = {
        "current_archive_total": row[2], "archive_member_total": row[3],
        "source_geography_total": row[4], "source_indicator_total": row[5],
        "source_value_total": row[6], "modeled_geography_total": row[7],
        "modeled_indicator_total": row[8], "modeled_observation_total": row[9],
        "modeled_value_coverage_ratio": row[12],
    }
    if any(coverage.get(key) != value for key, value in expected_coverage.items()):
        raise ReleaseValidationError("WDI modeled coverage summary differs from Gold")
    if coverage["modeled_value_coverage_ratio"] != 1.0:
        raise ReleaseValidationError("WDI modeled release does not cover every populated archive value")
    return {"release_id": manifest["release_id"], "format_version": manifest["format_version"], "datasets": reports}


def _artifact_bytes(store: Any, manifest: dict[str, Any]) -> dict[str, bytes]:
    result = {}
    for entry in manifest.get("artifacts", []):
        name = entry.get("name") if isinstance(entry, dict) else None
        if not isinstance(name, str) or name in result:
            raise ReleaseValidationError("release has invalid or duplicate artifacts")
        raw = store.read(entry["id"])
        if not isinstance(raw, bytes) or len(raw) != entry["size"] or sha256(raw).hexdigest() != entry["sha256"]:
            raise ReleaseValidationError("release artifact fingerprint changed")
        result[name] = raw
    return result


def _json_object(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseValidationError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ReleaseValidationError(f"{label} must be a JSON object")
    return value
