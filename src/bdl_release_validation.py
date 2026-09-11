"""Content and provenance validation for staged immutable BDL modeled releases."""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile
from typing import Any

from release_protocol import restore_release
from release_validation import verify_local_dataset, ReleaseValidationError


BDL_SOURCE_ID = "gus_bdl"
_CATALOG_DATASET_FIELDS = (
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


def validate_staged_bdl_release(store: Any, pointer: dict[str, Any]) -> dict[str, Any]:
    manifest = restore_release(store, pointer)
    if manifest.get("release_scope") != "bdl_platform":
        raise ReleaseValidationError("staged release is not a BDL modeled release")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ReleaseValidationError("staged BDL release has no datasets")

    reports: list[dict[str, Any]] = []
    by_dataset = {item["dataset_id"]: item for item in datasets if isinstance(item, dict) and "dataset_id" in item}
    with tempfile.TemporaryDirectory(prefix="zohelo-bdl-release-validation-") as temporary:
        directory = Path(temporary)
        import duckdb

        with duckdb.connect() as connection:
            for dataset_index, dataset in enumerate(datasets):
                files = dataset.get("files") if isinstance(dataset, dict) else None
                if not isinstance(files, list) or not files:
                    raise ReleaseValidationError("staged BDL release has invalid dataset files")
                local_files: list[Path] = []
                for file_index, entry in enumerate(files):
                    raw = store.read(entry["id"])
                    if not isinstance(raw, bytes) or len(raw) != entry["size"] or sha256(raw).hexdigest() != entry["sha256"]:
                        raise ReleaseValidationError("staged BDL dataset fingerprint changed")
                    path = directory / f"dataset-{dataset_index}-{file_index}.parquet"
                    path.write_bytes(raw)
                    local_files.append(path)
                reports.append(verify_local_dataset(connection, dataset, local_files))
            coverage_rows = connection.read_parquet([str(directory / f"dataset-{index}-0.parquet") for index, dataset in enumerate(datasets) if dataset.get("dataset_id") == "mart_bdl_coverage"]).fetchall() if "mart_bdl_coverage" in by_dataset else []

    artifacts = _artifact_bytes(store, manifest)
    state = _json_object(artifacts["ingestion-state.json"], "ingestion-state.json")
    catalogue = _json_object(artifacts["business-catalog.json"], "business-catalog.json")
    dbt_manifest = _json_object(artifacts["manifest.json"], "manifest.json")
    dbt_catalog = _json_object(artifacts["catalog.json"], "catalog.json")
    if state.get("source_id") != BDL_SOURCE_ID or state.get("code_sha") != manifest.get("code_sha"):
        raise ReleaseValidationError("BDL ingestion evidence has invalid source identity or code SHA")
    catalogue_sources = catalogue.get("sources")
    if not isinstance(catalogue_sources, list) or len(catalogue_sources) != 1 or catalogue_sources[0].get("source_id") != BDL_SOURCE_ID:
        raise ReleaseValidationError("business catalogue must contain exactly one BDL source")
    if catalogue.get("code_sha") != manifest.get("code_sha"):
        raise ReleaseValidationError("business catalogue code SHA differs from release")
    by_catalogue_dataset = {item.get("dataset_id"): item for item in catalogue.get("datasets", []) if isinstance(item, dict)}
    if set(by_catalogue_dataset) != set(by_dataset):
        raise ReleaseValidationError("business catalogue datasets differ from BDL release datasets")
    for dataset_id, dataset in by_dataset.items():
        expected = {field: dataset.get(field) for field in _CATALOG_DATASET_FIELDS}
        if by_catalogue_dataset[dataset_id] != expected:
            raise ReleaseValidationError("business catalogue dataset metadata differs from release")
        model_id = dataset["model_id"]
        node = dbt_manifest.get("nodes", {}).get(model_id)
        relation = dbt_catalog.get("nodes", {}).get(model_id)
        metadata = relation.get("metadata") if isinstance(relation, dict) else None
        if not isinstance(node, dict) or not isinstance(relation, dict) or not isinstance(metadata, dict):
            raise ReleaseValidationError(f"dbt artifacts omit released model {model_id}")
        if node.get("schema") != dataset["layer"] or node.get("alias") != dataset["table_name"] or metadata.get("schema") != dataset["layer"] or metadata.get("name") != dataset["table_name"]:
            raise ReleaseValidationError(f"dbt artifact relation differs for {model_id}")
    metrics = catalogue.get("metrics", [])
    if catalogue.get("metrics_status") == "source_defined":
        semantic = _json_object(artifacts["semantic_manifest.json"], "semantic_manifest.json")
        validation = _json_object(artifacts["metric-validation.json"], "metric-validation.json")
        metric_names = {item.get("name") for item in metrics if isinstance(item, dict)}
        semantic_names = {item.get("name") for item in semantic.get("metrics", []) if isinstance(item, dict)}
        if (
            validation.get("status") != "verified"
            or not metric_names.issubset(semantic_names)
            or metric_names != validated_names
        ):
            raise ReleaseValidationError("BDL semantic artifacts do not verify every source-defined metric")
    coverage = state.get("coverage")
    if not isinstance(coverage, dict):
        raise ReleaseValidationError("BDL ingestion evidence omits coverage summary")
    source_entry = catalogue_sources[0]
    if source_entry.get("raw_response_count") != state.get("raw_response_count"):
        raise ReleaseValidationError("business catalogue raw response count differs from ingestion evidence")
    if isinstance(source_entry.get("coverage"), dict) and source_entry["coverage"] != coverage:
        raise ReleaseValidationError("business catalogue coverage differs from ingestion evidence")
    if coverage_rows:
        if len(coverage_rows) != 1:
            raise ReleaseValidationError("BDL modeled coverage mart must contain exactly one snapshot row")
        row = coverage_rows[0]
        expected = {
            "source_universe_total": row[2],
            "discovered_total": row[3],
            "landed_accepted_total": row[4],
            "modeled_total": row[5],
            "modeled_observation_total": row[6],
        }
        if any(coverage.get(key) != value for key, value in expected.items()):
            raise ReleaseValidationError("BDL modeled coverage summary differs from gold mart")
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
