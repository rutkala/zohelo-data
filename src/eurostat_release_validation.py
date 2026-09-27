"""Content and provenance validation for staged immutable EUROSTAT modeled releases."""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile
from typing import Any

from release_protocol import restore_release
from release_validation import verify_local_dataset, ReleaseValidationError
from eurostat_platform_contract import EUROSTAT_RELEASE_SCOPE


EUROSTAT_SOURCE_ID = "eurostat"
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


def validate_staged_eurostat_release(store: Any, pointer: dict[str, Any]) -> dict[str, Any]:
    manifest = restore_release(store, pointer)
    if manifest.get("release_scope") != EUROSTAT_RELEASE_SCOPE:
        raise ReleaseValidationError("staged release is not a EUROSTAT modeled release")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ReleaseValidationError("staged EUROSTAT release has no datasets")

    reports: list[dict[str, Any]] = []
    by_dataset = {item["dataset_id"]: item for item in datasets if isinstance(item, dict) and "dataset_id" in item}
    with tempfile.TemporaryDirectory(prefix="zohelo-eurostat-release-validation-") as temporary:
        directory = Path(temporary)
        import duckdb

        with duckdb.connect() as connection:
            for dataset_index, dataset in enumerate(datasets):
                files = dataset.get("files") if isinstance(dataset, dict) else None
                if not isinstance(files, list) or not files:
                    raise ReleaseValidationError("staged EUROSTAT release has invalid dataset files")
                local_files: list[Path] = []
                for file_index, entry in enumerate(files):
                    raw = store.read(entry["id"])
                    if not isinstance(raw, bytes) or len(raw) != entry["size"] or sha256(raw).hexdigest() != entry["sha256"]:
                        raise ReleaseValidationError("staged EUROSTAT dataset fingerprint changed")
                    path = directory / f"dataset-{dataset_index}-{file_index}.parquet"
                    path.write_bytes(raw)
                    local_files.append(path)
                reports.append(verify_local_dataset(connection, dataset, local_files))
            coverage_rows = connection.read_parquet([str(directory / f"dataset-{index}-0.parquet") for index, dataset in enumerate(datasets) if dataset.get("dataset_id") == "mart_eurostat_coverage"]).fetchall() if "mart_eurostat_coverage" in by_dataset else []

    artifacts = _artifact_bytes(store, manifest)
    state = _json_object(artifacts["ingestion-state.json"], "ingestion-state.json")
    catalogue = _json_object(artifacts["business-catalog.json"], "business-catalog.json")
    dbt_manifest = _json_object(artifacts["manifest.json"], "manifest.json")
    dbt_catalog = _json_object(artifacts["catalog.json"], "catalog.json")
    if state.get("source_id") != EUROSTAT_SOURCE_ID or state.get("code_sha") != manifest.get("code_sha"):
        raise ReleaseValidationError("EUROSTAT ingestion evidence has invalid source identity or code SHA")
    catalogue_sources = catalogue.get("sources")
    if not isinstance(catalogue_sources, list) or len(catalogue_sources) != 1 or catalogue_sources[0].get("source_id") != EUROSTAT_SOURCE_ID:
        raise ReleaseValidationError("business catalogue must contain exactly one EUROSTAT source")
    if catalogue.get("code_sha") != manifest.get("code_sha"):
        raise ReleaseValidationError("business catalogue code SHA differs from release")
    by_catalogue_dataset = {item.get("dataset_id"): item for item in catalogue.get("datasets", []) if isinstance(item, dict)}
    if set(by_catalogue_dataset) != set(by_dataset):
        raise ReleaseValidationError("business catalogue datasets differ from EUROSTAT release datasets")
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
        validated_names = {item.get("name") for item in validation.get("metrics", []) if isinstance(item, dict)}
        if (
            validation.get("status") != "verified"
            or not metric_names.issubset(semantic_names)
            or metric_names != validated_names
        ):
            raise ReleaseValidationError("EUROSTAT semantic artifacts do not verify every source-defined metric")
    coverage = state.get("coverage")
    if not isinstance(coverage, dict):
        raise ReleaseValidationError("EUROSTAT ingestion evidence omits coverage summary")
    input_report = _validate_landing_inputs(store, manifest, state)
    source_entry = catalogue_sources[0]
    if source_entry.get("raw_response_count") != state.get("raw_response_count"):
        raise ReleaseValidationError("business catalogue raw response count differs from ingestion evidence")
    if isinstance(source_entry.get("coverage"), dict) and source_entry["coverage"] != coverage:
        raise ReleaseValidationError("business catalogue coverage differs from ingestion evidence")
    if coverage_rows:
        if len(coverage_rows) != 1:
            raise ReleaseValidationError("EUROSTAT modeled coverage mart must contain exactly one snapshot row")
        row = coverage_rows[0]
        expected = {
            "admitted_dataset_total": row[2],
            "admitted_series_total": row[3],
            "modeled_dataset_total": row[4],
            "modeled_series_total": row[5],
            "modeled_response_total": row[6],
            "modeled_cell_total": row[7],
            "modeled_value_total": row[8],
            "admitted_dataset_coverage_ratio": row[9],
            "admitted_series_coverage_ratio": row[10],
            "catalogue_distributions": row[11],
            "validated_current_distributions": row[12],
            "pending_distribution_tasks": row[13],
            "failed_pending_distribution_tasks": row[14],
            "accepted_distributions": row[15],
            "received_raw_bytes": row[16],
            "full_distribution_coverage_ratio": row[17],
            "inventories_current": row[18],
            "catalogue_checked_on": row[19].isoformat() if row[19] is not None else None,
            "latest_observation_date": row[20].isoformat() if row[20] is not None else None,
            "raw_catalogue_complete": row[21],
            "complete_official_catalogue": row[22],
        }
        if any(coverage.get(key) != value for key, value in expected.items()):
            raise ReleaseValidationError("EUROSTAT modeled coverage summary differs from gold mart")
    return {
        "release_id": manifest["release_id"],
        "format_version": manifest["format_version"],
        "datasets": reports,
        "landing_inputs": input_report,
    }


def _validate_landing_inputs(store: Any, manifest: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    inputs = manifest.get("inputs")
    recorded = state.get("landing_inputs")
    snapshot_id = state.get("landing_snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise ReleaseValidationError("EUROSTAT ingestion evidence omits Landing snapshot identity")
    if not isinstance(inputs, list) or not inputs or inputs != recorded:
        raise ReleaseValidationError("release inputs differ from the recorded Landing snapshot descriptors")
    if state.get("pending_publication_count") != 0 or state.get("accepted_response_count") != state.get("published_response_count"):
        raise ReleaseValidationError("recorded EUROSTAT Landing snapshot has a publication backlog")
    seen_ids: set[str] = set()
    total_bytes = 0
    for index, descriptor in enumerate(inputs, start=1):
        if (
            not isinstance(descriptor, dict)
            or descriptor.get("source_id") != EUROSTAT_SOURCE_ID
            or descriptor.get("ingestion_sequence") != index
            or not isinstance(descriptor.get("id"), str)
            or not descriptor["id"]
            or descriptor["id"] in seen_ids
            or type(descriptor.get("size")) is not int
            or descriptor["size"] <= 0
            or not isinstance(descriptor.get("sha256"), str)
            or len(descriptor["sha256"]) != 64
        ):
            raise ReleaseValidationError("release has an invalid EUROSTAT Landing input descriptor")
        seen_ids.add(descriptor["id"])
        raw = store.read(descriptor["id"])
        if (
            not isinstance(raw, bytes)
            or len(raw) != descriptor["size"]
            or sha256(raw).hexdigest() != descriptor["sha256"]
        ):
            raise ReleaseValidationError("EUROSTAT Landing input fingerprint changed before promotion")
        total_bytes += len(raw)
    return {
        "snapshot_id": snapshot_id,
        "fragment_count": len(inputs),
        "input_bytes": total_bytes,
    }


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
