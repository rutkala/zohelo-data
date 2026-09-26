"""Content and provenance validation for staged immutable DBW modeled releases."""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile
from typing import Any

from dbw_platform_contract import DBW_PLATFORM_DATASETS
from dbw_retained_source import (
    RETAINED_COVERAGE_STATUS,
    RETAINED_LINEAGE_STATUS,
    RETAINED_SOURCE_ID,
    validate_retained_source,
)
from release_protocol import restore_release
from release_validation import ReleaseValidationError, verify_local_dataset


DBW_SOURCE_ID = "gus_dbw"
_CATALOG_FIELDS = (
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
_OBSERVATION_DATASETS = (
    "bronze_dbw_observations",
    "dbw_observations",
    "fact_dbw_observations",
)
_RETAINED_ROW_DATASETS = {
    "bronze_dbw_dictionaries": "dictionaries",
    "dbw_dictionaries": "dictionaries",
    "bronze_dbw_indicators": "taxonomy",
    "dbw_indicators": "taxonomy",
    "dim_dbw_indicator": "taxonomy",
    "bronze_dbw_metadata": "metadata",
    "dbw_metadata": "metadata",
    "bronze_dbw_observations": "observations",
    "dbw_observations": "observations",
    "fact_dbw_observations": "observations",
}


def validate_staged_dbw_release(
    store: Any,
    pointer: dict[str, Any],
    *,
    retained_pointer_file_id: str,
) -> dict[str, Any]:
    """Read back and query every staged DBW file before pointer promotion."""
    manifest = restore_release(store, pointer)
    if manifest.get("release_scope") != "dbw_platform":
        raise ReleaseValidationError("staged release is not a DBW modeled release")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ReleaseValidationError("staged DBW release has no datasets")
    by_dataset = {
        item["dataset_id"]: item
        for item in datasets
        if isinstance(item, dict) and isinstance(item.get("dataset_id"), str)
    }
    if len(by_dataset) != len(datasets):
        raise ReleaseValidationError(
            "staged DBW release has duplicate or invalid datasets"
        )
    if set(by_dataset) != set(DBW_PLATFORM_DATASETS):
        raise ReleaseValidationError(
            "staged DBW release does not contain every required modeled dataset"
        )

    reports: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(
        prefix="zohelo-dbw-release-validation-"
    ) as temporary:
        directory = Path(temporary)
        import duckdb

        with duckdb.connect() as connection:
            for dataset_index, dataset in enumerate(datasets):
                entries = dataset.get("files")
                if not isinstance(entries, list) or not entries:
                    raise ReleaseValidationError(
                        "staged DBW release has invalid dataset files"
                    )
                paths: list[Path] = []
                try:
                    for file_index, entry in enumerate(entries):
                        raw = store.read(entry["id"])
                        if (
                            not isinstance(raw, bytes)
                            or len(raw) != entry["size"]
                            or sha256(raw).hexdigest() != entry["sha256"]
                        ):
                            raise ReleaseValidationError(
                                "staged DBW dataset fingerprint changed"
                            )
                        path = directory / (
                            f"dataset-{dataset_index}-{file_index}.parquet"
                        )
                        path.write_bytes(raw)
                        paths.append(path)
                    reports.append(
                        verify_local_dataset(connection, dataset, paths)
                    )
                finally:
                    for path in paths:
                        path.unlink(missing_ok=True)

    artifacts = _artifact_bytes(store, manifest)
    state = _json_object(
        artifacts["ingestion-state.json"], "ingestion-state.json"
    )
    catalogue = _json_object(
        artifacts["business-catalog.json"], "business-catalog.json"
    )
    dbt_manifest = _json_object(artifacts["manifest.json"], "manifest.json")
    dbt_catalog = _json_object(artifacts["catalog.json"], "catalog.json")

    code_sha = manifest.get("code_sha")
    if state.get("source_id") != DBW_SOURCE_ID or state.get("code_sha") != code_sha:
        raise ReleaseValidationError(
            "DBW ingestion evidence has invalid source identity or code SHA"
        )
    inputs = manifest.get("inputs")
    if not isinstance(inputs, list) or len(inputs) != 1:
        raise ReleaseValidationError(
            "DBW modeled release must bind exactly one retained Bronze source"
        )
    try:
        retained_manifest = validate_retained_source(
            store,
            inputs[0],
            expected_pointer_file_id=retained_pointer_file_id,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ReleaseValidationError(str(exc)) from exc
    retained_release_id = retained_manifest["snapshot_id"]
    retained_inventory_sha256 = retained_manifest["inventory_sha256"]
    if state.get("release_id") != retained_release_id:
        raise ReleaseValidationError(
            "DBW ingestion evidence differs from the retained snapshot"
        )
    source_states = state.get("sources")
    source_state = (
        source_states.get(DBW_SOURCE_ID)
        if isinstance(source_states, dict)
        else None
    )
    if (
        not isinstance(source_state, dict)
        or source_state.get("release_id") != retained_release_id
        or source_state.get("native_inventory_sha256")
        != retained_inventory_sha256
        or source_state.get("retained_manifest_file_id")
        != inputs[0]["manifest_file_id"]
        or source_state.get("retained_manifest_sha256")
        != inputs[0]["manifest_sha256"]
        or source_state.get("coverage_status")
        != RETAINED_COVERAGE_STATUS
        or source_state.get("lineage_status")
        != RETAINED_LINEAGE_STATUS
    ):
        raise ReleaseValidationError(
            "DBW evidence differs from the authoritative retained source"
        )

    sources = catalogue.get("sources")
    if (
        not isinstance(sources, list)
        or len(sources) != 1
        or not isinstance(sources[0], dict)
        or sources[0].get("source_id") != DBW_SOURCE_ID
        or sources[0].get("retained_snapshot_id")
        != retained_release_id
        or sources[0].get("coverage_status")
        != RETAINED_COVERAGE_STATUS
        or sources[0].get("lineage_status")
        != RETAINED_LINEAGE_STATUS
    ):
        raise ReleaseValidationError(
            "business catalogue does not preserve the retained DBW source"
        )
    if catalogue.get("code_sha") != code_sha:
        raise ReleaseValidationError(
            "business catalogue code SHA differs from release"
        )
    catalogue_datasets = {
        item.get("dataset_id"): item
        for item in catalogue.get("datasets", [])
        if isinstance(item, dict)
    }
    if set(catalogue_datasets) != set(by_dataset):
        raise ReleaseValidationError(
            "business catalogue datasets differ from DBW release datasets"
        )
    for dataset_id, dataset in by_dataset.items():
        expected = {field: dataset.get(field) for field in _CATALOG_FIELDS}
        if catalogue_datasets[dataset_id] != expected:
            raise ReleaseValidationError(
                "business catalogue dataset metadata differs from release"
            )
        model_id = dataset["model_id"]
        node = dbt_manifest.get("nodes", {}).get(model_id)
        relation = dbt_catalog.get("nodes", {}).get(model_id)
        metadata = relation.get("metadata") if isinstance(relation, dict) else None
        if (
            not isinstance(node, dict)
            or not isinstance(relation, dict)
            or not isinstance(metadata, dict)
        ):
            raise ReleaseValidationError(
                f"dbt artifacts omit released model {model_id}"
            )
        if (
            node.get("schema") != dataset["layer"]
            or node.get("alias") != dataset["table_name"]
            or metadata.get("schema") != dataset["layer"]
            or metadata.get("name") != dataset["table_name"]
        ):
            raise ReleaseValidationError(
                f"dbt artifact relation differs for {model_id}"
            )

    observation_rows = {
        dataset_id: by_dataset[dataset_id]["row_count"]
        for dataset_id in _OBSERVATION_DATASETS
    }
    if (
        any(
            not isinstance(rows, int)
            or isinstance(rows, bool)
            or rows <= 0
            for rows in observation_rows.values()
        )
        or len(set(observation_rows.values())) != 1
    ):
        raise ReleaseValidationError(
            "DBW Bronze, Silver and Gold observation row counts differ"
        )
    retained_dataset_rows = inputs[0]["dataset_rows"]
    for dataset_id, retained_dataset in _RETAINED_ROW_DATASETS.items():
        if (
            by_dataset[dataset_id]["row_count"]
            != retained_dataset_rows[retained_dataset]
        ):
            raise ReleaseValidationError(
                f"DBW modeled {dataset_id} rows differ from retained "
                f"{retained_dataset}"
            )
    metrics = catalogue.get("metrics")
    if (
        catalogue.get("metrics_status") != "awaiting_business_approval"
        or metrics != []
    ):
        raise ReleaseValidationError(
            "DBW release must not claim unapproved semantic metrics"
        )
    return {
        "release_id": manifest["release_id"],
        "format_version": manifest["format_version"],
        "retained_release_id": retained_release_id,
        "observation_rows": next(iter(observation_rows.values())),
        "datasets": reports,
    }


def _artifact_bytes(
    store: Any, manifest: dict[str, Any]
) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    for entry in manifest.get("artifacts", []):
        name = entry.get("name") if isinstance(entry, dict) else None
        if not isinstance(name, str) or name in result:
            raise ReleaseValidationError(
                "release has invalid or duplicate artifacts"
            )
        raw = store.read(entry["id"])
        if (
            not isinstance(raw, bytes)
            or len(raw) != entry["size"]
            or sha256(raw).hexdigest() != entry["sha256"]
        ):
            raise ReleaseValidationError(
                "release artifact fingerprint changed"
            )
        result[name] = raw
    for required in (
        "manifest.json",
        "catalog.json",
        "run_results.json",
        "business-catalog.json",
        "ingestion-state.json",
    ):
        if required not in result:
            raise ReleaseValidationError(
                f"release is missing required artifact {required}"
            )
    return result


def _json_object(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseValidationError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ReleaseValidationError(f"{label} must be a JSON object")
    return value
