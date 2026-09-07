"""Content and provenance validation for staged immutable data releases."""
from __future__ import annotations

from datetime import date
from hashlib import sha256
import json
from pathlib import Path
import tempfile
from typing import Any

from ingestion.nbp_state import NBP_SOURCE_IDS, list_successful_response_descriptors
from release_protocol import restore_release


MAX_DATASET_BYTES = 128 * 1024 * 1024
MAX_RELEASE_DATASET_BYTES = 512 * 1024 * 1024
_CATALOG_DATASET_FIELDS = (
    "dataset_id", "table_name", "layer", "model_name", "row_count",
    "min_date", "max_date", "date_column", "columns",
)


class ReleaseValidationError(RuntimeError):
    """A staged release is not safe to expose to consumers."""


def verify_local_dataset(connection: Any, dataset: dict[str, Any], files: list[Path]) -> dict[str, Any]:
    """Open Parquet files and compare their SQL-visible contract with metadata."""
    if not isinstance(dataset, dict) or not isinstance(dataset.get("dataset_id"), str):
        raise ReleaseValidationError("dataset metadata is invalid")
    paths = [Path(path) for path in files]
    if not paths or any(not path.is_file() for path in paths):
        raise ReleaseValidationError(f"{dataset['dataset_id']} has missing local files")
    try:
        relation = connection.read_parquet([str(path) for path in paths])
        relation.create_view("release_validation_dataset", replace=True)
        date_column = dataset.get("date_column", "effectiveDate")
        date_sql = f'min("{_quote_identifier(date_column)}"), max("{_quote_identifier(date_column)}")' if date_column else "NULL, NULL"
        count, first, last = connection.execute(
            f"SELECT count(*), {date_sql} FROM release_validation_dataset"
        ).fetchone()
        columns = [
            {"name": row[0], "type": row[1]}
            for row in connection.execute("DESCRIBE release_validation_dataset").fetchall()
        ]
    except Exception as exc:
        raise ReleaseValidationError(
            f"{dataset['dataset_id']} is not a readable Parquet dataset"
        ) from exc
    observed = {
        "dataset_id": dataset["dataset_id"],
        "rows": count,
        "min_date": first.isoformat() if first is not None else None,
        "max_date": last.isoformat() if last is not None else None,
        "columns": columns,
    }
    expected = {
        "rows": dataset.get("row_count"),
        "min_date": dataset.get("min_date"),
        "max_date": dataset.get("max_date"),
        "columns": dataset.get("columns"),
    }
    if {key: observed[key] for key in expected} != expected:
        raise ReleaseValidationError(
            f"{dataset['dataset_id']} SQL contents differ from release metadata"
        )
    return observed


def validate_staged_release(store: Any, pointer: dict[str, Any]) -> dict[str, Any]:
    """Fully validate staged bytes before a publisher changes the current pointer."""
    manifest = restore_release(store, pointer)
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise ReleaseValidationError("staged release has no datasets")
    declared_total = 0
    for dataset in datasets:
        files = dataset.get("files") if isinstance(dataset, dict) else None
        if not isinstance(files, list) or not files:
            raise ReleaseValidationError("staged release has invalid dataset files")
        for entry in files:
            size = entry.get("size") if isinstance(entry, dict) else None
            if not isinstance(size, int) or isinstance(size, bool) or not 0 < size <= MAX_DATASET_BYTES:
                raise ReleaseValidationError("staged dataset exceeds the validation file limit")
            declared_total += size
    if declared_total > MAX_RELEASE_DATASET_BYTES:
        raise ReleaseValidationError("staged release exceeds the validation working-set limit")

    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - repository runtime pins DuckDB
        raise ReleaseValidationError("DuckDB is required for staged release validation") from exc

    reports: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="zohelo-release-validation-") as temporary:
        directory = Path(temporary)
        with duckdb.connect() as connection:
            for dataset_index, dataset in enumerate(datasets):
                local_files: list[Path] = []
                for file_index, entry in enumerate(dataset["files"]):
                    raw = store.read(entry["id"])
                    if (
                        not isinstance(raw, bytes) or len(raw) != entry["size"]
                        or sha256(raw).hexdigest() != entry["sha256"]
                    ):
                        raise ReleaseValidationError("staged dataset fingerprint changed")
                    path = directory / f"dataset-{dataset_index}-{file_index}.parquet"
                    path.write_bytes(raw)
                    local_files.append(path)
                reports.append(verify_local_dataset(connection, dataset, local_files))

    if manifest.get("format_version") == 2:
        _validate_platform_bindings(store, manifest)
    return {
        "release_id": manifest["release_id"],
        "format_version": manifest["format_version"],
        "datasets": reports,
        "validated_dataset_bytes": declared_total,
    }


def _validate_platform_bindings(store: Any, manifest: dict[str, Any]) -> None:
    artifacts = _artifact_bytes(store, manifest)
    state = _json_object(artifacts["ingestion-state.json"], "ingestion-state.json")
    catalogue = _json_object(artifacts["business-catalog.json"], "business-catalog.json")
    dbt_manifest = _json_object(artifacts["manifest.json"], "manifest.json")
    dbt_catalog = _json_object(artifacts["catalog.json"], "catalog.json")

    metrics_status = catalogue.get("metrics_status")
    if metrics_status == "source_defined":
        if "semantic_manifest.json" not in artifacts or "metric-validation.json" not in artifacts:
            raise ReleaseValidationError("source-defined metrics omit semantic validation artifacts")
        semantic = _json_object(artifacts["semantic_manifest.json"], "semantic_manifest.json")
        validation = _json_object(artifacts["metric-validation.json"], "metric-validation.json")
        declared_metrics = _source_defined_metrics(dbt_manifest)
        if catalogue.get("metrics") != declared_metrics:
            raise ReleaseValidationError("business catalogue metrics differ from source-defined dbt metrics")
        metric_names = {item.get("name") for item in declared_metrics}
        semantic_names = {
            item.get("name") for item in semantic.get("metrics", []) if isinstance(item, dict)
        }
        validated_names = {
            item.get("name") for item in validation.get("metrics", []) if isinstance(item, dict)
        }
        if (
            not metric_names or not metric_names.issubset(semantic_names)
            or validation.get("status") != "verified" or validated_names != metric_names
        ):
            raise ReleaseValidationError("semantic metric artifacts do not verify every source-defined metric")

    if state.get("code_sha") != manifest.get("code_sha"):
        raise ReleaseValidationError("ingestion evidence code SHA differs from release")
    try:
        cutoff = date.fromisoformat(state["cutoff"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ReleaseValidationError("ingestion evidence has no valid cutoff") from exc
    sources = state.get("sources")
    if not isinstance(sources, dict) or set(sources) != set(NBP_SOURCE_IDS):
        raise ReleaseValidationError("ingestion evidence must contain exactly the four NBP sources")
    for source_id, source in sources.items():
        try:
            checked = date.fromisoformat(source["last_checked_through_date"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ReleaseValidationError(f"{source_id} has invalid checked-through evidence") from exc
        if checked < cutoff:
            raise ReleaseValidationError(f"{source_id} is not checked through the release cutoff")

    state_inputs = [_state_input_key(item) for item in list_successful_response_descriptors(state)]
    manifest_inputs = manifest.get("inputs")
    if not isinstance(manifest_inputs, list) or not manifest_inputs:
        raise ReleaseValidationError("platform release input inventory is empty")
    released_inputs = [_manifest_input_key(item) for item in manifest_inputs]
    if len(set(state_inputs)) != len(state_inputs) or len(set(released_inputs)) != len(released_inputs):
        raise ReleaseValidationError("platform release input inventory contains duplicate evidence")
    if set(state_inputs) != set(released_inputs):
        raise ReleaseValidationError("platform release inputs differ from ingestion evidence")
    if {item[0] for item in released_inputs} != set(NBP_SOURCE_IDS):
        raise ReleaseValidationError("platform release inputs omit an NBP source")

    catalogue_sources = catalogue.get("sources")
    if not isinstance(catalogue_sources, list):
        raise ReleaseValidationError("business catalogue has invalid sources")
    by_source = {
        item.get("source_id"): item for item in catalogue_sources if isinstance(item, dict)
    }
    if len(by_source) != len(catalogue_sources) or set(by_source) != set(NBP_SOURCE_IDS):
        raise ReleaseValidationError("business catalogue must contain exactly the four NBP sources")
    for source_id, source in sources.items():
        expected = {
            "checked_through": source.get("last_checked_through_date"),
            "latest_observation_date": source.get("latest_observation_date"),
            "last_successful_ingestion_at": source.get("last_successful_ingestion_at_utc"),
            "last_attempt_at": source.get("last_attempt_at_utc"),
            "raw_response_count": len(source.get("successful_responses", [])),
        }
        if any(by_source[source_id].get(key) != value for key, value in expected.items()):
            raise ReleaseValidationError("business catalogue source status differs from ingestion evidence")

    release_datasets = {item["dataset_id"]: item for item in manifest["datasets"]}
    for source_id, source in sources.items():
        if release_datasets.get(source_id, {}).get("max_date") != source.get("latest_observation_date"):
            raise ReleaseValidationError("released silver freshness differs from ingestion evidence")
    catalogue_datasets = catalogue.get("datasets")
    if not isinstance(catalogue_datasets, list):
        raise ReleaseValidationError("business catalogue has invalid datasets")
    by_catalogue_dataset = {
        item.get("dataset_id"): item for item in catalogue_datasets if isinstance(item, dict)
    }
    if len(by_catalogue_dataset) != len(catalogue_datasets) or set(by_catalogue_dataset) != set(release_datasets):
        raise ReleaseValidationError("business catalogue datasets differ from release datasets")
    for dataset_id, dataset in release_datasets.items():
        expected = {field: dataset.get(field) for field in _CATALOG_DATASET_FIELDS}
        if by_catalogue_dataset[dataset_id] != expected:
            raise ReleaseValidationError("business catalogue dataset metadata differs from release")
        model_id = dataset["model_id"]
        model = dbt_manifest.get("nodes", {}).get(model_id)
        relation = dbt_catalog.get("nodes", {}).get(model_id)
        if not isinstance(model, dict) or not isinstance(relation, dict):
            raise ReleaseValidationError(f"dbt artifacts omit released model {model_id}")
        catalog_metadata = relation.get("metadata")
        if (
            model.get("schema") != dataset["layer"]
            or model.get("alias") != dataset["table_name"]
            or not isinstance(catalog_metadata, dict)
            or catalog_metadata.get("schema") != dataset["layer"]
            or catalog_metadata.get("name") != dataset["table_name"]
        ):
            raise ReleaseValidationError(f"dbt artifact relation differs for {model_id}")


def _artifact_bytes(store: Any, manifest: dict[str, Any]) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    for entry in manifest.get("artifacts", []):
        name = entry.get("name") if isinstance(entry, dict) else None
        if not isinstance(name, str) or name in result:
            raise ReleaseValidationError("release has invalid or duplicate artifacts")
        raw = store.read(entry["id"])
        if (
            not isinstance(raw, bytes) or len(raw) != entry["size"]
            or sha256(raw).hexdigest() != entry["sha256"]
        ):
            raise ReleaseValidationError("release artifact fingerprint changed")
        result[name] = raw
    return result


def _source_defined_metrics(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    metrics = manifest.get("metrics")
    if not isinstance(metrics, dict):
        return []
    result: list[dict[str, Any]] = []
    for unique_id, record in sorted(metrics.items()):
        if not isinstance(record, dict):
            continue
        config = record.get("config")
        meta = config.get("meta") if isinstance(config, dict) else None
        if not meta:
            meta = record.get("meta")
        if not isinstance(meta, dict) or meta.get("definition_status") != "source_defined":
            continue
        copied = {
            field: record[field]
            for field in ("name", "label", "description", "type", "type_params", "filter", "depends_on")
            if field in record
        }
        result.append({"unique_id": unique_id, **copied, "meta": meta})
    return result


def _state_input_key(item: dict[str, Any]) -> tuple[str, str, int, str, int]:
    return _input_key(
        item.get("source_id"), item.get("raw_file_id"), item.get("size_bytes"),
        item.get("response_sha256"), item.get("ingestion_sequence"),
    )


def _manifest_input_key(item: Any) -> tuple[str, str, int, str, int]:
    if not isinstance(item, dict):
        raise ReleaseValidationError("release input entry is invalid")
    return _input_key(
        item.get("source_id"), item.get("id"), item.get("size"),
        item.get("sha256"), item.get("ingestion_sequence"),
    )


def _input_key(source_id: Any, file_id: Any, size: Any, digest: Any, sequence: Any) -> tuple[str, str, int, str, int]:
    if (
        source_id not in NBP_SOURCE_IDS or not isinstance(file_id, str) or not file_id
        or not isinstance(size, int) or isinstance(size, bool) or size <= 0
        or not isinstance(digest, str) or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1
    ):
        raise ReleaseValidationError("platform release input evidence is invalid")
    return source_id, file_id, size, digest, sequence


def _json_object(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseValidationError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ReleaseValidationError(f"{label} must be a JSON object")
    return value


def _quote_identifier(value: Any) -> str:
    if not isinstance(value, str) or not value or '"' in value or "\x00" in value:
        raise ReleaseValidationError("release date column is not a safe SQL identifier")
    return value.replace('"', '""')


__all__ = [
    "ReleaseValidationError", "validate_staged_release", "verify_local_dataset",
]
