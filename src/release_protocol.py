"""Immutable NBP silver release publication protocol.

The publisher is storage-backend agnostic.  A Drive adapter only needs the small
``find/create/read/replace/mkdir`` interface used here; this module performs no
Google API calls itself.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any, Callable, Protocol
from uuid import UUID, uuid4

from bdl_platform_contract import (
    BDL_PLATFORM_DATASETS,
    BDL_PLATFORM_DATE_COLUMNS,
)


REQUIRED_DATASETS = frozenset(
    {
        "nbp_exchange_rates_table_a",
        "nbp_exchange_rates_table_b",
        "nbp_exchange_rates_table_c",
        "nbp_gold_prices",
    }
)
EXPECTED_STAGING_MODELS = {
    "nbp_exchange_rates_table_a": "model.zohelo_data.stg_nbp_table_a",
    "nbp_exchange_rates_table_b": "model.zohelo_data.stg_nbp_table_b",
    "nbp_exchange_rates_table_c": "model.zohelo_data.stg_nbp_table_c",
    "nbp_gold_prices": "model.zohelo_data.stg_nbp_gold_prices",
}
REQUIRED_ARTIFACTS = frozenset({"manifest.json", "catalog.json", "run_results.json"})
PLATFORM_REQUIRED_ARTIFACTS = REQUIRED_ARTIFACTS | frozenset({"business-catalog.json", "ingestion-state.json"})
PLATFORM_DATASETS = {
    "bronze_nbp_exchange_rates_table_a": ("02_bronze", "model.zohelo_data.br_nbp_table_a"),
    "bronze_nbp_exchange_rates_table_b": ("02_bronze", "model.zohelo_data.br_nbp_table_b"),
    "bronze_nbp_exchange_rates_table_c": ("02_bronze", "model.zohelo_data.br_nbp_table_c"),
    "bronze_nbp_gold_prices": ("02_bronze", "model.zohelo_data.br_nbp_gold_prices"),
    "nbp_exchange_rates_table_a": ("03_silver", "model.zohelo_data.stg_nbp_table_a"),
    "nbp_exchange_rates_table_b": ("03_silver", "model.zohelo_data.stg_nbp_table_b"),
    "nbp_exchange_rates_table_c": ("03_silver", "model.zohelo_data.stg_nbp_table_c"),
    "nbp_gold_prices": ("03_silver", "model.zohelo_data.stg_nbp_gold_prices"),
    "nbp_change_events": ("03_silver", "model.zohelo_data.nbp_change_events"),
    "fact_fx_quotes": ("04_gold", "model.zohelo_data.fact_fx_quotes"),
    "fact_gold_prices": ("04_gold", "model.zohelo_data.fact_gold_prices"),
    "dim_date": ("04_gold", "model.zohelo_data.dim_date"),
    "dim_currency": ("04_gold", "model.zohelo_data.dim_currency"),
    "dim_source_table": ("04_gold", "model.zohelo_data.dim_source_table"),
    "dim_commodity": ("04_gold", "model.zohelo_data.dim_commodity"),
}
PLATFORM_DATE_COLUMNS = {
    "bronze_nbp_exchange_rates_table_a": "effective_date",
    "bronze_nbp_exchange_rates_table_b": "effective_date",
    "bronze_nbp_exchange_rates_table_c": "effective_date",
    "bronze_nbp_gold_prices": "effective_date",
    "nbp_exchange_rates_table_a": "effectiveDate",
    "nbp_exchange_rates_table_b": "effectiveDate",
    "nbp_exchange_rates_table_c": "effectiveDate",
    "nbp_gold_prices": "effectiveDate",
    "nbp_change_events": "effective_date",
    "fact_fx_quotes": "effective_date",
    "fact_gold_prices": "effective_date",
    "dim_date": "date_key",
    "dim_currency": None,
    "dim_source_table": None,
    "dim_commodity": None,
}
PLATFORM_RELEASES = {
    "nbp_platform": {
        "label": "NBP platform",
        "datasets": PLATFORM_DATASETS,
        "date_columns": PLATFORM_DATE_COLUMNS,
        "required_artifacts": PLATFORM_REQUIRED_ARTIFACTS,
        "allow_zero_rows": frozenset({"nbp_change_events"}),
    },
    "bdl_platform": {
        "label": "BDL platform",
        "datasets": BDL_PLATFORM_DATASETS,
        "date_columns": BDL_PLATFORM_DATE_COLUMNS,
        "required_artifacts": PLATFORM_REQUIRED_ARTIFACTS,
        "allow_zero_rows": frozenset(),
    },
}
_ALLOWED_RESULT_STATUSES = frozenset({"success", "pass"})
_MAX_ITEMS = 2_048
_MAX_METADATA_BYTES = 1_000_000
_MAX_FILE_NAME = 180
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,255}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_CODE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,199}$")


class ReleaseStore(Protocol):
    def find(self, name: str, parent_id: str) -> list[str]: ...

    def create(self, name: str, data: bytes, parent_id: str) -> str: ...

    def read(self, file_id: str) -> bytes: ...

    def replace(self, file_id: str, data: bytes) -> None: ...

    def mkdir(self, name: str, parent_id: str) -> str: ...


class ReleaseProtocolError(RuntimeError):
    """The candidate is invalid or could not be safely made current."""


def publish_release(
    store: ReleaseStore,
    root_id: str,
    *,
    datasets: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
    inputs: list[dict[str, Any]],
    code_sha: str,
    measurements: dict[str, Any],
    release_id: str | None = None,
    release_scope: str = "nbp_silver",
    pre_promote_validator: Callable[[ReleaseStore, dict[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    """Publish a validated, immutable NBP silver release.

    The caller must serialize writers (for example with GitHub Actions
    concurrency).  Drive has no compare-and-swap, so this function detects a
    pointer change before switching it but cannot make competing writers
    transactional.
    """
    root_id = _require_id(root_id, "root_id")
    candidate = _validate_candidate(
        datasets=datasets,
        artifacts=artifacts,
        inputs=inputs,
        code_sha=code_sha,
        measurements=measurements,
        release_id=release_id,
        release_scope=release_scope,
    )
    if candidate["format_version"] == 2 and pre_promote_validator is None:
        raise ReleaseProtocolError(
            "NBP platform publication requires staged content validation before promotion"
        )

    # A valid old pointer is read before *any* candidate remote write.
    previous = _read_pointer(store, root_id)
    if previous is not None:
        # Do not silently replace a damaged "current" state: preserve an
        # operator-visible failure until the existing pointer is repaired.
        previous_manifest = restore_release(store, previous["value"])
        if previous_manifest["format_version"] == 2 and candidate["release_scope"] == "nbp_silver":
            raise ReleaseProtocolError("A legacy silver candidate cannot replace a platform release")

    releases_ids = store.find("releases", root_id)
    if len(releases_ids) > 1:
        raise ReleaseProtocolError("ambiguous releases folders under publication root")
    if releases_ids:
        releases_id = _require_id(releases_ids[0], "releases folder id")
    else:
        releases_id = _require_id(store.mkdir("releases", root_id), "releases folder id")

    if store.find(candidate["release_id"], releases_id):
        raise ReleaseProtocolError(f"release folder already exists: {candidate['release_id']}")
    release_folder_id = _require_id(
        store.mkdir(candidate["release_id"], releases_id), "release folder id"
    )

    published_datasets: list[dict[str, Any]] = []
    for dataset in candidate["datasets"]:
        file_id = _upload_verified(
            store, release_folder_id, dataset["upload_name"], dataset["data"]
        )
        metadata = dict(dataset["metadata"])
        metadata["files"] = [
            {
                "id": file_id,
                "name": dataset["upload_name"],
                "size": len(dataset["data"]),
                "sha256": _sha256(dataset["data"]),
            }
        ]
        published_datasets.append(metadata)

    published_artifacts: list[dict[str, Any]] = []
    for artifact in candidate["artifacts"]:
        file_id = _upload_verified(store, release_folder_id, artifact["name"], artifact["data"])
        published_artifacts.append(
            {
                "id": file_id,
                "name": artifact["name"],
                "size": len(artifact["data"]),
                "sha256": _sha256(artifact["data"]),
            }
        )

    manifest = {
        "format_version": candidate["format_version"],
        "release_id": candidate["release_id"],
        "release_scope": candidate["release_scope"],
        "status": "validated",
        "created_at_utc": _utc_now(),
        "code_sha": candidate["code_sha"],
        "datasets": published_datasets,
        "artifacts": published_artifacts,
        "inputs": candidate["inputs"],
        "measurements": candidate["measurements"],
        "tests": {"passed": True},
    }
    manifest_bytes = _json_bytes(manifest)
    manifest_file_id = _upload_verified(store, release_folder_id, "release.json", manifest_bytes)
    manifest_sha = _sha256(manifest_bytes)

    staged_pointer: dict[str, Any] = {
        "format_version": 1,
        "release_id": candidate["release_id"],
        "manifest_file_id": manifest_file_id,
        "manifest_sha256": manifest_sha,
        "updated_at_utc": _utc_now(),
    }
    if previous is not None:
        staged_pointer["previous_manifest_file_id"] = previous["value"]["manifest_file_id"]
    if pre_promote_validator is not None:
        try:
            pre_promote_validator(store, dict(staged_pointer))
        except Exception as exc:
            raise ReleaseProtocolError(
                "staged release failed pre-promotion validation; current release retained"
            ) from exc

    # Check again immediately before the only mutable operation.
    current = _read_pointer(store, root_id)
    current_raw = current["raw"] if current is not None else None
    previous_raw = previous["raw"] if previous is not None else None
    if current_raw != previous_raw:
        raise ReleaseProtocolError("current-release pointer changed during candidate upload")

    pointer = staged_pointer
    # Record the promotion instant after potentially long candidate validation.
    pointer["updated_at_utc"] = _utc_now()
    pointer_bytes = _json_bytes(pointer)
    pointer_file_id = _write_pointer(store, root_id, previous, pointer_bytes)

    return {
        "release_id": candidate["release_id"],
        "release_folder_id": release_folder_id,
        "manifest_file_id": manifest_file_id,
        "manifest_sha256": manifest_sha,
        "pointer_file_id": pointer_file_id,
        "manifest": manifest,
    }


def promote_retained_release(
    store: ReleaseStore,
    root_id: str,
    *,
    target_release_id: str,
    expected_current_release_id: str,
    pre_promote_validator: Callable[[ReleaseStore, dict[str, Any]], Any],
) -> dict[str, Any]:
    """Promote one retained, fully verified release behind an exact current pin.

    Callers must serialize this operation with every production publisher. Drive
    does not provide compare-and-swap, so the exact-current check and final
    readback detect drift but do not make competing external writers safe.
    """
    root_id = _require_id(root_id, "root_id")
    target_release_id = _parse_release_id(target_release_id)
    expected_current_release_id = _parse_release_id(expected_current_release_id)
    if not callable(pre_promote_validator):
        raise ReleaseProtocolError("retained release promotion requires a content validator")

    current = _read_pointer(store, root_id)
    if current is None:
        raise ReleaseProtocolError("no current release exists to pin for retained promotion")
    if current["value"]["release_id"] != expected_current_release_id:
        raise ReleaseProtocolError("current release does not match expected-current release ID")
    if target_release_id == expected_current_release_id:
        raise ReleaseProtocolError("target release is already current")
    # Recovery must remain possible when a current dataset or artifact is lost.
    # Pin and verify the current manifest itself, then fully validate the target.
    current_manifest = _read_release_manifest(store, current["value"])

    releases_ids = store.find("releases", root_id)
    if len(releases_ids) != 1:
        raise ReleaseProtocolError("releases folder is missing or ambiguous")
    releases_id = _require_id(releases_ids[0], "releases folder id")
    target_folders = store.find(target_release_id, releases_id)
    if len(target_folders) != 1:
        raise ReleaseProtocolError("target retained release folder is missing or ambiguous")
    target_folder_id = _require_id(target_folders[0], "target release folder id")
    manifest_ids = store.find("release.json", target_folder_id)
    if len(manifest_ids) != 1:
        raise ReleaseProtocolError("target retained release manifest is missing or ambiguous")
    manifest_file_id = _require_id(manifest_ids[0], "target release manifest id")
    manifest_bytes = _read_bytes(store, manifest_file_id, "target retained release manifest")
    manifest = _parse_json_object(manifest_bytes, "target retained release manifest")
    if manifest.get("release_id") != target_release_id:
        raise ReleaseProtocolError("target folder and retained release manifest IDs differ")
    if manifest.get("format_version") != current_manifest.get("format_version"):
        raise ReleaseProtocolError(
            "target retained release protocol differs from the current release"
        )

    target_pointer: dict[str, Any] = {
        "format_version": 1,
        "release_id": target_release_id,
        "manifest_file_id": manifest_file_id,
        "manifest_sha256": _sha256(manifest_bytes),
        "updated_at_utc": _utc_now(),
        "previous_manifest_file_id": current["value"]["manifest_file_id"],
    }
    try:
        pre_promote_validator(store, dict(target_pointer))
    except Exception as exc:
        raise ReleaseProtocolError(
            "target retained release failed validation; current release retained"
        ) from exc

    audit_id = str(uuid4())
    audit = {
        "format_version": 1,
        "event_id": audit_id,
        "event_type": "retained_release_promotion_requested",
        "created_at_utc": _utc_now(),
        "expected_current_release_id": expected_current_release_id,
        "expected_current_manifest_file_id": current["value"]["manifest_file_id"],
        "target_release_id": target_release_id,
        "target_manifest_file_id": manifest_file_id,
        "target_manifest_sha256": target_pointer["manifest_sha256"],
    }
    audit_folders = store.find("promotion-audits", root_id)
    if len(audit_folders) > 1:
        raise ReleaseProtocolError("promotion-audits folder is ambiguous")
    audit_folder_id = (
        _require_id(audit_folders[0], "promotion-audits folder id")
        if audit_folders
        else _require_id(store.mkdir("promotion-audits", root_id), "promotion-audits folder id")
    )
    audit_file_id = _upload_verified(
        store, audit_folder_id, f"{audit_id}.json", _json_bytes(audit)
    )
    target_pointer["promotion_audit_file_id"] = audit_file_id

    observed = _read_pointer(store, root_id)
    if observed is None or observed["raw"] != current["raw"]:
        raise ReleaseProtocolError("current-release pointer changed during retained release validation")
    pointer_file_id = _write_pointer(store, root_id, current, _json_bytes(target_pointer))
    return {
        "status": "retained_release_promoted",
        "target_release_id": target_release_id,
        "previous_release_id": expected_current_release_id,
        "pointer_file_id": pointer_file_id,
        "audit_file_id": audit_file_id,
        "manifest": manifest,
    }


def restore_release(store: ReleaseStore, pointer: str | dict[str, Any]) -> dict[str, Any]:
    """Verify and return a release manifest from a pointer ID or pointer object.

    ``pointer`` is either the stable ``current-release.json`` file ID or the
    decoded JSON object stored in that file.  The returned value is the decoded
    release manifest only after all listed data and dbt artifact checksums have
    been read and verified.
    """
    manifest = _read_release_manifest(store, pointer)
    if manifest["format_version"] == 1:
        _verify_manifest_files(store, manifest)
    else:
        _verify_platform_manifest_files(store, manifest)
    return manifest


def _read_release_manifest(store: ReleaseStore, pointer: str | dict[str, Any]) -> dict[str, Any]:
    """Verify pinned manifest identity without requiring its data files intact."""
    if isinstance(pointer, str):
        value = _parse_json_object(_read_bytes(store, _require_id(pointer, "pointer id"), "current-release pointer"), "current-release pointer")
    elif isinstance(pointer, dict):
        value = pointer
    else:
        raise ReleaseProtocolError("pointer must be a pointer file ID or JSON object")
    _validate_pointer(value)
    manifest_bytes = _read_bytes(store, value["manifest_file_id"], "release manifest")
    if _sha256(manifest_bytes) != value["manifest_sha256"]:
        raise ReleaseProtocolError("release manifest checksum does not match current pointer")
    manifest = _parse_json_object(manifest_bytes, "release manifest")
    format_version = manifest.get("format_version")
    if format_version not in {1, 2} or manifest.get("status") != "validated":
        raise ReleaseProtocolError("release manifest is not a validated supported release")
    expected_scope = "nbp_silver" if format_version == 1 else manifest.get("release_scope")
    if format_version == 2 and expected_scope not in PLATFORM_RELEASES:
        raise ReleaseProtocolError("release manifest has an unexpected scope")
    if manifest.get("release_scope") != expected_scope:
        raise ReleaseProtocolError("release manifest has an unexpected scope")
    if not isinstance(manifest.get("code_sha"), str) or not _CODE_SHA_RE.fullmatch(manifest["code_sha"]):
        raise ReleaseProtocolError("release manifest has an invalid code SHA")
    if manifest.get("tests") != {"passed": True}:
        raise ReleaseProtocolError("release manifest does not record passed tests")
    if manifest.get("release_id") != value["release_id"]:
        raise ReleaseProtocolError("release ID does not match current pointer")
    return manifest


def restore_current_release(store: ReleaseStore, root_id: str) -> dict[str, Any]:
    """Resolve the stable pointer under ``root_id`` and restore its release."""
    root_id = _require_id(root_id, "root_id")
    pointer = _read_pointer(store, root_id)
    if pointer is None:
        raise ReleaseProtocolError("no current-release pointer exists")
    return restore_release(store, pointer["value"])


def read_current_release_manifest(store: ReleaseStore, root_id: str) -> dict[str, Any]:
    """Return the checksum-pinned current manifest without reading every dataset.

    Consumers that need only one large-release dataset can validate that selected
    file against the returned manifest.  Full restore remains the publication and
    operational acceptance path.
    """
    root_id = _require_id(root_id, "root_id")
    pointer = _read_pointer(store, root_id)
    if pointer is None:
        raise ReleaseProtocolError("no current-release pointer exists")
    return _read_release_manifest(store, pointer["value"])


# Useful, short name for adapters/consumers.
read_current_release = restore_current_release


def _validate_candidate(**kwargs: Any) -> dict[str, Any]:
    scope = kwargs.get("release_scope")
    if scope == "nbp_silver":
        candidate = _validate_silver_candidate(**kwargs)
        candidate["release_scope"] = "nbp_silver"
        candidate["format_version"] = 1
        return candidate
    if scope in PLATFORM_RELEASES:
        return _validate_platform_candidate(**kwargs)
    raise ReleaseProtocolError("release_scope must be 'nbp_silver', 'nbp_platform', or 'bdl_platform'")


def _validate_silver_candidate(**kwargs: Any) -> dict[str, Any]:
    datasets = kwargs["datasets"]
    artifacts = kwargs["artifacts"]
    inputs = kwargs["inputs"]
    measurements = kwargs["measurements"]
    code_sha = kwargs["code_sha"]
    supplied_release_id = kwargs["release_id"]
    if not isinstance(datasets, list) or not isinstance(artifacts, list) or not isinstance(inputs, list):
        raise ReleaseProtocolError("datasets, artifacts, and inputs must be lists")
    if len(datasets) > _MAX_ITEMS or len(artifacts) > _MAX_ITEMS or len(inputs) > _MAX_ITEMS:
        raise ReleaseProtocolError("candidate list exceeds publication limit")
    if not isinstance(code_sha, str) or not _CODE_SHA_RE.fullmatch(code_sha):
        raise ReleaseProtocolError("code_sha must be a 40-character lowercase hexadecimal Git SHA")
    if not isinstance(measurements, dict):
        raise ReleaseProtocolError("measurements must be an object")
    _bounded_json(inputs, "inputs")
    _bounded_json(measurements, "measurements")
    release_id = str(uuid4()) if supplied_release_id is None else _parse_release_id(supplied_release_id)

    by_id: dict[str, dict[str, Any]] = {}
    for raw in datasets:
        if not isinstance(raw, dict):
            raise ReleaseProtocolError("each dataset must be an object")
        dataset_id = raw.get("dataset_id")
        if not isinstance(dataset_id, str) or dataset_id in by_id:
            raise ReleaseProtocolError("dataset IDs must be unique strings")
        by_id[dataset_id] = raw
    if set(by_id) != REQUIRED_DATASETS:
        missing = sorted(REQUIRED_DATASETS - set(by_id))
        extra = sorted(set(by_id) - REQUIRED_DATASETS)
        raise ReleaseProtocolError(f"NBP silver release must contain exactly all four datasets; missing={missing}, extra={extra}")

    validated_datasets: list[dict[str, Any]] = []
    names: set[str] = {"release.json"}
    for dataset_id in sorted(REQUIRED_DATASETS):
        raw = by_id[dataset_id]
        if raw.get("layer") != "03_silver":
            raise ReleaseProtocolError(f"{dataset_id} must be published from layer 03_silver")
        table_name = raw.get("table_name")
        if not isinstance(table_name, str) or not table_name.strip() or len(table_name) > 200:
            raise ReleaseProtocolError(f"{dataset_id} has an invalid table_name")
        row_count = raw.get("row_count")
        if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count <= 0:
            raise ReleaseProtocolError(f"{dataset_id} must have a positive row_count")
        date_values: dict[str, date] = {}
        for field in ("min_date", "max_date"):
            if not isinstance(raw.get(field), str) or not raw[field].strip() or len(raw[field]) > 64:
                raise ReleaseProtocolError(f"{dataset_id} has invalid {field}")
            try:
                date_values[field] = date.fromisoformat(raw[field])
            except ValueError as exc:
                raise ReleaseProtocolError(f"{dataset_id} has invalid {field}") from exc
        if date_values["min_date"] > date_values["max_date"]:
            raise ReleaseProtocolError(f"{dataset_id} min_date is after max_date")
        columns = raw.get("columns")
        if not isinstance(columns, list) or not columns or len(columns) > _MAX_ITEMS:
            raise ReleaseProtocolError(f"{dataset_id} must declare nonempty columns")
        seen_columns: set[str] = set()
        for column in columns:
            if not isinstance(column, dict) or not isinstance(column.get("name"), str) or not isinstance(column.get("type"), str):
                raise ReleaseProtocolError(f"{dataset_id} has an invalid column declaration")
            if (not column["name"].strip() or not column["type"].strip()
                    or len(column["name"]) > 200 or len(column["type"]) > 200
                    or column["name"] in seen_columns):
                raise ReleaseProtocolError(f"{dataset_id} has duplicate or blank column metadata")
            seen_columns.add(column["name"])
        data = _read_local_file(raw.get("path"), f"dataset {dataset_id}")
        source_name = _safe_filename(Path(raw["path"]).name, f"dataset {dataset_id} filename")
        upload_name = _safe_filename(f"{dataset_id}--{source_name}", f"dataset {dataset_id} upload filename")
        if upload_name in names:
            raise ReleaseProtocolError(f"duplicate candidate filename {upload_name}")
        names.add(upload_name)
        metadata = {
            "dataset_id": dataset_id,
            "layer": "03_silver",
            "table_name": table_name,
            "row_count": row_count,
            "min_date": raw["min_date"],
            "max_date": raw["max_date"],
            "columns": [{"name": column["name"], "type": column["type"]} for column in columns],
        }
        validated_datasets.append({"metadata": metadata, "data": data, "upload_name": upload_name})

    artifact_by_name: dict[str, dict[str, Any]] = {}
    for raw in artifacts:
        if not isinstance(raw, dict):
            raise ReleaseProtocolError("each artifact must be an object")
        name = _safe_filename(raw.get("name"), "artifact name")
        if name in artifact_by_name or name in names:
            raise ReleaseProtocolError(f"duplicate or reserved candidate filename {name}")
        artifact_by_name[name] = raw
        names.add(name)
    missing = sorted(REQUIRED_ARTIFACTS - set(artifact_by_name))
    if missing:
        raise ReleaseProtocolError(f"release artifacts are missing required files: {missing}")

    validated_artifacts = [
        {"name": name, "data": _read_local_file(raw.get("path"), f"artifact {name}")}
        for name, raw in sorted(artifact_by_name.items())
    ]
    run_results = next(item["data"] for item in validated_artifacts if item["name"] == "run_results.json")
    _validate_run_results(run_results)
    return {
        "release_id": release_id,
        "datasets": validated_datasets,
        "artifacts": validated_artifacts,
        "inputs": json.loads(_bounded_json(inputs, "inputs")),
        "measurements": json.loads(_bounded_json(measurements, "measurements")),
        "code_sha": code_sha,
    }


def _validate_run_results(raw: bytes) -> None:
    document = _parse_json_object(raw, "run_results.json")
    metadata = document.get("metadata")
    if not isinstance(metadata, dict) or not isinstance(metadata.get("dbt_schema_version"), str):
        raise ReleaseProtocolError("run_results.json is incomplete: dbt metadata is missing")
    results = document.get("results")
    if not isinstance(results, list) or not results:
        raise ReleaseProtocolError("run_results.json is incomplete: results must be nonempty")
    found_models: set[str] = set()
    successful_tests = 0
    for result in results:
        if not isinstance(result, dict):
            raise ReleaseProtocolError("run_results.json contains an invalid result")
        status = result.get("status")
        unique_id = result.get("unique_id")
        if status not in _ALLOWED_RESULT_STATUSES or not isinstance(unique_id, str):
            raise ReleaseProtocolError("run_results.json contains failed, skipped, or malformed results")
        if unique_id in EXPECTED_STAGING_MODELS.values():
            found_models.add(unique_id)
        if unique_id.startswith("test."):
            successful_tests += 1
    missing = sorted(set(EXPECTED_STAGING_MODELS.values()) - found_models)
    if missing:
        raise ReleaseProtocolError(f"run_results.json is incomplete: missing successful staging models {missing}")
    if not successful_tests:
        raise ReleaseProtocolError("run_results.json is incomplete: no successful dbt tests")


def _validate_platform_candidate(**kwargs: Any) -> dict[str, Any]:
    scope = kwargs["release_scope"]
    spec = PLATFORM_RELEASES.get(scope)
    if spec is None:
        raise ReleaseProtocolError(f"unsupported platform release scope: {scope}")
    dataset_specs = spec["datasets"]
    date_columns = spec["date_columns"]
    required_artifacts = spec["required_artifacts"]
    allow_zero_rows = spec["allow_zero_rows"]
    label = spec["label"]
    datasets = kwargs["datasets"]
    artifacts = kwargs["artifacts"]
    inputs = kwargs["inputs"]
    measurements = kwargs["measurements"]
    code_sha = kwargs["code_sha"]
    supplied_release_id = kwargs["release_id"]
    if not isinstance(datasets, list) or not isinstance(artifacts, list) or not isinstance(inputs, list):
        raise ReleaseProtocolError("datasets, artifacts, and inputs must be lists")
    if len(datasets) > _MAX_ITEMS or len(artifacts) > _MAX_ITEMS or len(inputs) > _MAX_ITEMS:
        raise ReleaseProtocolError("candidate list exceeds publication limit")
    if not isinstance(code_sha, str) or not _CODE_SHA_RE.fullmatch(code_sha):
        raise ReleaseProtocolError("code_sha must be a 40-character lowercase hexadecimal Git SHA")
    if not isinstance(measurements, dict):
        raise ReleaseProtocolError("measurements must be an object")
    _bounded_json(inputs, "inputs")
    _bounded_json(measurements, "measurements")
    release_id = str(uuid4()) if supplied_release_id is None else _parse_release_id(supplied_release_id)

    by_id: dict[str, dict[str, Any]] = {}
    for raw in datasets:
        if not isinstance(raw, dict):
            raise ReleaseProtocolError("each dataset must be an object")
        dataset_id = raw.get("dataset_id")
        if not isinstance(dataset_id, str) or dataset_id in by_id:
            raise ReleaseProtocolError("dataset IDs must be unique strings")
        by_id[dataset_id] = raw
    expected_ids = set(dataset_specs)
    if set(by_id) != expected_ids:
        missing = sorted(expected_ids - set(by_id))
        extra = sorted(set(by_id) - expected_ids)
        raise ReleaseProtocolError(
            f"{label} release must contain exactly {len(expected_ids)} datasets; "
            f"missing={missing}, extra={extra}"
        )

    names: set[str] = {"release.json"}
    table_names: set[tuple[str, str]] = set()
    validated_datasets: list[dict[str, Any]] = []
    for dataset_id in sorted(dataset_specs):
        raw = by_id[dataset_id]
        layer, model_id = dataset_specs[dataset_id]
        if raw.get("layer") != layer:
            raise ReleaseProtocolError(f"{dataset_id} must be published from layer {layer}")
        if raw.get("model_id") != model_id:
            raise ReleaseProtocolError(f"{dataset_id} must identify model {model_id}")
        if raw.get("model_name") != model_id.rsplit(".", 1)[-1]:
            raise ReleaseProtocolError(f"{dataset_id} must identify model_name {model_id.rsplit('.', 1)[-1]}")
        table_name = raw.get("table_name")
        if not isinstance(table_name, str) or not _TABLE_NAME_RE.fullmatch(table_name):
            raise ReleaseProtocolError(f"{dataset_id} has an invalid table_name identifier")
        table_key = (layer, table_name)
        if table_key in table_names:
            raise ReleaseProtocolError("NBP platform release has duplicate table_name values")
        table_names.add(table_key)
        rows = raw.get("row_count")
        allows_zero = dataset_id in allow_zero_rows
        if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0 or (rows == 0 and not allows_zero):
            qualifier = "a nonnegative" if allows_zero else "a positive"
            raise ReleaseProtocolError(f"{dataset_id} must have {qualifier} row_count")
        _validate_platform_dates(dataset_id, raw, rows, date_columns, allow_zero_rows)
        columns = _validate_columns(raw.get("columns"), dataset_id)
        date_column = _validate_platform_date_column(dataset_id, raw.get("date_column"), columns, date_columns)
        data = _read_local_file(raw.get("path"), f"dataset {dataset_id}")
        source_name = _safe_filename(Path(raw["path"]).name, f"dataset {dataset_id} filename")
        upload_name = _safe_filename(f"{dataset_id}--{source_name}", f"dataset {dataset_id} upload filename")
        if upload_name in names:
            raise ReleaseProtocolError(f"duplicate candidate filename {upload_name}")
        names.add(upload_name)
        metadata = {
            "dataset_id": dataset_id, "layer": layer, "model_name": raw["model_name"], "model_id": model_id,
            "table_name": table_name, "row_count": rows,
            "date_column": date_column, "min_date": raw.get("min_date"), "max_date": raw.get("max_date"),
            "columns": columns,
        }
        validated_datasets.append({"metadata": metadata, "data": data, "upload_name": upload_name})

    artifact_by_name: dict[str, dict[str, Any]] = {}
    for raw in artifacts:
        if not isinstance(raw, dict):
            raise ReleaseProtocolError("each artifact must be an object")
        name = _safe_filename(raw.get("name"), "artifact name")
        if name in artifact_by_name or name in names:
            raise ReleaseProtocolError(f"duplicate or reserved candidate filename {name}")
        artifact_by_name[name] = raw
        names.add(name)
    missing = sorted(required_artifacts - set(artifact_by_name))
    if missing:
        raise ReleaseProtocolError(f"{label} release artifacts are missing required files: {missing}")
    validated_artifacts = [
        {"name": name, "data": _read_local_file(raw.get("path"), f"artifact {name}")}
        for name, raw in sorted(artifact_by_name.items())
    ]
    artifact_data = {item["name"]: item["data"] for item in validated_artifacts}
    _validate_platform_artifacts(artifact_data, code_sha)
    _validate_platform_run_results(artifact_data["run_results.json"], dataset_specs)
    return {
        "release_id": release_id, "release_scope": scope, "format_version": 2,
        "datasets": validated_datasets, "artifacts": validated_artifacts,
        "inputs": json.loads(_bounded_json(inputs, "inputs")),
        "measurements": json.loads(_bounded_json(measurements, "measurements")), "code_sha": code_sha,
    }


def _validate_platform_dates(
    dataset_id: str,
    raw: dict[str, Any],
    rows: int,
    date_columns: dict[str, str | None],
    allow_zero_rows: set[str] | frozenset[str],
) -> None:
    minimum, maximum = raw.get("min_date"), raw.get("max_date")
    if date_columns[dataset_id] is None and (minimum is not None or maximum is not None):
        raise ReleaseProtocolError(f"{dataset_id} must not declare date bounds without a date_column")
    allow_null = (date_columns[dataset_id] is None) or (dataset_id in allow_zero_rows and rows == 0)
    if minimum is None and maximum is None and allow_null:
        return
    if not isinstance(minimum, str) or not isinstance(maximum, str) or not minimum.strip() or not maximum.strip() or len(minimum) > 64 or len(maximum) > 64:
        raise ReleaseProtocolError(f"{dataset_id} has invalid min_date or max_date")
    try:
        minimum_date, maximum_date = date.fromisoformat(minimum), date.fromisoformat(maximum)
    except ValueError as exc:
        raise ReleaseProtocolError(f"{dataset_id} has invalid min_date or max_date") from exc
    if minimum_date > maximum_date:
        raise ReleaseProtocolError(f"{dataset_id} min_date is after max_date")


def _validate_columns(columns: Any, dataset_id: str) -> list[dict[str, str]]:
    if not isinstance(columns, list) or not columns or len(columns) > _MAX_ITEMS:
        raise ReleaseProtocolError(f"{dataset_id} must declare nonempty columns")
    seen: set[str] = set()
    result: list[dict[str, str]] = []
    for column in columns:
        if not isinstance(column, dict) or not isinstance(column.get("name"), str) or not isinstance(column.get("type"), str):
            raise ReleaseProtocolError(f"{dataset_id} has an invalid column declaration")
        if not column["name"].strip() or not column["type"].strip() or len(column["name"]) > 200 or len(column["type"]) > 200 or column["name"] in seen:
            raise ReleaseProtocolError(f"{dataset_id} has duplicate or blank column metadata")
        seen.add(column["name"])
        result.append({"name": column["name"], "type": column["type"]})
    return result


def _validate_platform_date_column(
    dataset_id: str,
    value: Any,
    columns: list[dict[str, str]],
    date_columns: dict[str, str | None],
) -> str | None:
    expected = date_columns[dataset_id]
    if value != expected:
        raise ReleaseProtocolError(f"{dataset_id} must declare date_column {expected!r}")
    if value is not None and (not isinstance(value, str) or not _TABLE_NAME_RE.fullmatch(value)):
        raise ReleaseProtocolError(f"{dataset_id} has an invalid date_column identifier")
    if value is not None and value not in {column["name"] for column in columns}:
        raise ReleaseProtocolError(f"{dataset_id} date_column is not present in columns")
    return value


def _validate_platform_artifacts(artifacts: dict[str, bytes], code_sha: str) -> None:
    manifest = _parse_json_object(artifacts["manifest.json"], "manifest.json")
    if not isinstance(manifest.get("metadata"), dict) or not isinstance(manifest.get("nodes"), dict):
        raise ReleaseProtocolError("manifest.json is not a minimal dbt manifest")
    catalog = _parse_json_object(artifacts["catalog.json"], "catalog.json")
    if not isinstance(catalog.get("nodes"), dict):
        raise ReleaseProtocolError("catalog.json is not a minimal dbt catalog")
    state = _parse_json_object(artifacts["ingestion-state.json"], "ingestion-state.json")
    if state.get("format_version") != 1 or not isinstance(state.get("sources"), dict):
        raise ReleaseProtocolError("ingestion-state.json must be a format-version-1 state snapshot")
    catalogue = _parse_json_object(artifacts["business-catalog.json"], "business-catalog.json")
    if catalogue.get("format_version") != 1 or catalogue.get("code_sha") != code_sha:
        raise ReleaseProtocolError("business-catalog.json must bind format-version-1 metadata to candidate code_sha")
    sources = catalogue.get("sources")
    lineage = catalogue.get("lineage")
    if not isinstance(sources, list) or not isinstance(lineage, dict) or not isinstance(lineage.get("nodes"), list) or not isinstance(lineage.get("edges"), list):
        raise ReleaseProtocolError("business-catalog.json has invalid sources or lineage")
    if not isinstance(catalogue.get("metrics"), list) or catalogue.get("metrics_status") not in {"awaiting_business_approval", "proposed", "source_defined"}:
        raise ReleaseProtocolError("business-catalog.json has invalid metrics status")
    if catalogue.get("metrics_status") == "source_defined":
        if not {"semantic_manifest.json", "metric-validation.json"}.issubset(artifacts):
            raise ReleaseProtocolError("source-defined metrics require semantic validation artifacts")
        semantic = _parse_json_object(artifacts["semantic_manifest.json"], "semantic_manifest.json")
        validation = _parse_json_object(artifacts["metric-validation.json"], "metric-validation.json")
        if not isinstance(semantic.get("metrics"), list) or validation.get("status") != "verified" or not isinstance(validation.get("metrics"), list):
            raise ReleaseProtocolError("source-defined metric artifacts are invalid")
    for source in sources:
        required = ("source_id", "name", "description", "status", "checked_through", "latest_observation_date", "last_successful_ingestion_at", "last_attempt_at", "raw_response_count")
        if not isinstance(source, dict) or any(key not in source for key in required) or source.get("status") != "published_snapshot":
            raise ReleaseProtocolError("business-catalog.json has invalid source metadata")
        if not isinstance(source["source_id"], str) or not isinstance(source["name"], str) or not isinstance(source["description"], str) or not isinstance(source["raw_response_count"], int) or isinstance(source["raw_response_count"], bool) or source["raw_response_count"] < 0:
            raise ReleaseProtocolError("business-catalog.json has invalid source metadata")
    for node in lineage["nodes"]:
        if not isinstance(node, dict) or not all(isinstance(node.get(key), str) and node[key] for key in ("id", "label", "kind", "layer", "description")):
            raise ReleaseProtocolError("business-catalog.json has invalid lineage node")
    for edge in lineage["edges"]:
        if not isinstance(edge, dict) or not all(isinstance(edge.get(key), str) and edge[key] for key in ("from", "to")):
            raise ReleaseProtocolError("business-catalog.json has invalid lineage edge")


def _validate_platform_run_results(raw: bytes, dataset_specs: dict[str, tuple[str, str]]) -> None:
    document = _parse_json_object(raw, "run_results.json")
    if not isinstance(document.get("metadata"), dict) or not isinstance(document.get("metadata", {}).get("dbt_schema_version"), str):
        raise ReleaseProtocolError("run_results.json is incomplete: dbt metadata is missing")
    results = document.get("results")
    if not isinstance(results, list) or not results:
        raise ReleaseProtocolError("run_results.json is incomplete: results must be nonempty")
    successful_models: set[str] = set()
    successful_tests: list[str] = []
    expected_models = {model for _layer, model in dataset_specs.values()}
    for result in results:
        if not isinstance(result, dict) or result.get("status") not in _ALLOWED_RESULT_STATUSES or not isinstance(result.get("unique_id"), str):
            raise ReleaseProtocolError("run_results.json contains failed, skipped, or malformed results")
        unique_id = result["unique_id"]
        if unique_id in expected_models:
            successful_models.add(unique_id)
        if unique_id.startswith("test."):
            successful_tests.append(unique_id)
    missing = sorted(expected_models - successful_models)
    if missing:
        raise ReleaseProtocolError(f"run_results.json is incomplete: missing successful platform models {missing}")
    uncovered = sorted(model for model in expected_models if not any(model.rsplit(".", 1)[-1] in test_id for test_id in successful_tests))
    if uncovered:
        raise ReleaseProtocolError(f"run_results.json is incomplete: missing successful test coverage for platform models {uncovered}")


def _read_pointer(store: ReleaseStore, root_id: str) -> dict[str, Any] | None:
    ids = store.find("current-release.json", root_id)
    if len(ids) > 1:
        raise ReleaseProtocolError("ambiguous current-release pointers under publication root")
    if not ids:
        return None
    file_id = _require_id(ids[0], "current-release pointer id")
    raw = _read_bytes(store, file_id, "current-release pointer")
    value = _parse_json_object(raw, "current-release pointer")
    _validate_pointer(value)
    return {"file_id": file_id, "raw": raw, "value": value}


def _write_pointer(store: ReleaseStore, root_id: str, previous: dict[str, Any] | None, data: bytes) -> str:
    try:
        if previous is None:
            file_id = _require_id(store.create("current-release.json", data, root_id), "current-release pointer id")
        else:
            file_id = previous["file_id"]
            store.replace(file_id, data)
    except Exception as exc:
        # A timeout may have happened after Drive applied the replace.  Only
        # accept it when a fresh exact-byte read proves the desired pointer.
        observed = _find_exact_pointer(store, root_id, data)
        if observed is not None:
            return observed
        raise ReleaseProtocolError(
            "current-release pointer write has an uncertain outcome; prior release was not deliberately changed"
        ) from exc
    observed = _find_exact_pointer(store, root_id, data)
    if observed is None:
        raise ReleaseProtocolError("current-release pointer did not read back as the requested bytes")
    return observed


def _find_exact_pointer(store: ReleaseStore, root_id: str, expected: bytes) -> str | None:
    try:
        ids = store.find("current-release.json", root_id)
        if len(ids) != 1:
            return None
        file_id = _require_id(ids[0], "current-release pointer id")
        return file_id if _read_bytes(store, file_id, "current-release pointer") == expected else None
    except Exception:
        return None


def _verify_manifest_files(store: ReleaseStore, manifest: dict[str, Any]) -> None:
    datasets = manifest.get("datasets")
    artifacts = manifest.get("artifacts")
    if not isinstance(datasets, list) or len(datasets) != len(REQUIRED_DATASETS):
        raise ReleaseProtocolError("release manifest must contain exactly four datasets")
    if not isinstance(artifacts, list) or not artifacts or len(artifacts) > _MAX_ITEMS:
        raise ReleaseProtocolError("release manifest has invalid artifacts")
    dataset_ids: set[str] = set()
    artifact_names: set[str] = set()
    file_ids: set[str] = set()
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise ReleaseProtocolError("release manifest has an invalid dataset")
        dataset_id = dataset.get("dataset_id")
        if not isinstance(dataset_id, str) or dataset_id in dataset_ids:
            raise ReleaseProtocolError("release manifest has duplicate or invalid dataset IDs")
        dataset_ids.add(dataset_id)
        _validate_dataset_metadata(dataset, dataset_id)
        files = dataset.get("files")
        if not isinstance(files, list) or not files or len(files) > _MAX_ITEMS:
            raise ReleaseProtocolError("release manifest has invalid dataset files")
        for file_entry in files:
            file_ids.add(_verify_file_entry(store, file_entry, file_ids))
    if dataset_ids != REQUIRED_DATASETS:
        raise ReleaseProtocolError("release manifest must contain exactly the NBP silver datasets")
    for file_entry in artifacts:
        if not isinstance(file_entry, dict):
            raise ReleaseProtocolError("release manifest has an invalid artifact")
        name = _safe_filename(file_entry.get("name"), "artifact name")
        if name in artifact_names:
            raise ReleaseProtocolError("release manifest has duplicate artifact names")
        artifact_names.add(name)
        file_ids.add(_verify_file_entry(store, file_entry, file_ids))
    if not REQUIRED_ARTIFACTS.issubset(artifact_names):
        raise ReleaseProtocolError("release manifest is missing required dbt artifacts")


def _verify_platform_manifest_files(store: ReleaseStore, manifest: dict[str, Any]) -> None:
    spec = PLATFORM_RELEASES[manifest["release_scope"]]
    dataset_specs = spec["datasets"]
    date_columns = spec["date_columns"]
    allow_zero_rows = spec["allow_zero_rows"]
    required_artifacts = spec["required_artifacts"]
    datasets = manifest.get("datasets")
    artifacts = manifest.get("artifacts")
    if not isinstance(datasets, list) or len(datasets) != len(dataset_specs):
        raise ReleaseProtocolError("platform release manifest must contain exactly the scoped datasets")
    if not isinstance(artifacts, list) or not artifacts or len(artifacts) > _MAX_ITEMS:
        raise ReleaseProtocolError("platform release manifest has invalid artifacts")
    dataset_ids: set[str] = set()
    table_names: set[tuple[str, str]] = set()
    artifact_names: set[str] = set()
    file_ids: set[str] = set()
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise ReleaseProtocolError("platform release manifest has an invalid dataset")
        dataset_id = dataset.get("dataset_id")
        if not isinstance(dataset_id, str) or dataset_id in dataset_ids or dataset_id not in dataset_specs:
            raise ReleaseProtocolError("platform release manifest has duplicate or invalid dataset IDs")
        dataset_ids.add(dataset_id)
        _validate_platform_dataset_metadata(dataset, dataset_id, table_names, dataset_specs, date_columns, allow_zero_rows)
        files = dataset.get("files")
        if not isinstance(files, list) or len(files) != 1:
            raise ReleaseProtocolError("platform release manifest has invalid dataset files")
        file_ids.add(_verify_file_entry(store, files[0], file_ids))
    if dataset_ids != set(dataset_specs):
        raise ReleaseProtocolError("platform release manifest has incomplete platform datasets")
    artifact_data: dict[str, bytes] = {}
    for entry in artifacts:
        if not isinstance(entry, dict):
            raise ReleaseProtocolError("platform release manifest has an invalid artifact")
        name = _safe_filename(entry.get("name"), "artifact name")
        if name in artifact_names:
            raise ReleaseProtocolError("platform release manifest has duplicate artifact names")
        artifact_names.add(name)
        file_id = _verify_file_entry(store, entry, file_ids)
        file_ids.add(file_id)
        artifact_data[name] = _read_bytes(store, file_id, f"released artifact {name}")
    if not required_artifacts.issubset(artifact_names):
        raise ReleaseProtocolError("platform release manifest is missing required artifacts")
    _validate_platform_artifacts(artifact_data, manifest["code_sha"])
    _validate_platform_run_results(artifact_data["run_results.json"], dataset_specs)


def _validate_platform_dataset_metadata(
    dataset: dict[str, Any],
    dataset_id: str,
    table_names: set[tuple[str, str]],
    dataset_specs: dict[str, tuple[str, str]],
    date_columns: dict[str, str | None],
    allow_zero_rows: set[str] | frozenset[str],
) -> None:
    layer, model_id = dataset_specs[dataset_id]
    if dataset.get("layer") != layer or dataset.get("model_id") != model_id:
        raise ReleaseProtocolError(f"platform release manifest has invalid model metadata for {dataset_id}")
    if dataset.get("model_name") != model_id.rsplit(".", 1)[-1]:
        raise ReleaseProtocolError(f"platform release manifest has invalid model name for {dataset_id}")
    table_name = dataset.get("table_name")
    table_key = (layer, table_name) if isinstance(table_name, str) else None
    if not isinstance(table_name, str) or not _TABLE_NAME_RE.fullmatch(table_name) or table_key in table_names:
        raise ReleaseProtocolError(f"platform release manifest has invalid table name for {dataset_id}")
    table_names.add(table_key)
    rows = dataset.get("row_count")
    if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0 or (rows == 0 and dataset_id not in allow_zero_rows):
        raise ReleaseProtocolError(f"platform release manifest has invalid row count for {dataset_id}")
    _validate_platform_dates(dataset_id, dataset, rows, date_columns, allow_zero_rows)
    columns = _validate_columns(dataset.get("columns"), dataset_id)
    _validate_platform_date_column(dataset_id, dataset.get("date_column"), columns, date_columns)


def _validate_dataset_metadata(dataset: dict[str, Any], dataset_id: str) -> None:
    if dataset.get("layer") != "03_silver":
        raise ReleaseProtocolError(f"release manifest has invalid layer for {dataset_id}")
    if not isinstance(dataset.get("table_name"), str) or not dataset["table_name"].strip() or len(dataset["table_name"]) > 200:
        raise ReleaseProtocolError(f"release manifest has invalid table name for {dataset_id}")
    rows = dataset.get("row_count")
    if not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0:
        raise ReleaseProtocolError(f"release manifest has invalid row count for {dataset_id}")
    for field in ("min_date", "max_date"):
        value = dataset.get(field)
        if not isinstance(value, str) or not value.strip() or len(value) > 64:
            raise ReleaseProtocolError(f"release manifest has invalid {field} for {dataset_id}")
        try:
            date.fromisoformat(value)
        except ValueError as exc:
            raise ReleaseProtocolError(f"release manifest has invalid {field} for {dataset_id}") from exc
    if date.fromisoformat(dataset["min_date"]) > date.fromisoformat(dataset["max_date"]):
        raise ReleaseProtocolError(f"release manifest has inverted dates for {dataset_id}")
    columns = dataset.get("columns")
    if not isinstance(columns, list) or not columns or len(columns) > _MAX_ITEMS:
        raise ReleaseProtocolError(f"release manifest has invalid columns for {dataset_id}")
    column_names: set[str] = set()
    for column in columns:
        if not isinstance(column, dict) or not isinstance(column.get("name"), str) or not isinstance(column.get("type"), str):
            raise ReleaseProtocolError(f"release manifest has invalid columns for {dataset_id}")
        if (not column["name"].strip() or not column["type"].strip()
                or len(column["name"]) > 200 or len(column["type"]) > 200
                or column["name"] in column_names):
            raise ReleaseProtocolError(f"release manifest has invalid columns for {dataset_id}")
        column_names.add(column["name"])


def _verify_file_entry(store: ReleaseStore, entry: Any, existing_ids: set[str]) -> str:
    if not isinstance(entry, dict):
        raise ReleaseProtocolError("release manifest has an invalid file entry")
    file_id = _require_id(entry.get("id"), "manifest file id")
    if file_id in existing_ids:
        raise ReleaseProtocolError("release manifest reuses a file ID")
    _safe_filename(entry.get("name"), "manifest file name")
    expected_sha = entry.get("sha256")
    expected_size = entry.get("size")
    if (not isinstance(expected_size, int) or isinstance(expected_size, bool) or expected_size <= 0
            or not isinstance(expected_sha, str) or not _SHA_RE.fullmatch(expected_sha)):
        raise ReleaseProtocolError("release manifest has invalid file checksum metadata")
    data = _read_bytes(store, file_id, "released file")
    if len(data) != expected_size or _sha256(data) != expected_sha:
        raise ReleaseProtocolError("released file checksum mismatch")
    return file_id


def _validate_pointer(value: dict[str, Any]) -> None:
    if value.get("format_version") != 1:
        raise ReleaseProtocolError("current-release pointer has unsupported format version")
    _parse_release_id(value.get("release_id"))
    _require_id(value.get("manifest_file_id"), "pointer manifest_file_id")
    if not isinstance(value.get("manifest_sha256"), str) or not _SHA_RE.fullmatch(value["manifest_sha256"]):
        raise ReleaseProtocolError("current-release pointer has invalid manifest checksum")
    if not isinstance(value.get("updated_at_utc"), str) or not value["updated_at_utc"].strip():
        raise ReleaseProtocolError("current-release pointer has invalid updated_at_utc")
    if "previous_manifest_file_id" in value:
        _require_id(value["previous_manifest_file_id"], "pointer previous_manifest_file_id")
    if "promotion_audit_file_id" in value:
        _require_id(value["promotion_audit_file_id"], "pointer promotion_audit_file_id")


def _upload_verified(store: ReleaseStore, parent_id: str, name: str, data: bytes) -> str:
    file_id = _require_id(store.create(name, data, parent_id), f"uploaded {name} id")
    returned = _read_bytes(store, file_id, f"uploaded {name}")
    if returned != data or _sha256(returned) != _sha256(data):
        raise ReleaseProtocolError(f"uploaded {name} did not read back with exact bytes")
    return file_id


def _read_local_file(value: Any, label: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise ReleaseProtocolError(f"{label} path must be a nonempty string")
    path = Path(value)
    if not path.is_file():
        raise ReleaseProtocolError(f"{label} path is not a regular file")
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise ReleaseProtocolError(f"could not read {label}") from exc
    if not data:
        raise ReleaseProtocolError(f"{label} must not be empty")
    return data


def _safe_filename(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > _MAX_FILE_NAME:
        raise ReleaseProtocolError(f"{label} is invalid")
    if value in {".", ".."} or Path(value).name != value or "/" in value or "\\" in value or "\x00" in value:
        raise ReleaseProtocolError(f"{label} must be a bounded basename")
    return value


def _bounded_json(value: Any, label: str) -> str:
    try:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ReleaseProtocolError(f"{label} must be JSON serializable") from exc
    if len(encoded.encode("utf-8")) > _MAX_METADATA_BYTES:
        raise ReleaseProtocolError(f"{label} exceeds metadata size limit")
    return encoded


def _parse_release_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ReleaseProtocolError("release_id must be a canonical UUID string")
    try:
        parsed = str(UUID(value))
    except (ValueError, AttributeError) as exc:
        raise ReleaseProtocolError("release_id must be a canonical UUID string") from exc
    if parsed != value:
        raise ReleaseProtocolError("release_id must be a canonical UUID string")
    return parsed


def _require_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise ReleaseProtocolError(f"{label} is invalid")
    return value


def _read_bytes(store: ReleaseStore, file_id: str, label: str) -> bytes:
    try:
        data = store.read(file_id)
    except Exception as exc:
        raise ReleaseProtocolError(f"could not read {label}") from exc
    if not isinstance(data, bytes):
        raise ReleaseProtocolError(f"{label} did not return bytes")
    return data


def _parse_json_object(data: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseProtocolError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ReleaseProtocolError(f"{label} must be a JSON object")
    return value


def _json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _sha256(data: bytes) -> str:
    return sha256(data).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
