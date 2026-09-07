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
from typing import Any, Protocol
from uuid import UUID, uuid4


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
_ALLOWED_RESULT_STATUSES = frozenset({"success", "pass"})
_MAX_ITEMS = 2_048
_MAX_METADATA_BYTES = 1_000_000
_MAX_FILE_NAME = 180
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,255}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_CODE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


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
    )

    # A valid old pointer is read before *any* candidate remote write.
    previous = _read_pointer(store, root_id)
    if previous is not None:
        # Do not silently replace a damaged "current" state: preserve an
        # operator-visible failure until the existing pointer is repaired.
        restore_release(store, previous["value"])

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
        "format_version": 1,
        "release_id": candidate["release_id"],
        "release_scope": "nbp_silver",
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

    # Check again immediately before the only mutable operation.
    current = _read_pointer(store, root_id)
    current_raw = current["raw"] if current is not None else None
    previous_raw = previous["raw"] if previous is not None else None
    if current_raw != previous_raw:
        raise ReleaseProtocolError("current-release pointer changed during candidate upload")

    pointer: dict[str, Any] = {
        "format_version": 1,
        "release_id": candidate["release_id"],
        "manifest_file_id": manifest_file_id,
        "manifest_sha256": manifest_sha,
        "updated_at_utc": _utc_now(),
    }
    if previous is not None:
        pointer["previous_manifest_file_id"] = previous["value"]["manifest_file_id"]
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


def restore_release(store: ReleaseStore, pointer: str | dict[str, Any]) -> dict[str, Any]:
    """Verify and return a release manifest from a pointer ID or pointer object.

    ``pointer`` is either the stable ``current-release.json`` file ID or the
    decoded JSON object stored in that file.  The returned value is the decoded
    release manifest only after all listed data and dbt artifact checksums have
    been read and verified.
    """
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
    if manifest.get("format_version") != 1 or manifest.get("status") != "validated":
        raise ReleaseProtocolError("release manifest is not a validated format-version-1 release")
    if manifest.get("release_scope") != "nbp_silver":
        raise ReleaseProtocolError("release manifest has an unexpected scope")
    if not isinstance(manifest.get("code_sha"), str) or not _CODE_SHA_RE.fullmatch(manifest["code_sha"]):
        raise ReleaseProtocolError("release manifest has an invalid code SHA")
    if manifest.get("tests") != {"passed": True}:
        raise ReleaseProtocolError("release manifest does not record passed tests")
    if manifest.get("release_id") != value["release_id"]:
        raise ReleaseProtocolError("release ID does not match current pointer")
    _verify_manifest_files(store, manifest)
    return manifest


def restore_current_release(store: ReleaseStore, root_id: str) -> dict[str, Any]:
    """Resolve the stable pointer under ``root_id`` and restore its release."""
    root_id = _require_id(root_id, "root_id")
    pointer = _read_pointer(store, root_id)
    if pointer is None:
        raise ReleaseProtocolError("no current-release pointer exists")
    return restore_release(store, pointer["value"])


# Useful, short name for adapters/consumers.
read_current_release = restore_current_release


def _validate_candidate(**kwargs: Any) -> dict[str, Any]:
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
