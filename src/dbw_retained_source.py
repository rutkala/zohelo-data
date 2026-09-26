"""Exact binding between DBW modeled candidates and retained-Bronze evidence."""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any
from uuid import UUID


RETAINED_SOURCE_ID = "gus_dbw_retained_bronze"
RETAINED_COVERAGE_STATUS = "incomplete_retained_inventory"
RETAINED_LINEAGE_STATUS = "unresolved_native_to_bronze"
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_DATASET_NAMES = {"observations", "dictionaries", "metadata", "taxonomy"}


def load_retained_source_descriptor(
    pointer_path: Path,
    manifest_path: Path,
    *,
    pointer_file_id: str,
    expected_inventory_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate exact saved source bytes and build modeled-release input evidence."""
    pointer_raw = Path(pointer_path).read_bytes()
    manifest_raw = Path(manifest_path).read_bytes()
    return _validate_source_bytes(
        pointer_raw,
        manifest_raw,
        pointer_file_id=pointer_file_id,
        expected_inventory_sha256=expected_inventory_sha256,
    )


def validate_retained_source(
    store: Any,
    descriptor: Any,
    *,
    expected_pointer_file_id: str,
) -> dict[str, Any]:
    """Read the live retained pointer and immutable manifest, then match a descriptor."""
    if not isinstance(descriptor, dict):
        raise ValueError("DBW retained source descriptor must be an object")
    pointer_file_id = descriptor.get("pointer_file_id")
    if (
        not isinstance(expected_pointer_file_id, str)
        or not expected_pointer_file_id
        or pointer_file_id != expected_pointer_file_id
    ):
        raise ValueError(
            "DBW retained source pointer differs from canonical discovery"
        )
    pointer_raw = store.read(pointer_file_id)
    if not isinstance(pointer_raw, bytes):
        raise ValueError("DBW retained source pointer is not bytes")
    pointer = _json_object(pointer_raw, "DBW retained source pointer")
    manifest_file_id = pointer.get("manifest_file_id")
    if not isinstance(manifest_file_id, str) or not manifest_file_id:
        raise ValueError("DBW retained source manifest file ID is invalid")
    manifest_raw = store.read(manifest_file_id)
    if not isinstance(manifest_raw, bytes):
        raise ValueError("DBW retained source manifest is not bytes")
    expected, manifest = _validate_source_bytes(
        pointer_raw,
        manifest_raw,
        pointer_file_id=pointer_file_id,
        expected_inventory_sha256=descriptor.get("inventory_sha256"),
    )
    if descriptor != expected:
        raise ValueError(
            "DBW modeled release input differs from the live retained source"
        )
    return manifest


def validate_native_bronze_tree(
    data_root: Path,
    release_id: str,
    audit_dir: Path,
    *,
    retained_manifest: dict[str, Any],
) -> dict[str, Any]:
    """Bind every local dbt input byte to the exact reviewed native inventory."""
    try:
        report, inventory, audit_sha = _reviewed_audit(Path(audit_dir))
    except Exception as exc:
        raise ValueError("DBW reviewed native audit is invalid") from exc
    if (
        inventory.get("inventory_sha256") != release_id
        or retained_manifest.get("inventory_sha256") != release_id
        or retained_manifest.get("audit_report_sha256") != audit_sha
        or report.get("inventory_sha256") != release_id
    ):
        raise ValueError(
            "DBW native audit does not match the retained source"
        )
    objects = inventory.get("objects")
    if not isinstance(objects, list) or not objects:
        raise ValueError("DBW reviewed native inventory is empty")
    release_root = (
        Path(data_root)
        / "02_bronze"
        / "gus_dbw"
        / "releases"
        / release_id
    ).resolve()
    expected_paths: set[str] = set()
    verified_bytes = 0
    for item in objects:
        if not isinstance(item, dict):
            raise ValueError("DBW reviewed native inventory entry is invalid")
        relative = item.get("path")
        if not isinstance(relative, str) or relative in expected_paths:
            raise ValueError("DBW reviewed native inventory path is invalid")
        path = (release_root / relative).resolve()
        if path == release_root or release_root not in path.parents:
            raise ValueError("DBW reviewed native inventory path escapes release")
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size != item.get("size")
        ):
            raise ValueError(
                f"DBW native input identity changed: {relative}"
            )
        if _path_sha256(path) != item.get("sha256"):
            raise ValueError(
                f"DBW native input digest changed: {relative}"
            )
        expected_paths.add(relative)
        verified_bytes += path.stat().st_size
    actual_paths = {
        path.relative_to(release_root).as_posix()
        for folder in ("observations", "dictionaries", "taxonomy", "metadata")
        for path in (release_root / folder).rglob("*")
        if path.is_file()
    }
    if actual_paths != expected_paths:
        raise ValueError(
            "DBW native input tree differs from the reviewed inventory"
        )
    return {
        "audit_report_sha256": audit_sha,
        "inventory_sha256": release_id,
        "verified_files": len(expected_paths),
        "verified_bytes": verified_bytes,
    }


def _reviewed_audit(
    audit_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    from retained_dbw_publication import validate_audit

    return validate_audit(audit_dir, require_reviewed_snapshot=True)


def _validate_source_bytes(
    pointer_raw: bytes,
    manifest_raw: bytes,
    *,
    pointer_file_id: str,
    expected_inventory_sha256: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    pointer = _json_object(pointer_raw, "DBW retained source pointer")
    manifest = _json_object(manifest_raw, "DBW retained source manifest")
    if (
        pointer.get("format_version") != 1
        or pointer.get("source_id") != RETAINED_SOURCE_ID
    ):
        raise ValueError("DBW retained source pointer identity is invalid")
    snapshot_id = _uuid(pointer.get("snapshot_id"), "snapshot ID")
    manifest_file_id = pointer.get("manifest_file_id")
    manifest_sha256 = pointer.get("manifest_sha256")
    manifest_size = pointer.get("manifest_size_bytes")
    if (
        not isinstance(manifest_file_id, str)
        or not manifest_file_id
        or not isinstance(manifest_sha256, str)
        or not _SHA_RE.fullmatch(manifest_sha256)
        or not isinstance(manifest_size, int)
        or isinstance(manifest_size, bool)
        or manifest_size <= 0
        or len(manifest_raw) != manifest_size
        or sha256(manifest_raw).hexdigest() != manifest_sha256
    ):
        raise ValueError("DBW retained source manifest fingerprint changed")
    if (
        manifest.get("format_version") != 2
        or manifest.get("kind") != "retained_bronze_snapshot"
        or manifest.get("source_id") != RETAINED_SOURCE_ID
        or manifest.get("status") != "validated"
        or _uuid(manifest.get("snapshot_id"), "manifest snapshot ID")
        != snapshot_id
    ):
        raise ValueError("DBW retained source manifest identity is invalid")
    tests = manifest.get("tests")
    if (
        not isinstance(tests, dict)
        or tests.get("passed") is not True
        or tests.get("rows_and_schemas_preserved") is not True
    ):
        raise ValueError("DBW retained source manifest is not accepted")
    inventory_sha256 = manifest.get("inventory_sha256")
    audit_report_sha256 = manifest.get("audit_report_sha256")
    if (
        not isinstance(inventory_sha256, str)
        or not _SHA_RE.fullmatch(inventory_sha256)
        or inventory_sha256 != expected_inventory_sha256
        or not isinstance(audit_report_sha256, str)
        or not _SHA_RE.fullmatch(audit_report_sha256)
    ):
        raise ValueError(
            "DBW native inventory or audit does not match the retained snapshot"
        )
    if manifest.get("coverage_status") != RETAINED_COVERAGE_STATUS:
        raise ValueError("DBW retained coverage status was changed or upgraded")
    if manifest.get("lineage_status") != RETAINED_LINEAGE_STATUS:
        raise ValueError("DBW retained lineage status was changed or upgraded")
    indicator_count = manifest.get("indicator_count")
    if (
        not isinstance(indicator_count, int)
        or isinstance(indicator_count, bool)
        or indicator_count <= 0
        or manifest.get("published_indicator_count") != indicator_count
        or manifest.get("pending_indicator_count") != 0
    ):
        raise ValueError("DBW retained indicator inventory is incomplete")
    indicator_index = _index_descriptor(manifest.get("indicator_index"))
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list):
        raise ValueError("DBW retained source datasets are invalid")
    dataset_rows: dict[str, int] = {}
    dataset_files: dict[str, list[dict[str, Any]]] = {}
    seen_ids = {indicator_index["id"]}
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise ValueError("DBW retained source dataset is invalid")
        name = dataset.get("name")
        rows = dataset.get("row_count")
        files = dataset.get("files")
        if (
            name not in _DATASET_NAMES
            or name in dataset_rows
            or not isinstance(rows, int)
            or isinstance(rows, bool)
            or rows <= 0
            or not isinstance(files, list)
        ):
            raise ValueError("DBW retained source dataset inventory is incomplete")
        descriptors = [_file_descriptor(value) for value in files]
        if name == "observations":
            if descriptors:
                raise ValueError("DBW retained observations must use the indicator index")
        elif (
            not descriptors
            or sum(value["row_count"] for value in descriptors) != rows
        ):
            raise ValueError("DBW retained dataset file rows are incomplete")
        for value in descriptors:
            if value["id"] in seen_ids:
                raise ValueError("DBW retained object identity is duplicated")
            seen_ids.add(value["id"])
        dataset_rows[name] = rows
        dataset_files[name] = descriptors
    if set(dataset_rows) != _DATASET_NAMES or dataset_rows["taxonomy"] != indicator_count:
        raise ValueError("DBW retained source dataset inventory is incomplete")
    descriptor = {
        "source_id": RETAINED_SOURCE_ID,
        "pointer_file_id": pointer_file_id,
        "snapshot_id": snapshot_id,
        "manifest_file_id": manifest_file_id,
        "manifest_sha256": manifest_sha256,
        "manifest_size_bytes": manifest_size,
        "inventory_sha256": inventory_sha256,
        "audit_report_sha256": audit_report_sha256,
        "coverage_status": RETAINED_COVERAGE_STATUS,
        "lineage_status": RETAINED_LINEAGE_STATUS,
        "indicator_count": indicator_count,
        "indicator_index": indicator_index,
        "dataset_rows": dataset_rows,
        "dataset_files": dataset_files,
    }
    return descriptor, manifest


def _index_descriptor(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "id", "name", "size", "sha256"
    }:
        raise ValueError("DBW retained indicator index descriptor is invalid")
    if (
        not isinstance(value["id"], str)
        or not value["id"]
        or not isinstance(value["name"], str)
        or not value["name"].endswith(".json")
        or not isinstance(value["size"], int)
        or isinstance(value["size"], bool)
        or value["size"] <= 0
        or not isinstance(value["sha256"], str)
        or not _SHA_RE.fullmatch(value["sha256"])
    ):
        raise ValueError("DBW retained indicator index fingerprint is invalid")
    return dict(value)


def _file_descriptor(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "id", "name", "size", "sha256", "row_count"
    }:
        raise ValueError("DBW retained dataset file descriptor is invalid")
    if (
        not isinstance(value["id"], str)
        or not value["id"]
        or not isinstance(value["name"], str)
        or not value["name"].endswith(".parquet")
        or not isinstance(value["size"], int)
        or isinstance(value["size"], bool)
        or value["size"] <= 0
        or not isinstance(value["row_count"], int)
        or isinstance(value["row_count"], bool)
        or value["row_count"] <= 0
        or not isinstance(value["sha256"], str)
        or not _SHA_RE.fullmatch(value["sha256"])
    ):
        raise ValueError("DBW retained dataset file fingerprint is invalid")
    return dict(value)


def _path_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_object(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _uuid(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"DBW retained {label} is invalid")
    try:
        parsed = str(UUID(value))
    except ValueError as exc:
        raise ValueError(f"DBW retained {label} is invalid") from exc
    if parsed != value:
        raise ValueError(f"DBW retained {label} is not canonical")
    return value


__all__ = [
    "RETAINED_COVERAGE_STATUS",
    "RETAINED_LINEAGE_STATUS",
    "RETAINED_SOURCE_ID",
    "load_retained_source_descriptor",
    "validate_native_bronze_tree",
    "validate_retained_source",
]
