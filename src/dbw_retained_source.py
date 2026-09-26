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
    if (
        not isinstance(inventory_sha256, str)
        or not _SHA_RE.fullmatch(inventory_sha256)
        or inventory_sha256 != expected_inventory_sha256
    ):
        raise ValueError(
            "DBW native inventory does not match the retained snapshot"
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
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list):
        raise ValueError("DBW retained source datasets are invalid")
    dataset_rows = {
        dataset.get("name"): dataset.get("row_count")
        for dataset in datasets
        if isinstance(dataset, dict)
    }
    expected_datasets = {
        "observations", "dictionaries", "metadata", "taxonomy"
    }
    if (
        len(dataset_rows) != len(datasets)
        or set(dataset_rows) != expected_datasets
        or any(
            not isinstance(rows, int)
            or isinstance(rows, bool)
            or rows <= 0
            for rows in dataset_rows.values()
        )
        or dataset_rows["taxonomy"] != indicator_count
    ):
        raise ValueError(
            "DBW retained source dataset inventory is incomplete"
        )
    descriptor = {
        "source_id": RETAINED_SOURCE_ID,
        "pointer_file_id": pointer_file_id,
        "snapshot_id": snapshot_id,
        "manifest_file_id": manifest_file_id,
        "manifest_sha256": manifest_sha256,
        "manifest_size_bytes": manifest_size,
        "inventory_sha256": inventory_sha256,
        "coverage_status": RETAINED_COVERAGE_STATUS,
        "lineage_status": RETAINED_LINEAGE_STATUS,
        "indicator_count": indicator_count,
        "dataset_rows": dataset_rows,
    }
    return descriptor, manifest


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
    "validate_retained_source",
]
