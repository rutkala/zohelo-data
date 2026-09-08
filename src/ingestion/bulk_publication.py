"""Publish bounded, queryable indexes for retained full source distributions.

The Parquet rows contain archive metadata only.  Raw WDI ZIP and Eurostat TSV
objects remain immutable Drive files and are never copied into browser DuckDB.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping
from urllib.parse import urlsplit
from uuid import uuid4

import duckdb

from ingestion.source_campaign_store import (
    CampaignCapacityError,
    CampaignStoreError,
    MAX_LANDING_FILE_BYTES,
    MAX_LANDING_MANIFEST_BYTES,
)


MAX_NEW_DISTRIBUTIONS = 256
MAX_NEW_METADATA_BYTES = 4 * 1024 * 1024
# The active tail is the only prior data fragment read during publication.
# Keeping it below these targets bounds both each append and the lifetime
# manifest descriptor count (21,233 Eurostat distributions need about 83 files).
MAX_TAIL_DISTRIBUTIONS = 256
MAX_TAIL_PARQUET_BYTES = 4 * 1024 * 1024

BULK_INDEX_COLUMNS = (
    ("dataset_id", "VARCHAR"),
    ("source_id", "VARCHAR"),
    ("version", "VARCHAR"),
    ("kind", "VARCHAR"),
    ("retrieved_at_utc", "TIMESTAMP"),
    ("raw_file_id", "VARCHAR"),
    ("raw_file_name", "VARCHAR"),
    ("raw_size_bytes", "BIGINT"),
    ("raw_sha256", "VARCHAR"),
    ("request_json", "VARCHAR"),
    ("inspection_json", "VARCHAR"),
)

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_PARQUET_NAME_RE = re.compile(
    r"^fragment-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.parquet$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MD5_RE = re.compile(r"^[0-9a-f]{32}$")
_CODE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SOURCE_RE = re.compile(r"^[a-z][a-z0-9_]{0,119}$")
_SAFE_TEXT_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.:@/+\-]{0,1023}$")
_SECRET_KEY_RE = re.compile(
    r"(?:auth|credential|password|secret|token|api[-_]?key|subscription[-_]?key)",
    re.I,
)


class BulkPublicationError(CampaignStoreError):
    """A full-distribution index failed admission or verification."""


def publish_bulk_index(store: Any, code_sha: str) -> dict[str, Any] | None:
    """Publish the next accepted-receipt prefix and atomically promote its index.

    The store is the ``<provider>_bulk`` campaign store.  Existing immutable
    Parquet fragments are authenticated by the current pointer and manifest;
    only newly appended fragments are read back before promotion.
    """
    campaign_id, provider_id = _source_identity(store)
    _require_code_sha(code_sha)
    cached_loader = getattr(store, "load_cached", None)
    state = cached_loader() if callable(cached_loader) else store.load()
    if state is None:
        return None
    receipts = _accepted_receipts(state, campaign_id, provider_id)
    if not receipts:
        return None

    pointer = store.load_landing_pointer()
    previous = _read_manifest(store, pointer, campaign_id, provider_id) if pointer else None
    published = 0 if previous is None else previous["published_distribution_count"]
    previous_files = [] if previous is None else previous["files"]
    if published > len(receipts):
        raise BulkPublicationError("bulk index publishes receipts absent from campaign state")
    if previous is not None:
        checkpoint = _receipt_checkpoint(receipts[:published])
        if previous["receipt_checkpoint_sha256"] != checkpoint:
            raise BulkPublicationError("bulk index receipt checkpoint does not match campaign state")
    if published == len(receipts):
        return previous

    rows: list[tuple[Any, ...]] = []
    metadata_bytes = 0
    for descriptor in receipts[published:]:
        if len(rows) >= MAX_NEW_DISTRIBUTIONS:
            break
        receipt = store.read_receipt(descriptor)
        row, size = _index_row(receipt, descriptor, state, provider_id)
        if rows and metadata_bytes + size > MAX_NEW_METADATA_BYTES:
            break
        if metadata_bytes + size > MAX_NEW_METADATA_BYTES:
            raise CampaignCapacityError(
                "one distribution receipt exceeds the bulk-index metadata limit"
            )
        rows.append(row)
        metadata_bytes += size
    if not rows:
        raise BulkPublicationError("no accepted distribution could be indexed")

    # Compact only the authenticated current tail.  Sealed fragments remain
    # referenced without being downloaded again, and the replaced immutable
    # tail remains in storage for recovery/audit even though the new manifest
    # no longer points at it.
    retained_files = [dict(item) for item in previous_files]
    rows_to_write = rows
    if previous_files:
        tail_payload = store.read_landing_object(previous_files[-1])
        tail_rows = _read_parquet_rows(tail_payload)
        if len(tail_rows) > published:
            raise BulkPublicationError("bulk index tail exceeds its published row count")
        if (
            len(tail_rows) < MAX_TAIL_DISTRIBUTIONS
            and len(tail_payload) < MAX_TAIL_PARQUET_BYTES
        ):
            retained_files.pop()
            rows_to_write = tail_rows + rows

    new_files: list[dict[str, Any]] = []
    for payload in _build_fragments(rows_to_write):
        new_files.append(
            store.put_landing_object(f"fragment-{uuid4()}.parquet", payload)
        )

    snapshot_id = str(uuid4())
    new_published = published + len(rows)
    manifest = {
        "format_version": 2,
        "kind": "full_distribution_index",
        "source_id": campaign_id,
        "snapshot_id": snapshot_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "code_sha": code_sha,
        "status": "validated",
        "layer": "01_landing",
        "table_name": f"{provider_id}_distributions",
        "row_count": new_published,
        "coverage_status": _campaign_coverage(state),
        "files": retained_files + new_files,
        "columns": [
            {"name": name, "type": kind} for name, kind in BULK_INDEX_COLUMNS
        ],
        "accepted_distribution_count": len(receipts),
        "published_distribution_count": new_published,
        "pending_publication_count": len(receipts) - new_published,
        "receipt_checkpoint_sha256": _receipt_checkpoint(receipts[:new_published]),
        "tests": {"passed": True},
    }
    manifest_raw = _canonical_bytes(manifest, "bulk index manifest")
    if len(manifest_raw) > MAX_LANDING_MANIFEST_BYTES:
        raise CampaignCapacityError("bulk index manifest exceeds the 1 MiB safety limit")
    manifest_descriptor = store.put_landing_object(
        f"manifest-{snapshot_id}.json",
        manifest_raw,
        maximum_bytes=MAX_LANDING_MANIFEST_BYTES,
    )
    pointer_value = {
        "format_version": 1,
        "source_id": campaign_id,
        "snapshot_id": snapshot_id,
        "manifest_file_id": manifest_descriptor["id"],
        "manifest_file_name": manifest_descriptor["name"],
        "manifest_sha256": manifest_descriptor["sha256"],
        "manifest_size_bytes": manifest_descriptor["size"],
    }
    candidate = _read_manifest(store, pointer_value, campaign_id, provider_id)
    if candidate != manifest:
        raise BulkPublicationError("bulk index candidate metadata changed")
    _verify_files(store, new_files, len(rows_to_write))
    store.promote_landing_pointer(pointer_value)
    promoted = store.load_landing_pointer()
    if promoted != pointer_value:
        raise BulkPublicationError("promoted bulk index pointer was not readable")
    verified = _read_manifest(store, promoted, campaign_id, provider_id)
    if verified != manifest:
        raise BulkPublicationError("promoted bulk index was not readable")
    return verified


def verify_bulk_index(store: Any) -> dict[str, Any] | None:
    """Freshly verify the current bulk index manifest and all Parquet fragments."""
    campaign_id, provider_id = _source_identity(store)
    pointer = store.load_landing_pointer()
    if pointer is None:
        return None
    manifest = _read_manifest(store, pointer, campaign_id, provider_id)
    _verify_files(store, manifest["files"], manifest["row_count"])
    return manifest


def _source_identity(store: Any) -> tuple[str, str]:
    campaign_id = getattr(store, "source_id", None)
    if not isinstance(campaign_id, str) or not _SOURCE_RE.fullmatch(campaign_id):
        raise BulkPublicationError("bulk campaign store has an invalid source identity")
    if not campaign_id.endswith("_bulk"):
        raise BulkPublicationError("bulk campaign store source_id must end with _bulk")
    provider_id = campaign_id.removesuffix("_bulk")
    if not provider_id or not _SOURCE_RE.fullmatch(provider_id):
        raise BulkPublicationError("bulk campaign provider identity is invalid")
    return campaign_id, provider_id


def _campaign_coverage(state: Mapping[str, Any]) -> str:
    # Keep the catalogue/current-generation rules in their owning campaign module.
    # The local import avoids coupling module initialization to the publisher.
    from ingestion.full_source_campaign import coverage

    status = coverage(state).get("coverage_status")
    if status not in {"incomplete", "complete_current_catalogue"}:
        raise BulkPublicationError("bulk campaign returned an invalid coverage status")
    return status


def _accepted_receipts(
    state: Any, campaign_id: str, provider_id: str
) -> list[dict[str, Any]]:
    if (
        not isinstance(state, dict)
        or state.get("schema_version") != 1
        or state.get("source_id") != campaign_id
        or state.get("provider_id") != provider_id
    ):
        raise BulkPublicationError("bulk campaign state identity/version mismatch")
    receipts = state.get("receipts")
    accepted = state.get("accepted_responses")
    completed = state.get("completed")
    if (
        not isinstance(receipts, list)
        or type(accepted) is not int
        or accepted != len(receipts)
        or not isinstance(completed, dict)
    ):
        raise BulkPublicationError("bulk campaign receipt membership is inconsistent")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in receipts:
        if not isinstance(value, dict) or set(value) != {
            "task_id", "id", "sha256", "size_bytes"
        }:
            raise BulkPublicationError("bulk receipt descriptor has invalid fields")
        task_id = _safe_text(value.get("task_id"), "receipt task id")
        receipt_id = _safe_text(value.get("id"), "receipt object id")
        if receipt_id in seen:
            raise BulkPublicationError("bulk receipt membership contains duplicates")
        seen.add(receipt_id)
        _require_sha256(value.get("sha256"), "receipt SHA-256")
        size = value.get("size_bytes")
        if type(size) is not int or not 0 < size <= 4 * 1024 * 1024:
            raise BulkPublicationError("bulk receipt descriptor has invalid size")
        completed_item = completed.get(task_id)
        if not isinstance(completed_item, dict):
            raise BulkPublicationError("bulk receipt has no completed distribution")
        normalized.append(dict(value))
    return normalized


def _index_row(
    receipt: Any,
    descriptor: Mapping[str, Any],
    state: Mapping[str, Any],
    provider_id: str,
) -> tuple[tuple[Any, ...], int]:
    if not isinstance(receipt, dict):
        raise BulkPublicationError("bulk receipt must be an object")
    if (
        receipt.get("schema_version") != 1
        or receipt.get("source_id") != provider_id
        or receipt.get("accepted") is not True
        or receipt.get("kind") != "full_distribution"
    ):
        raise BulkPublicationError("receipt is not an accepted full distribution")
    distribution = receipt.get("distribution")
    raw = receipt.get("raw")
    inspection = receipt.get("inspection")
    if not isinstance(distribution, dict) or not isinstance(raw, dict):
        raise BulkPublicationError("bulk receipt is missing distribution or raw metadata")
    if not isinstance(inspection, dict) or inspection.get("status") != "complete":
        raise BulkPublicationError("bulk receipt inspection is not complete")

    dataset_id = _safe_text(distribution.get("dataset_id"), "dataset_id")
    kind = _safe_text(distribution.get("kind"), "distribution kind")
    version = distribution.get("version")
    if version is not None and not isinstance(version, str):
        raise BulkPublicationError("distribution version must be text or null")
    url = distribution.get("url")
    params = distribution.get("params")
    if (
        not isinstance(url, str)
        or urlsplit(url).scheme != "https"
        or not urlsplit(url).hostname
        or not isinstance(params, dict)
    ):
        raise BulkPublicationError("distribution request is invalid")
    request = {"url": url, "params": deepcopy(params)}
    _assert_no_secrets(request)

    raw_id = _safe_text(raw.get("id"), "raw file id")
    raw_name = _safe_text(raw.get("name"), "raw file name")
    raw_sha = _require_sha256(raw.get("sha256"), "raw file SHA-256")
    raw_md5 = raw.get("md5")
    raw_size = raw.get("size_bytes")
    if (
        not isinstance(raw_md5, str)
        or not _MD5_RE.fullmatch(raw_md5)
        or type(raw_size) is not int
        or raw_size <= 0
        or raw_name != f"raw-{raw_sha}.bin"
    ):
        raise BulkPublicationError("bulk raw file descriptor is invalid")
    completed = state["completed"].get(descriptor["task_id"])
    if (
        not isinstance(completed, dict)
        or completed.get("dataset_id") != dataset_id
        or completed.get("raw") != raw
        or completed.get("receipt") != {
            key: descriptor[key] for key in ("id", "sha256", "size_bytes")
        }
    ):
        raise BulkPublicationError("bulk receipt does not match completed campaign state")

    retrieved = _utc_datetime(receipt.get("retrieved_at_utc"))
    request_json = _canonical_text(request, "distribution request")
    inspection_json = _canonical_text(inspection, "distribution inspection")
    row = (
        dataset_id,
        provider_id,
        version,
        kind,
        retrieved.replace(tzinfo=None),
        raw_id,
        raw_name,
        raw_size,
        raw_sha,
        request_json,
        inspection_json,
    )
    return row, len(request_json.encode()) + len(inspection_json.encode())


def _build_fragments(rows: list[tuple[Any, ...]]) -> list[bytes]:
    """Build bounded tail fragments, splitting adaptively by rows and bytes."""
    pending = [
        rows[offset : offset + MAX_TAIL_DISTRIBUTIONS]
        for offset in range(0, len(rows), MAX_TAIL_DISTRIBUTIONS)
    ]
    payloads: list[bytes] = []
    while pending:
        group = pending.pop(0)
        payload = _parquet_bytes(group)
        if len(payload) <= MAX_TAIL_PARQUET_BYTES or len(group) == 1:
            _verify_parquet(payload)
            payloads.append(payload)
            continue
        middle = len(group) // 2
        pending[0:0] = [group[:middle], group[middle:]]
    return payloads


def _parquet_bytes(rows: list[tuple[Any, ...]]) -> bytes:
    definitions = ",".join(f'"{name}" {kind}' for name, kind in BULK_INDEX_COLUMNS)
    placeholders = ",".join("?" for _ in BULK_INDEX_COLUMNS)
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "bulk-index.parquet"
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(f"CREATE TABLE bulk_index ({definitions})")
            connection.executemany(
                f"INSERT INTO bulk_index VALUES ({placeholders})", rows
            )
            escaped = str(path).replace("'", "''")
            connection.execute(
                f"COPY bulk_index TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        finally:
            connection.close()
        return path.read_bytes()


def _verify_parquet(payload: bytes) -> int:
    return len(_read_parquet_rows(payload))


def _read_parquet_rows(payload: bytes) -> list[tuple[Any, ...]]:
    """Validate and return rows from one small authenticated tail fragment."""
    if not payload or len(payload) > MAX_LANDING_FILE_BYTES:
        raise BulkPublicationError("bulk index Parquet has invalid size")
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "bulk-index.parquet"
        path.write_bytes(payload)
        connection = duckdb.connect(":memory:")
        try:
            description = connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
            ).fetchall()
            if [(row[0], row[1]) for row in description] != list(BULK_INDEX_COLUMNS):
                raise BulkPublicationError("bulk index Parquet schema does not match contract")
            count = connection.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()[0]
            rows = connection.execute(
                "SELECT * FROM read_parquet(?)", [str(path)]
            ).fetchall()
        except BulkPublicationError:
            raise
        except Exception as exc:
            raise BulkPublicationError("bulk index publication is not valid Parquet") from exc
        finally:
            connection.close()
    if type(count) is not int or count < 1:
        raise BulkPublicationError("bulk index Parquet must contain rows")
    if count != len(rows):
        raise BulkPublicationError("bulk index Parquet row scan is inconsistent")
    return rows


def _verify_files(store: Any, files: list[dict[str, Any]], expected_rows: int) -> None:
    rows = 0
    for descriptor in files:
        rows += _verify_parquet(store.read_landing_object(descriptor))
    if rows != expected_rows:
        raise BulkPublicationError("bulk index row count does not match manifest")


def _read_manifest(
    store: Any,
    pointer: Mapping[str, Any],
    campaign_id: str,
    provider_id: str,
) -> dict[str, Any]:
    descriptor = {
        "id": pointer.get("manifest_file_id"),
        "name": pointer.get("manifest_file_name"),
        "size": pointer.get("manifest_size_bytes"),
        "sha256": pointer.get("manifest_sha256"),
    }
    raw = store.read_landing_object(
        descriptor, maximum_bytes=MAX_LANDING_MANIFEST_BYTES
    )
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BulkPublicationError("bulk index manifest is not UTF-8 JSON") from exc
    _validate_manifest(manifest, pointer, campaign_id, provider_id)
    return manifest


def _validate_manifest(
    value: Any,
    pointer: Mapping[str, Any],
    campaign_id: str,
    provider_id: str,
) -> None:
    fields = {
        "format_version", "kind", "source_id", "snapshot_id", "created_at_utc",
        "code_sha", "status", "layer", "table_name", "row_count",
        "coverage_status", "files", "columns", "accepted_distribution_count",
        "published_distribution_count", "pending_publication_count",
        "receipt_checkpoint_sha256", "tests",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise BulkPublicationError("bulk index manifest fields do not match contract")
    if (
        value["format_version"] != 2
        or value["kind"] != "full_distribution_index"
        or value["source_id"] != campaign_id
        or value["snapshot_id"] != pointer.get("snapshot_id")
        or value["status"] != "validated"
        or value["layer"] != "01_landing"
        or value["table_name"] != f"{provider_id}_distributions"
        or value["coverage_status"] not in {
            "incomplete", "complete_current_catalogue"
        }
        or value["tests"] != {"passed": True}
    ):
        raise BulkPublicationError("bulk index manifest identity/status is invalid")
    if not isinstance(value["snapshot_id"], str) or not _UUID_RE.fullmatch(value["snapshot_id"]):
        raise BulkPublicationError("bulk index snapshot identity is invalid")
    _require_code_sha(value["code_sha"])
    _utc_datetime(value["created_at_utc"])
    _require_sha256(value["receipt_checkpoint_sha256"], "receipt checkpoint SHA-256")
    if value["columns"] != [
        {"name": name, "type": kind} for name, kind in BULK_INDEX_COLUMNS
    ]:
        raise BulkPublicationError("bulk index columns do not match contract")
    files = value["files"]
    if not isinstance(files, list) or not files:
        raise BulkPublicationError("bulk index manifest has no Parquet files")
    seen_ids: set[str] = set()
    for item in files:
        if not isinstance(item, dict) or set(item) != {"id", "name", "size", "sha256"}:
            raise BulkPublicationError("bulk index file descriptor is invalid")
        file_id = _safe_text(item.get("id"), "index file id")
        name = item.get("name")
        size = item.get("size")
        if (
            not isinstance(name, str)
            or not _PARQUET_NAME_RE.fullmatch(name)
            or type(size) is not int
            or not 0 < size <= MAX_LANDING_FILE_BYTES
        ):
            raise BulkPublicationError("bulk index file descriptor is invalid")
        _require_sha256(item.get("sha256"), "index file SHA-256")
        if file_id in seen_ids:
            raise BulkPublicationError("bulk index manifest reuses a file id")
        seen_ids.add(file_id)
    counts = [
        value.get("row_count"),
        value.get("accepted_distribution_count"),
        value.get("published_distribution_count"),
        value.get("pending_publication_count"),
    ]
    if any(type(item) is not int or item < 0 for item in counts):
        raise BulkPublicationError("bulk index counts are invalid")
    row_count, accepted, published, pending = counts
    if row_count < 1 or published != row_count or accepted != published + pending:
        raise BulkPublicationError("bulk index counts are inconsistent")


def _receipt_checkpoint(receipts: list[dict[str, Any]]) -> str:
    return sha256(_canonical_bytes(receipts, "receipt checkpoint")).hexdigest()


def _canonical_text(value: Any, label: str) -> str:
    return _canonical_bytes(value, label).decode("utf-8")


def _canonical_bytes(value: Any, label: str) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise BulkPublicationError(f"{label} is not safely JSON serializable") from exc


def _assert_no_secrets(value: Any, path: str = "request") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise BulkPublicationError(f"{path} has a non-text key")
            if _SECRET_KEY_RE.search(key):
                raise BulkPublicationError(f"{path} contains an authentication field")
            _assert_no_secrets(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_no_secrets(child, f"{path}[{index}]")
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise BulkPublicationError(f"{path} contains a non-JSON value")


def _utc_datetime(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise BulkPublicationError("UTC timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BulkPublicationError("UTC timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise BulkPublicationError("timestamp must be UTC")
    return parsed


def _safe_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SAFE_TEXT_RE.fullmatch(value):
        raise BulkPublicationError(f"{label} is invalid")
    return value


def _require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise BulkPublicationError(f"{label} is invalid")
    return value


def _require_code_sha(value: Any) -> str:
    if not isinstance(value, str) or not _CODE_SHA_RE.fullmatch(value):
        raise BulkPublicationError("code_sha must be a 40-character lowercase Git SHA")
    return value


__all__ = [
    "BULK_INDEX_COLUMNS",
    "BulkPublicationError",
    "MAX_NEW_DISTRIBUTIONS",
    "MAX_TAIL_DISTRIBUTIONS",
    "MAX_TAIL_PARQUET_BYTES",
    "publish_bulk_index",
    "verify_bulk_index",
]
