"""Incremental publication of accepted source responses as queryable Landing rows.

This module copies verified transport envelopes into Parquet.  It deliberately
does not interpret provider records as business facts; dbt remains the business
transformation boundary.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping
from uuid import uuid4

import duckdb

from ingestion.source_campaign_store import (
    CampaignCapacityError,
    CampaignStoreError,
    MAX_LANDING_FILE_BYTES,
    MAX_LANDING_MANIFEST_BYTES,
)


MAX_NEW_RESPONSES = 24
MAX_NEW_RAW_BYTES = 16 * 1024 * 1024

LANDING_COLUMNS = (
    ("source_id", "VARCHAR"),
    ("task_id", "VARCHAR"),
    ("lane", "VARCHAR"),
    ("task_kind", "VARCHAR"),
    ("retrieved_at_utc", "TIMESTAMP"),
    ("record_count", "BIGINT"),
    ("raw_sha256", "VARCHAR"),
    ("raw_size_bytes", "BIGINT"),
    ("request_json", "VARCHAR"),
    ("metadata_json", "VARCHAR"),
    ("payload_utf8", "VARCHAR"),
    ("content_type", "VARCHAR"),
)

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_TEXT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/+\-]{0,511}$")
_SECRET_KEY_RE = re.compile(
    r"(?:auth|credential|password|secret|token|api[-_]?key|subscription[-_]?key)", re.I
)


class LandingPublicationError(CampaignStoreError):
    """A Landing snapshot candidate failed admission or verification."""


def publish_landing(store: Any, adapter: Any, code_sha: str) -> dict[str, Any] | None:
    """Publish one bounded accepted-receipt prefix and promote it atomically.

    The returned manifest is the verified current snapshot.  ``None`` means the
    campaign has no accepted response and therefore has no publishable snapshot.
    """
    source_id = _source_identity(store, adapter)
    _require_code_sha(code_sha)
    state = store.load()
    if state is None:
        return None
    receipts = _accepted_receipts(state, source_id)

    pointer = store.load_landing_pointer()
    previous = _read_snapshot(store, pointer) if pointer is not None else None
    published = 0 if previous is None else previous["published_response_count"]
    previous_files = [] if previous is None else previous["files"]

    if published > len(receipts):
        raise LandingPublicationError("Landing snapshot publishes receipts absent from campaign state")
    if previous is not None:
        expected_checkpoint = _receipt_checkpoint(receipts[:published])
        if previous["receipt_checkpoint_sha256"] != expected_checkpoint:
            raise LandingPublicationError("Landing receipt checkpoint does not match campaign state")
    if published == len(receipts):
        if previous is None:
            return None
        if previous["accepted_response_count"] != len(receipts):
            raise LandingPublicationError("Landing accepted response count is stale")
        return previous

    rows: list[tuple[Any, ...]] = []
    raw_bytes = 0
    for descriptor in receipts[published:]:
        if len(rows) >= MAX_NEW_RESPONSES:
            break
        receipt = store.read_receipt(descriptor)
        raw_descriptor = _validate_accepted_receipt(
            receipt, descriptor, source_id, adapter
        )
        body = store.read_raw(raw_descriptor)
        if rows and raw_bytes + len(body) > MAX_NEW_RAW_BYTES:
            break
        if raw_bytes + len(body) > MAX_NEW_RAW_BYTES:
            raise CampaignCapacityError("one accepted response exceeds the Landing batch limit")
        rows.append(_transport_row(receipt, body, adapter))
        raw_bytes += len(body)

    if not rows:  # guarded by the per-object and batch limits
        raise LandingPublicationError("no accepted response could be admitted to Landing")

    parquet_payloads = _build_fragments(rows)
    new_files: list[dict[str, Any]] = []
    for payload in parquet_payloads:
        name = f"fragment-{uuid4()}.parquet"
        new_files.append(store.put_landing_object(name, payload))

    snapshot_id = str(uuid4())
    now = datetime.now(timezone.utc).isoformat()
    new_published = published + len(rows)
    all_files = [dict(item) for item in previous_files] + new_files
    manifest = {
        "format_version": 1,
        "kind": "landing_snapshot",
        "source_id": source_id,
        "snapshot_id": snapshot_id,
        "created_at_utc": now,
        "code_sha": code_sha,
        "status": "validated",
        "layer": "01_landing",
        "table_name": f"{source_id}_responses",
        "row_count": new_published,
        "coverage_status": "incomplete",
        "files": all_files,
        "columns": [{"name": name, "type": kind} for name, kind in LANDING_COLUMNS],
        "accepted_response_count": len(receipts),
        "published_response_count": new_published,
        "pending_publication_count": len(receipts) - new_published,
        "receipt_checkpoint_sha256": _receipt_checkpoint(receipts[:new_published]),
        "tests": {"passed": True},
    }
    manifest_raw = _canonical_object(manifest, "Landing manifest")
    if len(manifest_raw) > MAX_LANDING_MANIFEST_BYTES:
        raise CampaignCapacityError("Landing manifest exceeds the 1 MiB safety limit")
    manifest_descriptor = store.put_landing_object(
        f"manifest-{snapshot_id}.json",
        manifest_raw,
        maximum_bytes=MAX_LANDING_MANIFEST_BYTES,
    )
    pointer_value = {
        "format_version": 1,
        "source_id": source_id,
        "snapshot_id": snapshot_id,
        "manifest_file_id": manifest_descriptor["id"],
        "manifest_file_name": manifest_descriptor["name"],
        "manifest_sha256": manifest_descriptor["sha256"],
        "manifest_size_bytes": manifest_descriptor["size"],
    }
    # Validate the complete candidate through the same reader used by consumers
    # before changing the only mutable publication object.
    candidate = _read_snapshot(store, pointer_value)
    if candidate["snapshot_id"] != snapshot_id:
        raise LandingPublicationError("Landing snapshot candidate identity changed")
    store.promote_landing_pointer(pointer_value)
    verified = verify_landing(store)
    if verified is None or verified["snapshot_id"] != snapshot_id:
        raise LandingPublicationError("promoted Landing snapshot was not readable")
    return verified


def verify_landing(store: Any) -> dict[str, Any] | None:
    """Verify the current source snapshot from a fresh pointer/object read."""
    pointer = store.load_landing_pointer()
    return None if pointer is None else _read_snapshot(store, pointer)


def _read_snapshot(store: Any, pointer: Mapping[str, Any]) -> dict[str, Any]:
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
        raise LandingPublicationError("Landing manifest is not valid UTF-8 JSON") from exc
    _validate_manifest(manifest, pointer, store.source_id)

    actual_rows = 0
    for file_descriptor in manifest["files"]:
        parquet = store.read_landing_object(file_descriptor)
        actual_rows += _verify_parquet(parquet)
    if actual_rows != manifest["row_count"]:
        raise LandingPublicationError("Landing Parquet row count does not match manifest")
    return manifest


def _accepted_receipts(state: Any, source_id: str) -> list[dict[str, Any]]:
    if not isinstance(state, dict):
        raise LandingPublicationError("campaign state must be an object")
    if state.get("schema_version") != 1 or state.get("source_id") != source_id:
        raise LandingPublicationError("campaign state identity/version mismatch")
    receipts = state.get("receipts")
    rejected = state.get("rejected_receipts", [])
    accepted_count = state.get("accepted_responses")
    if (
        not isinstance(receipts, list)
        or not isinstance(rejected, list)
        or type(accepted_count) is not int
        or accepted_count < 0
        or accepted_count != len(receipts)
    ):
        raise LandingPublicationError("campaign accepted receipt membership is inconsistent")
    accepted_ids: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for item in receipts:
        normalized_item = _receipt_descriptor(item)
        if normalized_item["id"] in accepted_ids:
            raise LandingPublicationError("campaign accepted receipt membership contains duplicates")
        accepted_ids.add(normalized_item["id"])
        normalized.append(normalized_item)
    for item in rejected:
        if isinstance(item, dict) and item.get("id") in accepted_ids:
            raise LandingPublicationError("a rejected receipt is present in accepted membership")
    return normalized


def _receipt_descriptor(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"task_id", "id", "sha256", "size_bytes"}:
        raise LandingPublicationError("accepted receipt descriptor has invalid fields")
    _safe_text(value["task_id"], "receipt task id")
    _safe_text(value["id"], "receipt object id", limit=1024)
    _require_digest(value["sha256"], "receipt SHA-256")
    if type(value["size_bytes"]) is not int or not 0 < value["size_bytes"] <= 4 * 1024 * 1024:
        raise LandingPublicationError("accepted receipt descriptor has invalid size")
    return dict(value)


def _validate_accepted_receipt(
    receipt: Any,
    descriptor: Mapping[str, Any],
    source_id: str,
    adapter: Any,
) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        raise LandingPublicationError("accepted receipt must be an object")
    if (
        receipt.get("schema_version") != 1
        or receipt.get("source_id") != source_id
        or receipt.get("accepted") is not True
        or receipt.get("http_status") != 200
    ):
        raise LandingPublicationError("receipt is not an accepted HTTP 200 response")
    task = receipt.get("task")
    if not isinstance(task, dict) or task.get("id") != descriptor["task_id"]:
        raise LandingPublicationError("accepted receipt task identity is inconsistent")
    for field in ("id", "lane", "kind"):
        _safe_text(task.get(field), f"task {field}")
    request = receipt.get("request")
    try:
        replayed_request = adapter.request_for(task)
    except Exception as exc:
        raise LandingPublicationError("accepted receipt request replay failed") from exc
    if not isinstance(request, dict) or replayed_request != request:
        raise LandingPublicationError("accepted receipt request does not replay exactly")
    _assert_no_auth(request)
    raw = receipt.get("raw")
    if not isinstance(raw, dict):
        raise LandingPublicationError("accepted receipt has no raw response descriptor")
    if type(receipt.get("record_count")) is not int or receipt["record_count"] < 0:
        raise LandingPublicationError("accepted receipt record count is invalid")
    if not isinstance(receipt.get("metadata", {}), dict):
        raise LandingPublicationError("accepted receipt metadata is invalid")
    _retrieved_datetime(receipt.get("retrieved_at_utc"))
    headers = receipt.get("headers", {})
    if not isinstance(headers, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in headers.items()):
        raise LandingPublicationError("accepted receipt headers are invalid")
    return raw


def _transport_row(receipt: Mapping[str, Any], body: bytes, adapter: Any) -> tuple[Any, ...]:
    try:
        payload = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise LandingPublicationError("accepted response is not UTF-8 transport text") from exc
    retrieved = _retrieved_datetime(receipt["retrieved_at_utc"])
    try:
        replay = adapter.interpret(receipt["task"], body, retrieved.date())
    except Exception as exc:
        raise LandingPublicationError("adapter replay rejected the accepted response") from exc
    if (
        not isinstance(replay, dict)
        or replay.get("record_count") != receipt["record_count"]
        or replay.get("metadata", {}) != receipt.get("metadata", {})
    ):
        raise LandingPublicationError("adapter replay does not match accepted receipt")
    raw = receipt["raw"]
    headers = receipt.get("headers", {})
    return (
        receipt["source_id"],
        receipt["task"]["id"],
        receipt["task"]["lane"],
        receipt["task"]["kind"],
        retrieved.replace(tzinfo=None),
        receipt["record_count"],
        _require_digest(raw.get("sha256"), "raw response SHA-256"),
        _positive_size(raw.get("size_bytes"), "raw response"),
        _canonical_text(receipt["request"], "request"),
        _canonical_text(receipt.get("metadata", {}), "metadata"),
        payload,
        headers.get("content-type"),
    )


def _build_fragments(rows: list[tuple[Any, ...]]) -> list[bytes]:
    groups: list[list[tuple[Any, ...]]] = []
    current: list[tuple[Any, ...]] = []
    current_raw = 0
    # A 4 MiB input target leaves headroom below the 8 MiB object bound even
    # for weakly compressible text.  Every payload is still measured after COPY.
    target = MAX_LANDING_FILE_BYTES // 2
    for row in rows:
        size = row[7]
        if current and current_raw + size > target:
            groups.append(current)
            current, current_raw = [], 0
        current.append(row)
        current_raw += size
    if current:
        groups.append(current)
    payloads = [_parquet_bytes(group) for group in groups]
    for payload in payloads:
        if len(payload) > MAX_LANDING_FILE_BYTES:
            raise CampaignCapacityError("Landing Parquet fragment exceeds the 8 MiB safety limit")
        _verify_parquet(payload)
    return payloads


def _parquet_bytes(rows: list[tuple[Any, ...]]) -> bytes:
    definitions = ",".join(f'"{name}" {kind}' for name, kind in LANDING_COLUMNS)
    placeholders = ",".join("?" for _ in LANDING_COLUMNS)
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "fragment.parquet"
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(f"CREATE TABLE landing_fragment ({definitions})")
            connection.executemany(
                f"INSERT INTO landing_fragment VALUES ({placeholders})", rows
            )
            escaped = str(path).replace("'", "''")
            connection.execute(
                f"COPY landing_fragment TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        finally:
            connection.close()
        return path.read_bytes()


def _verify_parquet(payload: bytes) -> int:
    if not payload or len(payload) > MAX_LANDING_FILE_BYTES:
        raise LandingPublicationError("Landing Parquet fragment has invalid size")
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "fragment.parquet"
        path.write_bytes(payload)
        connection = duckdb.connect(":memory:")
        try:
            escaped = str(path).replace("'", "''")
            description = connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{escaped}')"
            ).fetchall()
            actual = [(row[0], row[1]) for row in description]
            if actual != list(LANDING_COLUMNS):
                raise LandingPublicationError("Landing Parquet schema does not match contract")
            count = connection.execute(
                f"SELECT count(*) FROM read_parquet('{escaped}')"
            ).fetchone()[0]
        except LandingPublicationError:
            raise
        except Exception as exc:
            raise LandingPublicationError("Landing publication object is not valid Parquet") from exc
        finally:
            connection.close()
    if type(count) is not int or count < 1:
        raise LandingPublicationError("Landing Parquet fragment must contain rows")
    return count


def _validate_manifest(value: Any, pointer: Mapping[str, Any], source_id: str) -> None:
    required = {
        "format_version", "kind", "source_id", "snapshot_id", "created_at_utc",
        "code_sha", "status", "layer", "table_name", "row_count",
        "coverage_status", "files", "columns", "accepted_response_count",
        "published_response_count", "pending_publication_count",
        "receipt_checkpoint_sha256", "tests",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise LandingPublicationError("Landing manifest fields do not match contract")
    if (
        value["format_version"] != 1
        or value["kind"] != "landing_snapshot"
        or value["source_id"] != source_id
        or value["status"] != "validated"
        or value["layer"] != "01_landing"
        or value["table_name"] != f"{source_id}_responses"
        or value["coverage_status"] != "incomplete"
        or value["tests"] != {"passed": True}
    ):
        raise LandingPublicationError("Landing manifest identity/status is invalid")
    snapshot_id = value["snapshot_id"]
    if not isinstance(snapshot_id, str) or not _UUID_RE.fullmatch(snapshot_id):
        raise LandingPublicationError("Landing snapshot identity is invalid")
    if snapshot_id != pointer.get("snapshot_id"):
        raise LandingPublicationError("Landing pointer and manifest snapshot differ")
    _require_code_sha(value["code_sha"])
    _retrieved_datetime(value["created_at_utc"])
    _require_digest(value["receipt_checkpoint_sha256"], "receipt checkpoint SHA-256")
    if value["columns"] != [{"name": n, "type": t} for n, t in LANDING_COLUMNS]:
        raise LandingPublicationError("Landing manifest columns do not match contract")
    files = value["files"]
    if not isinstance(files, list) or not files:
        raise LandingPublicationError("Landing manifest must contain Parquet files")
    names: set[str] = set()
    ids: set[str] = set()
    for item in files:
        if not isinstance(item, dict) or set(item) != {"id", "name", "size", "sha256"}:
            raise LandingPublicationError("Landing file descriptor fields are invalid")
        if (
            not isinstance(item["name"], str)
            or not re.fullmatch(r"fragment-" + _UUID_RE.pattern[1:-1] + r"\.parquet", item["name"])
        ):
            raise LandingPublicationError("Landing fragment name is unsafe")
        _safe_text(item["id"], "Landing file id", limit=1024)
        _positive_size(item["size"], "Landing file", maximum=MAX_LANDING_FILE_BYTES)
        _require_digest(item["sha256"], "Landing file SHA-256")
        if item["name"] in names or item["id"] in ids:
            raise LandingPublicationError("Landing file descriptors contain duplicates")
        names.add(item["name"])
        ids.add(item["id"])
    counts = [
        value["row_count"], value["accepted_response_count"],
        value["published_response_count"], value["pending_publication_count"],
    ]
    if any(type(item) is not int or item < 0 for item in counts):
        raise LandingPublicationError("Landing manifest counts are invalid")
    if (
        value["row_count"] < 1
        or value["row_count"] != value["published_response_count"]
        or value["accepted_response_count"]
        != value["published_response_count"] + value["pending_publication_count"]
    ):
        raise LandingPublicationError("Landing manifest counts are inconsistent")


def _source_identity(store: Any, adapter: Any) -> str:
    source_id = getattr(store, "source_id", None)
    if source_id != getattr(adapter, "SOURCE_ID", None):
        raise LandingPublicationError("store and adapter source identities differ")
    _safe_text(source_id, "source id", limit=120)
    return source_id


def _receipt_checkpoint(receipts: list[dict[str, Any]]) -> str:
    raw = json.dumps(
        receipts, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return sha256(raw).hexdigest()


def _canonical_object(value: Any, label: str) -> bytes:
    if not isinstance(value, dict):
        raise LandingPublicationError(f"{label} must be an object")
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise LandingPublicationError(f"{label} is not safely JSON serializable") from exc


def _canonical_text(value: Any, label: str) -> str:
    return _canonical_object(value, label).decode("utf-8")


def _assert_no_auth(value: Any, path: str = "request") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str) or _SECRET_KEY_RE.search(key):
                raise LandingPublicationError(f"{path} contains an authentication field")
            _assert_no_auth(child, f"{path}.{key}")
    elif isinstance(value, list):
        for child in value:
            _assert_no_auth(child, path)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise LandingPublicationError(f"{path} contains a non-JSON value")


def _retrieved_datetime(value: Any) -> datetime:
    if not isinstance(value, str) or len(value) > 40:
        raise LandingPublicationError("UTC timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LandingPublicationError("UTC timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise LandingPublicationError("timestamp must be UTC")
    return parsed


def _require_code_sha(value: Any) -> str:
    return _safe_text(value, "code SHA", limit=80)


def _require_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA_RE.fullmatch(value):
        raise LandingPublicationError(f"{label} is invalid")
    return value


def _positive_size(value: Any, label: str, maximum: int = 8 * 1024 * 1024) -> int:
    if type(value) is not int or not 0 < value <= maximum:
        raise LandingPublicationError(f"{label} size is invalid")
    return value


def _safe_text(value: Any, label: str, *, limit: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > limit
        or not _SAFE_TEXT_RE.fullmatch(value)
    ):
        raise LandingPublicationError(f"{label} is unsafe")
    return value


__all__ = [
    "LANDING_COLUMNS",
    "LandingPublicationError",
    "MAX_LANDING_FILE_BYTES",
    "MAX_LANDING_MANIFEST_BYTES",
    "MAX_NEW_RAW_BYTES",
    "MAX_NEW_RESPONSES",
    "publish_landing",
    "verify_landing",
]
