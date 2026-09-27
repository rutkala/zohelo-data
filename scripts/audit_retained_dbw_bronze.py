#!/usr/bin/env python3
"""Restore and audit retained pre-release DBW Bronze files without writing Drive.

This diagnostic reconciles legacy per-indicator Landing receipts with retained
Bronze partitions, verifies every downloaded byte, and measures Parquet footer
rows and schemas.  It does not establish current provider coverage, create a
DBW release, or authorize downstream publication.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
from typing import Any, Callable
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import duckdb
from googleapiclient.http import MediaIoBaseDownload

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from storage_manager import StorageManager


DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
MAX_RECEIPT_BYTES = 1024 * 1024
EARLIER_REPORTED_OBSERVATION_ROWS = 820_345_903
EXPECTED_INDICATORS = 1550
DISK_RESERVE_BYTES = 512 * 1024 * 1024
SHA256_RE = re.compile(r"[0-9a-f]{64}")
MD5_RE = re.compile(r"[0-9a-f]{32}")

EXPECTED_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "observations": [
        ("indicator_id", "BIGINT"), ("przekroj_id", "BIGINT"),
        *[(name, "BIGINT") for index in range(1, 10) for name in (f"wymiar_{index}", f"pozycja_{index}")],
        ("okres_id", "INTEGER"), ("sposob_prezentacji_miara_id", "INTEGER"),
        ("period_year", "INTEGER"), ("wartosc_raw", "VARCHAR"),
        ("wartosc_numeric", "DOUBLE"), ("precyzja", "INTEGER"),
        ("brak_wartosci_id", "INTEGER"), ("tajnosci_id", "INTEGER"),
        ("flaga_id", "INTEGER"), ("raw_archive_file", "VARCHAR"),
        ("source_row_number", "BIGINT"), ("processed_at_utc", "VARCHAR"),
    ],
    "dictionaries": [
        ("indicator_id", "BIGINT"), ("column_name", "VARCHAR"),
        ("dictionary_name", "VARCHAR"), ("element_id", "BIGINT"),
        ("element_name", "VARCHAR"), ("processed_at_utc", "VARCHAR"),
    ],
    "taxonomy": [
        ("indicator_id", "BIGINT"), ("indicator_name", "VARCHAR"),
        ("indicator_name_en", "VARCHAR"), ("thematic_area", "VARCHAR"),
        ("domain", "VARCHAR"), ("taxonomy_path", "VARCHAR"),
        ("node_id", "VARCHAR"), ("parent_id", "VARCHAR"),
        ("processed_at_utc", "VARCHAR"),
    ],
    "metadata": [
        ("indicator_id", "BIGINT"), ("metric_name", "VARCHAR"),
        ("metric_name_en", "VARCHAR"), ("description", "VARCHAR"),
        ("frequency", "VARCHAR"), ("measure_unit", "VARCHAR"),
        ("data_source", "VARCHAR"), ("legal_basis", "VARCHAR"),
        ("last_update", "VARCHAR"), ("processed_at_utc", "VARCHAR"),
    ],
}


class RetainedDbwAuditError(RuntimeError):
    """Retained DBW evidence cannot be accepted by this diagnostic."""


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _folder(storage: StorageManager, parent_id: str, name: str) -> str:
    query = (
        f"name='{_escape(name)}' and mimeType='application/vnd.google-apps.folder' and "
        f"'{_escape(parent_id)}' in parents and trashed=false"
    )
    matches: list[dict[str, Any]] = []
    token = None
    while True:
        arguments: dict[str, Any] = {
            "q": query,
            "spaces": "drive",
            "pageSize": 1000,
            "fields": "nextPageToken,files(id,name,mimeType,trashed)",
        }
        if token:
            arguments["pageToken"] = token
        page = storage.drive_service.files().list(**arguments).execute(num_retries=4)
        matches.extend(
            item for item in page.get("files", [])
            if item.get("name") == name and item.get("trashed") is not True
        )
        token = page.get("nextPageToken")
        if not token:
            break
    if len(matches) != 1:
        raise RetainedDbwAuditError(
            f"Expected exactly one existing DBW folder {name!r}; found {len(matches)}."
        )
    return matches[0]["id"]


def _list_files(storage: StorageManager, parent_id: str) -> list[dict[str, Any]]:
    query = f"'{_escape(parent_id)}' in parents and trashed=false"
    result: list[dict[str, Any]] = []
    token = None
    while True:
        arguments: dict[str, Any] = {
            "q": query,
            "spaces": "drive",
            "pageSize": 1000,
            "fields": (
                "nextPageToken,files("
                "id,name,size,md5Checksum,sha256Checksum,appProperties,trashed)"
            ),
        }
        if token:
            arguments["pageToken"] = token
        page = storage.drive_service.files().list(**arguments).execute(num_retries=4)
        result.extend(page.get("files", []))
        token = page.get("nextPageToken")
        if not token:
            return result


def discover(storage: StorageManager) -> dict[str, list[dict[str, Any]]]:
    """Read the retained legacy paths without constructing a writer."""
    landing = storage.resolve_zone("landing", create=False)
    landing_dbw = _folder(storage, landing, "gus_dbw")
    landing_control = _folder(storage, landing_dbw, "_control")
    checkpoints = _folder(storage, landing_control, "checkpoints")

    bronze = storage.resolve_zone("bronze", create=False)
    bronze_dbw = _folder(storage, bronze, "gus_dbw")
    folders = {
        "receipts": checkpoints,
        "observations": _folder(storage, bronze_dbw, "observations"),
        "dictionaries": _folder(storage, bronze_dbw, "dictionaries"),
        "taxonomy": _folder(storage, bronze_dbw, "taxonomy"),
        "metadata": _folder(storage, bronze_dbw, "metadata"),
    }
    return {name: _list_files(storage, folder_id) for name, folder_id in folders.items()}


def _descriptor(item: dict[str, Any], logical_path: str) -> dict[str, Any]:
    props = item.get("appProperties") or {}
    sha256_hex = props.get("sha256")
    md5_hex = item.get("md5Checksum")
    try:
        size = int(item.get("size", -1))
    except (TypeError, ValueError) as exc:
        raise RetainedDbwAuditError(f"Invalid size for {logical_path}.") from exc
    if (
        not isinstance(item.get("id"), str) or not item["id"]
        or not isinstance(item.get("name"), str) or not item["name"]
        or size <= 0 or SHA256_RE.fullmatch(sha256_hex or "") is None
        or MD5_RE.fullmatch(md5_hex or "") is None
        or item.get("trashed") is True
        or (item.get("sha256Checksum") not in (None, "", sha256_hex))
    ):
        raise RetainedDbwAuditError(f"Remote object lacks valid integrity metadata: {logical_path}.")
    return {
        "path": logical_path,
        "id": item["id"],
        "name": item["name"],
        "size": size,
        "sha256": sha256_hex,
        "md5": md5_hex,
    }


def validate_inventory(
    groups: dict[str, list[dict[str, Any]]],
    *,
    expected_indicator_count: int = EXPECTED_INDICATORS,
) -> tuple[dict[str, Any], set[int]]:
    """Validate exact retained paths and reconcile receipt/partition identities."""
    expected_groups = {"receipts", "observations", "dictionaries", "taxonomy", "metadata"}
    if set(groups) != expected_groups:
        raise RetainedDbwAuditError("DBW retained inventory groups are incomplete.")
    descriptors: dict[str, Any] = {"receipts": {}, "observations": {}, "fixed": {}}
    seen_ids: set[str] = set()

    def retain(item: dict[str, Any], path: str) -> dict[str, Any]:
        value = _descriptor(item, path)
        if value["id"] in seen_ids:
            raise RetainedDbwAuditError(f"One Drive object occupies multiple retained paths: {path}.")
        seen_ids.add(value["id"])
        return value

    for group, pattern, destination in (
        ("receipts", re.compile(r"(\d+)\.json"), descriptors["receipts"]),
        ("observations", re.compile(r"part_(\d+)\.parquet"), descriptors["observations"]),
    ):
        names: set[str] = set()
        for item in groups[group]:
            name = item.get("name")
            match = pattern.fullmatch(name or "")
            if match is None:
                raise RetainedDbwAuditError(f"Unexpected retained DBW {group} object: {name!r}.")
            if name in names:
                raise RetainedDbwAuditError(f"Duplicate retained DBW path: {group}/{name}.")
            names.add(name)
            indicator_id = int(match.group(1))
            if indicator_id in destination:
                raise RetainedDbwAuditError(f"Duplicate DBW indicator identity in {group}: {indicator_id}.")
            destination[indicator_id] = retain(item, f"{group}/{name}")

    fixed = {
        "dictionaries": "br_dbw_dictionaries.parquet",
        "taxonomy": "br_dbw_indicators.parquet",
        "metadata": "br_dbw_metadata.parquet",
    }
    for group, expected_name in fixed.items():
        if len(groups[group]) != 1 or groups[group][0].get("name") != expected_name:
            raise RetainedDbwAuditError(
                f"Expected exactly one retained DBW object {group}/{expected_name}."
            )
        descriptors["fixed"][group] = retain(
            groups[group][0], f"{group}/{expected_name}"
        )

    receipt_ids = set(descriptors["receipts"])
    observation_ids = set(descriptors["observations"])
    if (
        len(receipt_ids) != expected_indicator_count
        or receipt_ids != observation_ids
    ):
        raise RetainedDbwAuditError(
            "Landing receipt indicators do not reconcile retained Bronze observation partitions."
        )
    return descriptors, receipt_ids


def inventory_document(descriptors: dict[str, Any]) -> dict[str, Any]:
    items = sorted(
        [*descriptors["receipts"].values(), *descriptors["observations"].values(),
         *descriptors["fixed"].values()],
        key=lambda item: item["path"],
    )
    canonical = json.dumps(items, sort_keys=True, separators=(",", ":")).encode()
    return {
        "format_version": 1,
        "source_id": "gus_dbw",
        "scope": "retained_pre_release_bronze_diagnostic",
        "inventory_sha256": hashlib.sha256(canonical).hexdigest(),
        "object_count": len(items),
        "total_bytes": sum(item["size"] for item in items),
        "objects": items,
    }


def _hash_path(path: Path) -> tuple[str, str, int]:
    sha256_digest = hashlib.sha256()
    md5_digest = hashlib.md5()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(DOWNLOAD_CHUNK_BYTES), b""):
            sha256_digest.update(chunk)
            md5_digest.update(chunk)
            size += len(chunk)
    return sha256_digest.hexdigest(), md5_digest.hexdigest(), size


class _BoundedWriter:
    """Reject a response before it can write beyond its pinned Drive size."""

    def __init__(self, stream, expected_size: int):
        self.stream = stream
        self.expected_size = expected_size
        self.written = 0

    def write(self, data: bytes) -> int:
        if self.written + len(data) > self.expected_size:
            raise RetainedDbwAuditError("Drive response exceeded its pinned byte size.")
        count = self.stream.write(data)
        self.written += count
        return count

    def __getattr__(self, name: str):
        return getattr(self.stream, name)


def _matches(path: Path, descriptor: dict[str, Any]) -> bool:
    if not path.is_file() or path.stat().st_size != descriptor["size"]:
        return False
    sha256_hex, md5_hex, size = _hash_path(path)
    return (
        size == descriptor["size"]
        and sha256_hex == descriptor["sha256"]
        and md5_hex == descriptor["md5"]
    )


def _download_drive(storage: StorageManager, item: dict[str, Any], target: Path) -> None:
    request = storage.drive_service.files().get_media(fileId=item["id"])
    with target.open("xb") as stream:
        bounded = _BoundedWriter(stream, item["size"])
        downloader = MediaIoBaseDownload(bounded, request, chunksize=DOWNLOAD_CHUNK_BYTES)
        complete = False
        while not complete:
            _, complete = downloader.next_chunk(num_retries=4)
        stream.flush()
        os.fsync(stream.fileno())


def restore_verified(
    storage: StorageManager,
    descriptor: dict[str, Any],
    target: Path,
    download: Callable[[StorageManager, dict[str, Any], Path], None] = _download_drive,
) -> bool:
    """Restore one verified object atomically; return True when cache was reused."""
    target.parent.mkdir(parents=True, exist_ok=True)
    if _matches(target, descriptor):
        return True
    temporary = target.with_name(f".{target.name}.{uuid.uuid4()}.tmp")
    try:
        download(storage, descriptor, temporary)
        if not _matches(temporary, descriptor):
            raise RetainedDbwAuditError(
                f"Downloaded DBW object failed byte verification: {descriptor['path']}."
            )
        os.replace(temporary, target)
        directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return False
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def validate_receipt(path: Path, indicator_id: int) -> dict[str, Any]:
    if path.stat().st_size > MAX_RECEIPT_BYTES:
        raise RetainedDbwAuditError(f"DBW receipt exceeds size bound: {indicator_id}.json.")
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RetainedDbwAuditError(f"DBW receipt is not valid UTF-8 JSON: {indicator_id}.json.") from exc
    if not isinstance(receipt, dict):
        raise RetainedDbwAuditError(
            f"DBW legacy receipt is not a completed indicator record: {indicator_id}.json."
        )
    files = receipt.get("files_landed")
    if (
        receipt.get("indicator_id") != indicator_id
        or not isinstance(receipt.get("name"), str) or not receipt["name"]
        or receipt.get("status") != "completed"
        or not isinstance(files, list) or not files
        or any(not isinstance(name, str) or not name for name in files)
        or not isinstance(receipt.get("updated_at_utc"), str)
        or not receipt["updated_at_utc"]
    ):
        raise RetainedDbwAuditError(f"DBW legacy receipt is not a completed indicator record: {indicator_id}.json.")
    return receipt


def parquet_evidence(path: Path, expected_schema: list[tuple[str, str]]) -> dict[str, Any]:
    connection = duckdb.connect(":memory:")
    try:
        described = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
        schema = [(row[0], row[1]) for row in described]
        if schema != expected_schema:
            raise RetainedDbwAuditError(f"Unexpected DBW Parquet schema: {path.name}.")
        rows = connection.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]
    except duckdb.Error as exc:
        raise RetainedDbwAuditError(f"DBW Parquet footer could not be read: {path.name}.") from exc
    finally:
        connection.close()
    return {"rows": rows, "columns": [{"name": name, "type": kind} for name, kind in schema]}


def _all_descriptors(descriptors: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        *descriptors["receipts"].values(),
        *descriptors["observations"].values(),
        *descriptors["fixed"].values(),
    ]


def _cache_path(cache: Path, descriptor: dict[str, Any]) -> Path:
    return cache / descriptor["path"]


def _require_disk_space(cache: Path, descriptors: dict[str, Any]) -> int:
    required = sum(
        descriptor["size"]
        for descriptor in _all_descriptors(descriptors)
        if not _matches(_cache_path(cache, descriptor), descriptor)
    )
    available = shutil.disk_usage(cache.parent).free
    if available < required + DISK_RESERVE_BYTES:
        raise RetainedDbwAuditError(
            "Insufficient local disk space for the retained DBW verified cache."
        )
    return required


def _run_status(output_dir: Path, run_id: str, status: str, **extra: Any) -> None:
    _atomic_json(output_dir / "run-status.json", {
        "format_version": 1,
        "source_id": "gus_dbw",
        "run_id": run_id,
        "status": status,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        **extra,
    })


def prefetch_verified(storage, descriptors, cache, *, workers=4, reader_factory=None, on_progress=None):
    """Bounded read-only restore with one independent Drive client per worker.

    No shared httplib2 transport, remote mutation, or unverified cache reuse. A
    failed worker is propagated after all workers have joined; partial cache
    files retain only successfully verified bytes, never successful run status.
    """
    if type(workers) is not int or not 1 <= workers <= 4:
        raise RetainedDbwAuditError("Restore workers must be between one and four")
    if reader_factory is None:
        root_id = storage.resolve_root(create=False)
        reader_factory = lambda: StorageManager(allow_interactive_auth=False, root_id=root_id)
    local = threading.local()
    def restore(item):
        if not hasattr(local, "reader"):
            local.reader = reader_factory()
        reused = restore_verified(local.reader, item, _cache_path(cache, item))
        return item["path"], reused
    result = {}
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dbw-read-only") as pool:
        futures = [pool.submit(restore, item) for item in descriptors]
        for future in as_completed(futures):
            path, reused = future.result()
            result[path] = reused
            if on_progress is not None:
                on_progress(len(result), len(descriptors))
    return result


def _audit_locked(
    storage: StorageManager, output_dir: Path, run_id: str, *, workers: int = 1
) -> dict[str, Any]:
    cache = output_dir / "verified-cache"
    groups = discover(storage)
    descriptors, indicator_ids = validate_inventory(groups)
    pinned = inventory_document(descriptors)
    _atomic_json(output_dir / "descriptor-inventory.json", pinned)
    required_download_bytes = _require_disk_space(cache, descriptors)

    progress = {
        "format_version": 1,
        "source_id": "gus_dbw",
        "run_id": run_id,
        "inventory_sha256": pinned["inventory_sha256"],
        "objects_total": pinned["object_count"],
        "objects_verified": 0,
        "bytes_verified": 0,
        "status": "running",
        "cache_files_reused": 0,
        "cache_files_downloaded": 0,
        "last_verified_path": None,
    }

    def record(descriptor: dict[str, Any], was_reused: bool) -> None:
        progress["objects_verified"] += 1
        progress["bytes_verified"] += descriptor["size"]
        progress["cache_files_reused"] += was_reused
        progress["cache_files_downloaded"] += not was_reused
        progress["last_verified_path"] = descriptor["path"]
        progress["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        _atomic_json(output_dir / "progress.json", progress)

    _atomic_json(output_dir / "progress.json", progress)

    prefetched = {}
    if workers > 1:
        def restore_progress(done, total):
            if done % 100 == 0 or done == total:
                print(json.dumps({"operation": "restore_existing_drive_inputs", "objects_verified": done,
                                  "objects_total": total, "read_only": True}), flush=True)
        prefetched = prefetch_verified(storage, _all_descriptors(descriptors), cache,
                                       workers=workers, on_progress=restore_progress)
    def restore_input(descriptor, path):
        reused = restore_verified(storage, descriptor, path)
        return prefetched.get(descriptor["path"], reused)

    receipt_names: set[str] = set()
    for indicator_id in sorted(indicator_ids):
        descriptor = descriptors["receipts"][indicator_id]
        path = _cache_path(cache, descriptor)
        was_reused = restore_input(descriptor, path)
        receipt = validate_receipt(path, indicator_id)
        receipt_names.update(receipt["files_landed"])
        record(descriptor, was_reused)

    # Validate the small fixed relations before restoring 4.7+ GB of observations.
    fixed_schema = {
        "dictionaries": "dictionaries",
        "taxonomy": "taxonomy",
        "metadata": "metadata",
    }
    fixed_rows: dict[str, int] = {}
    schema_evidence: dict[str, Any] = {}
    for group, schema_name in fixed_schema.items():
        descriptor = descriptors["fixed"][group]
        path = _cache_path(cache, descriptor)
        was_reused = restore_input(descriptor, path)
        evidence = parquet_evidence(path, EXPECTED_SCHEMAS[schema_name])
        fixed_rows[group] = evidence["rows"]
        schema_evidence[group] = evidence["columns"]
        record(descriptor, was_reused)

    observation_rows = 0
    for indicator_id in sorted(indicator_ids):
        descriptor = descriptors["observations"][indicator_id]
        path = _cache_path(cache, descriptor)
        was_reused = restore_input(descriptor, path)
        evidence = parquet_evidence(path, EXPECTED_SCHEMAS["observations"])
        observation_rows += evidence["rows"]
        schema_evidence.setdefault("observations", evidence["columns"])
        record(descriptor, was_reused)

    rechecked_descriptors, rechecked_ids = validate_inventory(discover(storage))
    rechecked = inventory_document(rechecked_descriptors)
    if rechecked_ids != indicator_ids or rechecked["inventory_sha256"] != pinned["inventory_sha256"]:
        raise RetainedDbwAuditError("Remote DBW retained inventory changed during the audit.")
    progress["status"] = "complete"
    progress["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    _atomic_json(output_dir / "progress.json", progress)

    report = {
        "format_version": 1,
        "status": "retained_outputs_audited",
        "source_id": "gus_dbw",
        "run_id": run_id,
        "audited_at_utc": datetime.now(timezone.utc).isoformat(),
        "diagnostic_scope": "retained_pre_release_bronze",
        "inventory_sha256": pinned["inventory_sha256"],
        "expected_retained_indicator_inventory": EXPECTED_INDICATORS,
        "indicator_receipts": len(indicator_ids),
        "observation_partitions": len(indicator_ids),
        "remote_object_count": pinned["object_count"],
        "remote_total_bytes": pinned["total_bytes"],
        "cache_files_reused": progress["cache_files_reused"],
        "cache_files_downloaded": progress["cache_files_downloaded"],
        "required_download_bytes_at_start": required_download_bytes,
        "receipt_reported_native_names": len(receipt_names),
        "measured_parquet_rows": {
            "observations": observation_rows,
            **fixed_rows,
        },
        "earlier_reported_observation_rows": EARLIER_REPORTED_OBSERVATION_ROWS,
        "measured_observation_rows_match_earlier_report": (
            observation_rows == EARLIER_REPORTED_OBSERVATION_ROWS
        ),
        "schemas": schema_evidence,
        "remote_inventory_stable_after_restore": True,
        "limitations": [
            "This is a diagnostic of retained Landing receipts and Bronze bytes, not a #136 release.",
            "The 1,550-indicator expectation is dated retained inventory, not source completeness.",
            "It does not prove current provider catalogue completeness or current provider coverage.",
            "Legacy receipt membership does not prove native-to-Bronze value lineage.",
            "It does not create a completion marker, publish data, or authorize downstream execution.",
        ],
    }
    _atomic_json(output_dir / "audit-report.json", report)
    return report


def audit_retained_dbw(storage: StorageManager, output_dir: Path, *, workers: int = 1) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    with (output_dir / "audit.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RetainedDbwAuditError(
                "Another retained DBW audit owns this output directory."
            ) from exc
        _run_status(output_dir, run_id, "running", started_at_utc=datetime.now(timezone.utc).isoformat())
        try:
            report = _audit_locked(storage, output_dir, run_id, workers=workers)
        except BaseException as exc:
            _run_status(
                output_dir,
                run_id,
                "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "failed",
                error_type=type(exc).__name__,
            )
            raise
        _run_status(
            output_dir,
            run_id,
            "complete",
            audit_report="audit-report.json",
            inventory_sha256=report["inventory_sha256"],
        )
        return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    storage = StorageManager(allow_interactive_auth=False)
    report = audit_retained_dbw(storage, args.output_dir)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
