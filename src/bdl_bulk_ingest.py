"""Store an authenticated BDL Web download unchanged in Landing.

This module performs transfer bookkeeping only. It never opens an archive,
parses CSV, counts records, infers a schema, or produces Parquet. A completion
marker means that the native object was stored, not that its data was validated.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import md5, sha256
import json
import os
from pathlib import Path
import re
from typing import Any

from googleapiclient.http import MediaFileUpload, MediaInMemoryUpload

from storage_manager import StorageManager

_SUBGROUP_RE = re.compile(r"^P[0-9]+$")
_MAX_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024
_CHUNK_BYTES = 8 * 1024 * 1024


def _hash_file(path: Path, algorithm: str) -> str:
    digest = sha256() if algorithm == "sha256" else md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _escape_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _find_exact_file(storage: StorageManager, name: str, parent_id: str) -> list[dict[str, Any]]:
    query = f"name='{_escape_query(name)}' and '{_escape_query(parent_id)}' in parents and trashed=false"
    result: list[dict[str, Any]] = []
    token = None
    while True:
        args: dict[str, Any] = {"q": query, "spaces": "drive", "fields": "nextPageToken, files(id,name,size,md5Checksum,appProperties,trashed)"}
        if token:
            args["pageToken"] = token
        response = storage.drive_service.files().list(**args).execute(num_retries=4)
        result.extend(response.get("files", []))
        token = response.get("nextPageToken")
        if not token:
            break
    return [item for item in result if item.get("name") == name and item.get("trashed") is not True]


def _upload_file(storage: StorageManager, local_path: Path, *, name: str, parent_id: str, sha256_hex: str, md5_hex: str, kind: str) -> dict[str, Any]:
    existing = _find_exact_file(storage, name, parent_id)
    if len(existing) > 1:
        raise RuntimeError(f"Ambiguous existing BDL bulk object: {name}")
    if existing:
        item = existing[0]
        props = item.get("appProperties") or {}
        if props.get("sha256") != sha256_hex or item.get("md5Checksum") != md5_hex or int(item.get("size", -1)) != local_path.stat().st_size:
            raise RuntimeError(f"Existing BDL bulk object conflicts with local bytes: {name}")
        return {"id": item["id"], "name": name, "size": int(item["size"]), "sha256": sha256_hex, "md5": md5_hex, "reused": True}
    metadata = {"name": name, "parents": [parent_id], "appProperties": {"sha256": sha256_hex, "kind": kind, "source_id": "gus_bdl", "transport": "web_bulk"}}
    media = MediaFileUpload(str(local_path), resumable=True, chunksize=_CHUNK_BYTES)
    request = storage.drive_service.files().create(body=metadata, media_body=media, fields="id,name,size,md5Checksum,appProperties")
    response = None
    while response is None:
        _, response = request.next_chunk(num_retries=4)
    if not response or response.get("name") != name or int(response.get("size", -1)) != local_path.stat().st_size or response.get("md5Checksum") != md5_hex or (response.get("appProperties") or {}).get("sha256") != sha256_hex:
        raise RuntimeError(f"BDL bulk Drive upload did not verify: {name}")
    return {"id": response["id"], "name": name, "size": int(response["size"]), "sha256": sha256_hex, "md5": md5_hex, "reused": False}


def _upload_manifest(storage: StorageManager, manifest: dict[str, Any], *, parent_id: str) -> dict[str, Any]:
    raw = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = sha256(raw).hexdigest()
    md5_digest = md5(raw).hexdigest()
    name = f"manifest-{digest}.json"
    existing = _find_exact_file(storage, name, parent_id)
    if len(existing) > 1:
        raise RuntimeError("Ambiguous BDL bulk manifest")
    if existing:
        item = existing[0]
        properties = item.get("appProperties") or {}
        if int(item.get("size", -1)) != len(raw) or item.get("md5Checksum") != md5_digest or properties.get("sha256") != digest:
            raise RuntimeError("Existing BDL bulk manifest conflicts with expected bytes")
        return {"id": item["id"], "name": name, "sha256": digest, "size": len(raw), "reused": True}
    media = MediaInMemoryUpload(raw, mimetype="application/json", resumable=False)
    response = storage.drive_service.files().create(body={"name": name, "parents": [parent_id], "appProperties": {"sha256": digest, "kind": "manifest", "source_id": "gus_bdl", "transport": "web_bulk"}}, media_body=media, fields="id,name,size,md5Checksum,appProperties").execute(num_retries=4)
    if not response or response.get("name") != name or int(response.get("size", -1)) != len(raw) or response.get("md5Checksum") != md5_digest or (response.get("appProperties") or {}).get("sha256") != digest:
        raise RuntimeError("BDL bulk manifest upload did not verify")
    return {"id": response["id"], "name": name, "sha256": digest, "size": len(raw), "reused": False}


def _upload_completion_marker(
    storage: StorageManager,
    *,
    control_id: str,
    subgroup_id: str,
    archive_sha: str,
    manifest_object: dict[str, Any],
    archive_bytes: int | None = None,
    row_count: int | None = None,
) -> dict[str, Any]:
    """Checkpoint native transfer only; row_count is a retired compatibility argument."""
    payload = {
        "format_version": 2,
        "source_id": "gus_bdl",
        "transport": "web_bulk",
        "subgroup_id": subgroup_id,
        "status": "landed",
        "landing_scope": "native_bytes_only",
        "content_validation": "not_performed",
        "archive_sha256": archive_sha,
        "archive_bytes": archive_bytes,
        "manifest_object": manifest_object,
        "completed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    name = f"{subgroup_id}.json"
    properties = {
        "source_id": "gus_bdl", "transport": "web_bulk", "subgroup_id": subgroup_id,
        "status": "landed", "manifest_sha256": manifest_object["sha256"],
    }
    existing = _find_exact_file(storage, name, control_id)
    if len(existing) > 1:
        raise RuntimeError("Ambiguous BDL bulk completion marker")
    if existing:
        item = existing[0]
        if (item.get("appProperties") or {}) != properties:
            raise RuntimeError("Existing BDL bulk completion marker conflicts with snapshot")
        return {"id": item["id"], "name": name, "reused": True}
    media = MediaInMemoryUpload(raw, mimetype="application/json", resumable=False)
    response = storage.drive_service.files().create(
        body={"name": name, "parents": [control_id], "appProperties": properties},
        media_body=media, fields="id,name,size,appProperties",
    ).execute(num_retries=4)
    if not response or response.get("name") != name or int(response.get("size", -1)) != len(raw) or (response.get("appProperties") or {}) != properties:
        raise RuntimeError("BDL bulk completion marker did not verify")
    return {"id": response["id"], "name": name, "reused": False}


def ingest_archive(
    archive: Path, subgroup_id: str, allow_production_write: bool,
    *, source_filename: str | None = None,
) -> dict[str, Any]:
    """Transfer native bytes and lightweight receipts; never inspect their data."""
    if not _SUBGROUP_RE.fullmatch(subgroup_id):
        raise ValueError("BDL subgroup must use P<digits> identity")
    archive = archive.resolve()
    if not archive.is_file():
        raise FileNotFoundError(f"BDL bulk archive not found: {archive}")
    if not allow_production_write:
        raise PermissionError("BDL bulk Drive publication requires --allow-production-write")
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise PermissionError("BDL bulk production publication must run in the main-branch Actions workflow")
    archive_bytes = archive.stat().st_size
    if archive_bytes <= 0 or archive_bytes > _MAX_ARCHIVE_BYTES:
        raise ValueError("BDL download size is outside the supported transfer envelope")
    filename = source_filename or archive.name
    if not isinstance(filename, str) or filename in {".", ".."} or any(char in filename for char in ("/", "\\", "\0", "\r", "\n")):
        raise ValueError("Unsafe source filename")
    archive_sha = _hash_file(archive, "sha256")
    archive_md5 = _hash_file(archive, "md5")
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    session = storage.begin_write_session()
    landing_root = storage.resolve_zone("landing", create=False)
    snapshot_root = storage.get_or_create_nested_folder(["gus_bdl", "web_bulk", subgroup_id, archive_sha], root_id=landing_root, write_session=session)
    control_root = storage.get_or_create_nested_folder(["gus_bdl", "web_bulk", "_control"], root_id=landing_root, write_session=session)
    archive_object = _upload_file(
        storage, archive, name=filename, parent_id=snapshot_root,
        sha256_hex=archive_sha, md5_hex=archive_md5, kind="source_native",
    )
    # Deterministic technical metadata makes a retry of the same transfer idempotent.
    # Retrieval time belongs to the final checkpoint, not a rewritten source file.
    object_reference = {key: value for key, value in archive_object.items() if key != "reused"}
    manifest = {
        "format_version": 2, "source_id": "gus_bdl", "transport": "web_bulk",
        "subgroup_id": subgroup_id, "landing_scope": "native_bytes_only",
        "content_validation": "not_performed", "archive_original_filename": filename,
        "archive_sha256": archive_sha, "archive_bytes": archive_bytes,
        "archive_object": object_reference,
    }
    # Control sidecars are kept outside the native-data snapshot.
    manifest_object = _upload_manifest(storage, manifest, parent_id=control_root)
    completion_marker = _upload_completion_marker(
        storage, control_id=control_root, subgroup_id=subgroup_id,
        archive_sha=archive_sha, archive_bytes=archive_bytes, manifest_object=manifest_object,
    )
    return {
        "status": "bdl_web_bulk_landed", "source_id": "gus_bdl", "transport": "web_bulk",
        "landing_scope": "native_bytes_only", "content_validation": "not_performed",
        "subgroup_id": subgroup_id, "archive_sha256": archive_sha, "archive_bytes": archive_bytes,
        "drive_folder_id": snapshot_root, "archive_object": archive_object,
        "manifest_object": manifest_object, "completion_marker": completion_marker,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--subgroup", required=True)
    parser.add_argument("--source-filename")
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()
    result = ingest_archive(args.archive, args.subgroup, args.allow_production_write, source_filename=args.source_filename)
    output = json.dumps(result, indent=2, sort_keys=True)
    print(output, flush=True)
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(output + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
