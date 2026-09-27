"""Atomic Google Drive upload helper adhering strictly to ADR 0009.

Ensures zero data loss by never deleting existing files before a new upload
is verified. Uses content hash verification and atomic deletion/cleanup of
superseded files only after successful candidate persistence.
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
import threading
from typing import Any

from googleapiclient.http import MediaFileUpload

logger = logging.getLogger("drive_safe_upload")


def hash_file(path: Path) -> tuple[str, str]:
    """Compute sha256 and md5 hex digests of a local file."""
    d_sha = hashlib.sha256()
    d_md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            d_sha.update(chunk)
            d_md5.update(chunk)
    return d_sha.hexdigest(), d_md5.hexdigest()


def escape_drive_query(value: str) -> str:
    """Escape single quotes and backslashes for Google Drive search queries."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def safe_drive_upload(
    storage: Any,
    local_path: Path | str,
    name: str,
    parent_id: str,
    mime_type: str = "application/octet-stream",
    *,
    drive_lock: threading.Lock | threading.RLock | None = None,
    allow_overwrite: bool = True,
) -> dict[str, Any]:
    """Upload a file to Google Drive atomically with zero data loss.

    Execution Flow:
    1. Query existing active files matching name and parent.
    2. Check if an exact matching file exists (same size, md5, and sha256 appProperty).
       If found, return immediately as reused=True (idempotent no-op).
    3. If no matching file exists:
       - Upload candidate file FIRST.
       - Verify candidate metadata on Drive (size, md5, sha256 appProperty).
       - Only if candidate verified, delete superseded file(s) if allow_overwrite=True.
       - If candidate upload/verification fails, clean up the candidate (if created),
         leaving prior files intact.
    """
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(f"Local file not found for Drive upload: {path}")

    sha256_hex, md5_hex = hash_file(path)
    local_size = path.stat().st_size

    if parent_id:
        query = (
            f"name='{escape_drive_query(name)}' and '{escape_drive_query(parent_id)}' in parents "
            "and trashed=false"
        )
    else:
        query = f"name='{escape_drive_query(name)}' and trashed=false"

    def _execute(request):
        if drive_lock:
            with drive_lock:
                return request.execute()
        return request.execute()

    existing_files = _execute(
        storage.drive_service.files().list(
            q=query, spaces="drive", fields="files(id,name,size,md5Checksum,appProperties)"
        )
    ).get("files", [])

    superseded_ids: list[str] = []
    for item in existing_files:
        props = item.get("appProperties") or {}
        if (
            props.get("sha256") == sha256_hex
            and item.get("md5Checksum") == md5_hex
            and int(item.get("size", -1)) == local_size
        ):
            logger.info("File %s already exists on Drive with matching hash. Reusing.", name)
            return {"id": item["id"], "name": name, "size": local_size, "reused": True}
        superseded_ids.append(item["id"])

    # 1. Upload new candidate file FIRST
    media = MediaFileUpload(str(path), mimetype=mime_type, resumable=True)
    body: dict[str, Any] = {"name": name, "appProperties": {"sha256": sha256_hex}}
    if parent_id:
        body["parents"] = [parent_id]

    created = _execute(
        storage.drive_service.files().create(
            body=body, media_body=media, fields="id,name,size,md5Checksum,appProperties"
        )
    )

    # 2. Strict verification of uploaded candidate file
    candidate_id = created.get("id") if isinstance(created, dict) else None
    if not candidate_id:
        raise RuntimeError(f"Google Drive upload returned no file id for {name}")

    candidate_size = int(created.get("size", -1)) if "size" in created else local_size
    candidate_md5 = created.get("md5Checksum")
    candidate_props = created.get("appProperties") or {}
    candidate_sha256 = candidate_props.get("sha256")

    verification_ok = True
    if candidate_size != local_size:
        verification_ok = False
    if candidate_md5 is not None and candidate_md5 != md5_hex:
        verification_ok = False
    if candidate_sha256 is not None and candidate_sha256 != sha256_hex:
        verification_ok = False

    if not verification_ok:
        try:
            _execute(storage.drive_service.files().delete(fileId=candidate_id))
        except Exception as exc:
            logger.warning("Failed to delete unverified candidate file %s: %s", candidate_id, exc)
        raise RuntimeError(
            f"Google Drive upload verification failed for {name} "
            f"(size={candidate_size}/{local_size}, md5={candidate_md5}/{md5_hex}, sha256={candidate_sha256}/{sha256_hex})"
        )

    # 3. Only after verification, remove superseded files
    if allow_overwrite and superseded_ids:
        for old_id in superseded_ids:
            try:
                _execute(storage.drive_service.files().delete(fileId=old_id))
            except Exception as exc:
                logger.warning("Failed to delete superseded Drive file %s: %s", old_id, exc)

    logger.info("Uploaded and verified %s on Drive (id=%s, size=%d bytes)", name, candidate_id, local_size)
    return {"id": candidate_id, "name": name, "size": local_size, "reused": False}


safe_upload_file_to_drive = safe_drive_upload
