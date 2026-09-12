"""Validate, convert, and publish one authenticated BDL web bulk archive.

The bulk archive is retained byte-for-byte in a dedicated immutable Landing
namespace. Its CSV member is converted with DuckDB to compressed Parquet while
preserving every provider column and appending explicit transport provenance.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import md5, sha256
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any
from zipfile import ZipFile, BadZipFile

import duckdb
from googleapiclient.http import MediaFileUpload, MediaInMemoryUpload

from storage_manager import StorageManager

_SUBGROUP_RE = re.compile(r"^P[0-9]+$")
_MAX_MEMBER_BYTES = 8 * 1024 * 1024 * 1024
_MAX_ARCHIVE_BYTES = 2 * 1024 * 1024 * 1024
_CHUNK_BYTES = 8 * 1024 * 1024


def _hash_file(path: Path, algorithm: str) -> str:
    digest = sha256() if algorithm == "sha256" else md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _safe_member(name: str) -> bool:
    if not name or name.startswith(("/", "\\")):
        return False
    parts = Path(name.replace("\\", "/")).parts
    return all(part not in {"", ".", ".."} for part in parts)


def _extract_csv(archive: Path, destination: Path) -> tuple[Path, dict[str, Any]]:
    if archive.stat().st_size <= 0 or archive.stat().st_size > _MAX_ARCHIVE_BYTES:
        raise ValueError("BDL bulk archive size is outside the supported safety envelope")
    try:
        with ZipFile(archive) as zipped:
            members = zipped.infolist()
            if not members or any(not _safe_member(item.filename) for item in members):
                raise ValueError("BDL bulk archive contains unsafe member names")
            if any(item.file_size < 0 or item.file_size > _MAX_MEMBER_BYTES for item in members):
                raise ValueError("BDL bulk archive contains an excessive member")
            csv_members = [item for item in members if item.filename.lower().endswith(".csv")]
            if len(csv_members) != 1:
                raise ValueError("BDL bulk archive must contain exactly one CSV member")
            info = csv_members[0]
            csv_path = destination / Path(info.filename).name
            with zipped.open(info, "r") as source, csv_path.open("wb") as target:
                shutil.copyfileobj(source, target, length=8 * 1024 * 1024)
            if csv_path.stat().st_size != info.file_size:
                raise ValueError("Extracted BDL CSV size does not match ZIP metadata")
            return csv_path, {
                "csv_member": info.filename,
                "csv_uncompressed_bytes": info.file_size,
                "archive_members": [
                    {"name": item.filename, "uncompressed_bytes": item.file_size, "compressed_bytes": item.compress_size}
                    for item in members
                ],
            }
    except BadZipFile as exc:
        raise ValueError("BDL bulk download is not a valid ZIP archive") from exc


def _convert_csv(csv_path: Path, parquet_path: Path, *, subgroup_id: str, archive_filename: str, archive_sha256: str, retrieved_at_utc: str) -> dict[str, Any]:
    csv_sql = _sql_literal(str(csv_path))
    parquet_sql = _sql_literal(str(parquet_path))
    with duckdb.connect() as connection:
        source = f"read_csv({csv_sql}, delim=';', header=true, quote='\"', escape='\"', all_varchar=true, strict_mode=true, null_padding=false)"
        row_count = connection.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
        if row_count <= 0:
            raise ValueError("BDL bulk CSV contains no data rows")
        description = connection.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
        source_columns = [row[0] for row in description]
        if len(source_columns) < 3:
            raise ValueError("BDL bulk CSV has an implausibly narrow schema")
        connection.execute(
            "COPY (SELECT *, "
            f"{_sql_literal('gus_bdl')} AS _source_id, "
            f"{_sql_literal('web_bulk')} AS _transport, "
            f"{_sql_literal(subgroup_id)} AS _subgroup_id, "
            f"{_sql_literal(archive_filename)} AS _archive_filename, "
            f"{_sql_literal(archive_sha256)} AS _archive_sha256, "
            f"{_sql_literal(retrieved_at_utc)} AS _retrieved_at_utc "
            f"FROM {source}) TO {parquet_sql} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    if not parquet_path.exists() or parquet_path.stat().st_size <= 0:
        raise RuntimeError("DuckDB did not create a BDL bulk Parquet artifact")
    return {
        "row_count": int(row_count),
        "source_columns": source_columns,
        "parquet_bytes": parquet_path.stat().st_size,
        "parquet_sha256": _hash_file(parquet_path, "sha256"),
        "parquet_md5": _hash_file(parquet_path, "md5"),
    }


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
        if (
            int(item.get("size", -1)) != len(raw)
            or item.get("md5Checksum") != md5_digest
            or properties.get("sha256") != digest
        ):
            raise RuntimeError("Existing BDL bulk manifest conflicts with expected bytes")
        return {"id": item["id"], "name": name, "sha256": digest, "size": len(raw), "reused": True}
    media = MediaInMemoryUpload(raw, mimetype="application/json", resumable=False)
    response = storage.drive_service.files().create(body={"name": name, "parents": [parent_id], "appProperties": {"sha256": digest, "kind": "manifest", "source_id": "gus_bdl", "transport": "web_bulk"}}, media_body=media, fields="id,name,size,md5Checksum,appProperties").execute(num_retries=4)
    if (
        not response
        or response.get("name") != name
        or int(response.get("size", -1)) != len(raw)
        or response.get("md5Checksum") != md5_digest
        or (response.get("appProperties") or {}).get("sha256") != digest
    ):
        raise RuntimeError("BDL bulk manifest upload did not verify")
    return {"id": response["id"], "name": name, "sha256": digest, "size": len(raw), "reused": False}


def _upload_completion_marker(
    storage: StorageManager,
    *,
    control_id: str,
    subgroup_id: str,
    archive_sha: str,
    row_count: int,
    manifest_object: dict[str, Any],
) -> dict[str, Any]:
    """Publish the final marker only after every immutable snapshot object verifies."""

    payload = {
        "format_version": 1,
        "source_id": "gus_bdl",
        "transport": "web_bulk",
        "subgroup_id": subgroup_id,
        "status": "landed",
        "archive_sha256": archive_sha,
        "row_count": row_count,
        "manifest_object": manifest_object,
        "completed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    name = f"{subgroup_id}.json"
    properties = {
        "source_id": "gus_bdl",
        "transport": "web_bulk",
        "subgroup_id": subgroup_id,
        "status": "landed",
        "manifest_sha256": manifest_object["sha256"],
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
        media_body=media,
        fields="id,name,size,appProperties",
    ).execute(num_retries=4)
    if (
        not response
        or response.get("name") != name
        or int(response.get("size", -1)) != len(raw)
        or (response.get("appProperties") or {}) != properties
    ):
        raise RuntimeError("BDL bulk completion marker did not verify")
    return {"id": response["id"], "name": name, "reused": False}


def ingest_archive(archive: Path, subgroup_id: str, allow_production_write: bool) -> dict[str, Any]:
    if not _SUBGROUP_RE.fullmatch(subgroup_id):
        raise ValueError("BDL subgroup must use P<digits> identity")
    archive = archive.resolve()
    if not archive.is_file():
        raise FileNotFoundError(f"BDL bulk archive not found: {archive}")
    if not allow_production_write:
        raise PermissionError("BDL bulk Drive publication requires --allow-production-write")
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise PermissionError("BDL bulk production publication must run in the main-branch Actions workflow")
    archive_sha = _hash_file(archive, "sha256")
    archive_md5 = _hash_file(archive, "md5")
    retrieved_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with tempfile.TemporaryDirectory(prefix="zohelo-bdl-bulk-") as temporary:
        workspace = Path(temporary)
        csv_path, zip_metadata = _extract_csv(archive, workspace)
        parquet_path = workspace / f"{subgroup_id}-{archive_sha}.parquet"
        converted = _convert_csv(csv_path, parquet_path, subgroup_id=subgroup_id, archive_filename=archive.name, archive_sha256=archive_sha, retrieved_at_utc=retrieved_at)
        storage = StorageManager(allow_interactive_auth=False)
        storage.resolve_root(create=False)
        session = storage.begin_write_session()
        landing_root = storage.resolve_zone("landing", create=False)
        snapshot_root = storage.get_or_create_nested_folder(["gus_bdl", "web_bulk", subgroup_id, archive_sha], root_id=landing_root, write_session=session)
        control_root = storage.get_or_create_nested_folder(["gus_bdl", "web_bulk", "_control"], root_id=landing_root, write_session=session)
        archive_object = _upload_file(storage, archive, name="source.zip", parent_id=snapshot_root, sha256_hex=archive_sha, md5_hex=archive_md5, kind="source_zip")
        parquet_object = _upload_file(storage, parquet_path, name="data.parquet", parent_id=snapshot_root, sha256_hex=converted["parquet_sha256"], md5_hex=converted["parquet_md5"], kind="source_parquet")
        manifest = {"format_version": 1, "source_id": "gus_bdl", "transport": "web_bulk", "subgroup_id": subgroup_id, "retrieved_at_utc": retrieved_at, "archive_original_filename": archive.name, "archive_sha256": archive_sha, "archive_bytes": archive.stat().st_size, **zip_metadata, "row_count": converted["row_count"], "source_columns": converted["source_columns"], "archive_object": archive_object, "parquet_object": parquet_object}
        manifest_object = _upload_manifest(storage, manifest, parent_id=snapshot_root)
        completion_marker = _upload_completion_marker(
            storage,
            control_id=control_root,
            subgroup_id=subgroup_id,
            archive_sha=archive_sha,
            row_count=converted["row_count"],
            manifest_object=manifest_object,
        )
    return {"status": "bdl_web_bulk_landed", "source_id": "gus_bdl", "transport": "web_bulk", "subgroup_id": subgroup_id, "archive_sha256": archive_sha, "archive_bytes": archive.stat().st_size, "row_count": converted["row_count"], "parquet_bytes": converted["parquet_bytes"], "drive_folder_id": snapshot_root, "archive_object": archive_object, "parquet_object": parquet_object, "manifest_object": manifest_object, "completion_marker": completion_marker}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--subgroup", required=True)
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()
    result = ingest_archive(args.archive, args.subgroup, args.allow_production_write)
    rendered = json.dumps(result, indent=2, sort_keys=True)
    print(rendered, flush=True)
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
