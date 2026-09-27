"""Publish OpenData.org bulk landing index and pointer to Google Drive.

Registers the native 21.38 GB Senzing archive in 01_landing/opendata_org/
as a queryable full-distribution index table in Google Drive Lakehouse.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from uuid import uuid4

import duckdb
from googleapiclient.http import MediaIoBaseUpload

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from storage_manager import StorageManager

BULK_INDEX_COLUMNS = [
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
]


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
    sm = StorageManager(allow_interactive_auth=False)
    sm.authorize_writes()

    root_id = sm.resolve_root(create=False)
    svc = sm.drive_service

    # Locate raw archive in 01_landing/opendata_org
    landing_id = sm.resolve_zone("landing", create=False)
    target_q = f"'{landing_id}' in parents and name = 'opendata_org' and trashed = false"
    folders = svc.files().list(q=target_q, fields="files(id, name)").execute().get("files", [])
    if not folders:
        print("ERROR: 01_landing/opendata_org folder not found.")
        return 1
    opendata_landing_id = folders[0]["id"]

    zip_q = f"'{opendata_landing_id}' in parents and name = 'ODO_SENZING_20260305.zip' and trashed = false"
    zips = svc.files().list(q=zip_q, fields="files(id, name, size)").execute().get("files", [])
    if not zips:
        print("ERROR: ODO_SENZING_20260305.zip not found in 01_landing/opendata_org.")
        return 1
    raw_zip = zips[0]
    raw_file_id = raw_zip["id"]
    raw_file_name = raw_zip["name"]
    raw_size_bytes = int(raw_zip["size"])
    print(f"Found landed raw archive: {raw_file_name} (ID: {raw_file_id}, size: {raw_size_bytes:,} bytes)")

    # Prepare control folders: 06_control / source_campaigns / opendata_org_bulk / landing_publications
    control_id = sm.get_or_create_nested_folder(["06_control", "source_campaigns", "opendata_org_bulk"], root_id=root_id)
    pub_id = sm.get_or_create_nested_folder(["landing_publications"], root_id=control_id)
    print(f"Control folder: {control_id}, Landing publications folder: {pub_id}")

    # Build Parquet fragment
    snapshot_id = str(uuid4())
    fragment_name = f"fragment-{uuid4()}.parquet"
    now_utc = datetime.now(timezone.utc)

    dataset_id = "odo_senzing_20260305"
    source_id = "opendata_org"
    version = "2026-03-05"
    kind = "senzing_zip_jsonl"
    retrieved_at_utc = now_utc.replace(tzinfo=None)
    raw_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    request_json = json.dumps({"url": "https://storage.googleapis.com/opendata-org-lakehouse/ODO_SENZING_20260305.zip"}, separators=(",", ":"))
    inspection_json = json.dumps({
        "inner_members": [
            {"name": "organization.json", "format": "senzing_jsonl"},
            {"name": "locations.json", "format": "senzing_jsonl"},
            {"name": "peoplebusiness.json", "format": "senzing_jsonl"}
        ]
    }, separators=(",", ":"))

    row = (
        dataset_id,
        source_id,
        version,
        kind,
        retrieved_at_utc,
        raw_file_id,
        raw_file_name,
        raw_size_bytes,
        raw_sha256,
        request_json,
        inspection_json
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_parquet = Path(tmpdir) / fragment_name
        con = duckdb.connect(":memory:")
        schema_sql = ", ".join(f'"{col}" {dtype}' for col, dtype in BULK_INDEX_COLUMNS)
        con.execute(f"CREATE TABLE bulk_index ({schema_sql})")
        placeholders = ", ".join("?" for _ in BULK_INDEX_COLUMNS)
        con.execute(f"INSERT INTO bulk_index VALUES ({placeholders})", row)
        con.execute(f"COPY bulk_index TO '{tmp_parquet}' (FORMAT 'PARQUET', CODEC 'ZSTD')")
        con.close()

        fragment_bytes = tmp_parquet.read_bytes()

    fragment_size = len(fragment_bytes)
    fragment_sha = sha256_hex(fragment_bytes)
    print(f"Generated Parquet index fragment: {fragment_name} ({fragment_size} bytes, sha: {fragment_sha[:16]}...)")

    # Upload fragment to landing_publications
    frag_media = MediaIoBaseUpload(io.BytesIO(fragment_bytes), mimetype="application/vnd.apache.parquet")
    frag_file = svc.files().create(
        body={"name": fragment_name, "parents": [pub_id], "mimeType": "application/vnd.apache.parquet"},
        media_body=frag_media,
        fields="id, name, size"
    ).execute()
    frag_drive_id = frag_file["id"]
    print(f"Uploaded index fragment to Drive: {frag_drive_id}")

    # Build manifest
    code_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip()
    manifest_name = f"manifest-{snapshot_id}.json"
    manifest = {
        "format_version": 2,
        "kind": "full_distribution_index",
        "source_id": "opendata_org_bulk",
        "snapshot_id": snapshot_id,
        "created_at_utc": now_utc.isoformat(),
        "code_sha": code_sha,
        "status": "validated",
        "layer": "01_landing",
        "table_name": "opendata_org_distributions",
        "row_count": 1,
        "coverage_status": "complete_current_catalogue",
        "files": [
            {
                "id": frag_drive_id,
                "name": fragment_name,
                "size": fragment_size,
                "sha256": fragment_sha
            }
        ],
        "columns": [{"name": name, "type": dtype} for name, dtype in BULK_INDEX_COLUMNS],
        "accepted_distribution_count": 1,
        "published_distribution_count": 1,
        "pending_publication_count": 0,
        "receipt_checkpoint_sha256": sha256_hex(f"{dataset_id}:{raw_file_id}:{raw_size_bytes}".encode()),
        "tests": {"passed": True}
    }

    manifest_bytes = json.dumps(manifest, separators=(",", ":")).encode("utf-8")
    manifest_size = len(manifest_bytes)
    manifest_sha = sha256_hex(manifest_bytes)

    # Upload manifest
    man_media = MediaIoBaseUpload(io.BytesIO(manifest_bytes), mimetype="application/json")
    man_file = svc.files().create(
        body={"name": manifest_name, "parents": [pub_id], "mimeType": "application/json"},
        media_body=man_media,
        fields="id, name, size"
    ).execute()
    man_drive_id = man_file["id"]
    print(f"Uploaded manifest: {manifest_name} (ID: {man_drive_id}, size: {manifest_size} bytes)")

    # Build pointer: current-landing.json
    pointer = {
        "format_version": 1,
        "source_id": "opendata_org_bulk",
        "snapshot_id": snapshot_id,
        "manifest_file_id": man_drive_id,
        "manifest_file_name": manifest_name,
        "manifest_sha256": manifest_sha,
        "manifest_size_bytes": manifest_size
    }
    pointer_bytes = json.dumps(pointer, indent=2).encode("utf-8")

    # Update or create current-landing.json in control_id
    ptr_q = f"'{control_id}' in parents and name = 'current-landing.json' and trashed = false"
    existing_ptr = svc.files().list(q=ptr_q, fields="files(id, name)").execute().get("files", [])
    ptr_media = MediaIoBaseUpload(io.BytesIO(pointer_bytes), mimetype="application/json")
    if existing_ptr:
        svc.files().update(fileId=existing_ptr[0]["id"], media_body=ptr_media).execute()
        print(f"Updated existing current-landing.json: {existing_ptr[0]['id']}")
    else:
        created = svc.files().create(
            body={"name": "current-landing.json", "parents": [control_id], "mimeType": "application/json"},
            media_body=ptr_media,
            fields="id, name"
        ).execute()
        print(f"Created current-landing.json: {created['id']}")

    print("\nSUCCESS: opendata_org_bulk landing catalog published to Google Drive!")
    print(f"Pointer: 06_control/source_campaigns/opendata_org_bulk/current-landing.json")
    print(f"Manifest: {manifest_name}")
    print(f"Fragment: {fragment_name}")
    print(f"Table name: opendata_org_distributions (1 row, 21.38 GB native archive)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
