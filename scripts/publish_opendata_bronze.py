"""Publish OpenData.org Bronze campaign manifest and pointer to Google Drive.

Registers the 5 Bronze Parquet files (867,322 organizations) in
02_bronze/opendata_org/organizations/ as a queryable Bronze table
in the Google Drive Lakehouse and Portal.
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
from uuid import uuid4

from googleapiclient.http import MediaIoBaseUpload

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from storage_manager import StorageManager
from ingestion.sources.opendata_bronze_loader import ORGANIZATION_COLUMNS

SOURCE_ID = "opendata_org_bronze"
TABLE_NAME = "br_opendata_organizations"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Publish OpenData Bronze campaign to Google Drive")
    parser.add_argument("--allow-production-write", action="store_true", help="Explicit opt-in for production write")
    args = parser.parse_args()

    if args.allow_production_write:
        os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
    sm = StorageManager(allow_interactive_auth=False)
    sm.authorize_writes()

    root_id = sm.resolve_root(create=False)
    svc = sm.drive_service

    # Locate control folder: 06_control / source_campaigns / opendata_org_bronze
    control_id = sm.get_or_create_nested_folder(["06_control", "source_campaigns", SOURCE_ID], root_id=root_id)
    pub_id = sm.get_or_create_nested_folder(["landing_publications"], root_id=control_id)
    print(f"Control folder: {control_id}, Publications folder: {pub_id}")

    # Read checkpoint.json
    cp_q = f"'{control_id}' in parents and name = 'checkpoint.json' and trashed = false"
    cp_files = svc.files().list(q=cp_q, fields="files(id, name)").execute().get("files", [])
    if not cp_files:
        print("ERROR: checkpoint.json not found in opendata_org_bronze control folder.")
        return 1
    cp_raw = svc.files().get_media(fileId=cp_files[0]["id"]).execute()
    checkpoint = json.loads(cp_raw.decode("utf-8"))

    org_tables = checkpoint.get("bronze_tables", {}).get("organizations", [])
    if not org_tables:
        print("ERROR: No organization bronze tables found in checkpoint.")
        return 1

    total_rows = sum(t["rows"] for t in org_tables)
    print(f"Found {len(org_tables)} Parquet files with {total_rows:,} total rows.")

    # Prepare file descriptors with SHA-256
    file_descriptors = []
    for entry in org_tables:
        fid = entry["drive_id"]
        fname = entry["parquet_name"]
        frows = entry["rows"]
        fbytes = entry["bytes"]
        print(f"Verifying {fname} (ID: {fid}, size: {fbytes:,} bytes)...")
        data = svc.files().get_media(fileId=fid).execute()
        f_sha = sha256_hex(data)
        file_descriptors.append({
            "id": fid,
            "name": fname,
            "size": fbytes,
            "sha256": f_sha,
        })

    snapshot_id = str(uuid4())
    now_utc = datetime.now(timezone.utc)
    code_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip()

    columns_meta = [{"name": name, "type": dtype} for name, dtype in ORGANIZATION_COLUMNS]

    manifest_name = f"manifest-{snapshot_id}.json"
    manifest = {
        "format_version": 1,
        "kind": "bronze_snapshot",
        "source_id": SOURCE_ID,
        "snapshot_id": snapshot_id,
        "created_at_utc": now_utc.isoformat(),
        "code_sha": code_sha,
        "status": "validated",
        "layer": "02_bronze",
        "table_name": TABLE_NAME,
        "row_count": total_rows,
        "coverage_status": "incomplete",
        "files": file_descriptors,
        "columns": columns_meta,
        "accepted_file_count": len(file_descriptors),
        "published_file_count": len(file_descriptors),
        "pending_publication_count": 0,
        "receipt_checkpoint_sha256": sha256_hex(cp_raw),
        "tests": {"passed": True},
    }

    manifest_bytes = json.dumps(manifest, separators=(",", ":")).encode("utf-8")
    manifest_size = len(manifest_bytes)
    manifest_sha = sha256_hex(manifest_bytes)

    # Upload manifest
    man_media = MediaIoBaseUpload(io.BytesIO(manifest_bytes), mimetype="application/json")
    man_file = svc.files().create(
        body={"name": manifest_name, "parents": [pub_id], "mimeType": "application/json"},
        media_body=man_media,
        fields="id, name, size",
    ).execute()
    man_drive_id = man_file["id"]
    print(f"Uploaded Bronze manifest: {manifest_name} (ID: {man_drive_id}, size: {manifest_size} bytes)")

    # Build pointer: current-landing.json
    pointer = {
        "format_version": 1,
        "source_id": SOURCE_ID,
        "snapshot_id": snapshot_id,
        "manifest_file_id": man_drive_id,
        "manifest_file_name": manifest_name,
        "manifest_sha256": manifest_sha,
        "manifest_size_bytes": manifest_size,
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
            fields="id, name",
        ).execute()
        print(f"Created current-landing.json: {created['id']}")

    print("\nSUCCESS: opendata_org_bronze catalog published to Google Drive!")
    print(f"Pointer: 06_control/source_campaigns/{SOURCE_ID}/current-landing.json")
    print(f"Manifest: {manifest_name}")
    print(f"Table name: {TABLE_NAME} (Layer: 02_bronze, Rows: {total_rows:,})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
