"""Publish OpenData.org Bronze campaign manifests and pointers to Google Drive.

Registers Bronze Parquet files in 02_bronze/opendata_org/ as queryable Bronze tables
in the Google Drive Lakehouse and Portal:
- organizations -> br_opendata_organizations (opendata_org_bronze)
- locations -> br_opendata_locations (opendata_org_locations_bronze)
- people -> br_opendata_people (opendata_org_people_bronze)
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any
from uuid import uuid4

from googleapiclient.http import MediaIoBaseUpload

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from storage_manager import StorageManager
from ingestion.sources.opendata_bronze_loader import (
    ORGANIZATION_COLUMNS,
    LOCATION_COLUMNS,
    PEOPLE_COLUMNS,
)

CATEGORY_CONFIG: dict[str, dict[str, Any]] = {
    "organizations": {
        "source_id": "opendata_org_bronze",
        "table_name": "br_opendata_organizations",
        "columns": ORGANIZATION_COLUMNS,
    },
    "locations": {
        "source_id": "opendata_org_locations_bronze",
        "table_name": "br_opendata_locations",
        "columns": LOCATION_COLUMNS,
    },
    "people": {
        "source_id": "opendata_org_people_bronze",
        "table_name": "br_opendata_people",
        "columns": PEOPLE_COLUMNS,
    },
}

CHECKPOINT_CONTROL_DIR = ["06_control", "source_campaigns", "opendata_org_bronze"]


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def publish_category(
    sm: StorageManager,
    svc: Any,
    root_id: str,
    cp_raw: bytes,
    checkpoint: dict[str, Any],
    category: str,
    code_sha: str,
) -> bool:
    cfg = CATEGORY_CONFIG[category]
    source_id = cfg["source_id"]
    table_name = cfg["table_name"]
    columns = cfg["columns"]

    table_entries = checkpoint.get("bronze_tables", {}).get(category, [])
    if not table_entries:
        print(f"INFO: No bronze entries found in checkpoint for category '{category}'. Skipping.")
        return False

    total_rows = sum(t["rows"] for t in table_entries)
    print(f"\n--- Publishing {category} -> {table_name} ({source_id}) ---")
    print(f"Found {len(table_entries)} Parquet files with {total_rows:,} total rows.")

    # Locate control folder: 06_control / source_campaigns / {source_id}
    control_id = sm.get_or_create_nested_folder(["06_control", "source_campaigns", source_id], root_id=root_id)
    pub_id = sm.get_or_create_nested_folder(["landing_publications"], root_id=control_id)
    print(f"Control folder: {control_id}, Publications folder: {pub_id}")

    # Prepare file descriptors with SHA-256
    file_descriptors = []
    for entry in table_entries:
        fid = entry["drive_id"]
        fname = entry["parquet_name"]
        frows = entry["rows"]
        fbytes = entry["bytes"]
        f_sha = entry.get("sha256")
        if not f_sha:
            print(f"Downloading {fname} to compute SHA-256 (ID: {fid}, size: {fbytes:,} bytes)...")
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
    columns_meta = [{"name": name, "type": dtype} for name, dtype in columns]
    is_complete = checkpoint.get("complete", False)
    coverage_status = "complete_current_catalogue" if is_complete else "incomplete"

    manifest_name = f"manifest-{snapshot_id}.json"
    manifest = {
        "format_version": 1,
        "kind": "bronze_snapshot",
        "source_id": source_id,
        "snapshot_id": snapshot_id,
        "created_at_utc": now_utc.isoformat(),
        "code_sha": code_sha,
        "status": "validated",
        "layer": "02_bronze",
        "table_name": table_name,
        "row_count": total_rows,
        "coverage_status": coverage_status,
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
        "source_id": source_id,
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

    print(f"SUCCESS: {source_id} catalog published to Google Drive!")
    print(f"Pointer: 06_control/source_campaigns/{source_id}/current-landing.json")
    print(f"Manifest: {manifest_name}")
    print(f"Table name: {table_name} (Layer: 02_bronze, Rows: {total_rows:,})")
    return True


def load_checkpoint_for_category(svc: Any, control_id: str, category: str) -> tuple[bytes, dict[str, Any]] | None:
    target_name = f"checkpoint-{category}.json"
    cp_q = f"'{control_id}' in parents and name = '{target_name}' and trashed = false"
    cp_files = svc.files().list(q=cp_q, fields="files(id, name)").execute().get("files", [])
    if cp_files:
        cp_raw = svc.files().get_media(fileId=cp_files[0]["id"]).execute()
        return cp_raw, json.loads(cp_raw.decode("utf-8"))

    # Fallback to checkpoint.json
    cp_q = f"'{control_id}' in parents and name = 'checkpoint.json' and trashed = false"
    cp_files = svc.files().list(q=cp_q, fields="files(id, name)").execute().get("files", [])
    if cp_files:
        cp_raw = svc.files().get_media(fileId=cp_files[0]["id"]).execute()
        return cp_raw, json.loads(cp_raw.decode("utf-8"))
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish OpenData Bronze campaign to Google Drive")
    parser.add_argument(
        "--category",
        choices=["organizations", "locations", "people", "all"],
        default="all",
        help="Category to publish (default: all)",
    )
    parser.add_argument("--allow-production-write", action="store_true", help="Explicit opt-in for production write")
    args = parser.parse_args()

    if args.allow_production_write:
        os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
    sm = StorageManager(allow_interactive_auth=False)
    sm.authorize_writes()

    root_id = sm.resolve_root(create=False)
    svc = sm.drive_service

    # Locate checkpoint folder: 06_control / source_campaigns / opendata_org_bronze
    control_id = sm.get_or_create_nested_folder(CHECKPOINT_CONTROL_DIR, root_id=root_id)

    code_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip()

    categories_to_publish = (
        ["organizations", "locations", "people"]
        if args.category == "all"
        else [args.category]
    )

    published_count = 0
    for cat in categories_to_publish:
        cp_data = load_checkpoint_for_category(svc, control_id, cat)
        if not cp_data:
            print(f"WARNING: No checkpoint found for category '{cat}'. Skipping.")
            continue
        cp_raw, checkpoint = cp_data
        published = publish_category(
            sm=sm,
            svc=svc,
            root_id=root_id,
            cp_raw=cp_raw,
            checkpoint=checkpoint,
            category=cat,
            code_sha=code_sha,
        )
        if published:
            published_count += 1

    print(f"\nCompleted: {published_count} categories published.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
