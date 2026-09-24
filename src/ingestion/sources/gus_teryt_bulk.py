"""Official bulk extractor for GUS TERYT (TERC, SIMC, ULIC).

In accordance with ADR 0009 (Native-Only Landing):
- Downloads official native ZIP archives from eTERYT via Playwright stealth worker
- Computes SHA-256 and MD5 checksums for byte integrity
- Stores native byte-for-byte archives in 01_landing/gus_teryt/native/bulk/
- Generates durable completion receipt in 06_control/source_campaigns/gus_teryt/
- Strictly decoupled from downstream Bronze parsing
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage_manager import StorageManager
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger("gus_teryt_bulk")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def _hash_file(path: Path) -> tuple[str, str]:
    d_sha = hashlib.sha256()
    d_md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            d_sha.update(chunk)
            d_md5.update(chunk)
    return d_sha.hexdigest(), d_md5.hexdigest()


def _escape_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _upload_file_to_drive(
    storage: StorageManager,
    local_path: Path,
    name: str,
    parent_id: str,
    mime_type: str = "application/zip",
) -> dict[str, Any]:
    sha256_hex, md5_hex = _hash_file(local_path)
    query = f"name='{_escape_query(name)}' and '{_escape_query(parent_id)}' in parents and trashed=false"
    existing = storage.drive_service.files().list(
        q=query, spaces="drive", fields="files(id,name,size,md5Checksum,appProperties)"
    ).execute().get("files", [])

    if existing:
        item = existing[0]
        props = item.get("appProperties") or {}
        if (
            props.get("sha256") == sha256_hex
            and item.get("md5Checksum") == md5_hex
            and int(item.get("size", -1)) == local_path.stat().st_size
        ):
            logger.info("File %s already exists on Drive with matching hash. Reusing.", name)
            return {"id": item["id"], "name": name, "size": local_path.stat().st_size, "reused": True}
        storage.drive_service.files().delete(fileId=item["id"]).execute()

    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)
    body = {"name": name, "parents": [parent_id], "appProperties": {"sha256": sha256_hex}}
    created = storage.drive_service.files().create(
        body=body, media_body=media, fields="id,name,size,md5Checksum"
    ).execute(num_retries=4)
    logger.info("Uploaded %s to Drive (id=%s, size=%d bytes)", name, created["id"], local_path.stat().st_size)
    return {"id": created["id"], "name": name, "size": local_path.stat().st_size, "reused": False}


def _resolve_or_create_folder(storage: StorageManager, folder_name: str, parent_id: str) -> str:
    query = f"name='{_escape_query(folder_name)}' and '{_escape_query(parent_id)}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    existing = storage.drive_service.files().list(q=query, spaces="drive", fields="files(id,name)").execute().get("files", [])
    if existing:
        return existing[0]["id"]
    created = storage.drive_service.files().create(
        body={"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]},
        fields="id,name",
    ).execute(num_retries=4)
    return created["id"]


def run_teryt_ingestion(workspace: Path, allow_codespace: bool = False, skip_download: bool = False) -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=True)
    worker_script = REPO_ROOT / "portal/scripts/teryt-bulk-worker.mjs"

    zip_files = list(workspace.glob("*.zip"))
    if not (skip_download and len(zip_files) >= 3):
        logger.info("Triggering Playwright TERYT download worker...")
        env = os.environ.copy()
        if "HTTP_PROXY" not in env:
            env["HTTP_PROXY"] = "http://127.0.0.1:8081"

        cmd = ["node", str(worker_script), str(workspace)]
        res = subprocess.run(cmd, cwd=REPO_ROOT / "portal", env=env, capture_output=True, text=True)
        if res.returncode != 0:
            logger.error("TERYT worker failed:\n%s\n%s", res.stdout, res.stderr)
            raise RuntimeError(f"TERYT download failed with code {res.returncode}")

        logger.info("TERYT worker output:\n%s", res.stdout)

    # Enumerate downloaded ZIP archives
    zip_files = list(workspace.glob("*.zip"))
    if not zip_files:
        raise RuntimeError("No ZIP files were downloaded by TERYT worker.")

    # Upload to Google Drive if authorized
    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    drive_receipts = []
    if in_actions or allow_codespace:
        if allow_codespace:
            os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
        storage = StorageManager(allow_interactive_auth=False)
        storage.resolve_root(create=False)
        storage.authorize_writes()

        landing_id = storage.resolve_zone("landing")
        teryt_landing_id = _resolve_or_create_folder(storage, "gus_teryt", landing_id)
        native_id = _resolve_or_create_folder(storage, "native", teryt_landing_id)
        bulk_id = _resolve_or_create_folder(storage, "bulk", native_id)

        control_id = storage.resolve_zone("control")
        campaigns_id = _resolve_or_create_folder(storage, "source_campaigns", control_id)
        teryt_control_id = _resolve_or_create_folder(storage, "gus_teryt", campaigns_id)

        for zf in zip_files:
            item = _upload_file_to_drive(storage, zf, zf.name, bulk_id)
            sha256_hex, md5_hex = _hash_file(zf)
            drive_receipts.append({
                "file_name": zf.name,
                "drive_id": item["id"],
                "size_bytes": item["size"],
                "sha256": sha256_hex,
                "md5": md5_hex,
                "reused": item["reused"]
            })

        # Save receipt
        receipt_data = {
            "source_id": "gus_teryt",
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
            "load_complete": True,
            "catalogue_exhausted": True,
            "archives": drive_receipts
        }
        receipt_path = workspace / "teryt_receipt.json"
        receipt_path.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")
        _upload_file_to_drive(storage, receipt_path, "teryt_receipt.json", teryt_control_id, mime_type="application/json")
        logger.info("TERYT Landing 100% complete and receipt uploaded to Drive.")
        return receipt_data

    return {"status": "downloaded_locally", "files": [f.name for f in zip_files]}


def main():
    parser = argparse.ArgumentParser(description="GUS TERYT Bulk Extractor")
    parser.add_argument("--workspace", type=str, default="portal/test-results/teryt")
    parser.add_argument("--allow-codespace", action="store_true")
    parser.add_argument("--skip-download", action="store_true", help="Reuse existing downloaded archives if present")
    args = parser.parse_args()

    run_teryt_ingestion(
        Path(args.workspace).resolve(),
        allow_codespace=args.allow_codespace,
        skip_download=args.skip_download,
    )


if __name__ == "__main__":
    main()
