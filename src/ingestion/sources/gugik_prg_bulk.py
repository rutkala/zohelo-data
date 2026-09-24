"""Official bulk extractor for GUGiK PRG (Państwowy Rejestr Granic).

In accordance with ADR 0009 (Native-Only Landing):
- Downloads official native packages from GUGiK Geoportal & Dane.gov.pl
  1. 00_jednostki_administracyjne.zip: official administrative boundaries SHP (voivodeships, powiats, gminas)
  2. wykaz_powierzchni_2026.xlsx: official geodetic land surface area register (m2, ha, km2)
- Computes SHA-256 and MD5 checksums for byte integrity
- Stores native byte-for-byte archives in 01_landing/gugik_prg/native/bulk/
- Generates durable completion receipt in 06_control/source_campaigns/gugik_prg/
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
import sys
import time
from typing import Any
import urllib.request

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage_manager import StorageManager
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger("gugik_prg_bulk")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

OFFICIAL_DOWNLOAD_SOURCES = [
    {
        "filename": "00_jednostki_administracyjne.zip",
        "url": "https://opendata.geoportal.gov.pl/prg/granice/00_jednostki_administracyjne.zip",
        "description": "Official GUGiK PRG administrative boundaries shapefile package",
        "mime_type": "application/zip",
    },
    {
        "filename": "wykaz_powierzchni_2026.xlsx",
        "url": "https://api.dane.gov.pl/media/resources/20260120/Wykaz_powierzchni_wg_stanu_na_01012026_m2_ha_km2_OeSziHx.xlsx",
        "description": "Official GUGiK geodetic surface area register 2026",
        "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    },
]


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


def _upload_file_to_drive(
    storage: StorageManager,
    local_path: Path,
    name: str,
    parent_id: str,
    mime_type: str = "application/octet-stream",
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


def _download_stream(url: str, target_path: Path, chunk_size: int = 1048576) -> None:
    temp_target = target_path.with_suffix(".tmp")
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    )
    logger.info("Downloading %s -> %s...", url, target_path.name)
    start_t = time.time()
    downloaded_bytes = 0
    with urllib.request.urlopen(req, timeout=120) as response, temp_target.open("wb") as out_f:
        total_len = int(response.headers.get("Content-Length", 0))
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            out_f.write(chunk)
            downloaded_bytes += len(chunk)
            if total_len > 0 and downloaded_bytes % (chunk_size * 20) < chunk_size:
                logger.info(
                    "  %s: %d / %d MB (%.1f%%)",
                    target_path.name,
                    downloaded_bytes // (1024 * 1024),
                    total_len // (1024 * 1024),
                    100.0 * downloaded_bytes / total_len,
                )
    temp_target.replace(target_path)
    elapsed = time.time() - start_t
    mb = downloaded_bytes / (1024 * 1024)
    logger.info("Downloaded %s (%.1f MB in %.1fs, %.2f MB/s)", target_path.name, mb, elapsed, mb / max(elapsed, 0.1))


def run_prg_ingestion(
    workspace: Path,
    allow_codespace: bool = False,
    skip_download: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=True)
    downloaded_files = []

    for src in OFFICIAL_DOWNLOAD_SOURCES:
        fname = src["filename"]
        target = workspace / fname
        if not (skip_download and target.exists() and target.stat().st_size > 0):
            _download_stream(src["url"], target)
        downloaded_files.append((target, src))

    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    can_upload = (in_actions or allow_codespace) and not skip_upload

    drive_receipts = []
    if can_upload:
        if allow_codespace:
            os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
        storage = StorageManager(allow_interactive_auth=False)
        storage.resolve_root(create=False)
        storage.authorize_writes()

        landing_id = storage.resolve_zone("landing")
        prg_landing_id = _resolve_or_create_folder(storage, "gugik_prg", landing_id)
        native_id = _resolve_or_create_folder(storage, "native", prg_landing_id)
        bulk_id = _resolve_or_create_folder(storage, "bulk", native_id)

        control_id = storage.resolve_zone("control")
        campaigns_id = _resolve_or_create_folder(storage, "source_campaigns", control_id)
        prg_control_id = _resolve_or_create_folder(storage, "gugik_prg", campaigns_id)

        for target_path, src in downloaded_files:
            item = _upload_file_to_drive(storage, target_path, target_path.name, bulk_id, mime_type=src["mime_type"])
            sha256_hex, md5_hex = _hash_file(target_path)
            drive_receipts.append({
                "file_name": target_path.name,
                "drive_id": item["id"],
                "size_bytes": item["size"],
                "sha256": sha256_hex,
                "md5": md5_hex,
                "reused": item["reused"],
                "description": src["description"],
            })

        receipt_data = {
            "source_id": "gugik_prg",
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
            "load_complete": True,
            "catalogue_exhausted": True,
            "archives": drive_receipts,
        }
        receipt_path = workspace / "prg_receipt.json"
        receipt_path.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")
        _upload_file_to_drive(storage, receipt_path, "prg_receipt.json", prg_control_id, mime_type="application/json")
        logger.info("GUGiK PRG Landing 100% complete and receipt uploaded to Drive.")
        return receipt_data

    return {"status": "downloaded_locally", "files": [f[0].name for f in downloaded_files]}


def main():
    parser = argparse.ArgumentParser(description="GUGiK PRG Bulk Extractor")
    parser.add_argument("--workspace", type=str, default="portal/test-results/prg")
    parser.add_argument("--allow-codespace", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    res = run_prg_ingestion(
        workspace=Path(args.workspace).resolve(),
        allow_codespace=args.allow_codespace,
        skip_download=args.skip_download,
        skip_upload=args.skip_upload,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
