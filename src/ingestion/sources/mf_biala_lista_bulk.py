"""Official bulk extractor for Ministerstwo Finansów Biała Lista Podatników VAT (PL-FIN-001).

In accordance with ADR 0009 (Native-Only Landing):
- Downloads official daily flat file packages from MF (plik płaski):
  https://plikplaski.mf.gov.pl/pliki//<YYYYMMDD>.7z
- Computes SHA-256 and MD5 checksums for byte integrity
- Stores native byte-for-byte archives in 01_landing/mf_biala_lista/native/bulk/<YYYYMMDD>/
- Generates durable completion receipt in 06_control/source_campaigns/mf_biala_lista/
- Strictly decoupled from downstream Bronze parsing
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any
import urllib.request
import urllib.error

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage_manager import StorageManager
from googleapiclient.http import MediaFileUpload

logger = logging.getLogger("mf_biala_lista_bulk")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

BASE_DOWNLOAD_URL = "https://plikplaski.mf.gov.pl/pliki"


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


from drive_safe_upload import safe_drive_upload


def _upload_file_to_drive(
    storage: StorageManager,
    local_path: Path,
    name: str,
    parent_id: str,
    mime_type: str = "application/x-7z-compressed",
) -> dict[str, Any]:
    return safe_drive_upload(
        storage,
        local_path,
        name,
        parent_id,
        mime_type=mime_type,
    )


def _check_url_exists(url: str) -> bool:
    req = urllib.request.Request(
        url,
        method="HEAD",
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status == 200
    except (urllib.error.HTTPError, urllib.error.URLError):
        return False


def _download_stream(url: str, target_path: Path, chunk_size: int = 1048576) -> None:
    temp_target = target_path.with_suffix(".tmp")
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    )
    logger.info("Downloading %s -> %s...", url, target_path.name)
    start_t = time.time()
    downloaded_bytes = 0
    with urllib.request.urlopen(req, timeout=300) as response, temp_target.open("wb") as out_f:
        total_len = int(response.headers.get("Content-Length", 0))
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            out_f.write(chunk)
            downloaded_bytes += len(chunk)
            if total_len > 0 and downloaded_bytes % (chunk_size * 25) < chunk_size:
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


def resolve_target_date(requested_date: str | None = None) -> tuple[str, str]:
    if requested_date:
        url = f"{BASE_DOWNLOAD_URL}/{requested_date}.7z"
        if not _check_url_exists(url):
            raise FileNotFoundError(f"Requested flat file not available at {url}")
        return requested_date, url

    # Default to current date or previous date
    now = datetime.now(timezone.utc)
    for offset in range(3):
        candidate_date = (now - timedelta(days=offset)).strftime("%Y%m%d")
        url = f"{BASE_DOWNLOAD_URL}/{candidate_date}.7z"
        if _check_url_exists(url):
            return candidate_date, url

    raise RuntimeError("Could not find any available MF Biała Lista flat file within the last 3 days")


def run_biala_lista_ingestion(
    workspace: Path,
    target_date: str | None = None,
    allow_codespace: bool = False,
    skip_download: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=True)
    date_str, url = resolve_target_date(target_date)
    filename = f"{date_str}.7z"
    target_path = workspace / filename

    if not (skip_download and target_path.exists() and target_path.stat().st_size > 0):
        _download_stream(url, target_path)

    sha256_hex, md5_hex = _hash_file(target_path)
    file_size = target_path.stat().st_size

    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    can_upload = (in_actions or allow_codespace) and not skip_upload

    drive_item = None
    if can_upload:
        if allow_codespace:
            os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
        storage = StorageManager(allow_interactive_auth=False)
        storage.resolve_root(create=False)
        storage.authorize_writes()

        landing_id = storage.resolve_zone("landing")
        source_landing_id = _resolve_or_create_folder(storage, "mf_biala_lista", landing_id)
        native_id = _resolve_or_create_folder(storage, "native", source_landing_id)
        bulk_id = _resolve_or_create_folder(storage, "bulk", native_id)
        date_folder_id = _resolve_or_create_folder(storage, date_str, bulk_id)

        control_id = storage.resolve_zone("control")
        campaigns_id = _resolve_or_create_folder(storage, "source_campaigns", control_id)
        source_control_id = _resolve_or_create_folder(storage, "mf_biala_lista", campaigns_id)

        drive_item = _upload_file_to_drive(
            storage,
            target_path,
            filename,
            date_folder_id,
            mime_type="application/x-7z-compressed",
        )

        receipt_data = {
            "source_id": "mf_biala_lista",
            "snapshot_date": date_str,
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
            "load_complete": True,
            "catalogue_exhausted": True,
            "archives": [
                {
                    "file_name": filename,
                    "drive_id": drive_item["id"],
                    "size_bytes": file_size,
                    "sha256": sha256_hex,
                    "md5": md5_hex,
                    "reused": drive_item["reused"],
                    "url": url,
                    "description": f"Official Ministerstwo Finansów VAT flat file for {date_str}",
                }
            ],
        }
        receipt_path = workspace / "biala_lista_receipt.json"
        receipt_path.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")
        _upload_file_to_drive(
            storage, receipt_path, "biala_lista_receipt.json", source_control_id, mime_type="application/json"
        )
        logger.info("MF Biała Lista Landing complete and receipt uploaded to Drive.")
        return receipt_data

    return {
        "status": "downloaded_locally",
        "file_name": filename,
        "size_bytes": file_size,
        "sha256": sha256_hex,
        "md5": md5_hex,
        "snapshot_date": date_str,
    }


def main():
    parser = argparse.ArgumentParser(description="Ministerstwo Finansów Biała Lista Bulk Extractor")
    parser.add_argument("--workspace", type=str, default="portal/test-results/biala-lista")
    parser.add_argument("--date", type=str, default=None, help="Target date YYYYMMDD")
    parser.add_argument("--allow-codespace", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    res = run_biala_lista_ingestion(
        workspace=Path(args.workspace).resolve(),
        target_date=args.date,
        allow_codespace=args.allow_codespace,
        skip_download=args.skip_download,
        skip_upload=args.skip_upload,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
