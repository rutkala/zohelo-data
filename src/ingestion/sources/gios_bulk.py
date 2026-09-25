"""Official bulk extractor for GIOŚ (Główny Inspektorat Ochrony Środowiska) air quality archives (PL-ENV-010).

In accordance with ADR 0009 (Native-Only Landing):
- Downloads official native Excel metadata workbooks and measurement ZIP archives directly from powietrze.gios.gov.pl
- Computes SHA-256 and MD5 checksums for byte integrity
- Stores native byte-for-byte archives in 01_landing/gios_pjp/native/bulk/
- Generates durable completion receipt in 06_control/source_campaigns/gios_pjp/
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

logger = logging.getLogger("gios_bulk")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

BASE_ARCHIVES_URL = "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/"

# Official document IDs from GIOŚ Portal Jakości Powietrza (Bank Danych Pomiarowych)
GIOS_FILE_CATALOG = {
    "metadata": {
        "id": 643,
        "default_name": "Metadane oraz kody stacji i stanowisk pomiarowych.xlsx",
        "description": "Station and measurement point metadata and codes",
        "category": "metadata",
    },
    "statistics_2000_2025": {
        "id": 642,
        "default_name": "Statystyki_2000_2025.zip",
        "description": "Annual aggregated air quality statistics 2000-2025",
        "category": "statistics",
    },
    "measurements_2020": {
        "id": 424,
        "default_name": "2020.zip",
        "year": 2020,
        "description": "Hourly and daily air quality measurements for 2020",
        "category": "measurements",
    },
    "measurements_2021": {
        "id": 486,
        "default_name": "2021.zip",
        "year": 2021,
        "description": "Hourly and daily air quality measurements for 2021",
        "category": "measurements",
    },
    "measurements_2022": {
        "id": 524,
        "default_name": "2022.zip",
        "year": 2022,
        "description": "Hourly and daily air quality measurements for 2022",
        "category": "measurements",
    },
    "measurements_2023": {
        "id": 564,
        "default_name": "2023.zip",
        "year": 2023,
        "description": "Hourly and daily air quality measurements for 2023",
        "category": "measurements",
    },
    "measurements_2024": {
        "id": 582,
        "default_name": "2024.zip",
        "year": 2024,
        "description": "Hourly and daily air quality measurements for 2024",
        "category": "measurements",
    },
    "measurements_2025": {
        "id": 644,
        "default_name": "2025.zip",
        "year": 2025,
        "description": "Hourly and daily air quality measurements for 2025",
        "category": "measurements",
    },
}


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


from drive_safe_upload import safe_drive_upload


def _upload_file_to_drive(
    storage: StorageManager,
    local_path: Path,
    name: str,
    parent_id: str,
    mime_type: str = "application/octet-stream",
) -> dict[str, Any]:
    return safe_drive_upload(
        storage,
        local_path,
        name,
        parent_id,
        mime_type=mime_type,
    )


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


def download_stream(url: str, target_path: Path, max_retries: int = 4) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "ZoheloData/1.0 (+data.zohelo.com)"})
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp, target_path.open("wb") as f_out:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f_out.write(chunk)
            return
        except Exception as exc:
            if attempt == max_retries - 1:
                raise
            logger.warning("Download attempt %d failed for %s: %s. Retrying...", attempt + 1, url, exc)


def run_gios_bulk_ingestion(
    workspace: Path,
    years: list[int] | None = None,
    include_metadata: bool = True,
    storage: StorageManager | None = None,
    allow_codespace: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    """Execute native-only bulk landing for GIOŚ public air quality archives."""
    workspace.mkdir(parents=True, exist_ok=True)
    downloaded_files: list[dict[str, Any]] = []

    # 1. Metadata workbook
    if include_metadata:
        meta_spec = GIOS_FILE_CATALOG["metadata"]
        meta_url = f"{BASE_ARCHIVES_URL}{meta_spec['id']}"
        meta_file = workspace / meta_spec["default_name"]
        if not meta_file.exists():
            logger.info("Downloading GIOŚ stations metadata workbook from %s...", meta_url)
            download_stream(meta_url, meta_file)
        sha, md5 = _hash_file(meta_file)
        downloaded_files.append({
            "file_name": meta_spec["default_name"],
            "local_path": str(meta_file),
            "source_url": meta_url,
            "document_id": meta_spec["id"],
            "size_bytes": meta_file.stat().st_size,
            "sha256": sha,
            "md5": md5,
            "category": "metadata",
        })

    # 2. Measurement archives by year
    if years:
        for yr in years:
            key = f"measurements_{yr}"
            if key not in GIOS_FILE_CATALOG:
                logger.warning("Year %d is not configured in GIOS_FILE_CATALOG. Skipping.", yr)
                continue
            spec = GIOS_FILE_CATALOG[key]
            url = f"{BASE_ARCHIVES_URL}{spec['id']}"
            local_file = workspace / spec["default_name"]
            if not local_file.exists():
                logger.info("Downloading GIOŚ air quality measurements archive for %d from %s...", yr, url)
                download_stream(url, local_file)
            sha, md5 = _hash_file(local_file)
            downloaded_files.append({
                "file_name": spec["default_name"],
                "local_path": str(local_file),
                "source_url": url,
                "document_id": spec["id"],
                "year": yr,
                "size_bytes": local_file.stat().st_size,
                "sha256": sha,
                "md5": md5,
                "category": "measurements",
            })

    receipt_data = {
        "source_id": "gios_pjp_bulk",
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "years": years or [],
        "include_metadata": include_metadata,
        "total_files": len(downloaded_files),
        "total_bytes": sum(f["size_bytes"] for f in downloaded_files),
        "files": downloaded_files,
    }

    if skip_upload:
        receipt_path = workspace / "gios_receipt.json"
        receipt_path.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")
        return {"status": "downloaded_locally", "receipt": str(receipt_path), **receipt_data}

    if storage is None:
        storage = StorageManager()

    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    if not in_actions and not allow_codespace:
        raise PermissionError("Production Drive upload requires --allow-codespace or GITHUB_ACTIONS=true.")

    landing_root = storage.resolve_zone("landing", create=False)
    gios_landing = _resolve_or_create_folder(storage, "gios_pjp", landing_root)
    native_folder = _resolve_or_create_folder(storage, "native", gios_landing)
    bulk_folder = _resolve_or_create_folder(storage, "bulk", native_folder)

    uploaded_files = []
    for item in downloaded_files:
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if item["file_name"].endswith(".xlsx") else "application/zip"
        res = _upload_file_to_drive(storage, Path(item["local_path"]), item["file_name"], bulk_folder, mime_type=mime)
        uploaded_files.append({
            "file_name": item["file_name"],
            "drive_id": res["id"],
            "size_bytes": res["size"],
            "sha256": item["sha256"],
            "reused": res["reused"],
        })

    # Save campaign control receipt
    control_root = storage.resolve_zone("control", create=True)
    campaigns_folder = _resolve_or_create_folder(storage, "source_campaigns", control_root)
    gios_control = _resolve_or_create_folder(storage, "gios_pjp", campaigns_folder)

    receipt_file = workspace / "gios_receipt.json"
    receipt_data["drive_files"] = uploaded_files
    receipt_data["status"] = "landed_natively"
    receipt_file.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")

    _upload_file_to_drive(storage, receipt_file, "receipt.json", gios_control, mime_type="application/json")

    return {
        "status": "completed",
        "total_files": len(uploaded_files),
        "total_bytes": sum(f["size_bytes"] for f in uploaded_files),
        "receipt": receipt_data,
    }


def main():
    parser = argparse.ArgumentParser(description="GIOŚ public air quality bulk extractor per ADR 0009.")
    parser.add_argument("--workspace", type=Path, default=REPO_ROOT / "portal" / "test-results" / "gios-bulk")
    parser.add_argument("--years", type=int, nargs="*", default=[2023])
    parser.add_argument("--metadata-only", action="store_true", help="Download only stations metadata workbook.")
    parser.add_argument("--skip-upload", action="store_true", help="Download locally without uploading to Google Drive.")
    parser.add_argument("--allow-codespace", action="store_true", help="Authorize Google Drive writes in devcontainer.")
    args = parser.parse_args()

    years = [] if args.metadata_only else args.years
    run_gios_bulk_ingestion(
        workspace=args.workspace,
        years=years,
        include_metadata=True,
        allow_codespace=args.allow_codespace,
        skip_upload=args.skip_upload,
    )


if __name__ == "__main__":
    main()
