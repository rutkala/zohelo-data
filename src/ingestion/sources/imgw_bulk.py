"""Official bulk extractor for IMGW-PIB public meteorological & hydrological observational archives (PL-ENV-008 / PL-ENV-009).

In accordance with ADR 0009 (Native-Only Landing):
- Downloads official native files and ZIP archives directly from danepubliczne.imgw.pl
- Computes SHA-256 and MD5 checksums for byte integrity
- Stores native byte-for-byte archives in 01_landing/imgw_pib/native/bulk/
- Generates durable completion receipt in 06_control/source_campaigns/imgw_pib/
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
import re
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

logger = logging.getLogger("imgw_bulk")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

BASE_METEO_URL = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/"
BASE_HYDRO_URL = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_hydrologiczne/"


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


def list_remote_directory(url: str) -> list[str]:
    """Parse apache index html to retrieve list of links."""
    req = urllib.request.Request(url, headers={"User-Agent": "ZoheloData/1.0 (+data.zohelo.com)"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        logger.warning("Could not read remote directory %s: %s", url, e)
        return []

    links = re.findall(r'<a\s+href="([^"?]+)"', html)
    # Exclude parent dir link
    valid = [l for l in links if not l.startswith("/") and not l.startswith("?") and l not in ("Parent Directory", "")]
    return valid


def download_stream(url: str, target_path: Path, max_retries: int = 4) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "ZoheloData/1.0 (+data.zohelo.com)"})
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp, target_path.open("wb") as f_out:
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


def run_imgw_bulk_ingestion(
    workspace: Path,
    years: list[int] | None = None,
    storage: StorageManager | None = None,
    allow_codespace: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    """Execute native-only bulk landing for IMGW public meteorological data."""
    workspace.mkdir(parents=True, exist_ok=True)
    if years is None:
        years = [2022, 2023]  # Default to recent validated historical years

    downloaded_files: list[dict[str, Any]] = []

    # 1. Download stations catalog (wykaz_stacji.csv)
    stations_url = BASE_METEO_URL + "wykaz_stacji.csv"
    stations_file = workspace / "wykaz_stacji.csv"
    logger.info("Downloading IMGW stations catalog from %s...", stations_url)
    download_stream(stations_url, stations_file)
    sha, md5 = _hash_file(stations_file)
    downloaded_files.append({
        "file_name": "wykaz_stacji.csv",
        "local_path": str(stations_file),
        "source_url": stations_url,
        "size_bytes": stations_file.stat().st_size,
        "sha256": sha,
        "md5": md5,
        "category": "stations_catalog",
    })

    # 2. Download synoptic daily archives for requested years
    for yr in years:
        yr_url = f"{BASE_METEO_URL}dobowe/synop/{yr}/"
        logger.info("Discovering synoptic archives for year %d at %s...", yr, yr_url)
        files = list_remote_directory(yr_url)
        zip_files = [f for f in files if f.endswith(".zip")]
        logger.info("Found %d station zip archives for year %d.", len(zip_files), yr)

        yr_dir = workspace / f"synop_{yr}"
        yr_dir.mkdir(parents=True, exist_ok=True)

        for zname in zip_files:
            zurl = yr_url + zname
            zlocal = yr_dir / zname
            if not zlocal.exists():
                download_stream(zurl, zlocal)
            z_sha, z_md5 = _hash_file(zlocal)
            downloaded_files.append({
                "file_name": zname,
                "local_path": str(zlocal),
                "source_url": zurl,
                "year": yr,
                "size_bytes": zlocal.stat().st_size,
                "sha256": z_sha,
                "md5": z_md5,
                "category": "synoptic_daily",
            })

    receipt_data = {
        "source_id": "imgw_pib_bulk",
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "years": years,
        "total_files": len(downloaded_files),
        "total_bytes": sum(f["size_bytes"] for f in downloaded_files),
        "files": downloaded_files,
    }

    if skip_upload:
        receipt_path = workspace / "imgw_receipt.json"
        receipt_path.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")
        return {"status": "downloaded_locally", "receipt": str(receipt_path), **receipt_data}

    # Upload to Google Drive if not skipped
    if storage is None:
        storage = StorageManager()

    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    if not in_actions and not allow_codespace:
        raise PermissionError("Production Drive upload requires --allow-codespace or GITHUB_ACTIONS=true.")

    landing_root = storage.resolve_zone("landing", create=False)
    imgw_landing = _resolve_or_create_folder(storage, "imgw_pib", landing_root)
    native_folder = _resolve_or_create_folder(storage, "native", imgw_landing)
    bulk_folder = _resolve_or_create_folder(storage, "bulk", native_folder)

    uploaded_items = []
    for item in downloaded_files:
        p = Path(item["local_path"])
        mime = "text/csv" if p.suffix == ".csv" else "application/zip"
        res = _upload_file_to_drive(storage, p, name=p.name, parent_id=bulk_folder, mime_type=mime)
        uploaded_items.append({"name": p.name, "drive_id": res["id"], "reused": res.get("reused", False)})

    # Upload receipt to control zone
    control_root = storage.resolve_zone("control", create=True)
    campaign_control = _resolve_or_create_folder(storage, "source_campaigns", control_root)
    imgw_control = _resolve_or_create_folder(storage, "imgw_pib", campaign_control)

    receipt_path = workspace / "imgw_receipt.json"
    receipt_data["drive_uploads"] = uploaded_items
    receipt_path.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")
    _upload_file_to_drive(storage, receipt_path, name="receipt.json", parent_id=imgw_control, mime_type="application/json")

    logger.info("IMGW bulk native landing completed: %d files uploaded to Drive.", len(uploaded_items))
    return {"status": "completed", **receipt_data}


def main():
    parser = argparse.ArgumentParser(description="IMGW-PIB Native Bulk Ingestion")
    parser.add_argument("--workspace", type=str, default="portal/test-results/imgw-bulk")
    parser.add_argument("--years", type=int, nargs="+", default=[2022, 2023])
    parser.add_argument("--allow-codespace", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    res = run_imgw_bulk_ingestion(
        workspace=Path(args.workspace).resolve(),
        years=args.years,
        allow_codespace=args.allow_codespace,
        skip_upload=args.skip_upload,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
