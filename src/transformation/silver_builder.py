import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

import duckdb
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# Ensure Python locates storage_manager from src
sys.path.append(str(Path(__file__).resolve().parents[1]))
from storage_manager import StorageManager


SOURCE_FOLDER = "nbp_exchange_rates_table_a"
LOCAL_ROOT = Path("/tmp/zohelo_data")
LOCAL_BRONZE_DIR = LOCAL_ROOT / "02_bronze" / SOURCE_FOLDER
LOCAL_SILVER_DIR = LOCAL_ROOT / "03_silver" / SOURCE_FOLDER
LOCAL_DUCKDB_PATH = LOCAL_ROOT / "silver_builder.duckdb"
OUTPUT_FILE = "nbp_exchange_rates_table_a.parquet"
MODEL_NAME = "stg_nbp_exchange_rates"
REPO_ROOT = Path(__file__).resolve().parents[2]


def _get_zone_id(storage: StorageManager, zone_name: str) -> str:
    master_id = storage._get_or_create_folder(storage.master_folder_name)
    return storage._get_or_create_folder(zone_name, parent_id=master_id)


def _list_files_recursively(drive_service, folder_id: str, relative_path: str = "") -> List[Tuple[dict, str]]:
    results = []
    page_token = None

    while True:
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            spaces="drive",
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token,
        ).execute()

        for item in response.get("files", []):
            item_path = f"{relative_path}/{item['name']}" if relative_path else item["name"]
            if item["mimeType"] == "application/vnd.google-apps.folder":
                results.extend(_list_files_recursively(drive_service, item["id"], item_path))
            else:
                results.append((item, item_path))

        page_token = response.get("nextPageToken")
        if not page_token:
            return results


def _download_file(drive_service, file_id: str, local_path: Path):
    request = drive_service.files().get_media(fileId=file_id)
    local_path.parent.mkdir(parents=True, exist_ok=True)

    with open(local_path, "wb") as handle:
        downloader = MediaIoBaseDownload(handle, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def _delete_matching_files(drive_service, folder_id: str, filename: str):
    query = (
        f"'{folder_id}' in parents and "
        f"name='{filename}' and trashed=false"
    )
    response = drive_service.files().list(
        q=query,
        spaces="drive",
        fields="files(id)",
    ).execute()

    for item in response.get("files", []):
        drive_service.files().delete(fileId=item["id"]).execute()


def _upload_file(drive_service, local_path: Path, filename: str, parent_id: str):
    _delete_matching_files(drive_service, parent_id, filename)
    media = MediaFileUpload(str(local_path), mimetype="application/octet-stream", resumable=True)
    drive_service.files().create(
        body={"name": filename, "parents": [parent_id]},
        media_body=media,
        fields="id",
    ).execute()


def _reset_local_workspace():
    shutil.rmtree(LOCAL_BRONZE_DIR, ignore_errors=True)
    shutil.rmtree(LOCAL_SILVER_DIR, ignore_errors=True)
    LOCAL_BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_SILVER_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_DUCKDB_PATH.unlink(missing_ok=True)


def _download_bronze_files(storage: StorageManager) -> int:
    bronze_zone_id = _get_zone_id(storage, "02_bronze")
    bronze_source_id = storage.get_or_create_nested_folder([SOURCE_FOLDER], root_id=bronze_zone_id)
    files = _list_files_recursively(storage.drive_service, bronze_source_id)

    downloaded = 0
    for file_item, relative_path in files:
        if not file_item["name"].endswith(".parquet"):
            continue

        local_path = LOCAL_BRONZE_DIR / relative_path
        print(f"⬇️  Downloading 02_bronze/{SOURCE_FOLDER}/{relative_path}...")
        _download_file(storage.drive_service, file_item["id"], local_path)
        downloaded += 1

    return downloaded


def _run_dbt() -> Path:
    env = os.environ.copy()
    env["ZOHELO_DUCKDB_PATH"] = str(LOCAL_DUCKDB_PATH)

    print(f"🔄 Running dbt model {MODEL_NAME}...")
    subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "run", "--profiles-dir", ".", "--select", MODEL_NAME],
        cwd=REPO_ROOT,
        check=True,
        env=env,
    )

    output_path = LOCAL_SILVER_DIR / OUTPUT_FILE
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"📦 Exporting {MODEL_NAME} to {output_path}...")
    con = duckdb.connect(str(LOCAL_DUCKDB_PATH))
    try:
        con.execute(
            f"COPY {MODEL_NAME} TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()

    return output_path


def _upload_silver_file(storage: StorageManager, output_path: Path):
    silver_zone_id = _get_zone_id(storage, "03_silver")
    silver_source_id = storage.get_or_create_nested_folder([SOURCE_FOLDER], root_id=silver_zone_id)
    print(f"⬆️  Uploading 03_silver/{SOURCE_FOLDER}/{output_path.name}...")
    _upload_file(storage.drive_service, output_path, output_path.name, silver_source_id)


def process_silver():
    print("🥈 Starting Silver Layer transformation...")
    _reset_local_workspace()

    storage = StorageManager(backend="gdrive")
    bronze_file_count = _download_bronze_files(storage)
    if bronze_file_count == 0:
        print(f"ℹ️  No Bronze Parquet files found in 02_bronze/{SOURCE_FOLDER}. Nothing to process.")
        return

    print(f"📂 Downloaded {bronze_file_count} Bronze Parquet file(s).")
    output_path = _run_dbt()
    _upload_silver_file(storage, output_path)
    print(f"✅ Silver Layer complete. Uploaded {output_path.name}.")


if __name__ == "__main__":
    process_silver()
