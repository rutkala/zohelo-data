import os
import site
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# Ensure Python locates storage_manager from src
sys.path.append(str(Path(__file__).resolve().parents[1]))
from storage_manager import StorageManager


LOCAL_ROOT = Path("/tmp/zohelo_data")
LOCAL_BRONZE_DIR = LOCAL_ROOT / "02_bronze"
LOCAL_SILVER_DIR = LOCAL_ROOT / "03_silver"
LOCAL_DUCKDB_PATH = LOCAL_ROOT / "silver_builder.duckdb"
REPO_ROOT = Path(__file__).resolve().parents[2]
STAGING_DIR = REPO_ROOT / "models" / "staging"
BRONZE_DATASET_PREFIX = "nbp_exchange_rates_"
STAGING_MODEL_PREFIX = "stg_nbp_"
STAGING_MODEL_GLOB = f"{STAGING_MODEL_PREFIX}table_*.sql"


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


def _list_child_folders(drive_service, folder_id: str) -> List[dict]:
    folders = []
    page_token = None

    while True:
        response = drive_service.files().list(
            q=(
                f"'{folder_id}' in parents and "
                "mimeType='application/vnd.google-apps.folder' and trashed=false"
            ),
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token,
        ).execute()

        folders.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            return sorted(folders, key=lambda item: item["name"])


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


def _download_bronze_files(storage: StorageManager) -> Dict[str, int]:
    bronze_zone_id = _get_zone_id(storage, "02_bronze")
    downloaded_by_dataset: Dict[str, int] = {}

    for folder in _list_child_folders(storage.drive_service, bronze_zone_id):
        dataset_name = folder["name"]
        files = _list_files_recursively(storage.drive_service, folder["id"])

        for file_item, relative_path in files:
            if not file_item["name"].endswith(".parquet"):
                continue

            local_path = LOCAL_BRONZE_DIR / dataset_name / relative_path
            print(f"⬇️  Downloading 02_bronze/{dataset_name}/{relative_path}...")
            _download_file(storage.drive_service, file_item["id"], local_path)
            downloaded_by_dataset[dataset_name] = downloaded_by_dataset.get(dataset_name, 0) + 1

    return downloaded_by_dataset


def _dataset_to_model_name(dataset_name: str) -> str | None:
    if not dataset_name.startswith(BRONZE_DATASET_PREFIX):
        return None
    return f"{STAGING_MODEL_PREFIX}{dataset_name.removeprefix(BRONZE_DATASET_PREFIX)}"


def _model_to_dataset_name(model_name: str) -> str | None:
    if not model_name.startswith(STAGING_MODEL_PREFIX):
        return None
    return f"{BRONZE_DATASET_PREFIX}{model_name.removeprefix(STAGING_MODEL_PREFIX)}"


def _discover_staging_models(dataset_names: List[str]) -> List[str]:
    available_models = {path.stem for path in STAGING_DIR.glob(STAGING_MODEL_GLOB)}
    selected_models = []

    for dataset_name in sorted(dataset_names):
        model_name = _dataset_to_model_name(dataset_name)
        if not model_name:
            print(f"ℹ️  Dataset 02_bronze/{dataset_name} does not match the NBP exchange-rate pattern; skipping.")
        elif model_name in available_models:
            selected_models.append(model_name)
        else:
            print(f"ℹ️  No staging model found for 02_bronze/{dataset_name}; skipping.")

    return selected_models


def _run_dbt(model_names: List[str]):
    if not model_names:
        print("ℹ️  No dbt models selected. Skipping dbt run.")
        return

    env = os.environ.copy()
    env["ZOHELO_DUCKDB_PATH"] = str(LOCAL_DUCKDB_PATH)
    env["ZOHELO_DATA_ROOT"] = str(LOCAL_ROOT)
    dbt_executable = shutil.which("dbt")
    if not dbt_executable:
        user_dbt = Path(site.USER_BASE) / "bin" / "dbt"
        if user_dbt.exists():
            dbt_executable = str(user_dbt)
        else:
            dbt_executable = "dbt"

    print(f"🔄 Running dbt models: {', '.join(model_names)}...")
    subprocess.run(
        [
            dbt_executable,
            "run",
            "--profiles-dir",
            ".",
            "--select",
            *model_names,
        ],
        cwd=REPO_ROOT,
        check=True,
        env=env,
    )

    con = duckdb.connect(str(LOCAL_DUCKDB_PATH))
    try:
        for model_name in model_names:
            dataset_name = _model_to_dataset_name(model_name)
            if not dataset_name:
                print(f"ℹ️  Model {model_name} does not map to an NBP silver dataset; skipping export.")
                continue
            output_path = LOCAL_SILVER_DIR / dataset_name / f"{dataset_name}.parquet"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            print(f"📦 Exporting {model_name} to {output_path}...")
            con.execute(
                f"COPY {model_name} TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
    finally:
        con.close()


def _upload_silver_outputs(storage: StorageManager):
    silver_zone_id = _get_zone_id(storage, "03_silver")
    for output_path in sorted(LOCAL_SILVER_DIR.rglob("*.parquet")):
        relative_path = output_path.relative_to(LOCAL_SILVER_DIR)
        folder_segments = list(relative_path.parts[:-1])
        if not folder_segments:
            continue

        silver_source_id = storage.get_or_create_nested_folder(folder_segments, root_id=silver_zone_id)
        print(f"⬆️  Uploading 03_silver/{relative_path}...")
        _upload_file(storage.drive_service, output_path, output_path.name, silver_source_id)


def process_silver():
    print("🥈 Starting Silver Layer transformation...")
    _reset_local_workspace()

    storage = StorageManager(backend="gdrive")
    bronze_files_by_dataset = _download_bronze_files(storage)
    bronze_file_count = sum(bronze_files_by_dataset.values())
    if bronze_file_count == 0:
        print("ℹ️  No Bronze Parquet files found in 02_bronze. Nothing to process.")
        return

    print(f"📂 Downloaded {bronze_file_count} Bronze Parquet file(s).")
    model_names = _discover_staging_models(list(bronze_files_by_dataset))
    if not model_names:
        print("ℹ️  No matching staging models found for downloaded Bronze datasets. Nothing to process.")
        return

    _run_dbt(model_names)
    _upload_silver_outputs(storage)
    print("✅ Silver Layer complete.")


if __name__ == "__main__":
    process_silver()
