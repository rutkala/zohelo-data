import os
import sys
import duckdb
from io import BytesIO
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# Ensure Python locates storage_manager from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage_manager import StorageManager


def _get_zone_id(storage: StorageManager, zone_name: str) -> str:
    """Resolves the Google Drive folder ID for a given zone."""
    master_id = storage._get_or_create_folder(storage.master_folder_name)
    return storage._get_or_create_folder(zone_name, parent_id=master_id)


def _list_files_recursively(drive_service, folder_id: str) -> list:
    """Returns all non-folder files under a given Drive folder (recursive)."""
    results = []
    page_token = None
    while True:
        query = f"'{folder_id}' in parents and trashed=false"
        response = drive_service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token,
        ).execute()
        for item in response.get("files", []):
            if item["mimeType"] == "application/vnd.google-apps.folder":
                results.extend(_list_files_recursively(drive_service, item["id"]))
            else:
                results.append(item)
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return results


def _download_file(drive_service, file_id: str, local_path: str):
    """Downloads a Drive file to a local path."""
    request = drive_service.files().get_media(fileId=file_id)
    with open(local_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def _upload_file(drive_service, local_path: str, filename: str, parent_id: str):
    """Uploads a local file to a Drive folder."""
    file_metadata = {"name": filename, "parents": [parent_id]}
    media = MediaFileUpload(local_path, mimetype="application/octet-stream", resumable=True)
    drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()


def _move_file(drive_service, file_id: str, source_parent_id: str, dest_parent_id: str):
    """Moves a Drive file from one folder to another."""
    drive_service.files().update(
        fileId=file_id,
        addParents=dest_parent_id,
        removeParents=source_parent_id,
        fields="id, parents",
    ).execute()


def process_bronze():
    print("🥉 Starting Bronze Layer transformation...")
    storage = StorageManager(backend="gdrive")
    drive = storage.drive_service
    con = duckdb.connect(":memory:")

    landing_id = _get_zone_id(storage, "01_landing")
    bronze_id = _get_zone_id(storage, "02_bronze")
    archive_id = _get_zone_id(storage, "05_archive")

    files = _list_files_recursively(drive, landing_id)
    if not files:
        print("ℹ️  No files found in 01_landing. Nothing to process.")
        return

    print(f"📂 Found {len(files)} file(s) in 01_landing.")
    success_count = 0
    fail_count = 0

    for file_item in files:
        file_id = file_item["id"]
        file_name = file_item["name"]
        stem = os.path.splitext(file_name)[0]

        # Include file_id in temp path to avoid collisions when files share the same stem
        local_json = f"/tmp/{file_id}_{stem}.json"
        local_parquet = f"/tmp/{file_id}_{stem}.parquet"

        # Guard: ensure constructed paths stay within /tmp and contain no quotes
        # (DuckDB COPY does not accept parameterised output paths)
        if not local_parquet.startswith("/tmp/") or "'" in local_parquet or '"' in local_parquet:
            print(f"  ❌ Skipping {file_name}: unsafe temp path derived from file metadata.")
            fail_count += 1
            continue

        try:
            # Download JSON from Drive to local disk
            print(f"  ⬇️  Downloading {file_name}...")
            _download_file(drive, file_id, local_json)

            # Convert JSON → Parquet via DuckDB
            # Note: DuckDB COPY does not support parameterised output paths;
            # local_parquet is a controlled /tmp path so interpolation is safe.
            print(f"  🔄 Converting {file_name} to Parquet...")
            con.execute(
                f"COPY (SELECT * FROM read_json_auto(?)) TO '{local_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                [local_json],
            )

            # Upload Parquet to 02_bronze
            parquet_name = f"{stem}.parquet"
            print(f"  ⬆️  Uploading {parquet_name} to 02_bronze...")
            _upload_file(drive, local_parquet, parquet_name, bronze_id)

            # Move original JSON from 01_landing to 05_archive
            print(f"  📦 Archiving {file_name} to 05_archive...")
            _move_file(drive, file_id, landing_id, archive_id)

            success_count += 1
            print(f"  ✅ {file_name} processed successfully.")

        except Exception as exc:
            fail_count += 1
            print(f"  ❌ Failed to process {file_name}: {exc}")

        finally:
            # Clean up local temp files
            for path in (local_json, local_parquet):
                if os.path.exists(path):
                    os.remove(path)

    print(
        f"\n🏁 Bronze Layer complete. "
        f"Success: {success_count} | Failed: {fail_count}"
    )
    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    process_bronze()
