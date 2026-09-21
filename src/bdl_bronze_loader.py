"""Landing-to-Bronze Parquet transformation loader for GUS BDL Web Bulk Downloads.

Vectorized streaming DuckDB architecture:
- Reads native BDL Web ZIP archives from Google Drive or local landing directory
- Extracts and parses native CSVs using DuckDB vectorized read_csv
- Preserves 12-character territorial unit codes with leading zeros
- Normalizes Polish decimal commas into IEEE 754 floating point numbers
- Captures dynamic dimensions into a structured JSON payload
- Writes partitioned Parquet files (part_{subgroup_id}.parquet) into 02_bronze/gus_bdl/
- Uploads to Google Drive 02_bronze/gus_bdl/observations/ idempotently
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import logging
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
from typing import Any
import zipfile

import duckdb
from googleapiclient.http import MediaFileUpload

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage_manager import StorageManager

logger = logging.getLogger("bdl_bronze_loader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

DRIVE_LOCK = threading.Lock()


def _require_production_context(allow_codespace: bool = False):
    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    if in_actions or allow_codespace:
        return
    raise PermissionError("Writes to Google Drive require --allow-codespace or GITHUB_ACTIONS=true.")


def _escape_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _hash_file(path: Path) -> tuple[str, str]:
    d_sha = hashlib.sha256()
    d_md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            d_sha.update(chunk)
            d_md5.update(chunk)
    return d_sha.hexdigest(), d_md5.hexdigest()


def _upload_file_to_drive(
    storage: StorageManager,
    local_path: Path,
    name: str,
    parent_id: str,
    mime_type: str = "application/octet-stream",
) -> dict[str, Any]:
    sha256_hex, md5_hex = _hash_file(local_path)
    query = f"name='{_escape_query(name)}' and '{_escape_query(parent_id)}' in parents and trashed=false"
    with DRIVE_LOCK:
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
            return {"id": item["id"], "name": name, "size": local_path.stat().st_size, "reused": True}
        with DRIVE_LOCK:
            storage.drive_service.files().delete(fileId=item["id"]).execute()

    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)
    body = {"name": name, "parents": [parent_id], "appProperties": {"sha256": sha256_hex}}
    with DRIVE_LOCK:
        created = storage.drive_service.files().create(
            body=body, media_body=media, fields="id,name,size,md5Checksum"
        ).execute(num_retries=4)
    return {"id": created["id"], "name": name, "size": local_path.stat().st_size, "reused": False}


def _resolve_or_create_folder(storage: StorageManager, folder_name: str, parent_id: str) -> str:
    query = f"name='{_escape_query(folder_name)}' and '{_escape_query(parent_id)}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    with DRIVE_LOCK:
        existing = storage.drive_service.files().list(q=query, spaces="drive", fields="files(id,name)").execute().get("files", [])
    if existing:
        return existing[0]["id"]
    with DRIVE_LOCK:
        created = storage.drive_service.files().create(
            body={"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]},
            fields="id,name",
        ).execute(num_retries=4)
    return created["id"]


def process_bdl_zip_to_parquet(
    zip_bytes: bytes,
    subgroup_id: str,
    output_parquet_path: Path,
    con: duckdb.DuckDBPyConnection,
) -> int:
    """Extract CSV from ZIP and convert to standardized Parquet via DuckDB."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        csv_names = [n for n in z.namelist() if n.endswith(".csv")]
        if not csv_names:
            return 0
        csv_name = csv_names[0]
        csv_bytes = z.read(csv_name)

    # Temporary file for DuckDB streaming CSV scan
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_csv:
        tmp_csv.write(csv_bytes)
        tmp_csv_path = tmp_csv.name

    try:
        # Inspect columns in CSV
        inspect_query = f"""
            select * from read_csv('{tmp_csv_path}',
                delim=';',
                header=true,
                all_varchar=true,
                quote='"',
                escape='"'
            ) limit 1
        """
        cols = [col[0] for col in con.execute(inspect_query).description]
        standard_cols = {"Kod", "Nazwa", "Rok", "Wartosc", "Jednostka miary", "Atrybut"}
        dim_cols = [c for c in cols if c not in standard_cols and c.strip()]

        if dim_cols:
            dim_json_expr = "json_object(" + ", ".join([f"'{c}', coalesce(\"{c}\", '')" for c in dim_cols]) + ")"
        else:
            dim_json_expr = "'{}'"

        transform_sql = f"""
            copy (
                select
                    '{subgroup_id}' as subgroup_id,
                    lpad(trim(coalesce("Kod", '')), 12, '0') as unit_id,
                    trim(coalesce("Nazwa", '')) as unit_name,
                    try_cast(trim("Rok") as integer) as period_year,
                    trim(coalesce("Wartosc", '')) as val_raw,
                    try_cast(replace(trim("Wartosc"), ',', '.') as double) as val_numeric,
                    trim(coalesce("Jednostka miary", '')) as measure_unit,
                    trim(coalesce("Atrybut", '')) as attr_name,
                    {dim_json_expr} as dimensions_json,
                    current_timestamp as processed_at_utc
                from read_csv('{tmp_csv_path}',
                    delim=';',
                    header=true,
                    all_varchar=true,
                    quote='"',
                    escape='"'
                )
                where "Rok" is not null and trim("Rok") != ''
            ) to '{output_parquet_path}' (format 'parquet', compression 'zstd');
        """
        con.execute(transform_sql)
        row_count = con.execute(f"select count(*) from '{output_parquet_path}'").fetchone()[0]
        return row_count
    finally:
        Path(tmp_csv_path).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="BDL Bronze Loader")
    parser.add_argument("--workspace", type=str, default="portal/test-results/bdl-bronze")
    parser.add_argument("--allow-codespace", action="store_true")
    args = parser.parse_args()

    _require_production_context(args.allow_codespace)
    workspace = Path(args.workspace).resolve()
    workspace.mkdir(parents=True, exist_ok=True)

    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    storage.authorize_writes()

    bronze_folder_id = storage.resolve_folder("02_bronze")
    gus_bdl_id = _resolve_or_create_folder(storage, "gus_bdl", bronze_folder_id)
    obs_folder_id = _resolve_or_create_folder(storage, "observations", gus_bdl_id)

    con = duckdb.connect()
    con.execute("PRAGMA memory_limit = '1GB';")

    logger.info("BDL Bronze Loader initialized successfully.")


if __name__ == "__main__":
    main()
