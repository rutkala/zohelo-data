"""Landing-to-Bronze Parquet transformation loader for GUS BDL Web Bulk Downloads.

Vectorized streaming DuckDB architecture:
- Reads native BDL Web ZIP archives from Google Drive or local landing directory
- Extracts and parses native CSVs using DuckDB vectorized read_csv
- Preserves 12-character territorial unit codes with leading zeros and 7-digit municipal TERC codes
- Normalizes Polish decimal commas and strips whitespace (space, nbsp, narrow nbsp)
- Captures dynamic dimensions into a structured JSON payload
- Writes partitioned Parquet files (part_{subgroup_id}_{selection_id}.parquet) into 02_bronze/gus_bdl/
- Uploads to Google Drive 02_bronze/gus_bdl/observations/ idempotently
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import logging
import os
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
        drive_lock=DRIVE_LOCK,
    )


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


def generate_partition_filename(
    subgroup_id: str,
    selection_id: str = "",
    part_hash: str = "",
) -> str:
    """Generate partition filename that includes subgroup_id and selection_id (or part hash).

    Prevents partition overwriting when subgroups are adaptively split into multiple slices.
    """
    clean_sub = subgroup_id.strip()
    clean_sel = selection_id.strip()
    if clean_sel:
        safe_sel = re.sub(r"[^A-Za-z0-9_-]", "_", clean_sel)
        return f"part_{clean_sub}_{safe_sel}.parquet"
    if part_hash:
        safe_hash = re.sub(r"[^A-Za-z0-9]", "", part_hash.strip())[:16]
        return f"part_{clean_sub}_{safe_hash}.parquet"
    return f"part_{clean_sub}.parquet"


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _escape_sql_ident(ident: str) -> str:
    return ident.replace('"', '""')


def process_bdl_zip_to_parquet(
    zip_bytes: bytes,
    subgroup_id: str,
    output_parquet_path: Path,
    con: duckdb.DuckDBPyConnection,
    selection_id: str = "",
    raw_archive_file: str = "",
) -> int:
    """Extract all CSVs from ZIP and convert to standardized Parquet via DuckDB."""
    target_path = Path(output_parquet_path)
    if target_path.is_dir():
        target_path = target_path / generate_partition_filename(subgroup_id, selection_id)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        csv_names = [n for n in z.namelist() if n.endswith(".csv")]
        if not csv_names:
            return 0

        with tempfile.TemporaryDirectory(prefix=f"bdl_{subgroup_id}_") as tmp_dir:
            tmp_csv_paths = []
            for idx, name in enumerate(csv_names):
                clean_name = f"{idx}_{Path(name).name}"
                csv_path = Path(tmp_dir) / clean_name
                csv_path.write_bytes(z.read(name))
                tmp_csv_paths.append(str(csv_path))

            paths_expr = "[" + ", ".join([f"'{_escape_sql_literal(p)}'" for p in tmp_csv_paths]) + "]"

            # Inspect unioned columns across all CSVs in archive
            inspect_query = f"""
                select * from read_csv({paths_expr},
                    delim=';',
                    header=true,
                    all_varchar=true,
                    quote='"',
                    escape='"',
                    union_by_name=true
                ) limit 1
            """
            cols = [col[0] for col in con.execute(inspect_query).description]
            standard_cols = {"Kod", "Nazwa", "Rok", "Okres", "Wartosc", "Jednostka miary", "Atrybut"}
            dim_cols = [c for c in cols if c not in standard_cols and c.strip()]

            if dim_cols:
                dim_json_expr = "json_object(" + ", ".join([f"'{_escape_sql_literal(c)}', coalesce(\"{_escape_sql_ident(c)}\", '')" for c in dim_cols]) + ")"
            else:
                dim_json_expr = "'{}'"

            period_col = "Rok" if "Rok" in cols else ("Okres" if "Okres" in cols else None)
            if period_col:
                escaped_period_col = f'"{_escape_sql_ident(period_col)}"'
                period_raw_expr = f'trim(coalesce({escaped_period_col}, \'\'))'
                period_year_expr = f"""coalesce(
                    try_cast(trim({escaped_period_col}) as integer),
                    try_cast(regexp_extract(trim({escaped_period_col}), '([0-9]{{4}})', 1) as integer)
                )"""
                where_clause = f'where {escaped_period_col} is not null and trim({escaped_period_col}) != \'\''
            else:
                period_raw_expr = "''"
                period_year_expr = "cast(null as integer)"
                where_clause = ""

            escaped_subgroup = _escape_sql_literal(subgroup_id)
            escaped_selection = _escape_sql_literal(selection_id)
            escaped_archive = _escape_sql_literal(raw_archive_file)

            kod_expr = '"Kod"' if "Kod" in cols else "''"
            nazwa_expr = 'trim(coalesce("Nazwa", \'\'))' if "Nazwa" in cols else "''"
            wartosc_expr = '"Wartosc"' if "Wartosc" in cols else "null"
            jm_expr = 'trim(coalesce("Jednostka miary", \'\'))' if "Jednostka miary" in cols else "''"
            attr_expr = 'trim(coalesce("Atrybut", \'\'))' if "Atrybut" in cols else "''"

            val_numeric_sql = f"""try_cast(
                replace(
                    replace(
                        replace(
                            replace(trim(coalesce({wartosc_expr}, '')), ' ', ''),
                            chr(160), ''
                        ),
                        chr(8239), ''
                    ),
                    ',', '.'
                ) as double
            )"""

            val_raw_sql = f'trim(coalesce({wartosc_expr}, \'\'))'

            unit_id_sql = f"""case
                when length(trim(coalesce({kod_expr}, ''))) = 0 then ''
                when length(trim(coalesce({kod_expr}, ''))) <= 2 then lpad(trim({kod_expr}), 2, '0')
                when length(trim(coalesce({kod_expr}, ''))) <= 4 then lpad(trim({kod_expr}), 4, '0')
                when length(trim(coalesce({kod_expr}, ''))) <= 7 then lpad(trim({kod_expr}), 7, '0')
                when length(trim(coalesce({kod_expr}, ''))) <= 12 then lpad(trim({kod_expr}), 12, '0')
                else trim({kod_expr})
            end"""

            terc_code_sql = f"""case
                when length(trim(coalesce({kod_expr}, ''))) = 7 then trim({kod_expr})
                when length(trim(coalesce({kod_expr}, ''))) = 6 then lpad(trim({kod_expr}), 7, '0')
                when length(trim(coalesce({kod_expr}, ''))) = 12 then substr(trim({kod_expr}), 3, 7)
                when length(trim(coalesce({kod_expr}, ''))) = 11 then substr(lpad(trim({kod_expr}), 12, '0'), 3, 7)
                else null
            end"""

            escaped_target_path = _escape_sql_literal(str(target_path))

            transform_sql = f"""
                copy (
                    select
                        '{escaped_subgroup}' as subgroup_id,
                        '{escaped_selection}' as selection_id,
                        {unit_id_sql} as unit_id,
                        {nazwa_expr} as unit_name,
                        {terc_code_sql} as terc_code,
                        {period_raw_expr} as period_raw,
                        {period_year_expr} as period_year,
                        {val_raw_sql} as val_raw,
                        {val_numeric_sql} as val_numeric,
                        {jm_expr} as measure_unit,
                        {attr_expr} as attr_name,
                        {dim_json_expr} as dimensions_json,
                        '{escaped_archive}' as raw_archive_file,
                        current_timestamp as processed_at_utc
                    from read_csv({paths_expr},
                        delim=';',
                        header=true,
                        all_varchar=true,
                        quote='"',
                        escape='"',
                        union_by_name=true
                    )
                    {where_clause}
                ) to '{escaped_target_path}' (format 'parquet', compression 'zstd');
            """
            con.execute(transform_sql)
            row_count = con.execute(f"select count(*) from '{escaped_target_path}'").fetchone()[0]
            return row_count



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
