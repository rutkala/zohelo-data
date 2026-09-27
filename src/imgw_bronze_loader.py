"""Landing-to-Bronze Parquet transformation loader for IMGW-PIB public observational data (PL-ENV-008 / PL-ENV-009).

Vectorized streaming DuckDB architecture per ADR 0007:
- Reads native files and zip archives from Landing (01_landing/imgw_pib/native/bulk/)
- Normalizes station catalog into br_imgw_stations.parquet
- Normalizes synoptic daily observations into br_imgw_synoptic_daily.parquet
- Writes typed ZSTD Parquet files
- Uploads to Google Drive 02_bronze/imgw_pib/ idempotently
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

logger = logging.getLogger("imgw_bronze_loader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

DRIVE_LOCK = threading.Lock()


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


def transform_stations_catalog(stations_csv_path: Path, output_parquet: Path) -> int:
    """Transform wykaz_stacji.csv into typed Parquet table."""
    logger.info("Transforming IMGW stations catalog from %s...", stations_csv_path.name)
    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA memory_limit = '500MB'")
    
    con.execute(f"""
        CREATE TABLE stations AS
        SELECT
            TRIM(column0) AS station_code,
            TRIM(column1) AS station_name,
            TRIM(column2) AS station_num,
            CAST('{now_utc_str}' AS TIMESTAMP) AS processed_at_utc
        FROM read_csv(
            '{stations_csv_path}',
            delim=',',
            header=false,
            all_varchar=true,
            ignore_errors=true
        )
        WHERE column0 IS NOT NULL AND TRIM(column0) != ''
    """)

    count = con.execute("SELECT count(*) FROM stations").fetchone()[0]
    con.execute(f"COPY stations TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    logger.info("Created %s with %d station records.", output_parquet.name, count)
    return count


def transform_synoptic_archives(zip_paths: list[Path], output_parquet: Path, workspace: Path) -> int:
    """Transform synoptic daily observation archives into typed Parquet table."""
    logger.info("Transforming %d synoptic archives into %s...", len(zip_paths), output_parquet.name)
    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    duckdb_tmp = workspace / "duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)
    db_file = duckdb_tmp / "imgw_synop.duckdb"
    if db_file.exists():
        db_file.unlink()

    con = duckdb.connect(str(db_file))
    con.execute("PRAGMA memory_limit = '750MB'")
    con.execute(f"PRAGMA temp_directory = '{duckdb_tmp}'")

    con.execute("""
        CREATE TABLE raw_sd (
            station_code VARCHAR,
            station_name VARCHAR,
            year_val VARCHAR,
            month_val VARCHAR,
            day_val VARCHAR,
            tmax_val VARCHAR,
            tmin_val VARCHAR,
            tmean_val VARCHAR,
            tmng_val VARCHAR,
            smdb_val VARCHAR,
            roop_val VARCHAR,
            pksn_val VARCHAR,
            usl_val VARCHAR,
            raw_archive VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE raw_sdt (
            station_code VARCHAR,
            station_name VARCHAR,
            year_val VARCHAR,
            month_val VARCHAR,
            day_val VARCHAR,
            nos_val VARCHAR,
            fws_val VARCHAR,
            temp_val VARCHAR,
            wlgs_val VARCHAR,
            ppps_val VARCHAR,
            pppm_val VARCHAR,
            raw_archive VARCHAR
        )
    """)

    with tempfile.TemporaryDirectory() as tmp_extract:
        for zpath in zip_paths:
            try:
                with zipfile.ZipFile(zpath, "r") as zf:
                    for member in zf.namelist():
                        if member.startswith("s_d_") and not member.startswith("s_d_t_") and member.endswith(".csv"):
                            content = zf.read(member).decode("latin2", errors="replace")
                            out_csv = Path(tmp_extract) / f"sd_{zpath.stem}_{member}"
                            out_csv.write_text(content, encoding="utf-8")
                            con.execute(f"""
                                INSERT INTO raw_sd
                                SELECT
                                    column00, column01, column02, column03, column04,
                                    column05, column07, column09, column11, column13,
                                    column15, column16, column20, '{zpath.name}'
                                FROM read_csv('{out_csv}', delim=',', header=false, all_varchar=true, ignore_errors=true)
                            """)
                            out_csv.unlink(missing_ok=True)
                        elif member.startswith("s_d_t_") and member.endswith(".csv"):
                            content = zf.read(member).decode("latin2", errors="replace")
                            out_csv = Path(tmp_extract) / f"sdt_{zpath.stem}_{member}"
                            out_csv.write_text(content, encoding="utf-8")
                            con.execute(f"""
                                INSERT INTO raw_sdt
                                SELECT
                                    column00, column01, column02, column03, column04,
                                    column05, column07, column09, column13, column15,
                                    column17, '{zpath.name}'
                                FROM read_csv('{out_csv}', delim=',', header=false, all_varchar=true, ignore_errors=true)
                            """)
                            out_csv.unlink(missing_ok=True)
            except Exception as e:
                logger.warning("Error reading archive %s: %s", zpath.name, e)

    # Join and typed projection
    con.execute(f"""
        CREATE TABLE synoptic_daily AS
        SELECT
            sd.station_code,
            sd.station_name,
            TRY_CAST(sd.year_val || '-' || LPAD(sd.month_val, 2, '0') || '-' || LPAD(sd.day_val, 2, '0') AS DATE) AS observation_date,
            TRY_CAST(sd.tmax_val AS DOUBLE) AS tmax_celsius,
            TRY_CAST(sd.tmin_val AS DOUBLE) AS tmin_celsius,
            TRY_CAST(sd.tmean_val AS DOUBLE) AS tmean_celsius,
            TRY_CAST(sd.smdb_val AS DOUBLE) AS precipitation_mm,
            TRIM(sd.roop_val) AS precipitation_type,
            TRY_CAST(sd.pksn_val AS DOUBLE) AS snow_depth_cm,
            TRY_CAST(sd.usl_val AS DOUBLE) AS sunshine_hours,
            TRY_CAST(sdt.fws_val AS DOUBLE) AS wind_speed_ms,
            TRY_CAST(sdt.wlgs_val AS DOUBLE) AS relative_humidity_pct,
            TRY_CAST(sdt.pppm_val AS DOUBLE) AS pressure_sea_level_hpa,
            sd.raw_archive AS raw_archive_file,
            CAST('{now_utc_str}' AS TIMESTAMP) AS processed_at_utc
        FROM raw_sd sd
        LEFT JOIN raw_sdt sdt
          ON sd.station_code = sdt.station_code
         AND sd.year_val = sdt.year_val
         AND sd.month_val = sdt.month_val
         AND sd.day_val = sdt.day_val
        WHERE sd.station_code IS NOT NULL
    """)

    count = con.execute("SELECT count(*) FROM synoptic_daily").fetchone()[0]
    con.execute(f"COPY synoptic_daily TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    if db_file.exists():
        db_file.unlink()
    logger.info("Created %s with %d observation records.", output_parquet.name, count)
    return count


def transform_imgw_bronze(
    landing_workspace: Path,
    output_workspace: Path,
    storage: StorageManager | None = None,
    allow_codespace: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    """Execute complete Landing-to-Bronze transformation for IMGW data."""
    output_workspace.mkdir(parents=True, exist_ok=True)

    stations_csv = landing_workspace / "wykaz_stacji.csv"
    if not stations_csv.exists():
        # Fallback: search in landing_workspace
        candidates = list(landing_workspace.rglob("wykaz_stacji.csv"))
        if candidates:
            stations_csv = candidates[0]
        else:
            raise FileNotFoundError(f"wykaz_stacji.csv not found in {landing_workspace}")

    stations_parquet = output_workspace / "br_imgw_stations.parquet"
    stations_count = transform_stations_catalog(stations_csv, stations_parquet)

    zip_archives = list(landing_workspace.rglob("*.zip"))
    logger.info("Discovered %d zip archives in %s.", len(zip_archives), landing_workspace)
    synop_parquet = output_workspace / "br_imgw_synoptic_daily.parquet"
    obs_count = transform_synoptic_archives(zip_archives, synop_parquet, output_workspace)

    summary_data = {
        "source_id": "imgw_pib_bronze",
        "stations_count": stations_count,
        "observations_count": obs_count,
        "status": "completed",
        "transformed_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": [stations_parquet.name, synop_parquet.name],
    }

    if skip_upload:
        summary_path = output_workspace / "summary.json"
        summary_path.write_text(json.dumps(summary_data, indent=2), encoding="utf-8")
        return {**summary_data, "status": "transformed_locally"}

    if storage is None:
        storage = StorageManager()

    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    if not in_actions and not allow_codespace:
        raise PermissionError("Production Drive upload requires --allow-codespace or GITHUB_ACTIONS=true.")

    bronze_root = storage.resolve_zone("bronze", create=True)
    imgw_bronze = _resolve_or_create_folder(storage, "imgw_pib", bronze_root)

    uploaded_files = []
    res_st = _upload_file_to_drive(storage, stations_parquet, stations_parquet.name, imgw_bronze)
    uploaded_files.append({"file_name": stations_parquet.name, "drive_id": res_st["id"], "size": stations_parquet.stat().st_size})
    
    res_obs = _upload_file_to_drive(storage, synop_parquet, synop_parquet.name, imgw_bronze)
    uploaded_files.append({"file_name": synop_parquet.name, "drive_id": res_obs["id"], "size": synop_parquet.stat().st_size})

    control_root = storage.resolve_zone("control", create=True)
    campaign_control = _resolve_or_create_folder(storage, "source_campaigns", control_root)
    imgw_control = _resolve_or_create_folder(storage, "imgw_pib", campaign_control)

    checkpoint_data = {
        **summary_data,
        "drive_files": uploaded_files,
    }
    cp_path = output_workspace / "checkpoint.json"
    cp_path.write_text(json.dumps(checkpoint_data, indent=2), encoding="utf-8")
    _upload_file_to_drive(storage, cp_path, "checkpoint.json", imgw_control, mime_type="application/json")

    logger.info("IMGW Bronze transformation complete and checkpoint recorded on Drive.")
    return checkpoint_data


def main():
    parser = argparse.ArgumentParser(description="IMGW-PIB Landing-to-Bronze Transformer")
    parser.add_argument("--landing-workspace", type=str, default="portal/test-results/imgw-bulk")
    parser.add_argument("--output-workspace", type=str, default="portal/test-results/imgw-bronze")
    parser.add_argument("--allow-codespace", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    res = transform_imgw_bronze(
        landing_workspace=Path(args.landing_workspace).resolve(),
        output_workspace=Path(args.output_workspace).resolve(),
        allow_codespace=args.allow_codespace,
        skip_upload=args.skip_upload,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
