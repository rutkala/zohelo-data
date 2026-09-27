"""Landing-to-Bronze vectorized loader for GUGiK PRG (Państwowy Rejestr Granic).

In accordance with ADR 0009 and ADR 0007:
- Reads native ZIP archives from local landing or Google Drive 01_landing/gugik_prg/native/bulk/
- Parses official administrative boundaries ESRI Shapefiles using DuckDB spatial ST_ReadSHP
- Produces clean, typed Bronze Parquet datasets with WKT geometries:
    1. br_prg_country.parquet: national boundary (Polska)
    2. br_prg_voivodeships.parquet: voivodeship boundaries (16 units)
    3. br_prg_counties.parquet: county boundaries (380 units)
    4. br_prg_municipalities.parquet: municipality boundaries (2,479 units)
- Uploads Parquet datasets to Google Drive 02_bronze/gugik_prg/
- Emits completion receipt in 06_control/source_campaigns/gugik_prg/
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
import tempfile
from typing import Any
import zipfile

import duckdb
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage_manager import StorageManager

logger = logging.getLogger("prg_bronze_loader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


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
    mime_type: str = "application/octet-stream",
) -> dict[str, Any]:
    return safe_drive_upload(
        storage,
        local_path,
        name,
        parent_id,
        mime_type=mime_type,
    )


def _download_prg_from_drive(storage: StorageManager, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    landing_id = storage.resolve_zone("landing")
    q_src = f"name='gugik_prg' and '{_escape_query(landing_id)}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res_src = storage.drive_service.files().list(q=q_src, spaces="drive", fields="files(id,name)").execute().get("files", [])
    if not res_src:
        raise RuntimeError("GUGiK PRG folder not found in 01_landing on Drive")

    q_nat = f"name='native' and '{res_src[0]['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res_nat = storage.drive_service.files().list(q=q_nat, spaces="drive", fields="files(id,name)").execute().get("files", [])
    if not res_nat:
        raise RuntimeError("native folder not found under gugik_prg in 01_landing on Drive")

    q_blk = f"name='bulk' and '{res_nat[0]['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res_blk = storage.drive_service.files().list(q=q_blk, spaces="drive", fields="files(id,name)").execute().get("files", [])
    if not res_blk:
        raise RuntimeError("bulk folder not found under native in gugik_prg on Drive")

    q_files = f"name='00_jednostki_administracyjne.zip' and '{res_blk[0]['id']}' in parents and trashed=false"
    files = storage.drive_service.files().list(q=q_files, spaces="drive", fields="files(id,name,size)").execute().get("files", [])
    if not files:
        raise RuntimeError("00_jednostki_administracyjne.zip not found on Drive")

    f_info = files[0]
    target_path = dest_dir / "00_jednostki_administracyjne.zip"
    if not target_path.exists() or target_path.stat().st_size != int(f_info.get("size", -1)):
        logger.info("Downloading %s from Drive...", target_path.name)
        req = storage.drive_service.files().get_media(fileId=f_info["id"])
        with target_path.open("wb") as f_out:
            downloader = MediaIoBaseDownload(f_out, req)
            done = False
            while not done:
                _, done = downloader.next_chunk()
    return target_path


def transform_prg_bronze(
    landing_dir: Path,
    output_dir: Path,
    allow_codespace: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    can_upload = (in_actions or allow_codespace) and not skip_upload

    storage = None
    target_zip = landing_dir / "00_jednostki_administracyjne.zip"
    if can_upload or not target_zip.exists():
        if allow_codespace:
            os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
        storage = StorageManager(allow_interactive_auth=False)
        storage.resolve_root(create=False)
        if can_upload:
            storage.authorize_writes()

    if not target_zip.exists():
        if not storage:
            raise RuntimeError(f"00_jednostki_administracyjne.zip not found in {landing_dir}")
        target_zip = _download_prg_from_drive(storage, landing_dir)

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    now_utc = datetime.now(timezone.utc).isoformat()
    generated_parquet = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        logger.info("Extracting administrative boundary shapefiles from %s...", target_zip.name)
        with zipfile.ZipFile(target_zip) as z:
            for fname in z.namelist():
                if fname.startswith("A0") and not fname.startswith("A05") and not fname.startswith("A06"):
                    z.extract(fname, tmp_path)

        # 1. Country boundary (A00_Granice_panstwa)
        shp_country = tmp_path / "A00_Granice_panstwa.shp"
        if shp_country.exists():
            pq_country = output_dir / "br_prg_country.parquet"
            logger.info("Transforming country boundary to %s...", pq_country.name)
            con.execute(f"""
                COPY (
                    SELECT
                        'PL' AS country_code,
                        JPT_NAZWA_ AS country_name,
                        JPT_POWIER AS surface_area_ha,
                        ST_AsText(geom) AS geometry_wkt,
                        TIMESTAMPTZ '{now_utc}' AS extracted_at_utc
                    FROM ST_ReadSHP('{shp_country}')
                ) TO '{pq_country}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """)
            cnt = con.execute(f"SELECT count(*) FROM '{pq_country}'").fetchone()[0]
            generated_parquet["br_prg_country.parquet"] = {"path": pq_country, "row_count": cnt}
            logger.info("Country boundary Bronze completed: %d rows.", cnt)

        # 2. Voivodeship boundaries (A01_Granice_wojewodztw)
        shp_woj = tmp_path / "A01_Granice_wojewodztw.shp"
        if shp_woj.exists():
            pq_woj = output_dir / "br_prg_voivodeships.parquet"
            logger.info("Transforming voivodeship boundaries to %s...", pq_woj.name)
            con.execute(f"""
                COPY (
                    SELECT
                        lpad(JPT_KOD_JE, 2, '0') AS teryt_code,
                        JPT_NAZWA_ AS voivodeship_name,
                        JPT_POWIER AS surface_area_ha,
                        REGON AS regon,
                        ST_AsText(geom) AS geometry_wkt,
                        TIMESTAMPTZ '{now_utc}' AS extracted_at_utc
                    FROM ST_ReadSHP('{shp_woj}')
                ) TO '{pq_woj}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """)
            cnt = con.execute(f"SELECT count(*) FROM '{pq_woj}'").fetchone()[0]
            generated_parquet["br_prg_voivodeships.parquet"] = {"path": pq_woj, "row_count": cnt}
            logger.info("Voivodeship boundaries Bronze completed: %d rows.", cnt)

        # 3. County boundaries (A02_Granice_powiatow)
        shp_pow = tmp_path / "A02_Granice_powiatow.shp"
        if shp_pow.exists():
            pq_pow = output_dir / "br_prg_counties.parquet"
            logger.info("Transforming county boundaries to %s...", pq_pow.name)
            con.execute(f"""
                COPY (
                    SELECT
                        lpad(JPT_KOD_JE, 4, '0') AS teryt_code,
                        JPT_NAZWA_ AS county_name,
                        JPT_POWIER AS surface_area_ha,
                        REGON AS regon,
                        ST_AsText(geom) AS geometry_wkt,
                        TIMESTAMPTZ '{now_utc}' AS extracted_at_utc
                    FROM ST_ReadSHP('{shp_pow}')
                ) TO '{pq_pow}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """)
            cnt = con.execute(f"SELECT count(*) FROM '{pq_pow}'").fetchone()[0]
            generated_parquet["br_prg_counties.parquet"] = {"path": pq_pow, "row_count": cnt}
            logger.info("County boundaries Bronze completed: %d rows.", cnt)

        # 4. Municipality boundaries (A03_Granice_gmin)
        shp_gmi = tmp_path / "A03_Granice_gmin.shp"
        if shp_gmi.exists():
            pq_gmi = output_dir / "br_prg_municipalities.parquet"
            logger.info("Transforming municipality boundaries to %s...", pq_gmi.name)
            con.execute(f"""
                COPY (
                    SELECT
                        JPT_KOD_JE AS teryt_code,
                        JPT_NAZWA_ AS municipality_name,
                        JPT_POWIER AS surface_area_ha,
                        REGON AS regon,
                        ST_AsText(geom) AS geometry_wkt,
                        TIMESTAMPTZ '{now_utc}' AS extracted_at_utc
                    FROM ST_ReadSHP('{shp_gmi}')
                ) TO '{pq_gmi}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """)
            cnt = con.execute(f"SELECT count(*) FROM '{pq_gmi}'").fetchone()[0]
            generated_parquet["br_prg_municipalities.parquet"] = {"path": pq_gmi, "row_count": cnt}
            logger.info("Municipality boundaries Bronze completed: %d rows.", cnt)

    # Upload to Google Drive if authorized
    drive_receipts = []
    if can_upload and storage:
        bronze_id = storage.resolve_zone("bronze")
        prg_bronze_id = _resolve_or_create_folder(storage, "gugik_prg", bronze_id)

        control_id = storage.resolve_zone("control")
        campaigns_id = _resolve_or_create_folder(storage, "source_campaigns", control_id)
        prg_control_id = _resolve_or_create_folder(storage, "gugik_prg", campaigns_id)

        for filename, item_info in generated_parquet.items():
            fpath = item_info["path"]
            item = _upload_file_to_drive(storage, fpath, filename, prg_bronze_id)
            sha256_hex, md5_hex = _hash_file(fpath)
            drive_receipts.append({
                "table_name": filename,
                "drive_id": item["id"],
                "row_count": item_info["row_count"],
                "size_bytes": item["size"],
                "sha256": sha256_hex,
                "md5": md5_hex,
                "reused": item["reused"],
            })

        receipt_data = {
            "source_id": "gugik_prg",
            "transformed_at_utc": now_utc,
            "bronze_complete": True,
            "tables": drive_receipts,
        }
        receipt_path = output_dir / "prg_bronze_receipt.json"
        receipt_path.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")
        _upload_file_to_drive(storage, receipt_path, "prg_bronze_receipt.json", prg_control_id, mime_type="application/json")
        logger.info("GUGiK PRG Bronze 100% complete and receipt uploaded to Drive.")

    con.close()
    return {
        "status": "success",
        "transformed_at_utc": now_utc,
        "tables": {k: v["row_count"] for k, v in generated_parquet.items()},
        "drive_uploaded": bool(drive_receipts),
    }


def main():
    parser = argparse.ArgumentParser(description="GUGiK PRG Bronze Transformation Loader")
    parser.add_argument("--workspace", type=str, default="portal/test-results/prg", help="Directory with native ZIP archive")
    parser.add_argument("--output-dir", type=str, default="portal/test-results/prg-bronze", help="Directory for Bronze Parquet output")
    parser.add_argument("--allow-codespace", action="store_true", help="Authorize Google Drive upload outside GitHub Actions")
    parser.add_argument("--skip-upload", action="store_true", help="Skip Google Drive upload")
    args = parser.parse_args()

    res = transform_prg_bronze(
        landing_dir=Path(args.workspace).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        allow_codespace=args.allow_codespace,
        skip_upload=args.skip_upload,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
