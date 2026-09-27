"""Landing-to-Bronze vectorized loader for GUS TERYT (TERC, SIMC, ULIC).

In accordance with ADR 0009 and ADR 0007:
- Reads native ZIP archives from local landing or Google Drive 01_landing/gus_teryt/native/bulk/
- Parses official TERYT CSVs using DuckDB vectorized read_csv with quote=''
- Produces clean, typed Bronze Parquet datasets:
    1. br_teryt_terc.parquet: territorial division hierarchy (voivodeship, powiat, gmina)
    2. br_teryt_simc.parquet: locality catalogue (towns, villages, parts of localities)
    3. br_teryt_ulic.parquet: streets catalogue with standard full names
- Uploads Parquet datasets to Google Drive 02_bronze/gus_teryt/
- Emits completion receipt in 06_control/source_campaigns/gus_teryt/
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

logger = logging.getLogger("teryt_bronze_loader")
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


def _download_teryt_from_drive(storage: StorageManager, dest_dir: Path) -> list[Path]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    landing_id = storage.resolve_zone("landing")
    q_src = f"name='gus_teryt' and '{_escape_query(landing_id)}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res_src = storage.drive_service.files().list(q=q_src, spaces="drive", fields="files(id,name)").execute().get("files", [])
    if not res_src:
        raise RuntimeError("GUS TERYT folder not found in 01_landing on Drive")

    q_nat = f"name='native' and '{res_src[0]['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res_nat = storage.drive_service.files().list(q=q_nat, spaces="drive", fields="files(id,name)").execute().get("files", [])
    if not res_nat:
        raise RuntimeError("native folder not found under gus_teryt in 01_landing on Drive")

    q_blk = f"name='bulk' and '{res_nat[0]['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res_blk = storage.drive_service.files().list(q=q_blk, spaces="drive", fields="files(id,name)").execute().get("files", [])
    if not res_blk:
        raise RuntimeError("bulk folder not found under native in gus_teryt on Drive")

    q_files = f"'{res_blk[0]['id']}' in parents and mimeType!='application/vnd.google-apps.folder' and trashed=false"
    files = storage.drive_service.files().list(q=q_files, spaces="drive", fields="files(id,name,size)").execute().get("files", [])

    downloaded = []
    for f_info in files:
        fname = f_info["name"]
        if not fname.endswith(".zip"):
            continue
        target_path = dest_dir / fname
        if not target_path.exists() or target_path.stat().st_size != int(f_info.get("size", -1)):
            logger.info("Downloading %s from Drive...", fname)
            req = storage.drive_service.files().get_media(fileId=f_info["id"])
            with target_path.open("wb") as f_out:
                downloader = MediaIoBaseDownload(f_out, req)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
        downloaded.append(target_path)
    return downloaded


def transform_teryt_bronze(
    landing_dir: Path,
    output_dir: Path,
    allow_codespace: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    can_upload = (in_actions or allow_codespace) and not skip_upload

    storage = None
    if can_upload or not any(landing_dir.glob("*.zip")):
        if allow_codespace:
            os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
        storage = StorageManager(allow_interactive_auth=False)
        storage.resolve_root(create=False)
        if can_upload:
            storage.authorize_writes()

    zip_files = list(landing_dir.glob("*.zip"))
    if not zip_files and storage:
        logger.info("No local archives in %s. Downloading from Drive...", landing_dir)
        zip_files = _download_teryt_from_drive(storage, landing_dir)

    if not zip_files:
        raise RuntimeError(f"No TERYT zip archives found in {landing_dir} or on Drive.")

    terc_zip = next((p for p in zip_files if "TERC" in p.name.upper()), None)
    simc_zip = next((p for p in zip_files if "SIMC" in p.name.upper()), None)
    ulic_zip = next((p for p in zip_files if "ULIC" in p.name.upper()), None)

    if not terc_zip or not simc_zip or not ulic_zip:
        raise RuntimeError(f"Incomplete TERYT dataset: TERC={terc_zip}, SIMC={simc_zip}, ULIC={ulic_zip}")

    con = duckdb.connect()
    now_utc = datetime.now(timezone.utc).isoformat()
    generated_parquet = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        for zf in [terc_zip, simc_zip, ulic_zip]:
            with zipfile.ZipFile(zf) as z:
                z.extractall(tmp_path)

        terc_csv = next(tmp_path.glob("TERC*.csv"))
        simc_csv = next(tmp_path.glob("SIMC*.csv"))
        ulic_csv = next(tmp_path.glob("ULIC*.csv"))

        # 1. Transform TERC
        terc_parquet = output_dir / "br_teryt_terc.parquet"
        logger.info("Transforming TERC to %s...", terc_parquet.name)
        con.execute(f"""
            COPY (
                SELECT
                    lpad(WOJ, 2, '0') AS woj,
                    CASE WHEN POW IS NOT NULL AND POW != '' THEN lpad(POW, 2, '0') ELSE NULL END AS pow,
                    CASE WHEN GMI IS NOT NULL AND GMI != '' THEN lpad(GMI, 2, '0') ELSE NULL END AS gmi,
                    CASE WHEN RODZ IS NOT NULL AND RODZ != '' THEN RODZ ELSE NULL END AS rodz,
                    NAZWA AS nazwa,
                    NAZWA_DOD AS nazwa_dod,
                    STAN_NA AS stan_na,
                    CASE
                        WHEN POW IS NULL OR POW = '' THEN 'wojewodztwo'
                        WHEN GMI IS NULL OR GMI = '' THEN 'powiat'
                        ELSE 'gmina'
                    END AS level,
                    CASE
                        WHEN POW IS NULL OR POW = '' THEN lpad(WOJ, 2, '0')
                        WHEN GMI IS NULL OR GMI = '' THEN lpad(WOJ, 2, '0') || lpad(POW, 2, '0')
                        ELSE lpad(WOJ, 2, '0') || lpad(POW, 2, '0') || lpad(GMI, 2, '0') || coalesce(RODZ, '')
                    END AS teryt_code,
                    CASE
                        WHEN POW IS NULL OR POW = '' THEN NULL
                        WHEN GMI IS NULL OR GMI = '' THEN lpad(WOJ, 2, '0')
                        ELSE lpad(WOJ, 2, '0') || lpad(POW, 2, '0')
                    END AS parent_teryt_code,
                    TIMESTAMPTZ '{now_utc}' AS extracted_at_utc
                FROM read_csv(
                    '{terc_csv}',
                    delim=';',
                    header=true,
                    quote='',
                    columns={{
                        'WOJ': 'VARCHAR',
                        'POW': 'VARCHAR',
                        'GMI': 'VARCHAR',
                        'RODZ': 'VARCHAR',
                        'NAZWA': 'VARCHAR',
                        'NAZWA_DOD': 'VARCHAR',
                        'STAN_NA': 'DATE'
                    }}
                )
            ) TO '{terc_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        terc_count = con.execute(f"SELECT count(*) FROM '{terc_parquet}'").fetchone()[0]
        generated_parquet["br_teryt_terc.parquet"] = {"path": terc_parquet, "row_count": terc_count}
        logger.info("TERC Bronze completed: %d rows.", terc_count)

        # 2. Transform SIMC
        simc_parquet = output_dir / "br_teryt_simc.parquet"
        logger.info("Transforming SIMC to %s...", simc_parquet.name)
        con.execute(f"""
            COPY (
                SELECT
                    lpad(WOJ, 2, '0') AS woj,
                    lpad(POW, 2, '0') AS pow,
                    lpad(GMI, 2, '0') AS gmi,
                    RODZ_GMI AS rodz_gmi,
                    lpad(WOJ, 2, '0') || lpad(POW, 2, '0') || lpad(GMI, 2, '0') || coalesce(RODZ_GMI, '') AS teryt_gmina_code,
                    RM AS rm,
                    MZ AS mz,
                    NAZWA AS nazwa,
                    lpad(SYM, 7, '0') AS sym,
                    lpad(SYMPOD, 7, '0') AS sympod,
                    (SYM = SYMPOD) AS is_parent_locality,
                    STAN_NA AS stan_na,
                    TIMESTAMPTZ '{now_utc}' AS extracted_at_utc
                FROM read_csv(
                    '{simc_csv}',
                    delim=';',
                    header=true,
                    quote='',
                    columns={{
                        'WOJ': 'VARCHAR',
                        'POW': 'VARCHAR',
                        'GMI': 'VARCHAR',
                        'RODZ_GMI': 'VARCHAR',
                        'RM': 'VARCHAR',
                        'MZ': 'VARCHAR',
                        'NAZWA': 'VARCHAR',
                        'SYM': 'VARCHAR',
                        'SYMPOD': 'VARCHAR',
                        'STAN_NA': 'DATE'
                    }}
                )
            ) TO '{simc_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        simc_count = con.execute(f"SELECT count(*) FROM '{simc_parquet}'").fetchone()[0]
        generated_parquet["br_teryt_simc.parquet"] = {"path": simc_parquet, "row_count": simc_count}
        logger.info("SIMC Bronze completed: %d rows.", simc_count)

        # 3. Transform ULIC
        ulic_parquet = output_dir / "br_teryt_ulic.parquet"
        logger.info("Transforming ULIC to %s...", ulic_parquet.name)
        con.execute(f"""
            COPY (
                SELECT
                    lpad(WOJ, 2, '0') AS woj,
                    lpad(POW, 2, '0') AS pow,
                    lpad(GMI, 2, '0') AS gmi,
                    RODZ_GMI AS rodz_gmi,
                    lpad(WOJ, 2, '0') || lpad(POW, 2, '0') || lpad(GMI, 2, '0') || coalesce(RODZ_GMI, '') AS teryt_gmina_code,
                    lpad(SYM, 7, '0') AS sym,
                    lpad(SYM_UL, 5, '0') AS sym_ul,
                    CECHA AS cecha,
                    NAZWA_1 AS nazwa_1,
                    CASE WHEN NAZWA_2 IS NOT NULL AND NAZWA_2 != '' THEN NAZWA_2 ELSE NULL END AS nazwa_2,
                    trim(
                        coalesce(CECHA, '') || ' ' ||
                        case when NAZWA_2 is not null and NAZWA_2 != '' then NAZWA_2 || ' ' else '' end ||
                        NAZWA_1
                    ) AS full_street_name,
                    STAN_NA AS stan_na,
                    TIMESTAMPTZ '{now_utc}' AS extracted_at_utc
                FROM read_csv(
                    '{ulic_csv}',
                    delim=';',
                    header=true,
                    quote='',
                    columns={{
                        'WOJ': 'VARCHAR',
                        'POW': 'VARCHAR',
                        'GMI': 'VARCHAR',
                        'RODZ_GMI': 'VARCHAR',
                        'SYM': 'VARCHAR',
                        'SYM_UL': 'VARCHAR',
                        'CECHA': 'VARCHAR',
                        'NAZWA_1': 'VARCHAR',
                        'NAZWA_2': 'VARCHAR',
                        'STAN_NA': 'DATE'
                    }}
                )
            ) TO '{ulic_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        ulic_count = con.execute(f"SELECT count(*) FROM '{ulic_parquet}'").fetchone()[0]
        generated_parquet["br_teryt_ulic.parquet"] = {"path": ulic_parquet, "row_count": ulic_count}
        logger.info("ULIC Bronze completed: %d rows.", ulic_count)

    # Upload to Google Drive if authorized
    drive_receipts = []
    if can_upload and storage:
        bronze_id = storage.resolve_zone("bronze")
        teryt_bronze_id = _resolve_or_create_folder(storage, "gus_teryt", bronze_id)

        control_id = storage.resolve_zone("control")
        campaigns_id = _resolve_or_create_folder(storage, "source_campaigns", control_id)
        teryt_control_id = _resolve_or_create_folder(storage, "gus_teryt", campaigns_id)

        for filename, item_info in generated_parquet.items():
            fpath = item_info["path"]
            item = _upload_file_to_drive(storage, fpath, filename, teryt_bronze_id)
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
            "source_id": "gus_teryt",
            "transformed_at_utc": now_utc,
            "bronze_complete": True,
            "tables": drive_receipts,
        }
        receipt_path = output_dir / "teryt_bronze_receipt.json"
        receipt_path.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")
        _upload_file_to_drive(storage, receipt_path, "teryt_bronze_receipt.json", teryt_control_id, mime_type="application/json")
        logger.info("TERYT Bronze 100% complete and receipt uploaded to Drive.")

    con.close()
    return {
        "status": "success",
        "transformed_at_utc": now_utc,
        "tables": {k: v["row_count"] for k, v in generated_parquet.items()},
        "drive_uploaded": bool(drive_receipts),
    }


def main():
    parser = argparse.ArgumentParser(description="GUS TERYT Bronze Transformation Loader")
    parser.add_argument("--workspace", type=str, default="portal/test-results/teryt", help="Directory with native ZIP archives")
    parser.add_argument("--output-dir", type=str, default="portal/test-results/teryt-bronze", help="Directory for Bronze Parquet output")
    parser.add_argument("--allow-codespace", action="store_true", help="Authorize Google Drive upload outside GitHub Actions")
    parser.add_argument("--skip-upload", action="store_true", help="Skip Google Drive upload")
    args = parser.parse_args()

    res = transform_teryt_bronze(
        landing_dir=Path(args.workspace).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        allow_codespace=args.allow_codespace,
        skip_upload=args.skip_upload,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
