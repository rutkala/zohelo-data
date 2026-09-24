"""Landing-to-Bronze Parquet transformation loader for GLEIF Global Legal Entity Identifier (CD-015).

Vectorized streaming DuckDB architecture:
- Reads native Golden Copy archives from Landing (01_landing/gleif/native/bulk/)
- Streams CSVs directly into DuckDB without loading full CSVs into Python memory
- Normalizes column names into clean snake_case
- Captures entity attributes, national register crosswalks (KRS, REGON), and parent-child hierarchies
- Writes typed ZSTD Parquet files into 02_bronze/gleif/:
  - br_gleif_lei2.parquet
  - br_gleif_relationship_records.parquet
  - br_gleif_reporting_exceptions.parquet
- Uploads to Google Drive 02_bronze/gleif/ idempotently
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

logger = logging.getLogger("gleif_bronze_loader")
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
            logger.info("File %s already exists on Drive with matching hash. Reusing.", name)
            return {"id": item["id"], "name": name, "size": local_path.stat().st_size, "reused": True}
        with DRIVE_LOCK:
            storage.drive_service.files().delete(fileId=item["id"]).execute()

    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)
    body = {"name": name, "parents": [parent_id], "appProperties": {"sha256": sha256_hex}}
    with DRIVE_LOCK:
        created = storage.drive_service.files().create(
            body=body, media_body=media, fields="id,name,size,md5Checksum"
        ).execute(num_retries=4)
    logger.info("Uploaded %s to Drive (id=%s, size=%d bytes)", name, created["id"], local_path.stat().st_size)
    return {"id": created["id"], "name": name, "size": local_path.stat().st_size, "reused": False}


def transform_gleif_rr_to_bronze(zip_path: Path, out_parquet: Path) -> int:
    """Transform GLEIF Relationship Records (who owns whom) into Parquet."""
    logger.info("Transforming GLEIF Relationship Records from %s...", zip_path.name)
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(zip_path) as z:
            csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
            extracted_csv = z.extract(csv_name, tmp_dir)

        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT
                    "Relationship.StartNode.NodeID" AS child_lei,
                    "Relationship.StartNode.NodeIDType" AS child_node_type,
                    "Relationship.EndNode.NodeID" AS parent_lei,
                    "Relationship.EndNode.NodeIDType" AS parent_node_type,
                    "Relationship.RelationshipType" AS relationship_type,
                    "Relationship.RelationshipStatus" AS relationship_status,
                    "Relationship.Period.1.startDate" AS period_start_date,
                    "Relationship.Period.1.endDate" AS period_end_date,
                    "Registration.RegistrationStatus" AS registration_status,
                    "Registration.ManagingLOU" AS managing_lou,
                    CURRENT_TIMESTAMP::VARCHAR AS processed_at_utc
                FROM read_csv('{extracted_csv}', delim=',', header=true, all_varchar=true, ignore_errors=true)
            ) TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        row_count = con.execute(f"SELECT count(*) FROM read_parquet('{out_parquet}')").fetchone()[0]
        con.close()
    logger.info("Transformed %d relationship records into %s.", row_count, out_parquet.name)
    return row_count


def transform_gleif_repex_to_bronze(zip_path: Path, out_parquet: Path) -> int:
    """Transform GLEIF Reporting Exceptions into Parquet."""
    logger.info("Transforming GLEIF Reporting Exceptions from %s...", zip_path.name)
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(zip_path) as z:
            csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
            extracted_csv = z.extract(csv_name, tmp_dir)

        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT
                    LEI AS lei,
                    "Exception.Category" AS exception_category,
                    "Exception.Reason.1" AS exception_reason,
                    "Exception.Reference.1" AS exception_reference,
                    CURRENT_TIMESTAMP::VARCHAR AS processed_at_utc
                FROM read_csv('{extracted_csv}', delim=',', header=true, all_varchar=true, ignore_errors=true)
            ) TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        row_count = con.execute(f"SELECT count(*) FROM read_parquet('{out_parquet}')").fetchone()[0]
        con.close()
    logger.info("Transformed %d reporting exceptions into %s.", row_count, out_parquet.name)
    return row_count


def transform_gleif_lei2_to_bronze(zip_path: Path, out_parquet: Path) -> int:
    """Transform GLEIF Golden Copy Level 1 LEI records into Parquet."""
    logger.info("Transforming GLEIF Level 1 LEI records from %s...", zip_path.name)
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(zip_path) as z:
            csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
            extracted_csv = z.extract(csv_name, tmp_dir)

        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT
                    LEI AS lei,
                    "Entity.LegalName" AS legal_name,
                    "Entity.LegalAddress.Country" AS legal_country,
                    "Entity.LegalAddress.City" AS legal_city,
                    "Entity.LegalAddress.PostalCode" AS legal_postal_code,
                    "Entity.HeadquartersAddress.Country" AS hq_country,
                    "Entity.HeadquartersAddress.City" AS hq_city,
                    "Entity.EntityStatus" AS entity_status,
                    "Entity.EntityCategory" AS entity_category,
                    "Entity.LegalForm.EntityLegalFormCode" AS legal_form_code,
                    "Registration.ValidationAuthority.ValidationAuthorityID" AS validation_authority_id,
                    "Registration.ValidationAuthority.ValidationAuthorityEntityID" AS validation_authority_entity_id,
                    "Registration.InitialRegistrationDate" AS initial_registration_date,
                    "Registration.LastUpdateDate" AS last_update_date,
                    "Registration.RegistrationStatus" AS registration_status,
                    "Registration.ManagingLOU" AS managing_lou,
                    CURRENT_TIMESTAMP::VARCHAR AS processed_at_utc
                FROM read_csv('{extracted_csv}', delim=',', header=true, all_varchar=true, ignore_errors=true)
            ) TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        row_count = con.execute(f"SELECT count(*) FROM read_parquet('{out_parquet}')").fetchone()[0]
        con.close()
    logger.info("Transformed %d LEI entity records into %s.", row_count, out_parquet.name)
    return row_count


def run_gleif_bronze_transformation(
    landing_workspace: Path,
    bronze_workspace: Path,
    storage: StorageManager | None = None,
    allow_codespace: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    bronze_workspace.mkdir(parents=True, exist_ok=True)
    t_start = time.time()
    results: dict[str, Any] = {}

    # 1. Level 2 Relationship Records (who owns whom)
    rr_zips = list(landing_workspace.glob("*gleif_goldencopy_rr*.zip"))
    if rr_zips:
        rr_parquet = bronze_workspace / "br_gleif_relationship_records.parquet"
        rr_count = transform_gleif_rr_to_bronze(rr_zips[0], rr_parquet)
        results["relationship_records"] = {"rows": rr_count, "file": rr_parquet.name}

    # 2. Level 2 Reporting Exceptions
    repex_zips = list(landing_workspace.glob("*gleif_goldencopy_repex*.zip"))
    if repex_zips:
        repex_parquet = bronze_workspace / "br_gleif_reporting_exceptions.parquet"
        repex_count = transform_gleif_repex_to_bronze(repex_zips[0], repex_parquet)
        results["reporting_exceptions"] = {"rows": repex_count, "file": repex_parquet.name}

    # 3. Level 1 LEI reference records
    lei2_zips = list(landing_workspace.glob("*gleif_goldencopy_lei2*.zip"))
    if lei2_zips:
        lei2_parquet = bronze_workspace / "br_gleif_lei2.parquet"
        lei2_count = transform_gleif_lei2_to_bronze(lei2_zips[0], lei2_parquet)
        results["lei_records"] = {"rows": lei2_count, "file": lei2_parquet.name}

    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    can_upload = (in_actions or allow_codespace) and not skip_upload

    if can_upload:
        if storage is None:
            if allow_codespace:
                os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
            storage = StorageManager(allow_interactive_auth=False)
            storage.resolve_root(create=False)
            storage.authorize_writes()

        bronze_id = storage.resolve_zone("bronze")
        source_bronze_id = _resolve_or_create_folder(storage, "gleif", bronze_id)

        uploaded_files = []
        for p_path in bronze_workspace.glob("br_gleif_*.parquet"):
            up = _upload_file_to_drive(storage, p_path, p_path.name, source_bronze_id)
            uploaded_files.append({
                "file_name": p_path.name,
                "drive_id": up["id"],
                "size_bytes": p_path.stat().st_size,
                "reused": up["reused"],
            })

        control_id = storage.resolve_zone("control")
        campaigns_id = _resolve_or_create_folder(storage, "source_campaigns", control_id)
        source_control_id = _resolve_or_create_folder(storage, "gleif_bronze", campaigns_id)

        cp_data = {
            "source_id": "gleif_bronze",
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "results": results,
            "uploaded_files": uploaded_files,
            "elapsed_seconds": round(time.time() - t_start, 2),
        }
        cp_path = bronze_workspace / "checkpoint.json"
        cp_path.write_text(json.dumps(cp_data, indent=2), encoding="utf-8")
        _upload_file_to_drive(storage, cp_path, "checkpoint.json", source_control_id, mime_type="application/json")
        logger.info("GLEIF Bronze transformation complete and uploaded to Drive.")
        return cp_data

    return {
        "status": "completed_locally",
        "results": results,
        "elapsed_seconds": round(time.time() - t_start, 2),
    }


def main():
    parser = argparse.ArgumentParser(description="GLEIF Landing-to-Bronze Transformer")
    parser.add_argument("--landing-workspace", type=str, default="portal/test-results/gleif")
    parser.add_argument("--workspace", type=str, default="portal/test-results/gleif-bronze")
    parser.add_argument("--allow-codespace", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    res = run_gleif_bronze_transformation(
        landing_workspace=Path(args.landing_workspace).resolve(),
        bronze_workspace=Path(args.workspace).resolve(),
        allow_codespace=args.allow_codespace,
        skip_upload=args.skip_upload,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
