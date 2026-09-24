"""Landing-to-Bronze Parquet transformation loader for Ministerstwo Finansów Biała Lista (PL-FIN-001).

Vectorized streaming DuckDB architecture:
- Reads native 7z archive from Landing (01_landing/mf_biala_lista/native/bulk/<date>/<date>.7z)
- Streams decompressed JSON through 7z CLI stdout to eliminate memory inflation
- Extracts metadata header, virtual account masks, active and exempt taxpayer hashes
- Writes typed ZSTD Parquet files:
  - br_biala_lista_header.parquet
  - br_biala_lista_masks.parquet
  - br_biala_lista_taxpayers.parquet
- Uploads to Google Drive 02_bronze/mf_biala_lista/ idempotently
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
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any

import duckdb
from googleapiclient.http import MediaFileUpload

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage_manager import StorageManager

logger = logging.getLogger("mf_biala_lista_bronze")
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


def transform_biala_lista_bronze(
    archive_path: Path,
    workspace: Path,
    storage: StorageManager | None = None,
    allow_codespace: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=True)
    t_start = time.time()

    # Extract date from filename, e.g. 20260920.7z -> 20260920
    date_match = re.search(r"(\d{8})", archive_path.name)
    if not date_match:
        raise ValueError(f"Could not parse YYYYMMDD date from archive name: {archive_path.name}")
    date_str = date_match.group(1)
    snapshot_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    json_internal_name = f"{date_str}.json"
    logger.info("Streaming and parsing %s from %s...", json_internal_name, archive_path.name)

    proc = subprocess.Popen(
        ["7z", "e", "-so", str(archive_path), json_internal_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1048576,
    )

    header_data: dict[str, Any] = {}
    masks: list[str] = []
    
    duckdb_tmp = workspace / "duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)
    db_file = duckdb_tmp / "biala_lista_bronze.duckdb"
    if db_file.exists():
        db_file.unlink()

    con = duckdb.connect(str(db_file))
    con.execute("PRAGMA memory_limit = '750MB'")
    con.execute(f"PRAGMA temp_directory = '{duckdb_tmp}'")
    con.execute("PRAGMA threads = 2")

    con.execute("""
        CREATE TABLE raw_hashes (
            hash VARCHAR,
            status VARCHAR,
            snapshot_date DATE,
            processed_at_utc TIMESTAMP
        )
    """)

    current_section: str | None = None
    in_naglowek = False
    naglowek_lines: list[str] = []
    
    batch_records: list[tuple[str, str, str, str]] = []
    BATCH_SIZE = 250000
    total_active_hashes = 0
    total_exempt_hashes = 0
    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def flush_batch():
        nonlocal batch_records
        if not batch_records:
            return
        df_chunk = batch_records
        batch_records = []
        con.executemany(
            "INSERT INTO raw_hashes VALUES (?, ?, CAST(? AS DATE), CAST(? AS TIMESTAMP))",
            df_chunk,
        )

    for raw_line in proc.stdout:
        line = raw_line.decode("utf-8", errors="ignore")
        
        # Check header
        if '"naglowek":' in line:
            in_naglowek = True
            naglowek_lines.append("{")
            continue
        if in_naglowek:
            naglowek_lines.append(line)
            if "}" in line:
                in_naglowek = False
                try:
                    header_str = "".join(naglowek_lines)
                    # clean trailing comma if present
                    header_str = re.sub(r",\s*$", "", header_str.strip())
                    header_data = json.loads(header_str)
                except Exception as e:
                    logger.warning("Error parsing naglowek: %s", e)
            continue

        # Check section change
        m_sec = re.match(r'^\s*"([^"]+)":\s*\[', line)
        if m_sec:
            current_section = m_sec.group(1)
            continue

        if current_section == "maski":
            m_val = re.search(r'"([^"]+)"', line)
            if m_val:
                masks.append(m_val.group(1))
            if "]" in line:
                current_section = None
            continue

        if current_section == "skrotyPodatnikowCzynnych":
            m_val = re.search(r'"([0-9a-fA-F]{64,128})"', line)
            if m_val:
                batch_records.append((m_val.group(1), "active", snapshot_date, now_utc_str))
                total_active_hashes += 1
                if len(batch_records) >= BATCH_SIZE:
                    flush_batch()
            if "]" in line:
                current_section = None
            continue

        if current_section == "skrotyPodatnikowZwolnionych":
            m_val = re.search(r'"([0-9a-fA-F]{64,128})"', line)
            if m_val:
                batch_records.append((m_val.group(1), "exempt", snapshot_date, now_utc_str))
                total_exempt_hashes += 1
                if len(batch_records) >= BATCH_SIZE:
                    flush_batch()
            if "]" in line:
                current_section = None
            continue

    flush_batch()
    if proc.stdout:
        proc.stdout.close()
    if proc.stderr:
        proc.stderr.close()
    proc.wait()

    logger.info(
        "Extracted %d active hashes, %d exempt hashes, %d masks from %s (%.1fs).",
        total_active_hashes,
        total_exempt_hashes,
        len(masks),
        archive_path.name,
        time.time() - t_start,
    )

    # 1. Output Header Parquet
    header_parquet = workspace / "br_biala_lista_header.parquet"
    con.execute("DROP TABLE IF EXISTS header_tbl")
    con.execute("""
        CREATE TABLE header_tbl (
            snapshot_date DATE,
            generation_date VARCHAR,
            transformation_count INTEGER,
            schema_description VARCHAR,
            processed_at_utc TIMESTAMP
        )
    """)
    con.execute(
        "INSERT INTO header_tbl VALUES (CAST(? AS DATE), ?, ?, ?, CAST(? AS TIMESTAMP))",
        [
            snapshot_date,
            str(header_data.get("dataGenerowaniaDanych", date_str)),
            int(header_data.get("liczbaTransformacji", 5000)),
            str(header_data.get("schemat", "")),
            now_utc_str,
        ],
    )
    con.execute(f"COPY header_tbl TO '{header_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    # 2. Output Masks Parquet
    masks_parquet = workspace / "br_biala_lista_masks.parquet"
    con.execute("DROP TABLE IF EXISTS masks_tbl")
    con.execute("""
        CREATE TABLE masks_tbl (
            account_mask VARCHAR,
            bank_prefix VARCHAR,
            snapshot_date DATE,
            processed_at_utc TIMESTAMP
        )
    """)
    mask_rows = [
        (m, m[2:10] if len(m) >= 10 else "", snapshot_date, now_utc_str)
        for m in masks
    ]
    con.executemany(
        "INSERT INTO masks_tbl VALUES (?, ?, CAST(? AS DATE), CAST(? AS TIMESTAMP))",
        mask_rows,
    )
    con.execute(f"COPY masks_tbl TO '{masks_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    # 3. Output Taxpayer Hashes Parquet
    taxpayers_parquet = workspace / "br_biala_lista_taxpayers.parquet"
    con.execute(f"COPY raw_hashes TO '{taxpayers_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    
    con.close()
    if db_file.exists():
        db_file.unlink()

    # Upload to Google Drive if authorized
    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    can_upload = (in_actions or allow_codespace) and not skip_upload

    uploaded_files = []
    if can_upload:
        if storage is None:
            if allow_codespace:
                os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
            storage = StorageManager(allow_interactive_auth=False)
            storage.resolve_root(create=False)
            storage.authorize_writes()

        bronze_id = storage.resolve_zone("bronze")
        source_bronze_id = _resolve_or_create_folder(storage, "mf_biala_lista", bronze_id)

        for p_file in [header_parquet, masks_parquet, taxpayers_parquet]:
            up_res = _upload_file_to_drive(storage, p_file, p_file.name, source_bronze_id)
            uploaded_files.append({
                "file_name": p_file.name,
                "drive_id": up_res["id"],
                "size_bytes": p_file.stat().st_size,
                "reused": up_res["reused"],
            })

        # Checkpoint
        control_id = storage.resolve_zone("control")
        campaigns_id = _resolve_or_create_folder(storage, "source_campaigns", control_id)
        source_control_id = _resolve_or_create_folder(storage, "mf_biala_lista_bronze", campaigns_id)

        checkpoint_data = {
            "source_id": "mf_biala_lista_bronze",
            "snapshot_date": snapshot_date,
            "total_active_hashes": total_active_hashes,
            "total_exempt_hashes": total_exempt_hashes,
            "total_masks": len(masks),
            "status": "completed",
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "files": uploaded_files,
        }
        cp_path = workspace / "checkpoint.json"
        cp_path.write_text(json.dumps(checkpoint_data, indent=2), encoding="utf-8")
        _upload_file_to_drive(storage, cp_path, "checkpoint.json", source_control_id, mime_type="application/json")
        logger.info("MF Biała Lista Bronze transformation complete and checkpoint recorded on Drive.")
        return checkpoint_data

    return {
        "status": "completed_locally",
        "snapshot_date": snapshot_date,
        "active_hashes": total_active_hashes,
        "exempt_hashes": total_exempt_hashes,
        "masks": len(masks),
        "files": [header_parquet.name, masks_parquet.name, taxpayers_parquet.name],
    }


def main():
    parser = argparse.ArgumentParser(description="MF Biała Lista Landing-to-Bronze Transformer")
    parser.add_argument("--archive", type=str, default="portal/test-results/biala-lista/20260920.7z")
    parser.add_argument("--workspace", type=str, default="portal/test-results/biala-lista-bronze")
    parser.add_argument("--allow-codespace", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    archive_path = Path(args.archive).resolve()
    if not archive_path.is_file():
        # Fallback: search in portal/test-results/biala-lista/
        candidates = list(Path("portal/test-results/biala-lista").resolve().glob("*.7z"))
        if candidates:
            archive_path = candidates[0]
        else:
            raise FileNotFoundError(f"Archive not found at {args.archive}")

    res = transform_biala_lista_bronze(
        archive_path=archive_path,
        workspace=Path(args.workspace).resolve(),
        allow_codespace=args.allow_codespace,
        skip_upload=args.skip_upload,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
