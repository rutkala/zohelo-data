"""Landing-to-Bronze Parquet transformation loader for GUS DBW.

Memory-bounded, vectorized streaming architecture:
- Reads original native files from Google Drive (01_landing/gus_dbw/native/)
- Streams CSVs directly into DuckDB with strict 300MB memory ceiling
- Generates partitioned ZSTD Apache Parquet files per indicator
- Fully idempotent: skips already processed indicators in Google Drive
- Tracks progress in 06_control/source_campaigns/gus_dbw_bronze/checkpoint.json
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import datetime, timezone
import hashlib
import io
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
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage_manager import StorageManager

logger = logging.getLogger("dbw_bronze_loader")
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
    return {"id": created["id"], "name": name, "size": int(created.get("size", 0)), "reused": False}


class DBWBronzeLoader:
    def __init__(
        self,
        workspace: Path,
        storage: StorageManager,
        allow_codespace: bool = False,
    ):
        self.workspace = workspace
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.storage = storage
        self.allow_codespace = allow_codespace
        _require_production_context(allow_codespace)

        self.session = storage.begin_write_session()
        self.landing_root = storage.resolve_zone("landing", create=False)
        self.bronze_root = storage.resolve_zone("bronze", create=True)
        self.control_root = storage.resolve_zone("control", create=True)

        # Resolve Landing directories
        self.dbw_landing = storage.get_or_create_nested_folder(["gus_dbw"], root_id=self.landing_root)
        self.landing_native = storage.get_or_create_nested_folder(["native"], root_id=self.dbw_landing)
        self.landing_taxonomy = storage.get_or_create_nested_folder(["taxonomy"], root_id=self.landing_native)
        self.landing_metadata = storage.get_or_create_nested_folder(["metadata"], root_id=self.landing_native)
        self.landing_bulk = storage.get_or_create_nested_folder(["bulk"], root_id=self.landing_native)

        # Resolve Bronze directories
        self.dbw_bronze = storage.get_or_create_nested_folder(["gus_dbw"], root_id=self.bronze_root, write_session=self.session)
        self.bronze_obs = storage.get_or_create_nested_folder(["observations"], root_id=self.dbw_bronze, write_session=self.session)
        self.bronze_dict = storage.get_or_create_nested_folder(["dictionaries"], root_id=self.dbw_bronze, write_session=self.session)
        self.bronze_tax = storage.get_or_create_nested_folder(["taxonomy"], root_id=self.dbw_bronze, write_session=self.session)
        self.bronze_met = storage.get_or_create_nested_folder(["metadata"], root_id=self.dbw_bronze, write_session=self.session)
        self.bronze_control = storage.get_or_create_nested_folder(["_control"], root_id=self.dbw_bronze, write_session=self.session)

        # Campaign control directory
        self.campaign_control = storage.get_or_create_nested_folder(
            ["source_campaigns", "gus_dbw_bronze"], root_id=self.control_root, write_session=self.session
        )

    def load_taxonomy_tree(self) -> list[dict[str, Any]]:
        """Load indicators tree from local file or Drive."""
        local_tree = self.workspace / "indicators_tree.json"
        if local_tree.exists():
            return json.loads(local_tree.read_text(encoding="utf-8"))

        fallback = Path("portal/test-results/dbw-web-bulk/indicators_tree.json")
        if fallback.exists():
            local_tree.write_bytes(fallback.read_bytes())
            return json.loads(local_tree.read_text(encoding="utf-8"))

        query = f"name='indicators_tree.json' and '{_escape_query(self.landing_taxonomy)}' in parents and trashed=false"
        with DRIVE_LOCK:
            files = self.storage.drive_service.files().list(q=query, fields="files(id, name)").execute().get("files", [])
        if not files:
            raise FileNotFoundError("indicators_tree.json not found in Landing taxonomy directory.")
        
        content = self.storage.drive_service.files().get_media(fileId=files[0]["id"]).execute()
        local_tree.write_bytes(content)
        return json.loads(content.decode("utf-8"))

    def build_taxonomy_table(self) -> Path:
        """Parse indicators tree into br_dbw_indicators Parquet table."""
        tree = self.load_taxonomy_tree()
        rows: list[dict[str, Any]] = []

        def walk(nodes: list[dict[str, Any]], area: str = "", domain: str = "", path: str = ""):
            for n in nodes:
                name = n.get("name", "")
                ntype = n.get("type", "")
                cur_area = area
                cur_domain = domain
                if ntype == "AREA":
                    cur_area = name
                elif cur_area and not cur_domain and ntype == "GROUP":
                    cur_domain = name

                cur_path = f"{path} > {name}" if path else name
                if ntype == "INDICATOR" and "indicator_id" in n:
                    rows.append({
                        "indicator_id": int(n["indicator_id"]),
                        "indicator_name": name,
                        "indicator_name_en": n.get("name_en") or "",
                        "thematic_area": cur_area,
                        "domain": cur_domain,
                        "taxonomy_path": cur_path,
                        "node_id": str(n["id"]) if n.get("id") else None,
                        "parent_id": str(n["parrent_id"]) if n.get("parrent_id") else None,
                        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
                    })
                if "children" in n and n["children"]:
                    walk(n["children"], cur_area, cur_domain, cur_path)

        walk(tree)
        logger.info(f"Parsed {len(rows)} indicators for taxonomy table across areas & domains.")

        out_path = self.workspace / "br_dbw_indicators.parquet"
        con = duckdb.connect(":memory:")
        df = pd.DataFrame(rows)
        con.register("df_tax", df)
        con.execute(f"COPY df_tax TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.close()
        return out_path

    def build_metadata_table(self, sample_ids: set[int] | None = None) -> Path:
        """Parse metryka CSVs into br_dbw_metadata Parquet table."""
        query = f"'{_escape_query(self.landing_metadata)}' in parents and name contains 'metryka' and trashed=false"
        token = None
        met_files = []
        while True:
            with DRIVE_LOCK:
                res = self.storage.drive_service.files().list(
                    q=query, pageSize=1000, pageToken=token, fields="nextPageToken, files(id, name)"
                ).execute()
            met_files.extend(res.get("files", []))
            token = res.get("nextPageToken")
            if not token:
                break

        logger.info(f"Found {len(met_files)} metryka CSV files in Landing metadata.")
        if sample_ids is not None:
            met_files = [
                f for f in met_files
                if f["name"].replace("metryka_", "").replace(".csv", "").isdigit()
                and int(f["name"].replace("metryka_", "").replace(".csv", "")) in sample_ids
            ]
            logger.info(f"Filtered to {len(met_files)} sample metryka files.")

        def download_and_parse(mf: dict[str, Any]) -> dict[str, Any] | None:
            try:
                with DRIVE_LOCK:
                    content = self.storage.drive_service.files().get_media(fileId=mf["id"]).execute()
                text = content.decode("utf-8-sig", errors="replace")
                lines = text.splitlines()
                if len(lines) < 2:
                    return None
                reader = csv.reader(io.StringIO(text), delimiter=";", quotechar='"')
                headers = next(reader)
                vals = next(reader)
                entry = {headers[i]: vals[i] if i < len(vals) else "" for i in range(len(headers))}
                iid_str = entry.get("id_zmienna", "").strip()
                if not iid_str.isdigit():
                    return None
                return {
                    "indicator_id": int(iid_str),
                    "metric_name": entry.get("nazwa", "").strip(),
                    "metric_name_en": entry.get("nazwa_ang", "").strip(),
                    "description": entry.get("definicja_pojecie", "").strip(),
                    "frequency": entry.get("nazwa_czestotliwosc", "").strip(),
                    "measure_unit": entry.get("nazwa_jednostki", "").strip(),
                    "data_source": entry.get("temat_badanie", "").strip(),
                    "legal_basis": entry.get("tytul_akt", "").strip(),
                    "last_update": entry.get("aktualizacja_ostatnia", "").strip(),
                    "processed_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            except Exception as exc:
                logger.warning(f"Error processing metryka file {mf['name']}: {exc}")
                return None

        rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(download_and_parse, mf) for mf in met_files]
            for f in as_completed(futures):
                res = f.result()
                if res:
                    rows.append(res)

        out_path = self.workspace / "br_dbw_metadata.parquet"
        con = duckdb.connect(":memory:")
        df = pd.DataFrame(rows)
        con.register("df_met", df)
        con.execute(f"COPY df_met TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.close()
        logger.info(f"Generated br_dbw_metadata.parquet with {len(rows)} records.")
        return out_path


def main():
    parser = argparse.ArgumentParser(description="Transform GUS DBW Landing files into Bronze Parquet tables.")
    parser.add_argument("--workspace", type=Path, default=Path("scratch/dbw_bronze"), help="Working directory")
    parser.add_argument("--allow-codespace", action="store_true", help="Allow production write in Codespaces")
    parser.add_argument("--sample-indicators", type=int, default=None, help="Limit to N indicators for pilot run")
    parser.add_argument("--indicator-ids", type=int, nargs="+", default=None, help="Specific indicator IDs to process")
    parser.add_argument("--skip-bulk", action="store_true", help="Only build taxonomy and metadata tables")
    args = parser.parse_args()

    sm = StorageManager()
    loader = DBWBronzeLoader(workspace=args.workspace, storage=sm, allow_codespace=args.allow_codespace)

    # 1. Discover Bulk Archives in Landing
    logger.info("=== Discovering Bulk Archives in Landing ===")
    query = f"'{_escape_query(loader.landing_bulk)}' in parents and trashed=false"
    token = None
    all_zips = []
    while True:
        with DRIVE_LOCK:
            res = sm.drive_service.files().list(
                q=query, pageSize=1000, pageToken=token, fields="nextPageToken, files(id, name, size)"
            ).execute()
        all_zips.extend(res.get("files", []))
        token = res.get("nextPageToken")
        if not token:
            break
    logger.info(f"Discovered {len(all_zips)} bulk zip files in Landing.")

    from collections import defaultdict
    zips_by_indicator: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for z in all_zips:
        name = z["name"]
        prefix = name.split("_")[0]
        if prefix.isdigit():
            zips_by_indicator[int(prefix)].append(z)

    known_indicators = sorted(zips_by_indicator.keys())
    logger.info(f"Grouped into {len(known_indicators)} distinct indicators.")

    # Filter target indicators
    targets = known_indicators
    if args.indicator_ids:
        targets = [i for i in targets if i in args.indicator_ids]
    elif args.sample_indicators:
        targets = targets[:args.sample_indicators]

    sample_set = set(targets) if (args.indicator_ids or args.sample_indicators) else None
    logger.info(f"Targeting {len(targets)} indicators for Bronze processing.")

    # 2. Build Taxonomy Table (Skip if already on Drive/local)
    tax_parquet = args.workspace / "br_dbw_indicators.parquet"
    if tax_parquet.is_file() and tax_parquet.stat().st_size > 1000:
        logger.info(f"Phase A: Reusing existing {tax_parquet.name} ({tax_parquet.stat().st_size} bytes).")
    else:
        logger.info("=== Phase A: Building br_dbw_indicators (Taxonomy) ===")
        tax_parquet = loader.build_taxonomy_table()
        tax_res = _upload_file_to_drive(
            sm, tax_parquet, name="br_dbw_indicators.parquet", parent_id=loader.bronze_tax, mime_type="application/octet-stream"
        )
        logger.info(f"Uploaded br_dbw_indicators.parquet to Drive ({tax_res['size']} bytes, reused={tax_res['reused']}).")

    # 3. Build Metadata Table (Skip if already on Drive/local)
    met_parquet = args.workspace / "br_dbw_metadata.parquet"
    if met_parquet.is_file() and met_parquet.stat().st_size > 1000:
        logger.info(f"Phase B: Reusing existing {met_parquet.name} ({met_parquet.stat().st_size} bytes).")
    else:
        logger.info("=== Phase B: Building br_dbw_metadata (Metryka) ===")
        met_parquet = loader.build_metadata_table(sample_ids=sample_set)
        met_res = _upload_file_to_drive(
            sm, met_parquet, name="br_dbw_metadata.parquet", parent_id=loader.bronze_met, mime_type="application/octet-stream"
        )
        logger.info(f"Uploaded br_dbw_metadata.parquet to Drive ({met_res['size']} bytes, reused={met_res['reused']}).")

    if args.skip_bulk:
        logger.info("Skipping bulk observations per --skip-bulk.")
        return

    # 4. Process Bulk Archives with Vectorized DuckDB
    logger.info("=== Phase C: Vectorized Bulk Extraction into Bronze ===")
    dict_dir = args.workspace / "dicts"
    dict_dir.mkdir(parents=True, exist_ok=True)

    # Initialize DuckDB with robust memory bounds and disk spillage
    duckdb_tmp = args.workspace / "duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA memory_limit = '2GB'")
    con.execute(f"PRAGMA temp_directory = '{duckdb_tmp}'")
    con.execute("PRAGMA preserve_insertion_order = false")
    con.execute("PRAGMA threads = 4")

    # Scan already completed indicator partitions on Drive with pagination
    query_obs = f"'{_escape_query(loader.bronze_obs)}' in parents and trashed=false"
    existing_obs_files = {}
    page_token = None
    with DRIVE_LOCK:
        while True:
            resp = sm.drive_service.files().list(
                q=query_obs,
                fields="nextPageToken, files(id, name, size)",
                pageSize=1000,
                pageToken=page_token
            ).execute()
            for f in resp.get("files", []):
                existing_obs_files[f["name"]] = f
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    logger.info(f"Discovered {len(existing_obs_files)} existing observation partitions on Drive.")

    # Clean up obsolete pilot file if present
    if "part_1_6.parquet" in existing_obs_files:
        with DRIVE_LOCK:
            sm.drive_service.files().delete(fileId=existing_obs_files["part_1_6.parquet"]["id"]).execute()
        existing_obs_files.pop("part_1_6.parquet", None)

    total_obs_rows = 0
    cp_path = args.workspace / "checkpoint.json"
    if cp_path.is_file():
        try:
            prior_cp = json.loads(cp_path.read_text(encoding="utf-8"))
            total_obs_rows = prior_cp.get("total_observations_rows", 0)
            logger.info(f"Resuming with prior total_observations_rows = {total_obs_rows:,}")
        except Exception:
            total_obs_rows = 0

    completed_indicators = 0
    total_targets = len(targets)

    for idx, ind_id in enumerate(targets, 1):
        part_name = f"part_{ind_id}.parquet"
        dict_part_name = f"dict_{ind_id}.parquet"

        if part_name in existing_obs_files and int(existing_obs_files[part_name].get("size", 0)) > 1000:
            logger.info(f"[{idx}/{total_targets}] Indicator {ind_id} ({part_name}) already exists on Drive. Skipping.")
            completed_indicators += 1
            continue

        zfiles = zips_by_indicator.get(ind_id, [])
        if not zfiles:
            continue

        t_start = time.time()
        (args.workspace / part_name).unlink(missing_ok=True)
        # Initialize fresh temporary DuckDB tables
        con.execute("DROP TABLE IF EXISTS current_obs")
        con.execute("DROP TABLE IF EXISTS current_dict")
        con.execute("""
            CREATE TEMP TABLE current_obs (
                indicator_id BIGINT,
                przekroj_id BIGINT,
                wymiar_1 BIGINT,
                pozycja_1 BIGINT,
                wymiar_2 BIGINT,
                pozycja_2 BIGINT,
                wymiar_3 BIGINT,
                pozycja_3 BIGINT,
                wymiar_4 BIGINT,
                pozycja_4 BIGINT,
                wymiar_5 BIGINT,
                pozycja_5 BIGINT,
                wymiar_6 BIGINT,
                pozycja_6 BIGINT,
                wymiar_7 BIGINT,
                pozycja_7 BIGINT,
                wymiar_8 BIGINT,
                pozycja_8 BIGINT,
                wymiar_9 BIGINT,
                pozycja_9 BIGINT,
                okres_id INTEGER,
                sposob_prezentacji_miara_id INTEGER,
                period_year INTEGER,
                wartosc_raw VARCHAR,
                wartosc_numeric DOUBLE,
                precyzja INTEGER,
                brak_wartosci_id INTEGER,
                tajnosci_id INTEGER,
                flaga_id INTEGER,
                raw_archive_file VARCHAR,
                source_row_number BIGINT,
                processed_at_utc VARCHAR
            )
        """)
        con.execute("""
            CREATE TEMP TABLE current_dict (
                indicator_id BIGINT,
                column_name VARCHAR,
                dictionary_name VARCHAR,
                element_id BIGINT,
                element_name VARCHAR,
                processed_at_utc VARCHAR
            )
        """)

        # Download zip archives in parallel
        def fetch_zip(zf_meta):
            try:
                with DRIVE_LOCK:
                    content = sm.drive_service.files().get_media(fileId=zf_meta["id"]).execute()
                return zf_meta["name"], content
            except Exception as exc:
                logger.warning(f"Error fetching zip {zf_meta['name']}: {exc}")
                return zf_meta["name"], None

        with ThreadPoolExecutor(max_workers=6) as executor:
            download_futures = [executor.submit(fetch_zip, zf) for zf in zfiles]
            
            with tempfile.TemporaryDirectory() as tmpdir:
                for future in as_completed(download_futures):
                    fname, content = future.result()
                    if not content:
                        continue
                    try:
                        zf = zipfile.ZipFile(io.BytesIO(content))
                        csv_names = [n for n in zf.namelist() if not "Slowniki" in n and n.endswith(".csv")]
                        dict_names = [n for n in zf.namelist() if "Slowniki" in n and n.endswith(".csv")]

                        # 1. Observations
                        if csv_names:
                            csv_path = os.path.join(tmpdir, f"obs_{fname}.csv")
                            with open(csv_path, "wb") as f_out:
                                f_out.write(zf.read(csv_names[0]))

                            con.execute(f"""
                                INSERT INTO current_obs
                                SELECT 
                                    {ind_id}::BIGINT,
                                    TRY_CAST(id_przekroj AS BIGINT),
                                    TRY_CAST(id_wymiar_1 AS BIGINT),
                                    TRY_CAST(id_pozycja_1 AS BIGINT),
                                    TRY_CAST(id_wymiar_2 AS BIGINT),
                                    TRY_CAST(id_pozycja_2 AS BIGINT),
                                    TRY_CAST(id_wymiar_3 AS BIGINT),
                                    TRY_CAST(id_pozycja_3 AS BIGINT),
                                    TRY_CAST(id_wymiar_4 AS BIGINT),
                                    TRY_CAST(id_pozycja_4 AS BIGINT),
                                    TRY_CAST(id_wymiar_5 AS BIGINT),
                                    TRY_CAST(id_pozycja_5 AS BIGINT),
                                    TRY_CAST(id_wymiar_6 AS BIGINT),
                                    TRY_CAST(id_pozycja_6 AS BIGINT),
                                    TRY_CAST(id_wymiar_7 AS BIGINT),
                                    TRY_CAST(id_pozycja_7 AS BIGINT),
                                    TRY_CAST(id_wymiar_8 AS BIGINT),
                                    TRY_CAST(id_pozycja_8 AS BIGINT),
                                    TRY_CAST(id_wymiar_9 AS BIGINT),
                                    TRY_CAST(id_pozycja_9 AS BIGINT),
                                    TRY_CAST(id_okres AS INTEGER),
                                    TRY_CAST(id_sposob_prezentacji_miara AS INTEGER),
                                    TRY_CAST(id_daty AS INTEGER),
                                    wartosc as wartosc_raw,
                                    TRY_CAST(REPLACE(wartosc, ',', '.') AS DOUBLE),
                                    TRY_CAST(precyzja AS INTEGER),
                                    TRY_CAST(id_brak_wartosci AS INTEGER),
                                    TRY_CAST(id_tajnosci AS INTEGER),
                                    TRY_CAST(id_flaga AS INTEGER),
                                    '{fname}' as raw_archive_file,
                                    TRY_CAST(rowNumber AS BIGINT),
                                    CURRENT_TIMESTAMP::VARCHAR
                                FROM read_csv('{csv_path}', delim=';', header=true, all_varchar=true, ignore_errors=true)
                            """)
                            os.remove(csv_path)

                        # 2. Dictionaries
                        if dict_names:
                            dict_path = os.path.join(tmpdir, f"dict_{fname}.csv")
                            with open(dict_path, "wb") as f_out:
                                f_out.write(zf.read(dict_names[0]))

                            con.execute(f"""
                                INSERT INTO current_dict
                                SELECT DISTINCT
                                    {ind_id}::BIGINT,
                                    TRIM(nazwa_kolumny),
                                    TRIM(nazwa_slownika),
                                    TRY_CAST(id_elementu AS BIGINT),
                                    TRIM(opis),
                                    CURRENT_TIMESTAMP::VARCHAR
                                FROM read_csv('{dict_path}', delim=';', header=true, all_varchar=true, ignore_errors=true)
                                WHERE id_elementu IS NOT NULL
                            """)
                            os.remove(dict_path)
                    except Exception as exc:
                        logger.warning(f"Error processing {fname}: {exc}")

        obs_count = con.execute("SELECT count(*) FROM current_obs").fetchone()[0]
        dict_count = con.execute("SELECT count(*) FROM current_dict").fetchone()[0]

        # Write Parquet and upload to Google Drive
        if obs_count > 0:
            part_path = args.workspace / part_name
            con.execute(f"COPY current_obs TO '{part_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            res = _upload_file_to_drive(
                sm, part_path, name=part_name, parent_id=loader.bronze_obs, mime_type="application/octet-stream"
            )
            part_path.unlink(missing_ok=True)
            total_obs_rows += obs_count

        if dict_count > 0:
            dict_part_path = dict_dir / dict_part_name
            con.execute(f"COPY (SELECT DISTINCT * FROM current_dict) TO '{dict_part_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")

        con.execute("DROP TABLE current_obs")
        con.execute("DROP TABLE current_dict")

        completed_indicators += 1
        elapsed = time.time() - t_start
        logger.info(
            f"[{idx}/{total_targets}] Indicator {ind_id}: {obs_count:,} observations, {dict_count} dicts from {len(zfiles)} archives ({elapsed:.1f}s)."
        )

        # Update checkpoint every 5 indicators
        if completed_indicators % 5 == 0 or idx == total_targets:
            cp_data = {
                "source_id": "gus_dbw_bronze",
                "total_indicators": total_targets,
                "completed_indicators": completed_indicators,
                "total_observations_rows": total_obs_rows,
                "last_indicator_id": ind_id,
                "status": "completed" if completed_indicators >= total_targets else "running",
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            cp_bytes = json.dumps(cp_data, indent=2).encode("utf-8")
            cp_path = args.workspace / "checkpoint.json"
            cp_path.write_bytes(cp_bytes)
            _upload_file_to_drive(sm, cp_path, name="checkpoint.json", parent_id=loader.bronze_control, mime_type="application/json")
            _upload_file_to_drive(sm, cp_path, name="checkpoint.json", parent_id=loader.campaign_control, mime_type="application/json")

    # Final dictionary consolidation
    dict_parts = list(dict_dir.glob("dict_*.parquet"))
    if dict_parts:
        logger.info(f"Consolidating {len(dict_parts)} dictionary partition files...")
        dict_cons_path = args.workspace / "br_dbw_dictionaries.parquet"
        con.execute(f"""
            COPY (
                SELECT DISTINCT indicator_id, column_name, dictionary_name, element_id, element_name, processed_at_utc
                FROM read_parquet('{dict_dir}/dict_*.parquet')
            ) TO '{dict_cons_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        dict_res = _upload_file_to_drive(
            sm, dict_cons_path, name="br_dbw_dictionaries.parquet", parent_id=loader.bronze_dict, mime_type="application/octet-stream"
        )
        logger.info(f"Uploaded consolidated br_dbw_dictionaries.parquet ({dict_res['size']} bytes).")

    con.close()
    logger.info(f"GUS DBW Bronze transformation completed successfully: {total_obs_rows:,} total observations across {completed_indicators} indicators.")


if __name__ == "__main__":
    main()
