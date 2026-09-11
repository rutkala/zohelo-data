"""Stream unpack and transform OpenData.org Senzing ZIP archive into Bronze Parquet tables.

Reads directly from Google Drive (or local mock) using HTTP Range seekable streams,
flattens Senzing entity resolution JSONL records into typed tabular columns, and
uploads compressed ZSTD Parquet files directly to 02_bronze/opendata_org/ with zero
disk extraction.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import io
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Callable, Iterator
import zipfile

import duckdb
from googleapiclient.http import MediaIoBaseUpload

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage_manager import StorageManager

logger = logging.getLogger("opendata_bronze_loader")

# Constants
OPENDATA_LANDING_DIR = ["01_landing", "opendata_org"]
OPENDATA_BRONZE_ROOT = ["02_bronze", "opendata_org"]
OPENDATA_CONTROL_DIR = ["06_control", "source_campaigns", "opendata_org_bronze"]
DEFAULT_ZIP_NAME = "ODO_SENZING_20260305.zip"
CHECKPOINT_NAME = "checkpoint.json"
CHUNK_SIZE = 16 * 1024 * 1024  # 16 MiB range cache for DriveZipStream

# Table column definitions
ORGANIZATION_COLUMNS = [
    ("record_id", "VARCHAR"),
    ("data_source", "VARCHAR"),
    ("bq_dataset", "VARCHAR"),
    ("name_org", "VARCHAR"),
    ("name_type", "VARCHAR"),
    ("record_type", "VARCHAR"),
    ("addr_line1", "VARCHAR"),
    ("addr_city", "VARCHAR"),
    ("addr_state", "VARCHAR"),
    ("addr_postal_code", "VARCHAR"),
    ("addr_country", "VARCHAR"),
    ("addr_type", "VARCHAR"),
    ("geo_latitude", "DOUBLE"),
    ("geo_longitude", "DOUBLE"),
    ("placekey", "VARCHAR"),
    ("bq_id", "VARCHAR"),
    ("rel_anchor_domain", "VARCHAR"),
    ("rel_anchor_key", "VARCHAR"),
]

LOCATION_COLUMNS = [
    ("record_id", "VARCHAR"),
    ("data_source", "VARCHAR"),
    ("bq_dataset", "VARCHAR"),
    ("name_full", "VARCHAR"),
    ("record_type", "VARCHAR"),
    ("addr_line1", "VARCHAR"),
    ("addr_city", "VARCHAR"),
    ("addr_state", "VARCHAR"),
    ("addr_postal_code", "VARCHAR"),
    ("addr_country", "VARCHAR"),
    ("geo_latitude", "DOUBLE"),
    ("geo_longitude", "DOUBLE"),
    ("placekey", "VARCHAR"),
    ("bq_id", "VARCHAR"),
]

PEOPLE_COLUMNS = [
    ("record_id", "VARCHAR"),
    ("data_source", "VARCHAR"),
    ("bq_dataset", "VARCHAR"),
    ("name_full", "VARCHAR"),
    ("name_first", "VARCHAR"),
    ("name_last", "VARCHAR"),
    ("record_type", "VARCHAR"),
    ("addr_country", "VARCHAR"),
    ("group_assn_id_number", "VARCHAR"),
    ("group_assn_id_type", "VARCHAR"),
    ("rel_pointer_domain", "VARCHAR"),
    ("rel_pointer_key", "VARCHAR"),
    ("rel_pointer_role", "VARCHAR"),
    ("linkedin", "VARCHAR"),
]

CATEGORIES = {
    "organizations": {
        "prefix": "Organization/",
        "folder": "organizations",
        "columns": ORGANIZATION_COLUMNS,
    },
    "locations": {
        "prefix": "Locations/",
        "folder": "locations",
        "columns": LOCATION_COLUMNS,
    },
    "people": {
        "prefix": "PeopleBusiness/",
        "folder": "people",
        "columns": PEOPLE_COLUMNS,
    },
}


class DriveZipStream(io.RawIOBase):
    """Seekable, range-cached read stream for a Google Drive file."""

    def __init__(self, service: Any, file_id: str, file_size: int, chunk_size: int = CHUNK_SIZE):
        self.service = service
        self.file_id = file_id
        self.size = file_size
        self.chunk_size = chunk_size
        self.pos = 0
        self._cache_start: int | None = None
        self._cache_data: bytes | None = None
        self.http_requests_count = 0

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.size + offset
        self.pos = max(0, min(self.pos, self.size))
        return self.pos

    def tell(self) -> int:
        return self.pos

    def readinto(self, b: Any) -> int:
        if self.pos >= self.size:
            return 0
        length = len(b)

        # Serve from in-memory chunk cache if within range
        if (
            self._cache_start is not None
            and self._cache_data is not None
            and self._cache_start <= self.pos < self._cache_start + len(self._cache_data)
        ):
            offset = self.pos - self._cache_start
            available = len(self._cache_data) - offset
            to_copy = min(length, available)
            b[:to_copy] = self._cache_data[offset : offset + to_copy]
            self.pos += to_copy
            return to_copy

        # Fetch a chunk from Google Drive using HTTP Range header
        fetch_len = max(length, self.chunk_size)
        end = min(self.pos + fetch_len - 1, self.size - 1)
        req = self.service.files().get_media(fileId=self.file_id)
        req.headers["Range"] = f"bytes={self.pos}-{end}"
        self.http_requests_count += 1
        chunk = req.execute()
        self._cache_start = self.pos
        self._cache_data = chunk

        to_copy = min(length, len(chunk))
        b[:to_copy] = chunk[:to_copy]
        self.pos += to_copy
        return to_copy


def parse_senzing_record(raw_record: dict[str, Any], columns: list[tuple[str, str]]) -> tuple[Any, ...]:
    """Extract and flatten nested Senzing FEATURES into the target schema tuple."""
    col_names = [col[0] for col in columns]
    row: dict[str, Any] = {
        "record_id": str(raw_record.get("RECORD_ID", "")),
        "data_source": raw_record.get("DATA_SOURCE"),
        "bq_dataset": raw_record.get("bq_dataset"),
    }
    for c in col_names:
        if c not in row:
            row[c] = None

    features = raw_record.get("FEATURES", [])
    if isinstance(features, list):
        for feat in features:
            if not isinstance(feat, dict):
                continue
            for k, v in feat.items():
                if k == "LIB_FEAT_TYPE":
                    continue
                k_lower = k.lower()
                if k_lower in row and row[k_lower] is None:
                    if k_lower in ("geo_latitude", "geo_longitude"):
                        try:
                            row[k_lower] = float(v) if v is not None else None
                        except (ValueError, TypeError):
                            row[k_lower] = None
                    else:
                        row[k_lower] = str(v) if v is not None else None

    return tuple(row[c] for c in col_names)


def process_member_stream(
    stream: io.IOBase,
    columns: list[tuple[str, str]],
    output_parquet_path: Path,
) -> int:
    """Vectorized C++ extraction: copy stream to temporary JSONL and convert via DuckDB in a single pass."""
    import shutil

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as temp_jsonl:
        temp_jsonl_path = Path(temp_jsonl.name)
        try:
            shutil.copyfileobj(stream, temp_jsonl, length=16 * 1024 * 1024)
            temp_jsonl.flush()

            root_cols = {"record_id", "data_source", "bq_dataset"}
            select_exprs = [
                "record_id",
                "data_source",
                "bq_dataset",
            ]
            for col_name, col_type in columns:
                if col_name in root_cols:
                    continue
                feat_key = col_name.upper()
                if col_type == "DOUBLE":
                    expr = (
                        f"min(try_cast(json_extract_string(feat, '$.{feat_key}') as double)) "
                        f"filter (where json_extract_string(feat, '$.{feat_key}') is not null) as {col_name}"
                    )
                else:
                    expr = (
                        f"min(json_extract_string(feat, '$.{feat_key}')) "
                        f"filter (where json_extract_string(feat, '$.{feat_key}') is not null) as {col_name}"
                    )
                select_exprs.append(expr)

            select_cols = ",\n                    ".join(select_exprs)
            sql = f"""
            COPY (
                with raw as (
                    select line from read_csv('{temp_jsonl_path}', columns={{'line': 'VARCHAR'}}, delim='\x1e', quote='', escape='', header=false, auto_detect=false)
                ),
                parsed as (
                    select 
                        json_extract_string(line, '$.RECORD_ID') as record_id,
                        json_extract_string(line, '$.DATA_SOURCE') as data_source,
                        coalesce(json_extract_string(line, '$.BQ_DATASET'), json_extract_string(line, '$.bq_dataset')) as bq_dataset,
                        item.value as feat
                    from raw
                    left join lateral json_each(line, '$.FEATURES') as item on true
                )
                select 
                    {select_cols}
                from parsed
                group by record_id, data_source, bq_dataset
            ) TO '{output_parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """

            con = duckdb.connect(":memory:")
            con.execute(sql)
            count = con.execute(f"select count(*) from read_parquet('{output_parquet_path}')").fetchone()[0]
            con.close()
            return int(count)
        finally:
            if temp_jsonl_path.is_file():
                temp_jsonl_path.unlink()


class OpenDataBronzeRunner:
    """Orchestrates streaming transformation from Landing ZIP to Bronze Parquet."""

    def __init__(self, storage: StorageManager, allow_production_write: bool = False):
        self.storage = storage
        self.allow_production_write = allow_production_write
        self.root_id = storage.resolve_root(create=False)

    def _resolve_folder(self, path_segments: list[str]) -> str | None:
        current_id = self.root_id
        for segment in path_segments:
            folders = self.storage._list_exact_folders(segment, parent_id=current_id)
            if not folders:
                return None
            current_id = folders[0]["id"]
        return current_id

    def _resolve_landing_zip(self, zip_name: str = DEFAULT_ZIP_NAME) -> tuple[str, int]:
        landing_folder_id = self._resolve_folder(OPENDATA_LANDING_DIR)
        if not landing_folder_id:
            raise FileNotFoundError(f"Landing folder {OPENDATA_LANDING_DIR} not found below root")
        res = (
            self.storage.drive_service.files()
            .list(
                q=f"name='{zip_name}' and '{landing_folder_id}' in parents and trashed=false",
                fields="files(id,size)",
                spaces="drive",
            )
            .execute()
        )
        files = res.get("files", [])
        if not files:
            raise FileNotFoundError(f"Landing archive {zip_name} not found in {OPENDATA_LANDING_DIR}")
        return files[0]["id"], int(files[0]["size"])

    def _load_checkpoint(self) -> dict[str, Any]:
        control_folder_id = self._resolve_folder(OPENDATA_CONTROL_DIR)
        if not control_folder_id:
            return {
                "schema_version": 1,
                "processed_members": [],
                "bronze_tables": {cat: [] for cat in CATEGORIES},
                "total_rows": 0,
                "last_updated": None,
            }
        res = (
            self.storage.drive_service.files()
            .list(
                q=f"name='{CHECKPOINT_NAME}' and '{control_folder_id}' in parents and trashed=false",
                fields="files(id)",
                spaces="drive",
            )
            .execute()
        )
        files = res.get("files", [])
        if not files:
            return {
                "schema_version": 1,
                "processed_members": [],
                "bronze_tables": {cat: [] for cat in CATEGORIES},
                "total_rows": 0,
                "last_updated": None,
            }
        data = self.storage.drive_service.files().get_media(fileId=files[0]["id"]).execute()
        return json.loads(data.decode("utf-8"))

    def _save_checkpoint(self, checkpoint: dict[str, Any]) -> str:
        if not self.allow_production_write:
            raise PermissionError("Updating checkpoint requires production write authorization")
        self.storage.authorize_writes()
        control_folder_id = self.storage.get_or_create_nested_folder(
            OPENDATA_CONTROL_DIR, root_id=self.root_id
        )
        checkpoint["last_updated"] = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(checkpoint, indent=2).encode("utf-8")

        res = (
            self.storage.drive_service.files()
            .list(
                q=f"name='{CHECKPOINT_NAME}' and '{control_folder_id}' in parents and trashed=false",
                fields="files(id)",
                spaces="drive",
            )
            .execute()
        )
        files = res.get("files", [])
        media = MediaIoBaseUpload(io.BytesIO(payload), mimetype="application/json")
        if files:
            file_id = files[0]["id"]
            self.storage.drive_service.files().update(
                fileId=file_id, media_body=media
            ).execute()
            return file_id
        else:
            meta = {"name": CHECKPOINT_NAME, "parents": [control_folder_id], "mimeType": "application/json"}
            created = self.storage.drive_service.files().create(body=meta, media_body=media, fields="id").execute()
            return created["id"]

    def _upload_parquet(self, category: str, file_name: str, local_parquet_path: Path) -> tuple[str, int]:
        if not self.allow_production_write:
            raise PermissionError("Writing Bronze Parquet requires production write authorization")
        self.storage.authorize_writes()
        category_info = CATEGORIES[category]
        target_folder = [*OPENDATA_BRONZE_ROOT, category_info["folder"]]
        folder_id = self.storage.get_or_create_nested_folder(target_folder, root_id=self.root_id)

        file_bytes = local_parquet_path.read_bytes()
        file_size = len(file_bytes)
        media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype="application/octet-stream", resumable=True)
        meta = {"name": file_name, "parents": [folder_id], "mimeType": "application/octet-stream"}
        created = self.storage.drive_service.files().create(body=meta, media_body=media, fields="id").execute()
        return created["id"], file_size

    def run(self, category: str = "all", max_files: int = 5, dry_run: bool = False) -> dict[str, Any]:
        """Process up to max_files member files across specified categories."""
        zip_id, zip_size = self._resolve_landing_zip()
        logger.info("Connecting seekable stream to Drive ZIP (ID: %s, Size: %d bytes)", zip_id, zip_size)

        stream = DriveZipStream(self.storage.drive_service, zip_id, zip_size)
        zf = zipfile.ZipFile(stream)
        all_members = zf.namelist()

        checkpoint = self._load_checkpoint()
        processed_set = set(checkpoint.get("processed_members", []))

        target_categories = list(CATEGORIES.keys()) if category == "all" else [category]
        files_processed = 0
        batch_summary: list[dict[str, Any]] = []

        # Collect pending members per category
        category_queues: dict[str, list[str]] = {}
        for cat in target_categories:
            cat_cfg = CATEGORIES[cat]
            prefix = cat_cfg["prefix"]
            cat_members = [
                m for m in all_members
                if m.startswith(prefix) and m.endswith(".json") and m not in processed_set
            ]
            cat_members.sort()
            category_queues[cat] = cat_members
            logger.info("Category '%s': %d pending members found", cat, len(cat_members))

        # Interleave across categories so all active tables make progress in every batch
        interleaved_members: list[tuple[str, str]] = []
        queue_indices = {cat: 0 for cat in target_categories}
        while len(interleaved_members) < max_files:
            added_any = False
            for cat in target_categories:
                idx = queue_indices[cat]
                q = category_queues[cat]
                if idx < len(q):
                    interleaved_members.append((cat, q[idx]))
                    queue_indices[cat] = idx + 1
                    added_any = True
                    if len(interleaved_members) >= max_files:
                        break
            if not added_any:
                break

        for cat, member_name in interleaved_members:
            cat_cfg = CATEGORIES[cat]
            t0 = time.time()
            base_name = Path(member_name).stem
            out_parquet_name = f"br_opendata_{cat}_{base_name}.parquet"

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_parquet = Path(temp_dir) / out_parquet_name
                with zf.open(member_name) as member_stream:
                    rows = process_member_stream(member_stream, cat_cfg["columns"], temp_parquet)

                if dry_run:
                    drive_id = "dry-run-staged"
                    file_bytes = temp_parquet.stat().st_size
                else:
                    drive_id, file_bytes = self._upload_parquet(cat, out_parquet_name, temp_parquet)

            t1 = time.time()
            files_processed += 1
            if not dry_run:
                processed_set.add(member_name)

            record_info = {
                "member": member_name,
                "category": cat,
                "parquet_name": out_parquet_name,
                "drive_id": drive_id,
                "rows": rows,
                "bytes": file_bytes,
                "elapsed_seconds": round(t1 - t0, 2),
                "dry_run": dry_run,
            }
            batch_summary.append(record_info)
            if not dry_run:
                checkpoint["processed_members"].append(member_name)
                checkpoint["bronze_tables"][cat].append(record_info)
                checkpoint["total_rows"] = checkpoint.get("total_rows", 0) + rows

            logger.info(
                "Processed %s -> %s (%d rows, %d bytes in %.2fs, dry_run=%s)",
                member_name,
                out_parquet_name,
                rows,
                file_bytes,
                t1 - t0,
                dry_run,
            )

        if files_processed > 0 and self.allow_production_write and not dry_run:
            self._save_checkpoint(checkpoint)

        return {
            "status": "completed",
            "files_processed": files_processed,
            "total_processed_members": len(checkpoint["processed_members"]),
            "total_bronze_rows": checkpoint["total_rows"],
            "batch_summary": batch_summary,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--category",
        choices=["all", "organizations", "locations", "people"],
        default="all",
        help="Entity category to process (default: all)",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=5,
        help="Maximum member files to stream and convert in this batch (default: 5)",
    )
    parser.add_argument(
        "--allow-production-write",
        action="store_true",
        help="Allow writing to Drive 02_bronze and 06_control folders",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Stream and parse records locally without uploading to Drive or updating checkpoint",
    )
    parser.add_argument(
        "--verify-checkpoint",
        action="store_true",
        help="Print current bronze checkpoint status and exit",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    storage = StorageManager(allow_interactive_auth=False)
    runner = OpenDataBronzeRunner(storage, allow_production_write=args.allow_production_write)

    if args.verify_checkpoint:
        cp = runner._load_checkpoint()
        print(json.dumps(cp, indent=2))
        return 0

    result = runner.run(category=args.category, max_files=args.max_files, dry_run=args.dry_run)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
