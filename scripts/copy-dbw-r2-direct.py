#!/usr/bin/env python3
"""Direct-copy the exact reviewed retained DBW Bronze Parquet files to R2.

The migration preserves each reviewed Parquet object byte-for-byte:
Drive -> one temporary file -> SHA-256/MD5 verification -> single R2 PutObject
with Content-MD5 -> R2 HEAD verification -> delete temporary file.

After each bounded batch, PyIceberg add_files() registers those existing R2
Parquet files in one Iceberg table without rewriting their contents.

Google Drive is read-only. No Drive pointer or file is changed.
"""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import audit_retained_dbw_bronze as audit
from retained_dbw_publication import EXPECTED_SCHEMAS, REVIEWED_INVENTORY_SHA256
from storage_manager import StorageManager


EXPECTED_INDICATORS = 1_550
EXPECTED_OBSERVATION_BYTES = 4_737_200_817
EXPECTED_OBSERVATION_ROWS = 879_999_727
EXPECTED_INVENTORY_OBJECTS = 3_103
EXPECTED_INVENTORY_BYTES = 4_803_673_234
COPY_WORKERS = 4
BATCH_SIZE = 50

NAMESPACE = "zohelo_pilot_dbw_direct"
TABLE_NAME = "gus_dbw_observations"


class DirectCopyError(RuntimeError):
    pass


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise DirectCopyError(f"missing_environment:{name}")
    return value


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def reviewed_inventory(storage: StorageManager) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    descriptors, indicator_ids = audit.validate_inventory(audit.discover(storage))
    inventory = audit.inventory_document(descriptors)
    if (
        inventory.get("inventory_sha256") != REVIEWED_INVENTORY_SHA256
        or int(inventory.get("object_count", -1)) != EXPECTED_INVENTORY_OBJECTS
        or int(inventory.get("total_bytes", -1)) != EXPECTED_INVENTORY_BYTES
        or len(indicator_ids) != EXPECTED_INDICATORS
    ):
        raise DirectCopyError("reviewed_drive_inventory_changed")

    observations = sorted(
        descriptors["observations"].values(),
        key=lambda item: int(re.fullmatch(r"observations/part_(\d+)\.parquet", item["path"]).group(1)),
    )
    if len(observations) != EXPECTED_INDICATORS:
        raise DirectCopyError("reviewed_observation_membership_incomplete")
    observation_bytes = sum(int(item["size"]) for item in observations)
    if observation_bytes != EXPECTED_OBSERVATION_BYTES:
        raise DirectCopyError(f"unexpected_reviewed_observation_bytes:{observation_bytes}")
    return inventory, observations


def indicator_id(descriptor: dict[str, Any]) -> int:
    match = re.fullmatch(r"observations/part_(\d+)\.parquet", descriptor["path"])
    if match is None:
        raise DirectCopyError("unexpected_observation_path")
    return int(match.group(1))


def object_key(descriptor: dict[str, Any]) -> str:
    return (
        "02_bronze/gus_dbw/retained/"
        + REVIEWED_INVENTORY_SHA256
        + "/observations/"
        + descriptor["name"]
    )


def object_uri(bucket: str, descriptor: dict[str, Any]) -> str:
    return f"s3://{bucket}/{object_key(descriptor)}"


def r2_client() -> Any:
    endpoint = required_env("R2_S3_ENDPOINT")
    parsed = urlsplit(endpoint)
    if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(
        ".r2.cloudflarestorage.com"
    ):
        raise DirectCopyError("invalid_r2_endpoint")
    return boto3.client(
        "s3",
        endpoint_url=endpoint.rstrip("/"),
        aws_access_key_id=required_env("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=required_env("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        region_name="auto",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )


def metadata_for(descriptor: dict[str, Any]) -> dict[str, str]:
    return {
        "source-sha256": descriptor["sha256"],
        "source-md5": descriptor["md5"],
        "source-drive-id": descriptor["id"],
        "source-inventory-sha256": REVIEWED_INVENTORY_SHA256,
    }


def head_existing(client: Any, bucket: str, descriptor: dict[str, Any]) -> bool:
    key = object_key(descriptor)
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
        if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise

    metadata = {str(k).lower(): str(v) for k, v in (head.get("Metadata") or {}).items()}
    expected_metadata = metadata_for(descriptor)
    etag = str(head.get("ETag", "")).strip('"').lower()
    if (
        int(head.get("ContentLength", -1)) != int(descriptor["size"])
        or any(metadata.get(k) != v for k, v in expected_metadata.items())
        or etag != descriptor["md5"]
    ):
        raise DirectCopyError(f"r2_existing_object_identity_mismatch:{descriptor['path']}")
    return True


_thread_local = threading.local()


def thread_drive_reader(root_id: str) -> StorageManager:
    if not hasattr(_thread_local, "drive_reader"):
        _thread_local.drive_reader = StorageManager(
            allow_interactive_auth=False,
            root_id=root_id,
        )
    return _thread_local.drive_reader


def thread_r2_client() -> Any:
    if not hasattr(_thread_local, "r2_client"):
        _thread_local.r2_client = r2_client()
    return _thread_local.r2_client


def copy_one(
    descriptor: dict[str, Any],
    *,
    bucket: str,
    root_id: str,
    work_dir: Path,
) -> dict[str, Any]:
    client = thread_r2_client()
    indicator = indicator_id(descriptor)
    uri = object_uri(bucket, descriptor)

    if head_existing(client, bucket, descriptor):
        return {
            "indicator_id": indicator,
            "uri": uri,
            "bytes": int(descriptor["size"]),
            "status": "reused",
            "rows": None,
        }

    reader = thread_drive_reader(root_id)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f"dbw-{indicator}-",
        suffix=".parquet",
        dir=work_dir,
    )
    os.close(fd)
    temporary = Path(temporary_name)
    temporary.unlink(missing_ok=True)
    try:
        audit.restore_verified(reader, descriptor, temporary)
        parquet = pq.ParquetFile(temporary)
        rows = int(parquet.metadata.num_rows)
        if rows <= 0:
            raise DirectCopyError(f"source_parquet_empty:{indicator}")

        with temporary.open("rb") as stream:
            response = client.put_object(
                Bucket=bucket,
                Key=object_key(descriptor),
                Body=stream,
                ContentLength=int(descriptor["size"]),
                ContentMD5=base64.b64encode(bytes.fromhex(descriptor["md5"])).decode("ascii"),
                ContentType="application/vnd.apache.parquet",
                Metadata=metadata_for(descriptor),
            )
        status = int(response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
        if status not in (200, 201):
            raise DirectCopyError(f"r2_put_status:{status}")

        if not head_existing(client, bucket, descriptor):
            raise DirectCopyError(f"r2_post_upload_missing:{indicator}")
        return {
            "indicator_id": indicator,
            "uri": uri,
            "bytes": int(descriptor["size"]),
            "status": "copied",
            "rows": rows,
        }
    finally:
        temporary.unlink(missing_ok=True)


def iceberg_schema() -> pa.Schema:
    types = {
        "BIGINT": pa.int64(),
        "INTEGER": pa.int32(),
        "VARCHAR": pa.string(),
        "DOUBLE": pa.float64(),
    }
    return pa.schema([pa.field(name, types[kind]) for name, kind in EXPECTED_SCHEMAS["observations"]])


def catalog() -> RestCatalog:
    uri = required_env("R2_CATALOG_URI")
    parsed = urlsplit(uri)
    if parsed.scheme != "https" or not parsed.hostname or "cloudflarestorage.com" not in parsed.hostname:
        raise DirectCopyError("invalid_catalog_uri")
    return RestCatalog(
        name="zohelo_r2",
        warehouse=required_env("R2_WAREHOUSE"),
        uri=uri,
        token=required_env("CLOUDFLARE_R2_CATALOG_TOKEN"),
    )


def ensure_table(cat: RestCatalog):
    try:
        cat.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    return cat.create_table_if_not_exists(
        (NAMESPACE, TABLE_NAME),
        schema=iceberg_schema(),
        properties={
            "format-version": "2",
            "write.metadata.metrics.default": "full",
        },
    )


def referenced_paths(table: Any) -> set[str]:
    return {str(task.file.file_path) for task in table.scan().plan_files()}


def duckdb_catalog() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='512MiB'")
    con.execute("SET enable_external_file_cache=true")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    token = required_env("CLOUDFLARE_R2_CATALOG_TOKEN")
    con.execute(
        f"CREATE SECRET r2_catalog_token (TYPE ICEBERG, TOKEN {sql_string(token)})"
    )
    con.execute(
        f"""
        ATTACH {sql_string(required_env("R2_WAREHOUSE"))} AS r2_catalog (
            TYPE ICEBERG,
            SECRET r2_catalog_token,
            ENDPOINT {sql_string(required_env("R2_CATALOG_URI"))}
        )
        """
    )
    return con


def validate_complete_table(expected_paths: set[str], first_id: int, last_id: int) -> dict[str, Any]:
    table_sql = f'r2_catalog."{NAMESPACE}"."{TABLE_NAME}"'
    con = duckdb_catalog()
    try:
        preview = con.execute(f"SELECT * FROM {table_sql} LIMIT 1000").fetchall()
        complete_count = int(con.execute(f"SELECT count(*) FROM {table_sql}").fetchone()[0])
        filtered = int(
            con.execute(
                f"SELECT count(*) FROM {table_sql} WHERE indicator_id IN (?, ?)",
                [first_id, last_id],
            ).fetchone()[0]
        )
        joined = int(
            con.execute(
                f"""
                SELECT count(*)
                FROM {table_sql} o
                JOIN (VALUES (?), (?)) d(indicator_id) USING (indicator_id)
                """,
                [first_id, last_id],
            ).fetchone()[0]
        )
        manifest_rows = int(
            con.execute(
                f"""
                SELECT coalesce(sum(record_count), 0)
                FROM iceberg_metadata({table_sql})
                WHERE content = 'EXISTING' AND status <> 'DELETED'
                """
            ).fetchone()[0]
        )
        data_files = int(
            con.execute(
                f"""
                SELECT count(*)
                FROM iceberg_metadata({table_sql})
                WHERE content = 'EXISTING' AND status <> 'DELETED'
                """
            ).fetchone()[0]
        )
        snapshots = con.execute(
            f"SELECT snapshot_id FROM iceberg_snapshots({table_sql}) ORDER BY sequence_number"
        ).fetchall()
        if (
            len(preview) != 1000
            or complete_count != EXPECTED_OBSERVATION_ROWS
            or manifest_rows != EXPECTED_OBSERVATION_ROWS
            or filtered <= 0
            or joined != filtered
            or data_files != len(expected_paths)
            or not snapshots
        ):
            raise DirectCopyError("direct_copy_table_acceptance_failed")
        return {
            "preview_rows": len(preview),
            "complete_count": complete_count,
            "filtered_count": filtered,
            "join_count": joined,
            "manifest_rows": manifest_rows,
            "data_files": data_files,
            "snapshot_id": str(int(snapshots[-1][0])),
        }
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--drive-root-id", required=True)
    args = parser.parse_args()

    bucket = required_env("R2_LAKEHOUSE_BUCKET")
    args.work_dir.mkdir(parents=True, exist_ok=True)

    receipt: dict[str, Any] = {
        "result": "running",
        "contract": "dbw-r2-direct-byte-preserving-copy",
        "source_inventory_sha256": REVIEWED_INVENTORY_SHA256,
        "drive_write_performed": False,
        "production_pointer_changed": False,
    }
    write_json(args.receipt, receipt)

    stage = "inventory"
    try:
        initial_reader = StorageManager(
            allow_interactive_auth=False,
            root_id=args.drive_root_id,
        )
        inventory, observations = reviewed_inventory(initial_reader)
        expected_uris = {object_uri(bucket, item) for item in observations}

        stage = "catalog"
        cat = catalog()
        table = ensure_table(cat)
        registered = referenced_paths(table)
        unexpected = registered - expected_uris
        if unexpected:
            raise DirectCopyError("pilot_table_contains_unexpected_files")

        copied_files = 0
        reused_files = 0
        copied_bytes = 0
        reused_bytes = 0
        known_rows = 0

        stage = "copy_and_register"
        for offset in range(0, len(observations), BATCH_SIZE):
            batch = observations[offset : offset + BATCH_SIZE]
            results: list[dict[str, Any]] = []
            with ThreadPoolExecutor(
                max_workers=COPY_WORKERS,
                thread_name_prefix="dbw-r2-direct",
            ) as pool:
                futures = [
                    pool.submit(
                        copy_one,
                        descriptor,
                        bucket=bucket,
                        root_id=args.drive_root_id,
                        work_dir=args.work_dir,
                    )
                    for descriptor in batch
                ]
                for future in as_completed(futures):
                    results.append(future.result())

            for result in results:
                if result["status"] == "copied":
                    copied_files += 1
                    copied_bytes += int(result["bytes"])
                    known_rows += int(result["rows"])
                else:
                    reused_files += 1
                    reused_bytes += int(result["bytes"])

            table = cat.load_table((NAMESPACE, TABLE_NAME))
            registered = referenced_paths(table)
            to_register = sorted(result["uri"] for result in results if result["uri"] not in registered)
            if to_register:
                table.add_files(
                    file_paths=to_register,
                    snapshot_properties={
                        "zohelo.source.inventory.sha256": REVIEWED_INVENTORY_SHA256,
                        "zohelo.migration": "direct-byte-preserving-copy",
                    },
                )
                table = cat.load_table((NAMESPACE, TABLE_NAME))
                registered = referenced_paths(table)

            completed = min(offset + len(batch), len(observations))
            print(
                json.dumps(
                    {
                        "operation": "dbw_r2_direct_copy",
                        "completed_files": completed,
                        "total_files": len(observations),
                        "copied_files_this_run": copied_files,
                        "reused_files_this_run": reused_files,
                        "registered_files": len(registered),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

            receipt.update(
                {
                    "stage": stage,
                    "completed_files": completed,
                    "total_files": len(observations),
                    "copied_files": copied_files,
                    "reused_files": reused_files,
                    "copied_bytes": copied_bytes,
                    "reused_bytes": reused_bytes,
                    "registered_files": len(registered),
                }
            )
            write_json(args.receipt, receipt)

        stage = "source_stability"
        final_reader = StorageManager(
            allow_interactive_auth=False,
            root_id=args.drive_root_id,
        )
        final_inventory, _ = reviewed_inventory(final_reader)
        if final_inventory["inventory_sha256"] != inventory["inventory_sha256"]:
            raise DirectCopyError("drive_inventory_changed_during_copy")

        stage = "iceberg_membership"
        table = cat.load_table((NAMESPACE, TABLE_NAME))
        registered = referenced_paths(table)
        if registered != expected_uris:
            raise DirectCopyError(
                f"iceberg_file_membership_mismatch:{len(registered)}:{len(expected_uris)}"
            )

        stage = "query_acceptance"
        first_id = indicator_id(observations[0])
        last_id = indicator_id(observations[-1])
        acceptance = validate_complete_table(expected_uris, first_id, last_id)

        receipt = {
            "result": "pass",
            "contract": "dbw-r2-direct-byte-preserving-copy",
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_inventory_sha256": REVIEWED_INVENTORY_SHA256,
            "source_observation_files": EXPECTED_INDICATORS,
            "source_observation_bytes": EXPECTED_OBSERVATION_BYTES,
            "source_observation_rows": EXPECTED_OBSERVATION_ROWS,
            "copy": {
                "copied_files_this_run": copied_files,
                "reused_files_this_run": reused_files,
                "copied_bytes_this_run": copied_bytes,
                "reused_bytes_this_run": reused_bytes,
                "r2_bucket": bucket,
                "r2_prefix": (
                    "02_bronze/gus_dbw/retained/"
                    + REVIEWED_INVENTORY_SHA256
                    + "/observations/"
                ),
                "byte_preserving": True,
                "single_put_content_md5": True,
            },
            "iceberg": {
                "namespace": NAMESPACE,
                "table": TABLE_NAME,
                "registered_files": len(registered),
                "registration_method": "pyiceberg_add_files_no_rewrite",
                **acceptance,
            },
            "source_limitations": [
                "Reviewed retained DBW inventory is not proof of current provider completeness.",
                "Legacy receipt membership does not prove native-to-Bronze value lineage.",
            ],
            "drive_write_performed": False,
            "production_pointer_changed": False,
        }
        write_json(args.receipt, receipt)
        print(json.dumps(receipt, sort_keys=True))
        return 0
    except Exception as exc:
        receipt.update(
            {
                "result": "fail",
                "stage": stage,
                "category": type(exc).__name__,
            }
        )
        write_json(args.receipt, receipt)
        print(json.dumps(receipt, sort_keys=True))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
