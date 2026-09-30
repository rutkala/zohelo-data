#!/usr/bin/env python3
"""Finalize the R2-only Iceberg cutover and remove obsolete release/current storage.

Safety order:
1. Validate production Iceberg namespaces and representative R2 SQL reads.
2. Copy every Iceberg data file still referenced from releases/current into
   canonical medallion paths.
3. Atomically replace those file references in each Iceberg table.
4. Revalidate catalog membership and R2 SQL.
5. Only then delete legacy releases/current objects.

No Google Drive access is performed here.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
import requests
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.typedef import Record

EXPECTED_COUNTS = {"bronze": 37, "silver": 16, "gold": 22}
LAYER_PREFIX = {"bronze": "02_bronze", "silver": "03_silver", "gold": "04_gold"}
LEGACY_PREFIXES = ("releases/", "02_bronze/current/", "03_silver/current/", "04_gold/current/")


class FinalizeError(RuntimeError):
    pass


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise FinalizeError(f"missing_environment:{name}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=required("R2_S3_ENDPOINT").rstrip("/"),
        region_name="auto",
        aws_access_key_id=required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts": 10, "mode": "standard"}),
    )


def catalog() -> RestCatalog:
    return RestCatalog(
        name="zohelo_r2",
        warehouse=required("R2_WAREHOUSE"),
        uri=required("R2_CATALOG_URI"),
        token=required("R2_DATA_CATALOG_TOKEN"),
        **{
            "s3.access-key-id": required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
            "s3.secret-access-key": required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
            "s3.endpoint": required("R2_S3_ENDPOINT"),
            "s3.region": "auto",
        },
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise FinalizeError(f"unexpected_data_file_uri:{uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def is_legacy_key(key: str) -> bool:
    parts = key.split("/")
    return key.startswith("releases/") or (
        parts and parts[0] in {"02_bronze", "03_silver", "04_gold"} and "current" in parts
    )


def canonical_key(namespace: str, table: str, source_key: str) -> str:
    digest = hashlib.sha256(source_key.encode("utf-8")).hexdigest()[:16]
    name = PurePosixPath(source_key).name
    return f"{LAYER_PREFIX[namespace]}/{table}/iceberg_data/{digest}-{name}"


def list_production_tables(cat: RestCatalog) -> dict[str, list[tuple[str, str]]]:
    result: dict[str, list[tuple[str, str]]] = {}
    for namespace, expected in EXPECTED_COUNTS.items():
        tables = sorted(cat.list_tables(namespace))
        if len(tables) != expected:
            raise FinalizeError(f"table_count_mismatch:{namespace}:{len(tables)}:{expected}")
        result[namespace] = tables
    return result


def r2_sql(query: str) -> dict[str, Any]:
    account = required("CLOUDFLARE_ACCOUNT_ID")
    bucket = required("R2_LAKEHOUSE_BUCKET")
    url = f"https://api.sql.cloudflarestorage.com/api/v1/accounts/{account}/r2-sql/query/{bucket}"
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {required('R2_DATA_CATALOG_TOKEN')}",
            "Content-Type": "application/json",
        },
        json={"query": query},
        timeout=120,
    )
    if response.status_code != 200:
        raise FinalizeError(f"r2_sql_failed:{response.status_code}:{response.text[:500]}")
    try:
        payload = response.json()
    except Exception as exc:
        raise FinalizeError(f"r2_sql_invalid_json:{query}") from exc
    return payload


def validate_sql() -> list[dict[str, Any]]:
    queries = [
        "SHOW NAMESPACES;",
        "SHOW TABLES IN bronze;",
        "SHOW TABLES IN silver;",
        "SHOW TABLES IN gold;",
        "SELECT * FROM bronze.dbw_observations LIMIT 1;",
        "SELECT * FROM silver.bdl_observations LIMIT 1;",
        "SELECT * FROM gold.fact_bdl_observations LIMIT 1;",
        "SELECT * FROM silver.nbp_change_events LIMIT 1;",
    ]
    result = []
    for query in queries:
        payload = r2_sql(query)
        result.append({"query": query, "response_keys": sorted(payload.keys()) if isinstance(payload, dict) else []})
        print(json.dumps({"operation": "r2_sql_validation", "query": query, "status": "pass"}), flush=True)
    return result


def copy_if_needed(client: Any, bucket: str, source_key: str, target_key: str, expected_size: int) -> None:
    try:
        head = client.head_object(Bucket=bucket, Key=target_key)
        if int(head.get("ContentLength", -1)) == expected_size:
            return
    except Exception:
        pass
    client.copy_object(Bucket=bucket, Key=target_key, CopySource={"Bucket": bucket, "Key": source_key})
    head = client.head_object(Bucket=bucket, Key=target_key)
    if int(head.get("ContentLength", -1)) != expected_size:
        raise FinalizeError(f"copy_size_mismatch:{source_key}:{target_key}")


def replacement_file(table: Any, original: Any, new_uri: str) -> DataFile:
    return DataFile.from_args(
        _table_format_version=int(table.metadata.format_version),
        content=DataFileContent.DATA,
        file_path=new_uri,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=int(original.record_count),
        file_size_in_bytes=int(original.file_size_in_bytes),
        spec_id=int(table.metadata.default_spec_id),
    )


def relocate_table(client: Any, cat: RestCatalog, bucket: str, identifier: tuple[str, str]) -> dict[str, Any]:
    namespace, table_name = identifier
    table = cat.load_table(identifier)
    tasks = list(table.scan().plan_files())
    legacy = []
    for task in tasks:
        source_bucket, source_key = parse_s3_uri(str(task.file.file_path))
        if source_bucket != bucket:
            raise FinalizeError(f"unexpected_bucket:{source_bucket}")
        if is_legacy_key(source_key):
            legacy.append((task.file, source_key))

    if not legacy:
        return {"table": f"{namespace}.{table_name}", "relocated": 0}

    replacements = []
    for original, source_key in legacy:
        target_key = canonical_key(namespace, table_name, source_key)
        copy_if_needed(client, bucket, source_key, target_key, int(original.file_size_in_bytes))
        replacements.append((original, f"s3://{bucket}/{target_key}"))

    with table.transaction() as tx:
        with tx.update_snapshot(
            snapshot_properties={"zohelo.relocation": "remove-legacy-release-current-paths"}
        ).overwrite() as overwrite:
            for original, new_uri in replacements:
                overwrite.delete_data_file(original)
                overwrite.append_data_file(replacement_file(table, original, new_uri))

    updated = cat.load_table(identifier)
    remaining_legacy = []
    for task in updated.scan().plan_files():
        _, key = parse_s3_uri(str(task.file.file_path))
        if is_legacy_key(key):
            remaining_legacy.append(key)
    if remaining_legacy:
        raise FinalizeError(f"legacy_references_remain:{namespace}.{table_name}:{len(remaining_legacy)}")

    print(json.dumps({
        "operation": "relocate_iceberg_data_files",
        "table": f"{namespace}.{table_name}",
        "relocated": len(replacements),
    }), flush=True)
    return {"table": f"{namespace}.{table_name}", "relocated": len(replacements)}


def assert_no_legacy_references(cat: RestCatalog) -> int:
    count = 0
    for namespace in EXPECTED_COUNTS:
        for identifier in cat.list_tables(namespace):
            for task in cat.load_table(identifier).scan().plan_files():
                _, key = parse_s3_uri(str(task.file.file_path))
                if is_legacy_key(key):
                    count += 1
    if count:
        raise FinalizeError(f"legacy_catalog_references:{count}")
    return count


def delete_prefix(client: Any, bucket: str, prefix: str) -> dict[str, int]:
    keys: list[dict[str, str]] = []
    objects = 0
    bytes_total = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            keys.append({"Key": str(item["Key"])})
            objects += 1
            bytes_total += int(item.get("Size", 0))
            if len(keys) == 1000:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
                keys = []
    if keys:
        client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
    check = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if check.get("KeyCount", 0):
        raise FinalizeError(f"prefix_cleanup_incomplete:{prefix}")
    return {"objects": objects, "bytes": bytes_total}


def main() -> int:
    receipt_path = Path(os.environ.get("FINALIZE_RECEIPT", "/tmp/r2-iceberg-finalize.json"))
    receipt: dict[str, Any] = {
        "operation": "r2-iceberg-finalize",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "google_drive_accessed": False,
        "legacy_deleted": False,
    }
    write_json(receipt_path, receipt)

    client = s3_client()
    cat = catalog()
    bucket = required("R2_LAKEHOUSE_BUCKET")

    tables = list_production_tables(cat)
    receipt["table_counts"] = {ns: len(items) for ns, items in tables.items()}
    receipt["pre_cleanup_sql"] = validate_sql()
    write_json(receipt_path, receipt)

    relocations = []
    for namespace in ("bronze", "silver", "gold"):
        for identifier in tables[namespace]:
            relocations.append(relocate_table(client, cat, bucket, identifier))
    receipt["relocations"] = relocations
    receipt["relocated_files"] = sum(item["relocated"] for item in relocations)
    assert_no_legacy_references(cat)
    receipt["post_relocation_sql"] = validate_sql()
    write_json(receipt_path, receipt)

    cleanup = {}
    for prefix in LEGACY_PREFIXES:
        cleanup[prefix] = delete_prefix(client, bucket, prefix)
        print(json.dumps({"operation": "delete_legacy_prefix", "prefix": prefix, **cleanup[prefix]}), flush=True)

    # Final catalog and SQL validation after destructive cleanup.
    assert_no_legacy_references(cat)
    receipt["post_cleanup_sql"] = validate_sql()
    receipt["cleanup"] = cleanup
    receipt["legacy_deleted"] = True
    receipt["result"] = "pass"
    receipt["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    write_json(receipt_path, receipt)
    print(json.dumps(receipt, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
