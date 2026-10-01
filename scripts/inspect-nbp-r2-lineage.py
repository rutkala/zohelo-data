#!/usr/bin/env python3
"""Read-only exact NBP raw/Bronze/Archive lineage inventory on R2.

This does not archive or delete anything. It hashes every retained NBP Landing and
Archive JSON object, reads the current four Bronze Iceberg tables, and compares
their distinct response_sha256 provenance to the exact raw bytes.

The report distinguishes:
- Landing raw bytes represented by current Bronze;
- represented Landing bytes already present in Archive;
- represented Landing bytes that still need an Archive copy;
- Landing bytes not represented by current Bronze;
- Bronze provenance hashes for which neither Landing nor Archive contains bytes.
"""

from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any

import boto3
from botocore.config import Config
import duckdb
from pyiceberg.catalog.rest import RestCatalog

SOURCE_PARTS = {
    "nbp",
    "nbp_exchange_rates_table_a",
    "nbp_exchange_rates_table_b",
    "nbp_exchange_rates_table_c",
    "nbp_gold_prices",
    "gold",
}
TABLES = {
    ("bronze", "nbp_exchange_rates_table_a"): "nbp_exchange_rates_table_a",
    ("bronze", "nbp_exchange_rates_table_b"): "nbp_exchange_rates_table_b",
    ("bronze", "nbp_exchange_rates_table_c"): "nbp_exchange_rates_table_c",
    ("bronze", "nbp_gold_prices"): "nbp_gold_prices",
}


def req(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def client():
    return boto3.client(
        "s3",
        endpoint_url=req("R2_S3_ENDPOINT").rstrip("/"),
        region_name="auto",
        aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts": 8, "mode": "standard"}),
    )


def catalog():
    return RestCatalog(
        name="zohelo",
        warehouse=req("R2_WAREHOUSE"),
        uri=req("R2_CATALOG_URI"),
        token=req("R2_DATA_CATALOG_TOKEN"),
        **{
            "s3.access-key-id": req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
            "s3.secret-access-key": req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
            "s3.endpoint": req("R2_S3_ENDPOINT"),
            "s3.region": "auto",
        },
    )


def belongs_to_nbp(key: str, root: str) -> bool:
    parts = [part for part in key.split("/") if part]
    if not parts or parts[0] != root:
        return False
    return any(part in SOURCE_PARTS for part in parts[1:])


def list_json_objects(s3, bucket: str, root: str) -> list[dict[str, Any]]:
    result = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=root + "/"):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            size = int(item.get("Size", 0))
            if size <= 0 or not key.lower().endswith(".json") or not belongs_to_nbp(key, root):
                continue
            result.append({"key": key, "size": size})
    result.sort(key=lambda item: item["key"])
    return result


def hash_object(s3, bucket: str, item: dict[str, Any]) -> dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=item["key"])
    body = response["Body"]
    digest = sha256()
    size = 0
    try:
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    finally:
        body.close()
    if size != item["size"]:
        raise RuntimeError(f"object size changed while hashing: {item['key']}")
    observed = digest.hexdigest()
    stem = Path(item["key"]).stem.lower()
    return {
        **item,
        "sha256": observed,
        "content_addressed_name_matches": stem == observed,
    }


def download_table_files(s3, bucket: str, table, directory: Path) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for index, task in enumerate(table.scan().plan_files()):
        uri = str(task.file.file_path)
        prefix = f"s3://{bucket}/"
        if not uri.startswith(prefix):
            raise RuntimeError(f"unexpected Iceberg data-file URI: {uri}")
        key = uri[len(prefix):]
        target = directory / f"{index:06d}.parquet"
        s3.download_file(bucket, key, str(target))
        if target.stat().st_size != int(task.file.file_size_in_bytes):
            raise RuntimeError(f"current table file size mismatch: {uri}")
        paths.append(target)
    if not paths:
        raise RuntimeError("NBP Bronze table has no data files")
    return paths


def bronze_provenance(s3, cat, lakehouse_bucket: str, root: Path) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for identifier, expected_source in TABLES.items():
        table = cat.load_table(identifier)
        names = {field.name for field in table.schema().fields}
        required = {"source_id", "response_sha256", "raw_file_id"}
        missing = required - names
        if missing:
            raise RuntimeError(
                f"{'.'.join(identifier)} lacks provenance columns: {sorted(missing)}"
            )
        paths = download_table_files(
            s3, lakehouse_bucket, table, root / identifier[-1]
        )
        glob = str(paths[0].parent / "*.parquet").replace("'", "''")
        con = duckdb.connect()
        try:
            rows = int(con.execute(
                f"SELECT count(*) FROM read_parquet('{glob}')"
            ).fetchone()[0])
            grouped = con.execute(
                f"""
                SELECT source_id, response_sha256, raw_file_id, count(*) AS rows
                FROM read_parquet('{glob}')
                GROUP BY ALL
                ORDER BY source_id, response_sha256, raw_file_id
                """
            ).fetchall()
        finally:
            con.close()
        if any(row[0] != expected_source for row in grouped):
            raise RuntimeError(
                f"{'.'.join(identifier)} contains unexpected source_id values"
            )
        values = [
            {
                "source_id": row[0],
                "response_sha256": row[1],
                "raw_file_id": row[2],
                "rows": int(row[3]),
            }
            for row in grouped
        ]
        if any(
            not isinstance(item["response_sha256"], str)
            or len(item["response_sha256"]) != 64
            or not isinstance(item["raw_file_id"], str)
            or not item["raw_file_id"]
            for item in values
        ):
            raise RuntimeError(f"{'.'.join(identifier)} has invalid provenance values")
        report[expected_source] = {
            "table": ".".join(identifier),
            "rows": rows,
            "distinct_provenance_records": len(values),
            "provenance": values,
        }
    return report


def summarize(objects: list[dict[str, Any]]) -> dict[str, Any]:
    by_sha: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in objects:
        by_sha[item["sha256"]].append(item)
    return {
        "objects": len(objects),
        "bytes": sum(int(item["size"]) for item in objects),
        "distinct_sha256": len(by_sha),
        "content_addressed_names_matching": sum(
            1 for item in objects if item["content_addressed_name_matches"]
        ),
        "duplicate_sha256_groups": sum(1 for items in by_sha.values() if len(items) > 1),
    }


def main() -> int:
    s3 = client()
    cat = catalog()
    landing_bucket = req("R2_LANDING_BUCKET")
    lakehouse_bucket = req("R2_LAKEHOUSE_BUCKET")

    landing_items = [
        hash_object(s3, landing_bucket, item)
        for item in list_json_objects(s3, landing_bucket, "01_landing")
    ]
    archive_items = [
        hash_object(s3, landing_bucket, item)
        for item in list_json_objects(s3, landing_bucket, "05_archive")
    ]

    with tempfile.TemporaryDirectory(prefix="zohelo-nbp-lineage-inspect-") as temporary:
        bronze = bronze_provenance(
            s3, cat, lakehouse_bucket, Path(temporary)
        )

    landing_by_sha: dict[str, list[dict[str, Any]]] = defaultdict(list)
    archive_by_sha: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in landing_items:
        landing_by_sha[item["sha256"]].append(item)
    for item in archive_items:
        archive_by_sha[item["sha256"]].append(item)

    bronze_shas = {
        item["response_sha256"]
        for table in bronze.values()
        for item in table["provenance"]
    }
    landing_shas = set(landing_by_sha)
    archive_shas = set(archive_by_sha)

    represented_landing = landing_shas & bronze_shas
    represented_archived = represented_landing & archive_shas
    represented_unarchived = represented_landing - archive_shas
    unrepresented_landing = landing_shas - bronze_shas
    bronze_without_raw = bronze_shas - (landing_shas | archive_shas)

    report = {
        "result": "pass",
        "operation": "inspect-nbp-r2-lineage",
        "writes_performed": False,
        "landing": summarize(landing_items),
        "archive": summarize(archive_items),
        "bronze": {
            "tables": bronze,
            "distinct_response_sha256": len(bronze_shas),
        },
        "comparison": {
            "represented_landing_sha256": len(represented_landing),
            "represented_and_archived_sha256": len(represented_archived),
            "represented_but_unarchived_sha256": len(represented_unarchived),
            "unrepresented_landing_sha256": len(unrepresented_landing),
            "bronze_sha256_without_landing_or_archive": len(bronze_without_raw),
            "all_landing_represented_by_bronze": landing_shas <= bronze_shas,
            "all_bronze_provenance_recoverable": not bronze_without_raw,
        },
        "represented_but_unarchived": [
            {
                "sha256": digest,
                "landing_keys": [item["key"] for item in landing_by_sha[digest]],
                "bytes": sum(int(item["size"]) for item in landing_by_sha[digest]),
            }
            for digest in sorted(represented_unarchived)
        ],
        "unrepresented_landing": [
            {
                "sha256": digest,
                "landing_keys": [item["key"] for item in landing_by_sha[digest]],
                "bytes": sum(int(item["size"]) for item in landing_by_sha[digest]),
            }
            for digest in sorted(unrepresented_landing)
        ],
        "bronze_without_raw": sorted(bronze_without_raw),
    }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
