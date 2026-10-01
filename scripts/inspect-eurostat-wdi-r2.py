#!/usr/bin/env python3
"""Read-only reconciliation diagnostics for Eurostat and World Bank WDI on R2.

This intentionally does not archive, delete, transform, or mutate table metadata.
It inventories:
- current Landing object namespaces and content-addressed identities;
- source and bulk campaign control objects and direct R2 Landing references;
- current Bronze Iceberg tables, schemas, snapshots, and lineage-relevant properties;
- existing Archive objects for the two sources.

The output is used to choose the next source-specific proof without guessing.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import PurePosixPath
import re
from typing import Any

import boto3
from botocore.config import Config
from pyiceberg.catalog.rest import RestCatalog

SOURCES = ("eurostat", "world_bank_wdi")
RAW_NAME = re.compile(r"^(?:raw|response)-([0-9a-f]{64})\.bin$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def req(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def s3():
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


def list_objects(client, bucket: str, prefix: str) -> list[dict[str, Any]]:
    rows = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            size = int(item.get("Size", 0))
            if key.endswith("/") and size == 0:
                continue
            rows.append({"key": key, "size": size})
    rows.sort(key=lambda item: item["key"])
    return rows


def landing_summary(client, bucket: str, source: str) -> dict[str, Any]:
    prefix = f"01_landing/{source}/"
    rows = list_objects(client, bucket, prefix)
    children = Counter()
    names = Counter()
    hashes = set()
    invalid_hash_names = []
    for item in rows:
        relative = item["key"][len(prefix):]
        child = relative.split("/", 1)[0] if "/" in relative else "<root>"
        children[child] += 1
        name = PurePosixPath(item["key"]).name
        if name.startswith("raw-"):
            names["raw"] += 1
        elif name.startswith("response-"):
            names["response"] += 1
        else:
            names["other"] += 1
        match = RAW_NAME.fullmatch(name)
        if match:
            hashes.add(match.group(1))
        elif name.startswith(("raw-", "response-")):
            invalid_hash_names.append(item["key"])
    return {
        "objects": len(rows),
        "bytes": sum(item["size"] for item in rows),
        "by_child": dict(sorted(children.items())),
        "by_name_kind": dict(sorted(names.items())),
        "content_addressed_hashes": len(hashes),
        "invalid_content_addressed_names": invalid_hash_names[:20],
        "sample_keys": [item["key"] for item in rows[:12]],
    }


def archive_summary(client, bucket: str, source: str) -> dict[str, Any]:
    rows = []
    for root in (f"05_archive/{source}/",):
        rows.extend(list_objects(client, bucket, root))
    return {
        "objects": len(rows),
        "bytes": sum(item["size"] for item in rows),
        "sample_keys": [item["key"] for item in rows[:12]],
    }


def walk(value: Any, landing_prefix: str, direct_ids: set[str], hashes: set[str]) -> None:
    if isinstance(value, dict):
        object_id = value.get("id")
        digest = value.get("sha256")
        if (
            isinstance(object_id, str)
            and object_id.startswith("r2://")
            and landing_prefix in object_id
        ):
            direct_ids.add(object_id)
            if isinstance(digest, str) and SHA256.fullmatch(digest):
                hashes.add(digest)
        for item in value.values():
            walk(item, landing_prefix, direct_ids, hashes)
    elif isinstance(value, list):
        for item in value:
            walk(item, landing_prefix, direct_ids, hashes)
    elif isinstance(value, str):
        if value.startswith("r2://") and landing_prefix in value:
            direct_ids.add(value)


def control_summary(client, bucket: str, source: str) -> dict[str, Any]:
    prefixes = [
        f"06_control/source_campaigns/{source}/",
        f"06_control/source_campaigns/{source}_bulk/",
    ]
    if source == "eurostat":
        prefixes.append("06_control/source_campaigns/eurostat_bulk_bronze/")
    direct_ids: set[str] = set()
    hashes: set[str] = set()
    files = []
    pointer_docs = {}
    landing_prefix = f"/01_landing/{source}/"
    for prefix in prefixes:
        rows = list_objects(client, bucket, prefix)
        files.extend(rows)
        for item in rows:
            if item["size"] <= 0 or item["size"] > 8 * 1024 * 1024:
                continue
            if not item["key"].lower().endswith(".json"):
                continue
            raw = client.get_object(Bucket=bucket, Key=item["key"])["Body"].read()
            try:
                value = json.loads(raw)
            except Exception:
                continue
            walk(value, landing_prefix, direct_ids, hashes)
            name = PurePosixPath(item["key"]).name
            if name in {
                "current-ingestion-state.json",
                "current-landing.json",
                "publication-owner.json",
            }:
                pointer_docs[item["key"]] = value
    return {
        "prefixes": prefixes,
        "objects": len(files),
        "bytes": sum(item["size"] for item in files),
        "direct_landing_object_ids": len(direct_ids),
        "direct_landing_sha256": len(hashes),
        "sample_direct_ids": sorted(direct_ids)[:12],
        "pointers": pointer_docs,
    }


def source_for_table(name: str) -> str | None:
    if name.startswith("eurostat_"):
        return "eurostat"
    if name.startswith("wdi_"):
        return "world_bank_wdi"
    return None


def table_summary(cat, namespace: str, source: str) -> list[dict[str, Any]]:
    rows = []
    for identifier in sorted(cat.list_tables(namespace)):
        name = identifier[-1]
        if source_for_table(name) != source:
            continue
        table = cat.load_table(identifier)
        files = list(table.scan().plan_files())
        fields = [field.name for field in table.schema().fields]
        props = {
            key: value for key, value in sorted(table.properties.items())
            if key.startswith("zohelo.")
            or any(token in key.lower() for token in ("lineage", "source", "archive", "input"))
        }
        snapshot = table.current_snapshot()
        rows.append({
            "table": ".".join(identifier),
            "snapshot_id": str(snapshot.snapshot_id) if snapshot is not None else None,
            "rows": sum(int(task.file.record_count) for task in files),
            "files": len(files),
            "bytes": sum(int(task.file.file_size_in_bytes) for task in files),
            "columns": fields,
            "lineage_columns": [
                field for field in fields
                if field in {
                    "raw_sha256", "raw_file_id", "response_sha256",
                    "archive_sha256", "source_file_sha256", "source_file_id",
                }
            ],
            "properties": props,
        })
    return rows


def main() -> int:
    client = s3()
    cat = catalog()
    landing_bucket = req("R2_LANDING_BUCKET")
    lakehouse_bucket = req("R2_LAKEHOUSE_BUCKET")
    report = {
        "result": "pass",
        "operation": "inspect-eurostat-wdi-r2",
        "writes_performed": False,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {},
    }
    for source in SOURCES:
        report["sources"][source] = {
            "landing": landing_summary(client, landing_bucket, source),
            "archive": archive_summary(client, landing_bucket, source),
            "control": control_summary(client, lakehouse_bucket, source),
            "bronze": table_summary(cat, "bronze", source),
            "silver": table_summary(cat, "silver", source),
            "gold": table_summary(cat, "gold", source),
        }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
