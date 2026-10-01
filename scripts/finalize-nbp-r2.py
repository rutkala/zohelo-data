#!/usr/bin/env python3
"""Finalize current NBP Landing objects after exact Bronze provenance reconciliation.

The operation is deliberately source-scoped and idempotent:
1. prove the four Bronze Iceberg tables contain exactly the current NBP Landing SHA-256 set;
2. reuse an exact legacy Archive copy when present, otherwise copy the missing raw JSON;
3. verify every Archive object by full SHA-256 readback;
4. block deletion if live R2 NBP control JSON directly references a Landing key;
5. write one immutable lineage receipt and attach it to all four Bronze tables;
6. delete only the Landing keys recorded by that receipt;
7. write an immutable lifecycle finalization receipt.

No Google Drive access is performed and no Bronze/Silver/Gold rows are rewritten.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import duckdb
from pyiceberg.catalog.rest import RestCatalog

SOURCE = "nbp"
LANDING_ROOT = "01_landing"
ARCHIVE_ROOT = "05_archive"
LINEAGE_PREFIX = "06_control/lineage/nbp/"
LIFECYCLE_PREFIX = "06_control/lifecycle/nbp/"
CONTROL_PREFIXES = ("06_control/nbp/", "06_control/source_campaigns/nbp")
SOURCE_IDS = (
    "nbp_exchange_rates_table_a",
    "nbp_exchange_rates_table_b",
    "nbp_exchange_rates_table_c",
    "nbp_gold_prices",
)
TABLES = tuple(("bronze", source_id) for source_id in SOURCE_IDS)


def req(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def s3_client():
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


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def hash_object(s3, bucket: str, key: str, expected_size: int | None = None) -> tuple[str, int]:
    response = s3.get_object(Bucket=bucket, Key=key)
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
    if expected_size is not None and size != expected_size:
        raise RuntimeError(f"object size changed while hashing: {key}")
    return digest.hexdigest(), size


def list_objects(s3, bucket: str, prefix: str) -> list[dict[str, Any]]:
    result = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            size = int(item.get("Size", 0))
            if size > 0:
                result.append({"key": key, "size": size})
    result.sort(key=lambda item: item["key"])
    return result


def landing_source_id(key: str) -> str | None:
    parts = [part for part in key.split("/") if part]
    if len(parts) < 3 or parts[0] != LANDING_ROOT:
        return None
    return parts[1] if parts[1] in SOURCE_IDS else None


def current_landing(s3, bucket: str) -> list[dict[str, Any]]:
    result = []
    for item in list_objects(s3, bucket, LANDING_ROOT + "/"):
        source_id = landing_source_id(item["key"])
        if source_id is None or not item["key"].lower().endswith(".json"):
            continue
        digest, size = hash_object(s3, bucket, item["key"], item["size"])
        if Path(item["key"]).stem.lower() != digest:
            raise RuntimeError(f"NBP Landing key is not content-addressed by its SHA-256: {item['key']}")
        result.append({
            "key": item["key"],
            "source_id": source_id,
            "size": size,
            "sha256": digest,
        })
    result.sort(key=lambda item: item["key"])
    return result


def archive_inventory(s3, bucket: str) -> dict[str, list[dict[str, Any]]]:
    by_sha: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in list_objects(s3, bucket, ARCHIVE_ROOT + "/"):
        parts = [part for part in item["key"].split("/") if part]
        if not any(part in set(SOURCE_IDS) | {SOURCE, "gold"} for part in parts[1:]):
            continue
        if not item["key"].lower().endswith(".json"):
            continue
        digest, size = hash_object(s3, bucket, item["key"], item["size"])
        by_sha[digest].append({"key": item["key"], "size": size, "sha256": digest})
    for values in by_sha.values():
        values.sort(key=lambda item: item["key"])
    return by_sha


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


def bronze_provenance(s3, cat, lakehouse_bucket: str, root: Path) -> tuple[dict[str, Any], set[str]]:
    proofs: dict[str, Any] = {}
    all_shas: set[str] = set()
    for identifier in TABLES:
        table = cat.load_table(identifier)
        names = {field.name for field in table.schema().fields}
        required = {"source_id", "response_sha256", "raw_file_id"}
        if not required <= names:
            raise RuntimeError(
                f"{'.'.join(identifier)} lacks provenance columns: {sorted(required - names)}"
            )
        paths = download_table_files(s3, lakehouse_bucket, table, root / identifier[-1])
        glob = str(paths[0].parent / "*.parquet").replace("'", "''")
        con = duckdb.connect()
        try:
            rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0])
            grouped = con.execute(
                f"""
                SELECT source_id, response_sha256, count(*) AS rows
                FROM read_parquet('{glob}')
                GROUP BY source_id, response_sha256
                ORDER BY response_sha256
                """
            ).fetchall()
        finally:
            con.close()
        expected = identifier[-1]
        if any(row[0] != expected for row in grouped):
            raise RuntimeError(f"{'.'.join(identifier)} contains unexpected source_id values")
        shas = {row[1] for row in grouped}
        if any(not isinstance(value, str) or len(value) != 64 for value in shas):
            raise RuntimeError(f"{'.'.join(identifier)} has invalid response_sha256 values")
        snapshot = table.current_snapshot()
        proofs[".".join(identifier)] = {
            "table": ".".join(identifier),
            "snapshot_id": str(snapshot.snapshot_id) if snapshot is not None else None,
            "rows": rows,
            "distinct_response_sha256": len(shas),
        }
        all_shas.update(shas)
    return proofs, all_shas


def input_set_digest(inputs: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        [
            {
                "key": item["key"],
                "source_id": item["source_id"],
                "size": item["size"],
                "sha256": item["sha256"],
            }
            for item in sorted(inputs, key=lambda item: item["key"])
        ],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(payload).hexdigest()


def scan_control_references(s3, bucket: str, inputs: list[dict[str, Any]]) -> list[str]:
    needles = []
    landing_bucket = req("R2_LANDING_BUCKET")
    for item in inputs:
        needles.append(item["key"].encode("utf-8"))
        needles.append(f"r2://{landing_bucket}/{item['key']}".encode("utf-8"))
    matches = []
    for prefix in CONTROL_PREFIXES:
        for item in list_objects(s3, bucket, prefix):
            if item["size"] > 8 * 1024 * 1024 or not item["key"].lower().endswith(".json"):
                continue
            body = s3.get_object(Bucket=bucket, Key=item["key"])["Body"].read()
            if any(needle in body for needle in needles):
                matches.append(item["key"])
    return sorted(set(matches))


def canonical_archive_key(item: dict[str, Any]) -> str:
    return f"{ARCHIVE_ROOT}/{SOURCE}/{item['source_id']}/{item['sha256']}.json"


def ensure_archives(
    s3,
    bucket: str,
    inputs: list[dict[str, Any]],
    existing: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], int]:
    mapped = []
    copied = 0
    for item in inputs:
        candidates = existing.get(item["sha256"], [])
        if candidates:
            selected = candidates[0]
            archive_key = selected["key"]
        else:
            archive_key = canonical_archive_key(item)
            if not object_exists(s3, bucket, archive_key):
                s3.copy_object(
                    Bucket=bucket,
                    Key=archive_key,
                    CopySource={"Bucket": bucket, "Key": item["key"]},
                )
                copied += 1
        observed_sha, observed_size = hash_object(s3, bucket, archive_key)
        if observed_sha != item["sha256"] or observed_size != item["size"]:
            raise RuntimeError(f"Archive readback mismatch for {item['key']}")
        mapped.append({
            "source_key": item["key"],
            "source_id": item["source_id"],
            "archive_key": archive_key,
            "size": item["size"],
            "sha256": item["sha256"],
            "compression": "none_exact_json",
        })
    return mapped, copied


def immutable_json(s3, bucket: str, key: str, value: dict[str, Any]) -> None:
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if object_exists(s3, bucket, key):
        existing = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if existing != raw:
            raise RuntimeError(f"immutable receipt conflict: {key}")
        return
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=raw,
        ContentType="application/json",
        Metadata={"sha256": sha256(raw).hexdigest()},
    )


def existing_lineage_receipts(s3, bucket: str) -> list[dict[str, Any]]:
    receipts = []
    for item in list_objects(s3, bucket, LINEAGE_PREFIX):
        if not item["key"].endswith(".json"):
            continue
        value = json.loads(s3.get_object(Bucket=bucket, Key=item["key"])["Body"].read())
        if (
            isinstance(value, dict)
            and value.get("format_version") == 1
            and value.get("kind") == "landing_bronze_lineage_receipt"
            and value.get("source_id") == SOURCE
        ):
            receipts.append({"key": item["key"], "value": value})
    return receipts


def attach_receipt(cat, receipt_uri: str, digest: str, proofs: dict[str, Any]) -> None:
    for identifier in TABLES:
        table = cat.load_table(identifier)
        proof = proofs[".".join(identifier)]
        desired = {
            "zohelo.lineage.mode": "r2-landing-provenance-hash-membership-v1",
            "zohelo.lineage.source": SOURCE,
            "zohelo.lineage.input-set-sha256": digest,
            "zohelo.lineage.receipt": receipt_uri,
            "zohelo.lineage.snapshot-id": str(proof["snapshot_id"]),
        }
        if all(table.properties.get(key) == value for key, value in desired.items()):
            continue
        with table.transaction() as tx:
            tx.set_properties(**desired)


def main() -> int:
    s3 = s3_client()
    cat = catalog()
    landing_bucket = req("R2_LANDING_BUCKET")
    lakehouse_bucket = req("R2_LAKEHOUSE_BUCKET")

    existing_receipts = existing_lineage_receipts(s3, lakehouse_bucket)
    if len(existing_receipts) > 1:
        raise RuntimeError(f"multiple NBP lineage receipts already exist: {len(existing_receipts)}")

    landing = current_landing(s3, landing_bucket)
    if existing_receipts:
        receipt_key = existing_receipts[0]["key"]
        receipt = existing_receipts[0]["value"]
        expected_inputs = receipt.get("landing_inputs")
        if not isinstance(expected_inputs, list) or not expected_inputs:
            raise RuntimeError("existing NBP lineage receipt has no Landing inputs")
        by_key = {item["key"]: item for item in landing}
        for expected in expected_inputs:
            if not isinstance(expected, dict):
                raise RuntimeError("existing NBP lineage receipt has invalid Landing descriptor")
            current = by_key.get(expected.get("key"))
            if current is not None and (
                current["sha256"] != expected.get("sha256")
                or current["size"] != expected.get("size")
            ):
                raise RuntimeError(f"NBP Landing changed after lineage receipt: {current['key']}")
        inputs = expected_inputs
        digest = str(receipt.get("input_set_sha256", ""))
    else:
        if len(landing) != 404:
            raise RuntimeError(f"expected 404 current NBP Landing JSON objects, observed {len(landing)}")
        inputs = landing
        digest = input_set_digest(inputs)

    with tempfile.TemporaryDirectory(prefix="zohelo-nbp-finalize-") as temporary:
        proofs, bronze_shas = bronze_provenance(
            s3, cat, lakehouse_bucket, Path(temporary)
        )

    input_shas = {item["sha256"] for item in inputs}
    if len(input_shas) != len(inputs):
        raise RuntimeError("NBP current Landing contains duplicate SHA-256 identities")
    if input_shas != bronze_shas:
        raise RuntimeError(
            "NBP Landing/Bronze provenance set mismatch: "
            f"landing_only={len(input_shas - bronze_shas)} "
            f"bronze_only={len(bronze_shas - input_shas)}"
        )

    existing_archive = archive_inventory(s3, landing_bucket)
    archives, copied = ensure_archives(s3, landing_bucket, inputs, existing_archive)

    references = scan_control_references(s3, lakehouse_bucket, inputs)
    if references:
        raise RuntimeError(
            "NBP Landing deletion blocked by live R2 control references: "
            + ", ".join(references[:10])
        )

    if not existing_receipts:
        receipt_key = f"{LINEAGE_PREFIX}{digest}.json"
        receipt_uri = f"s3://{lakehouse_bucket}/{receipt_key}"
        receipt = {
            "format_version": 1,
            "kind": "landing_bronze_lineage_receipt",
            "source_id": SOURCE,
            "proof": "bronze_response_sha256_equals_current_landing_sha256_set",
            "input_set_sha256": digest,
            "landing_inputs": inputs,
            "bronze_proofs": [proofs[".".join(identifier)] for identifier in TABLES],
            "archive": archives,
            "archive_readback_verified": True,
            "landing_delete_eligible": True,
            "blocking_control_references": [],
            "landing_deleted": False,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        immutable_json(s3, lakehouse_bucket, receipt_key, receipt)
    else:
        receipt_uri = f"s3://{lakehouse_bucket}/{receipt_key}"
        if receipt.get("input_set_sha256") != digest:
            raise RuntimeError("existing NBP lineage receipt digest mismatch")
        if receipt.get("archive_readback_verified") is not True:
            raise RuntimeError("existing NBP lineage receipt lacks archive acceptance")

    attach_receipt(cat, receipt_uri, digest, proofs)
    for identifier in TABLES:
        table = cat.load_table(identifier)
        if table.properties.get("zohelo.lineage.receipt") != receipt_uri:
            raise RuntimeError(f"NBP lineage property did not commit for {'.'.join(identifier)}")
        if table.properties.get("zohelo.lineage.input-set-sha256") != digest:
            raise RuntimeError(f"NBP lineage digest did not commit for {'.'.join(identifier)}")

    archive_by_source = {item["source_key"]: item for item in archives}
    for item in inputs:
        archive = archive_by_source[item["key"]]
        observed_sha, observed_size = hash_object(s3, landing_bucket, archive["archive_key"])
        if observed_sha != item["sha256"] or observed_size != item["size"]:
            raise RuntimeError(f"NBP Archive revalidation failed immediately before delete: {item['key']}")

    remaining = []
    for item in inputs:
        if object_exists(s3, landing_bucket, item["key"]):
            remaining.append(item["key"])
    if remaining:
        for offset in range(0, len(remaining), 1000):
            batch = remaining[offset:offset + 1000]
            response = s3.delete_objects(
                Bucket=landing_bucket,
                Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
            )
            errors = response.get("Errors", [])
            if errors:
                raise RuntimeError(f"NBP Landing delete returned errors: {errors[:3]}")

    still_present = [
        item["key"] for item in inputs
        if object_exists(s3, landing_bucket, item["key"])
    ]
    if still_present:
        raise RuntimeError(f"NBP Landing deletion incomplete: {len(still_present)} objects remain")

    lifecycle_key = f"{LIFECYCLE_PREFIX}{digest}.json"
    lifecycle = {
        "format_version": 1,
        "kind": "landing_archive_finalization_receipt",
        "source_id": SOURCE,
        "input_set_sha256": digest,
        "lineage_receipt": receipt_uri,
        "bronze_tables": [".".join(identifier) for identifier in TABLES],
        "deleted_landing_objects": len(inputs),
        "deleted_landing_bytes": sum(int(item["size"]) for item in inputs),
        "archive_objects_verified": len(archives),
        "new_archive_objects_copied": copied,
        "landing_deleted": True,
        "archive_verified_before_delete": True,
        "blocking_control_references": [],
        "google_drive_accessed": False,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    immutable_json(s3, lakehouse_bucket, lifecycle_key, lifecycle)

    print(json.dumps({
        "result": "pass",
        "operation": "finalize-nbp-r2",
        "source": SOURCE,
        "input_set_sha256": digest,
        "bronze_tables_proven": len(TABLES),
        "bronze_distinct_raw_sha256": len(bronze_shas),
        "archive_objects_verified": len(archives),
        "new_archive_objects_copied": copied,
        "deleted_landing_objects": len(inputs),
        "deleted_landing_bytes": sum(int(item["size"]) for item in inputs),
        "blocking_control_references": [],
        "lineage_receipt": receipt_uri,
        "lifecycle_receipt": f"s3://{lakehouse_bucket}/{lifecycle_key}",
        "google_drive_accessed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
