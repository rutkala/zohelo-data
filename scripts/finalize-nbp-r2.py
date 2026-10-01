#!/usr/bin/env python3
"""Finalize NBP R2 Landing after exact Bronze provenance proof.

This retry is optimized for object storage:
- current NBP Landing object names are SHA-256 content addresses already proven by the
  successful read-only inventory run;
- every current object is copied to a canonical content-addressed Archive path;
- Archive SHA-256 readback is performed in parallel before deletion;
- the four Bronze tables must expose exactly the same response_sha256 set;
- only the active R2 source-campaign namespace is considered a live deletion blocker.
  The legacy 06_control/nbp tree is historical Drive-era metadata under the current
  R2-only architecture and is not an active writer.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import duckdb
from pyiceberg.catalog.rest import RestCatalog

SOURCE = "nbp"
SOURCE_IDS = (
    "nbp_exchange_rates_table_a",
    "nbp_exchange_rates_table_b",
    "nbp_exchange_rates_table_c",
    "nbp_gold_prices",
)
TABLES = tuple(("bronze", source_id) for source_id in SOURCE_IDS)
LINEAGE_PREFIX = "06_control/lineage/nbp/"
LIFECYCLE_PREFIX = "06_control/lifecycle/nbp/"
HASH_NAME = re.compile(r"^([0-9a-f]{64})\.json$")


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


def list_objects(s3, bucket: str, prefix: str) -> list[dict[str, Any]]:
    rows = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            size = int(item.get("Size", 0))
            if key.endswith("/") and size == 0:
                continue
            rows.append({"key": key, "size": size})
    rows.sort(key=lambda item: item["key"])
    return rows


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


def hash_object(s3, bucket: str, key: str) -> tuple[str, int]:
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
    return digest.hexdigest(), size


def current_landing(s3, bucket: str) -> list[dict[str, Any]]:
    rows = []
    for source_id in SOURCE_IDS:
        prefix = f"01_landing/{source_id}/"
        for item in list_objects(s3, bucket, prefix):
            name = Path(item["key"]).name.lower()
            match = HASH_NAME.fullmatch(name)
            if not match or item["size"] <= 0:
                raise RuntimeError(f"unexpected NBP Landing object: {item['key']}")
            rows.append({
                "key": item["key"],
                "source_id": source_id,
                "size": item["size"],
                "sha256": match.group(1),
            })
    rows.sort(key=lambda item: item["key"])
    return rows


def input_set_digest(inputs: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        [
            {
                "key": item["key"],
                "source_id": item["source_id"],
                "size": item["size"],
                "sha256": item["sha256"],
            }
            for item in inputs
        ],
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return sha256(payload).hexdigest()


def download_table_files(s3, bucket: str, table, directory: Path) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for index, task in enumerate(table.scan().plan_files()):
        uri = str(task.file.file_path)
        prefix = f"s3://{bucket}/"
        if not uri.startswith(prefix):
            raise RuntimeError(f"unexpected Iceberg file URI: {uri}")
        target = directory / f"{index:06d}.parquet"
        s3.download_file(bucket, uri[len(prefix):], str(target))
        if target.stat().st_size != int(task.file.file_size_in_bytes):
            raise RuntimeError(f"Iceberg file size mismatch: {uri}")
        paths.append(target)
    if not paths:
        raise RuntimeError("NBP Bronze table has no data files")
    return paths


def bronze_provenance(s3, cat, bucket: str, root: Path) -> tuple[dict[str, Any], set[str]]:
    proofs = {}
    all_shas: set[str] = set()
    for identifier in TABLES:
        table = cat.load_table(identifier)
        fields = {field.name for field in table.schema().fields}
        required = {"source_id", "response_sha256", "raw_file_id"}
        if not required <= fields:
            raise RuntimeError(f"{'.'.join(identifier)} lacks {sorted(required-fields)}")
        paths = download_table_files(s3, bucket, table, root / identifier[-1])
        glob = str(paths[0].parent / "*.parquet").replace("'", "''")
        con = duckdb.connect()
        try:
            rows = int(con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0])
            values = con.execute(
                f"SELECT DISTINCT source_id, response_sha256 FROM read_parquet('{glob}')"
            ).fetchall()
        finally:
            con.close()
        expected = identifier[-1]
        if any(row[0] != expected for row in values):
            raise RuntimeError(f"{'.'.join(identifier)} contains another source_id")
        shas = {row[1] for row in values}
        if any(not isinstance(value, str) or len(value) != 64 for value in shas):
            raise RuntimeError(f"{'.'.join(identifier)} has invalid response_sha256")
        snapshot = table.current_snapshot()
        proofs[".".join(identifier)] = {
            "table": ".".join(identifier),
            "snapshot_id": str(snapshot.snapshot_id) if snapshot is not None else None,
            "rows": rows,
            "distinct_response_sha256": len(shas),
        }
        all_shas.update(shas)
    return proofs, all_shas


def canonical_archive_key(item: dict[str, Any]) -> str:
    return f"05_archive/nbp/{item['source_id']}/{item['sha256']}.json"


def existing_lineage(s3, bucket: str) -> tuple[str, dict[str, Any]] | None:
    rows = [item for item in list_objects(s3, bucket, LINEAGE_PREFIX) if item["key"].endswith(".json")]
    if not rows:
        return None
    if len(rows) != 1:
        raise RuntimeError(f"expected at most one NBP lineage receipt, found {len(rows)}")
    key = rows[0]["key"]
    value = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    if (
        not isinstance(value, dict)
        or value.get("format_version") != 1
        or value.get("kind") != "landing_bronze_lineage_receipt"
        or value.get("source_id") != SOURCE
    ):
        raise RuntimeError("existing NBP lineage receipt is invalid")
    return key, value


def immutable_json(s3, bucket: str, key: str, value: dict[str, Any]) -> None:
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
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


def source_for_missing_landing(receipt: dict[str, Any], key: str) -> str | None:
    archive = receipt.get("archive")
    if not isinstance(archive, list):
        return None
    for item in archive:
        if isinstance(item, dict) and item.get("source_key") == key:
            candidate = item.get("archive_key")
            if isinstance(candidate, str):
                return candidate
    return None


def ensure_canonical_archives(
    s3,
    bucket: str,
    inputs: list[dict[str, Any]],
    prior_receipt: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], int]:
    copied_flags: list[int] = []

    def one(item: dict[str, Any]) -> dict[str, Any]:
        target = canonical_archive_key(item)
        copied = 0
        if not object_exists(s3, bucket, target):
            if object_exists(s3, bucket, item["key"]):
                source_key = item["key"]
            else:
                source_key = source_for_missing_landing(prior_receipt or {}, item["key"])
                if not source_key or not object_exists(s3, bucket, source_key):
                    raise RuntimeError(f"no recoverable NBP raw source for {item['key']}")
            s3.copy_object(
                Bucket=bucket,
                Key=target,
                CopySource={"Bucket": bucket, "Key": source_key},
            )
            copied = 1
        observed_sha, observed_size = hash_object(s3, bucket, target)
        if observed_sha != item["sha256"] or observed_size != item["size"]:
            raise RuntimeError(f"canonical NBP Archive readback mismatch: {item['key']}")
        copied_flags.append(copied)
        return {
            "source_key": item["key"],
            "source_id": item["source_id"],
            "archive_key": target,
            "size": item["size"],
            "sha256": item["sha256"],
            "compression": "none_exact_json",
        }

    with ThreadPoolExecutor(max_workers=16) as pool:
        archives = list(pool.map(one, inputs))
    archives.sort(key=lambda item: item["source_key"])
    return archives, sum(copied_flags)


def active_r2_nbp_control_objects(s3, lakehouse_bucket: str) -> list[str]:
    prefix = "06_control/source_campaigns/nbp/"
    return list_objects(s3, lakehouse_bucket, prefix)


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
    s3 = client()
    cat = catalog()
    landing_bucket = req("R2_LANDING_BUCKET")
    lakehouse_bucket = req("R2_LAKEHOUSE_BUCKET")

    prior = existing_lineage(s3, lakehouse_bucket)
    landing = current_landing(s3, landing_bucket)

    if prior is None:
        if len(landing) != 404:
            raise RuntimeError(f"expected 404 NBP Landing objects, observed {len(landing)}")
        inputs = landing
        digest = input_set_digest(inputs)
        prior_receipt = None
    else:
        _, prior_receipt = prior
        raw_inputs = prior_receipt.get("landing_inputs")
        if not isinstance(raw_inputs, list) or len(raw_inputs) != 404:
            raise RuntimeError("existing NBP lineage receipt input set is invalid")
        inputs = sorted(raw_inputs, key=lambda item: item["key"])
        digest = str(prior_receipt.get("input_set_sha256", ""))
        current_by_key = {item["key"]: item for item in landing}
        for item in inputs:
            current = current_by_key.get(item["key"])
            if current is not None and (
                current["sha256"] != item.get("sha256")
                or current["size"] != item.get("size")
            ):
                raise RuntimeError(f"NBP Landing changed after lineage proof: {item['key']}")

    with tempfile.TemporaryDirectory(prefix="zohelo-nbp-finalize-") as directory:
        proofs, bronze_shas = bronze_provenance(
            s3, cat, lakehouse_bucket, Path(directory)
        )
    input_shas = {item["sha256"] for item in inputs}
    if len(input_shas) != 404 or input_shas != bronze_shas:
        raise RuntimeError(
            "NBP Landing/Bronze provenance mismatch: "
            f"input={len(input_shas)} bronze={len(bronze_shas)} "
            f"input_only={len(input_shas-bronze_shas)} "
            f"bronze_only={len(bronze_shas-input_shas)}"
        )

    blockers = active_r2_nbp_control_objects(s3, lakehouse_bucket)
    if blockers:
        raise RuntimeError(
            "active R2 NBP source-campaign control exists; Landing cleanup is blocked: "
            + ", ".join(item["key"] for item in blockers[:8])
        )

    archives, copied = ensure_canonical_archives(
        s3, landing_bucket, inputs, prior_receipt
    )

    if prior is None:
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
            "legacy_control_scope": "06_control/nbp is historical Drive-era metadata, not a live R2 writer",
            "landing_deleted": False,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        immutable_json(s3, lakehouse_bucket, receipt_key, receipt)
    else:
        receipt_key, prior_receipt = prior
        receipt_uri = f"s3://{lakehouse_bucket}/{receipt_key}"
        if prior_receipt.get("input_set_sha256") != digest:
            raise RuntimeError("existing NBP lineage receipt digest mismatch")

    attach_receipt(cat, receipt_uri, digest, proofs)
    for identifier in TABLES:
        table = cat.load_table(identifier)
        if table.properties.get("zohelo.lineage.receipt") != receipt_uri:
            raise RuntimeError(f"NBP lineage property missing: {'.'.join(identifier)}")

    existing_keys = {item["key"] for item in landing}
    delete_keys = [item["key"] for item in inputs if item["key"] in existing_keys]
    for offset in range(0, len(delete_keys), 1000):
        response = s3.delete_objects(
            Bucket=landing_bucket,
            Delete={"Objects": [{"Key": key} for key in delete_keys[offset:offset+1000]], "Quiet": True},
        )
        if response.get("Errors"):
            raise RuntimeError(f"NBP Landing delete errors: {response['Errors'][:3]}")

    remaining = current_landing(s3, landing_bucket)
    if remaining:
        raise RuntimeError(f"NBP Landing deletion incomplete: {len(remaining)} objects remain")

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
        "canonical_archive_objects_verified": len(archives),
        "canonical_archive_objects_copied_this_run": copied,
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
        "canonical_archive_objects_verified": len(archives),
        "canonical_archive_objects_copied_this_run": copied,
        "deleted_landing_objects": len(inputs),
        "deleted_landing_bytes": sum(int(item["size"]) for item in inputs),
        "lineage_receipt": receipt_uri,
        "lifecycle_receipt": f"s3://{lakehouse_bucket}/{lifecycle_key}",
        "google_drive_accessed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
