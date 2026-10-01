#!/usr/bin/env python3
"""Finalize Landing cleanup for sources with verified immutable lineage receipts.

Only explicitly listed sources are eligible. For each source this script:
- loads exactly one lineage receipt from 06_control/lineage/<source>/;
- requires archive readback and landing-delete eligibility from that receipt;
- re-hashes every current Landing input and archived copy;
- proves each Bronze table still points at the lineage receipt in table properties;
- deletes only the exact Landing keys recorded in the receipt;
- writes a separate immutable lifecycle finalization receipt.

The original lineage receipt and Archive objects remain immutable. This finalizer is intentionally source-scoped.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pyiceberg.catalog.rest import RestCatalog



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


def lineage_receipt(s3, lakehouse_bucket: str, source: str) -> tuple[str, dict[str, Any]]:
    prefix = f"06_control/lineage/{source}/"
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=lakehouse_bucket, Prefix=prefix
    ):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            if key.endswith(".json") and int(item.get("Size", 0)) > 0:
                keys.append(key)
    if len(keys) != 1:
        raise RuntimeError(
            f"expected exactly one immutable lineage receipt for {source}, found {len(keys)}"
        )
    key = keys[0]
    raw = s3.get_object(Bucket=lakehouse_bucket, Key=key)["Body"].read()
    value = json.loads(raw)
    if (
        not isinstance(value, dict)
        or value.get("format_version") != 1
        or value.get("kind") != "landing_bronze_lineage_receipt"
        or value.get("source_id") != source
    ):
        raise RuntimeError(f"invalid lineage receipt for {source}")
    return key, value


def verify_receipt(
    s3,
    cat,
    landing_bucket: str,
    lakehouse_bucket: str,
    source: str,
    receipt_key: str,
    receipt: dict[str, Any],
) -> dict[str, Any]:
    if receipt.get("archive_readback_verified") is not True:
        raise RuntimeError(f"archive was not accepted for {source}")
    if receipt.get("landing_delete_eligible") is not True:
        raise RuntimeError(f"Landing deletion is not eligible for {source}")
    if receipt.get("blocking_control_references") not in ([], None):
        raise RuntimeError(f"blocking control references remain for {source}")

    receipt_uri = f"s3://{lakehouse_bucket}/{receipt_key}"
    inputs = receipt.get("landing_inputs")
    archived = receipt.get("archive")
    proofs = receipt.get("bronze_proofs")
    if (
        not isinstance(inputs, list)
        or not inputs
        or not isinstance(archived, list)
        or len(archived) != len(inputs)
        or not isinstance(proofs, list)
        or not proofs
    ):
        raise RuntimeError(f"incomplete lineage receipt for {source}")

    archive_by_source = {
        item.get("source_key"): item for item in archived if isinstance(item, dict)
    }
    verified_inputs = []
    for item in inputs:
        if not isinstance(item, dict):
            raise RuntimeError(f"invalid Landing descriptor for {source}")
        key = item.get("key")
        expected_sha = item.get("sha256")
        expected_size = item.get("size")
        if not isinstance(key, str) or not isinstance(expected_sha, str) or type(expected_size) is not int:
            raise RuntimeError(f"invalid Landing identity for {source}")
        if not object_exists(s3, landing_bucket, key):
            # Idempotent completion is allowed only when a lifecycle receipt already exists.
            raise RuntimeError(f"Landing object is already absent before finalization: {key}")
        observed_sha, observed_size = hash_object(s3, landing_bucket, key)
        if observed_sha != expected_sha or observed_size != expected_size:
            raise RuntimeError(f"Landing readback mismatch: {key}")

        archive = archive_by_source.get(key)
        if not isinstance(archive, dict):
            raise RuntimeError(f"Archive mapping missing for {key}")
        archive_key = archive.get("archive_key")
        if not isinstance(archive_key, str):
            raise RuntimeError(f"Archive key missing for {key}")
        archive_sha, archive_size = hash_object(s3, landing_bucket, archive_key)
        if archive_sha != expected_sha or archive_size != expected_size:
            raise RuntimeError(f"Archive no longer reproduces Landing object: {key}")
        verified_inputs.append({
            "landing_key": key,
            "archive_key": archive_key,
            "sha256": expected_sha,
            "size": expected_size,
        })

    verified_tables = []
    for proof in proofs:
        if not isinstance(proof, dict) or not isinstance(proof.get("table"), str):
            raise RuntimeError(f"invalid Bronze proof for {source}")
        parts = proof["table"].split(".", 1)
        if len(parts) != 2 or parts[0] != "bronze":
            raise RuntimeError(f"unexpected lineage table for {source}: {proof['table']}")
        table = cat.load_table(tuple(parts))
        if table.properties.get("zohelo.lineage.receipt") != receipt_uri:
            raise RuntimeError(f"Bronze table lost lineage receipt: {proof['table']}")
        if table.properties.get("zohelo.lineage.source") != source:
            raise RuntimeError(f"Bronze table lineage source mismatch: {proof['table']}")
        if (
            table.properties.get("zohelo.lineage.input-set-sha256")
            != receipt.get("input_set_sha256")
        ):
            raise RuntimeError(f"Bronze input-set lineage mismatch: {proof['table']}")
        verified_tables.append(proof["table"])

    return {
        "source": source,
        "lineage_receipt": receipt_uri,
        "input_set_sha256": receipt.get("input_set_sha256"),
        "inputs": verified_inputs,
        "bronze_tables": verified_tables,
    }


def lifecycle_receipt_key(source: str, digest: str) -> str:
    return f"06_control/lifecycle/{source}/{digest}.json"


def put_immutable_json(s3, bucket: str, key: str, value: dict[str, Any]) -> None:
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if object_exists(s3, bucket, key):
        existing = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if existing != raw:
            raise RuntimeError(f"immutable lifecycle receipt conflict: {key}")
        return
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=raw,
        ContentType="application/json",
        Metadata={"sha256": sha256(raw).hexdigest()},
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete verified Landing inputs only after immutable lineage and Archive readback."
    )
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        help="Source with exactly one eligible lineage receipt; repeat to finalize more than one.",
    )
    args = parser.parse_args()
    sources = tuple(dict.fromkeys(args.source))
    if any(not source or "/" in source or source.startswith(".") for source in sources):
        raise RuntimeError("invalid source selector")

    s3 = client()
    cat = catalog()
    landing_bucket = req("R2_LANDING_BUCKET")
    lakehouse_bucket = req("R2_LAKEHOUSE_BUCKET")

    verified = []
    for source in sources:
        key, receipt = lineage_receipt(s3, lakehouse_bucket, source)
        verified.append(
            verify_receipt(
                s3,
                cat,
                landing_bucket,
                lakehouse_bucket,
                source,
                key,
                receipt,
            )
        )

    # Destructive phase begins only after every source passed the full revalidation.
    deleted = []
    for source_result in verified:
        source = source_result["source"]
        for item in source_result["inputs"]:
            key = item["landing_key"]
            s3.delete_object(Bucket=landing_bucket, Key=key)
            if object_exists(s3, landing_bucket, key):
                raise RuntimeError(f"Landing deletion did not complete: {key}")
            deleted.append({"source": source, **item})

        digest = str(source_result["input_set_sha256"])
        final = {
            "format_version": 1,
            "kind": "landing_archive_finalization_receipt",
            "source_id": source,
            "input_set_sha256": digest,
            "lineage_receipt": source_result["lineage_receipt"],
            "bronze_tables": source_result["bronze_tables"],
            "deleted_landing_objects": [
                item for item in deleted if item["source"] == source
            ],
            "archive_objects_preserved": [
                item["archive_key"]
                for item in source_result["inputs"]
            ],
            "landing_deleted": True,
            "archive_verified_before_delete": True,
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        put_immutable_json(
            s3,
            lakehouse_bucket,
            lifecycle_receipt_key(source, digest),
            final,
        )

    print(json.dumps({
        "result": "pass",
        "operation": "finalize-verified-landing-archive",
        "sources": list(sources),
        "deleted_landing_objects": len(deleted),
        "deleted_landing_bytes": sum(int(item["size"]) for item in deleted),
        "archives_preserved": [item["archive_key"] for item in deleted],
        "google_drive_accessed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
