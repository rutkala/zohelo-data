#!/usr/bin/env python3
"""Live-source smoke for the shared R2 campaign ingestion backend."""
from __future__ import annotations

import hashlib
import json
import os
import urllib.request

import boto3
from botocore.config import Config

from ingestion.source_campaign_store import R2CampaignStore


def req(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing {name}")
    return value


def cleanup_prefix(s3, bucket: str, prefix: str) -> int:
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    batch = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            batch.append({"Key": item["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
                deleted += len(batch)
                batch = []
    if batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
        deleted += len(batch)
    return deleted


def main() -> int:
    source_id = "r2_smoke"
    url = "https://api.nbp.pl/api/exchangerates/tables/A/?format=json"
    with urllib.request.urlopen(url, timeout=30) as response:
        body = response.read()
    payload = json.loads(body)
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("NBP smoke response is empty or invalid")
    digest = hashlib.sha256(body).hexdigest()

    store = R2CampaignStore(source_id)
    if store.load() is not None:
        raise RuntimeError("smoke namespace was not clean at start")

    descriptor = store.put_raw(
        body,
        {
            "source_url": url,
            "http_status": 200,
            "purpose": "r2_campaign_backend_smoke",
        },
    )
    restored = store.read_raw(descriptor)
    if restored != body or hashlib.sha256(restored).hexdigest() != digest:
        raise RuntimeError("R2 campaign raw readback mismatch")

    state = {
        "source_id": source_id,
        "accepted_responses": 1,
        "pending": [],
        "completed": {},
        "recent_roots": {},
        "receipts": [],
        "rejected_receipts": [],
    }
    store.save(state)
    loaded = store.load()
    if loaded != state:
        raise RuntimeError("R2 campaign state round-trip mismatch")

    s3 = boto3.client(
        "s3",
        endpoint_url=req("R2_S3_ENDPOINT"),
        region_name="auto",
        aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts": 8, "mode": "standard"}),
    )
    landing_deleted = cleanup_prefix(
        s3, req("R2_LANDING_BUCKET"), f"01_landing/{source_id}/"
    )
    control_deleted = cleanup_prefix(
        s3, req("R2_LAKEHOUSE_BUCKET"), f"06_control/source_campaigns/{source_id}/"
    )

    print(
        json.dumps(
            {
                "result": "pass",
                "backend": "r2",
                "live_source": "NBP table A",
                "raw_bytes": len(body),
                "sha256": digest,
                "landing_objects_cleaned": landing_deleted,
                "control_objects_cleaned": control_deleted,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
