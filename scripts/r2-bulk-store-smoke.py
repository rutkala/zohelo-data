#!/usr/bin/env python3
"""Smoke-test the R2 backend used by full-distribution ingestion."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile

import boto3
from botocore.config import Config

from ingestion.bulk_transport import BulkR2RawStore
from ingestion.source_campaign_store import R2CampaignStore


def req(name: str) -> str:
    value=os.environ.get(name,"").strip()
    if not value: raise RuntimeError(f"missing {name}")
    return value


def cleanup_prefix(client, bucket: str, prefix: str) -> int:
    deleted=0
    batch=[]
    paginator=client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket,Prefix=prefix):
        for item in page.get("Contents",[]):
            batch.append({"Key":item["Key"]})
            if len(batch)==1000:
                client.delete_objects(Bucket=bucket,Delete={"Objects":batch,"Quiet":True})
                deleted+=len(batch); batch=[]
    if batch:
        client.delete_objects(Bucket=bucket,Delete={"Objects":batch,"Quiet":True})
        deleted+=len(batch)
    return deleted


def main() -> int:
    source="r2_smoke_bulk"
    campaign=source+"_bulk"
    payload=(
        b"zohelo-r2-bulk-smoke\n"
        + bytes(range(256))*4096
        + b"\ncomplete\n"
    )
    sha=hashlib.sha256(payload).hexdigest()

    raw=BulkR2RawStore(source)
    state=R2CampaignStore(campaign,publication_only=True)
    if state.load() is not None:
        raise RuntimeError("bulk smoke campaign namespace was not clean at start")

    with tempfile.TemporaryDirectory(prefix="zohelo-r2-bulk-smoke-") as directory:
        src=Path(directory)/"source.bin"
        dst=Path(directory)/"restored.bin"
        src.write_bytes(payload)
        descriptor=raw.put_file(src,{"purpose":"r2_bulk_backend_smoke"})
        raw.verify(descriptor)
        raw.read_to_file(descriptor,dst)
        restored=dst.read_bytes()
        if restored!=payload or hashlib.sha256(restored).hexdigest()!=sha:
            raise RuntimeError("bulk R2 readback mismatch")

    state_value={
        "source_id":campaign,
        "accepted_responses":0,
        "pending":[],
        "completed":{},
        "recent_roots":{},
        "receipts":[],
        "rejected_receipts":[],
    }
    state.save(state_value)
    if state.load()!=state_value:
        raise RuntimeError("bulk R2 campaign state round-trip mismatch")

    client=boto3.client(
        "s3",endpoint_url=req("R2_S3_ENDPOINT"),region_name="auto",
        aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4",retries={"max_attempts":8,"mode":"standard"}),
    )
    landing_deleted=cleanup_prefix(
        client,req("R2_LANDING_BUCKET"),f"01_landing/{source}/"
    )
    control_deleted=cleanup_prefix(
        client,req("R2_LAKEHOUSE_BUCKET"),f"06_control/source_campaigns/{campaign}/"
    )
    print(json.dumps({
        "result":"pass",
        "backend":"r2",
        "mode":"full_distribution_bulk",
        "bytes":len(payload),
        "sha256":sha,
        "landing_objects_cleaned":landing_deleted,
        "control_objects_cleaned":control_deleted,
    },sort_keys=True))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
