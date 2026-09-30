#!/usr/bin/env python3
"""Read legacy R2 navigation/release metadata needed for one-time Iceberg consolidation."""

from __future__ import annotations

import json
import os
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def get_json(client: Any, bucket: str, key: str) -> Any | None:
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
        if status == 404 or code in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise
    return json.loads(body)


def main() -> int:
    client = boto3.client(
        "s3",
        endpoint_url=required("R2_S3_ENDPOINT").rstrip("/"),
        region_name="auto",
        aws_access_key_id=required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )
    bucket = required("R2_LAKEHOUSE_BUCKET")
    result: dict[str, Any] = {"navigation": {}, "release_pointers": {}, "direct_bronze": {}}

    for layer in ("02_bronze", "03_silver", "04_gold"):
        for source in ("bdl", "eurostat", "nbp", "wdi"):
            key = f"{layer}/current/{source}/navigation-index.json"
            payload = get_json(client, bucket, key)
            if payload is not None:
                result["navigation"][key] = payload

    for source in ("bdl", "eurostat", "nbp", "wdi"):
        key = f"releases/{source}/current-release.json"
        payload = get_json(client, bucket, key)
        if payload is not None:
            result["release_pointers"][key] = payload

    groups: dict[str, list[dict[str, Any]]] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="02_bronze/"):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            if "/current/" in key or not key.lower().endswith(".parquet"):
                continue
            parts = key.split("/")
            if len(parts) < 3:
                continue
            # Parent directory is the safest first grouping signal. Keep samples only.
            parent = "/".join(parts[:-1])
            rows = groups.setdefault(parent, [])
            if len(rows) < 20:
                rows.append({"key": key, "bytes": int(item.get("Size", 0))})
    result["direct_bronze"] = groups

    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
