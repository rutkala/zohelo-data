#!/usr/bin/env python3
"""Read-only inventory of Zohelo R2 storage for Iceberg cutover planning."""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import PurePosixPath
import json
import os
from typing import Any

import boto3
from botocore.config import Config


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def suffix(key: str) -> str:
    name = PurePosixPath(key).name.lower()
    if "." not in name:
        return "<none>"
    if name.endswith(".json.gz"):
        return ".json.gz"
    if name.endswith(".csv.gz"):
        return ".csv.gz"
    return "." + name.rsplit(".", 1)[-1]


def size_band(size: int) -> str:
    mib = 1024 * 1024
    if size < mib:
        return "<1MiB"
    if size < 16 * mib:
        return "1-16MiB"
    if size < 64 * mib:
        return "16-64MiB"
    if size < 256 * mib:
        return "64-256MiB"
    return ">=256MiB"


def main() -> int:
    endpoint = required("R2_S3_ENDPOINT")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint.rstrip("/"),
        region_name="auto",
        aws_access_key_id=required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )

    buckets = [required("R2_LANDING_BUCKET"), required("R2_LAKEHOUSE_BUCKET")]
    result: dict[str, Any] = {"format_version": 1, "buckets": {}}
    publication_markers: list[dict[str, Any]] = []

    for bucket in buckets:
        total_objects = 0
        total_bytes = 0
        top: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        top_ext: dict[str, Counter[str]] = defaultdict(Counter)
        top_ext_bytes: dict[str, Counter[str]] = defaultdict(Counter)
        second: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        parquet_bands: dict[str, Counter[str]] = defaultdict(Counter)
        parquet_parents: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        data_extensions: Counter[str] = Counter()
        data_extension_bytes: Counter[str] = Counter()

        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                key = str(obj["Key"])
                size = int(obj.get("Size", 0))
                if key.endswith("/") and size == 0:
                    continue
                total_objects += 1
                total_bytes += size
                parts = [p for p in key.split("/") if p]
                first = parts[0] if parts else "<root>"
                second_key = "/".join(parts[:2]) if len(parts) >= 2 else first
                ext = suffix(key)

                top[first][0] += 1
                top[first][1] += size
                second[second_key][0] += 1
                second[second_key][1] += size
                top_ext[first][ext] += 1
                top_ext_bytes[first][ext] += size

                lowered_segments = {p.lower() for p in parts}
                if lowered_segments.intersection({"current", "release", "releases"}):
                    publication_markers.append({"bucket": bucket, "key": key, "bytes": size})

                if first in {"02_bronze", "03_silver", "04_gold"}:
                    data_extensions[ext] += 1
                    data_extension_bytes[ext] += size
                    if ext == ".parquet":
                        parquet_bands[first][size_band(size)] += 1
                        parent = str(PurePosixPath(key).parent)
                        parquet_parents[parent][0] += 1
                        parquet_parents[parent][1] += size

        result["buckets"][bucket] = {
            "objects": total_objects,
            "bytes": total_bytes,
            "top_level": {
                k: {"objects": v[0], "bytes": v[1]} for k, v in sorted(top.items())
            },
            "top_level_extensions": {
                k: {
                    ext: {"objects": count, "bytes": int(top_ext_bytes[k][ext])}
                    for ext, count in counts.most_common()
                }
                for k, counts in sorted(top_ext.items())
            },
            "second_level": {
                k: {"objects": v[0], "bytes": v[1]}
                for k, v in sorted(second.items(), key=lambda item: (-item[1][1], item[0]))[:200]
            },
            "medallion_extensions": {
                ext: {"objects": count, "bytes": int(data_extension_bytes[ext])}
                for ext, count in data_extensions.most_common()
            },
            "parquet_size_bands": {
                layer: dict(counts) for layer, counts in sorted(parquet_bands.items())
            },
            "largest_parquet_groups": [
                {"path": path, "objects": stats[0], "bytes": stats[1]}
                for path, stats in sorted(
                    parquet_parents.items(), key=lambda item: (-item[1][1], item[0])
                )[:200]
            ],
        }

    result["publication_markers"] = {
        "count": len(publication_markers),
        "bytes": sum(x["bytes"] for x in publication_markers),
        "sample": publication_markers[:200],
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
