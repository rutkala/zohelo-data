#!/usr/bin/env python3
"""Advance retained Eurostat full-distribution receipts into the R2 Iceberg Bronze table.

This is an R2-only, bounded reconciliation worker. It never calls Eurostat and never
removes Landing. Historical receipt/raw Drive IDs are resolved through the verified
Drive->R2 migration index. Iceberg raw_sha256 membership is the idempotent checkpoint.
"""

from __future__ import annotations

import argparse
from hashlib import md5, sha256
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import TableProperties
from pyiceberg.typedef import Record
import requests

from eurostat_bulk_decode import decode_full_distribution
from ingestion.full_source_campaign import task_for
from ingestion.source_campaign_store import R2CampaignStore, _R2ObjectStore

SOURCE_ID = "eurostat_bulk"
TABLE_ID = ("bronze", "eurostat_full_observations")
OUTPUT_PREFIX = "02_bronze/eurostat_full_observations/data/retained/"
DEFAULT_MAX_DISTRIBUTIONS = 64
SHA_RE = __import__("re").compile(r"^[0-9a-f]{64}$")


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


def sql_rows(query: str) -> list[dict[str, Any]]:
    url = (
        "https://api.sql.cloudflarestorage.com/api/v1/accounts/"
        f"{req('CLOUDFLARE_ACCOUNT_ID')}/r2-sql/query/{req('R2_LAKEHOUSE_BUCKET')}"
    )
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {req('R2_DATA_CATALOG_TOKEN')}",
            "Content-Type": "application/json",
        },
        json={"query": query},
        timeout=180,
    )
    if response.status_code != 200:
        raise RuntimeError(f"R2 SQL {response.status_code}: {response.text[:500]}")
    payload = response.json()
    if payload.get("success") is False:
        raise RuntimeError(f"R2 SQL failed: {json.dumps(payload)[:800]}")
    candidates: list[Any] = []
    if isinstance(payload.get("result"), list):
        candidates.extend(payload["result"])
    elif isinstance(payload.get("result"), dict):
        candidates.append(payload["result"])
    candidates.append(payload)
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        value = candidate.get("results")
        if isinstance(value, list) and (not value or isinstance(value[0], dict)):
            return value
        if isinstance(value, dict):
            columns, rows = value.get("columns"), value.get("rows")
            if isinstance(columns, list) and isinstance(rows, list):
                return [dict(zip(columns, row)) for row in rows]
        rows = candidate.get("rows")
        if isinstance(rows, list) and (not rows or isinstance(rows[0], dict)):
            return rows
    raise RuntimeError(f"unsupported R2 SQL response: {json.dumps(payload)[:1000]}")


def existing_raw_hashes() -> set[str]:
    values = set()
    for row in sql_rows(
        "SELECT DISTINCT raw_sha256 FROM bronze.eurostat_full_observations "
        "WHERE raw_sha256 IS NOT NULL;"
    ):
        value = row.get("raw_sha256")
        if not isinstance(value, str) or SHA_RE.fullmatch(value) is None:
            raise RuntimeError(f"invalid Iceberg raw_sha256 value: {value!r}")
        values.add(value)
    return values


def canonical_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def is_data_receipt(receipt: Any) -> bool:
    return (
        isinstance(receipt, dict)
        and receipt.get("schema_version") == 1
        and receipt.get("source_id") == "eurostat"
        and receipt.get("accepted") is True
        and receipt.get("kind") == "full_distribution"
        and isinstance(receipt.get("distribution"), dict)
        and receipt["distribution"].get("kind") == "eurostat_tsv_gzip"
        and isinstance(receipt.get("raw"), dict)
    )


def stream_legacy_raw(
    client,
    resolver: _R2ObjectStore,
    descriptor: dict[str, Any],
    destination: Path,
) -> None:
    object_id = descriptor.get("id")
    expected_sha = descriptor.get("sha256")
    expected_md5 = descriptor.get("md5")
    expected_size = descriptor.get("size_bytes")
    if (
        not isinstance(object_id, str)
        or not isinstance(expected_sha, str)
        or SHA_RE.fullmatch(expected_sha) is None
        or not isinstance(expected_md5, str)
        or len(expected_md5) != 32
        or type(expected_size) is not int
        or expected_size <= 0
    ):
        raise RuntimeError("Eurostat raw descriptor is invalid")
    bucket, key = resolver._parse(object_id)
    if bucket != req("R2_LANDING_BUCKET"):
        raise RuntimeError(f"Eurostat raw object resolved outside Landing: {bucket}/{key}")
    if shutil.disk_usage(destination.parent).free < expected_size * 4 + 1024 * 1024 * 1024:
        raise RuntimeError("runner disk headroom is insufficient for Eurostat decode")
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    sha = sha256()
    m5 = md5(usedforsecurity=False)
    size = 0
    try:
        with destination.open("xb") as handle:
            while True:
                block = body.read(8 * 1024 * 1024)
                if not block:
                    break
                size += len(block)
                if size > expected_size:
                    raise RuntimeError("Eurostat raw object exceeded its descriptor size")
                sha.update(block)
                m5.update(block)
                handle.write(block)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        body.close()
    if (
        size != expected_size
        or sha.hexdigest() != expected_sha
        or m5.hexdigest() != expected_md5
    ):
        raise RuntimeError("Eurostat raw bytes do not match their accepted receipt")


def output_identity(path: Path) -> tuple[str, int, int]:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    metadata = pq.read_metadata(path)
    return digest.hexdigest(), path.stat().st_size, int(metadata.num_rows)


def put_output(client, bucket: str, raw_sha: str, path: Path) -> tuple[str, int, int]:
    out_sha, size, rows = output_identity(path)
    key = f"{OUTPUT_PREFIX}{raw_sha}.parquet"
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        metadata = head.get("Metadata") or {}
        if (
            int(head.get("ContentLength", -1)) != size
            or metadata.get("sha256") != out_sha
            or metadata.get("raw-sha256") != raw_sha
        ):
            raise RuntimeError(f"existing retained Eurostat output conflicts: {key}")
    except RuntimeError:
        raise
    except Exception:
        client.upload_file(
            str(path),
            bucket,
            key,
            ExtraArgs={
                "ContentType": "application/vnd.apache.parquet",
                "Metadata": {"sha256": out_sha, "raw-sha256": raw_sha},
            },
        )
        head = client.head_object(Bucket=bucket, Key=key)
        metadata = head.get("Metadata") or {}
        if (
            int(head.get("ContentLength", -1)) != size
            or metadata.get("sha256") != out_sha
            or metadata.get("raw-sha256") != raw_sha
        ):
            raise RuntimeError(f"retained Eurostat output upload verification failed: {key}")
    return f"s3://{bucket}/{key}", size, rows


def data_file(table, uri: str, rows: int, size: int) -> DataFile:
    return DataFile.from_args(
        _table_format_version=int(table.metadata.format_version),
        content=DataFileContent.DATA,
        file_path=uri,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=rows,
        file_size_in_bytes=size,
        spec_id=int(table.metadata.default_spec_id),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-distributions", type=int, default=DEFAULT_MAX_DISTRIBUTIONS)
    parser.add_argument("--session-seconds", type=int, default=3000)
    args = parser.parse_args()
    if not 1 <= args.max_distributions <= 64:
        parser.error("--max-distributions must be 1-64")
    if not 60 <= args.session_seconds <= 6600:
        parser.error("--session-seconds must be 60-6600")

    started = time.monotonic()
    client = s3()
    cat = catalog()
    table = cat.load_table(TABLE_ID)
    bucket = req("R2_LAKEHOUSE_BUCKET")
    landing_bucket = req("R2_LANDING_BUCKET")
    resolver = _R2ObjectStore.from_env()
    if resolver.landing_bucket != landing_bucket:
        raise RuntimeError("R2 campaign resolver Landing bucket mismatch")

    source = R2CampaignStore(SOURCE_ID, publication_only=True)
    state = source.load()
    if (
        not isinstance(state, dict)
        or state.get("source_id") != SOURCE_ID
        or state.get("provider_id") != "eurostat"
        or not isinstance(state.get("receipts"), list)
    ):
        raise RuntimeError("Eurostat bulk campaign state is invalid")

    existing = existing_raw_hashes()
    completed = state.get("completed")
    if not isinstance(completed, dict):
        raise RuntimeError("Eurostat bulk campaign completed map is invalid")

    recent_roots = state.get("recent_roots")
    if not isinstance(recent_roots, dict):
        raise RuntimeError("Eurostat bulk campaign recent_roots map is invalid")
    current_data_roots = {
        task_for(spec)["id"]
        for spec in recent_roots.values()
        if isinstance(spec, dict) and spec.get("kind") == "eurostat_tsv_gzip"
    }
    if not current_data_roots:
        raise RuntimeError("Eurostat current catalogue has no TSV data roots")

    candidates: list[tuple[int, str, dict[str, Any], dict[str, Any]]] = []
    seen_raw: set[str] = set()
    accepted_data = 0
    for task_id, value in completed.items():
        if not isinstance(task_id, str) or not isinstance(value, dict):
            continue
        parent_task_id = task_id.split("::", 1)[0]
        if parent_task_id not in current_data_roots:
            continue
        raw = value.get("raw")
        descriptor = value.get("receipt")
        if not isinstance(raw, dict) or not isinstance(descriptor, dict):
            # Aggregate partition completions have no raw/receipt pair.
            continue
        raw_sha = raw.get("sha256")
        raw_size = raw.get("size_bytes")
        receipt_sha = descriptor.get("sha256")
        receipt_size = descriptor.get("size_bytes")
        if (
            not isinstance(raw_sha, str)
            or SHA_RE.fullmatch(raw_sha) is None
            or type(raw_size) is not int
            or raw_size <= 0
            or not isinstance(receipt_sha, str)
            or SHA_RE.fullmatch(receipt_sha) is None
            or type(receipt_size) is not int
            or receipt_size <= 0
        ):
            raise RuntimeError(f"invalid completed Eurostat descriptor: {task_id}")
        accepted_data += 1
        if raw_sha in existing or raw_sha in seen_raw:
            continue
        seen_raw.add(raw_sha)
        candidates.append((raw_size, task_id, descriptor, raw))

    candidates.sort(key=lambda item: (item[0], item[1]))
    selected_descriptors = candidates[: args.max_distributions]
    missing_data = len(candidates)
    selected: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for _raw_size, task_id, descriptor, expected_raw in selected_descriptors:
        if time.monotonic() - started >= args.session_seconds:
            break
        receipt = source.read_receipt(descriptor)
        if not is_data_receipt(receipt):
            raise RuntimeError(f"completed Eurostat item is not a data receipt: {task_id}")
        if receipt.get("raw") != expected_raw:
            raise RuntimeError(f"completed Eurostat raw descriptor drifted: {task_id}")
        selected.append((descriptor, receipt))

    print(json.dumps({
        "status": "eurostat_retained_candidates_selected",
        "current_catalogue_data_roots": len(current_data_roots),
        "accepted_data_completed_entries": accepted_data,
        "existing_distinct_raw_sha256": len(existing),
        "missing_distinct_raw_sha256": missing_data,
        "selected": len(selected),
        "selected_raw_bytes": sum(
            int(receipt["raw"]["size_bytes"]) for _descriptor, receipt in selected
        ),
    }, sort_keys=True), flush=True)

    if not selected:
        print(json.dumps({
            "result": "pass",
            "operation": "catch-up-eurostat-r2-bronze",
            "processed": 0,
            "existing_distinct_raw_sha256": len(existing),
            "accepted_data_completed_entries": accepted_data,
            "missing_distinct_raw_sha256": missing_data,
            "reason": "no_missing_receipts_within_scanned_prefix",
            "landing_deleted": False,
            "google_drive_accessed": False,
        }, sort_keys=True))
        return 0

    prepared: list[dict[str, Any]] = []
    current_names = [field.name for field in table.schema().fields]
    with tempfile.TemporaryDirectory(prefix="zohelo-eurostat-r2-catchup-") as temporary:
        root = Path(temporary)
        for index, (descriptor, receipt) in enumerate(selected):
            raw_sha = receipt["raw"]["sha256"]
            work = root / f"{index:03d}-{raw_sha}"
            work.mkdir()
            raw_path = work / "distribution.tsv.gz"
            receipt_path = work / "receipt.json"
            output_path = work / "observations.parquet"

            receipt_raw = canonical_json(receipt)
            if (
                len(receipt_raw) != descriptor.get("size_bytes")
                or sha256(receipt_raw).hexdigest() != descriptor.get("sha256")
            ):
                raise RuntimeError("Eurostat receipt bytes do not match their descriptor")
            receipt_path.write_bytes(receipt_raw)
            stream_legacy_raw(client, resolver, receipt["raw"], raw_path)
            report = decode_full_distribution(
                raw_path,
                receipt_path,
                output_path,
                receipt_descriptor={
                    "id": descriptor.get("id"),
                    "sha256": descriptor.get("sha256"),
                    "size_bytes": descriptor.get("size_bytes"),
                },
            )
            output_names = list(pq.read_schema(output_path).names)
            if output_names != current_names:
                raise RuntimeError(
                    f"Eurostat decoder schema differs from Iceberg table: {output_names}"
                )
            uri, size, rows = put_output(client, bucket, raw_sha, output_path)
            if rows != int(report["observation_cells"]):
                raise RuntimeError("Eurostat decoder row count differs from Parquet footer")
            prepared.append({
                "raw_sha256": raw_sha,
                "uri": uri,
                "rows": rows,
                "bytes": size,
                "dataset_id": report.get("dataset_id"),
                "distribution_id": report.get("distribution_id"),
            })
            print(json.dumps({
                "status": "eurostat_retained_distribution_prepared",
                **prepared[-1],
            }, sort_keys=True), flush=True)

    table = cat.load_table(TABLE_ID)
    existing_paths = {str(task.file.file_path) for task in table.scan().plan_files()}
    to_append = [item for item in prepared if item["uri"] not in existing_paths]
    if to_append:
        with table.transaction() as tx:
            if tx.table_metadata.name_mapping() is None:
                tx.set_properties(**{
                    TableProperties.DEFAULT_NAME_MAPPING:
                    tx.table_metadata.schema().name_mapping.model_dump_json()
                })
            with tx.update_snapshot(snapshot_properties={
                "zohelo.operation": "catch-up-eurostat-r2-bronze",
                "zohelo.source": "eurostat_bulk",
            }).fast_append() as append:
                for item in to_append:
                    append.append_data_file(
                        data_file(table, item["uri"], int(item["rows"]), int(item["bytes"]))
                    )

    after = existing_raw_hashes()
    missing_after_commit = [
        item["raw_sha256"] for item in prepared if item["raw_sha256"] not in after
    ]
    if missing_after_commit:
        raise RuntimeError(
            f"Eurostat Iceberg commit lacks prepared raw hashes: {missing_after_commit}"
        )

    print(json.dumps({
        "result": "pass",
        "operation": "catch-up-eurostat-r2-bronze",
        "processed": len(prepared),
        "appended_files": len(to_append),
        "rows_added": sum(int(item["rows"]) for item in to_append),
        "bytes_added": sum(int(item["bytes"]) for item in to_append),
        "existing_distinct_raw_sha256_before": len(existing),
        "existing_distinct_raw_sha256_after": len(after),
        "accepted_data_completed_entries": accepted_data,
        "missing_distinct_raw_sha256_before": missing_data,
        "landing_deleted": False,
        "google_drive_accessed": False,
        "elapsed_seconds": round(time.monotonic() - started, 1),
    }, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
