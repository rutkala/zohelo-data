#!/usr/bin/env python3
"""Prove exact R2 Landing -> Bronze lineage for the retained GLEIF Golden Copy.

This job is intentionally conservative:
- reads exactly the three current GLEIF Golden Copy ZIPs from private R2 Landing;
- hashes the exact native bytes and reruns the existing GLEIF Bronze transforms locally;
- proves every regenerated Bronze business row equals the current production Iceberg table;
- copies the native ZIPs into Archive and verifies full SHA-256 readback;
- writes one immutable lineage receipt and attaches it to the three Bronze tables.

It does not delete Landing and does not rewrite production table data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Any

import boto3
from botocore.config import Config
import duckdb
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog

from gleif_bronze_loader import (
    transform_gleif_lei2_to_bronze,
    transform_gleif_repex_to_bronze,
    transform_gleif_rr_to_bronze,
)

SOURCE = "gleif"
LANDING_PREFIX = "01_landing/gleif/"
ARCHIVE_PREFIX = "05_archive/gleif/"
CONTROL_PREFIX = "06_control/lineage/gleif/"

TABLES = {
    "br_gleif_lei2.parquet": ("bronze", "gleif_lei2"),
    "br_gleif_relationship_records.parquet": ("bronze", "gleif_relationship_records"),
    "br_gleif_reporting_exceptions.parquet": ("bronze", "gleif_reporting_exceptions"),
}
TOKENS = {
    "lei2": "br_gleif_lei2.parquet",
    "rr": "br_gleif_relationship_records.parquet",
    "repex": "br_gleif_reporting_exceptions.parquet",
}
TECHNICAL_COLUMNS = {"processed_at_utc"}


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


def hash_stream(body, destination: Path | None = None) -> tuple[str, int]:
    digest = sha256()
    size = 0
    handle = destination.open("wb") if destination is not None else None
    try:
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            if handle is not None:
                handle.write(chunk)
    finally:
        if handle is not None:
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
        try:
            body.close()
        except Exception:
            pass
    return digest.hexdigest(), size


def member_for_name(name: str) -> str | None:
    lower = name.lower()
    matches = [
        key for key in TOKENS
        if f"gleif_goldencopy_{key}" in lower or f"gleif-goldencopy-{key}" in lower
    ]
    if len(matches) > 1:
        raise RuntimeError(f"ambiguous GLEIF member filename: {name}")
    return matches[0] if matches else None


def list_landing_zips(s3, bucket: str) -> list[dict[str, Any]]:
    items = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=LANDING_PREFIX):
        for obj in page.get("Contents", []):
            key = str(obj["Key"])
            size = int(obj.get("Size", 0))
            if not key.lower().endswith(".zip") or size <= 0:
                continue
            member = member_for_name(Path(key).name)
            if member is not None:
                items.append({"key": key, "size": size, "member": member})
    items.sort(key=lambda item: item["key"])
    if len(items) != 3:
        raise RuntimeError(f"expected exactly 3 GLEIF Landing ZIPs, observed {len(items)}")
    observed = [item["member"] for item in items]
    if sorted(observed) != sorted(TOKENS):
        raise RuntimeError(f"GLEIF Landing member set mismatch: {observed}")
    return items


def download_inputs(s3, bucket: str, items: list[dict[str, Any]], directory: Path) -> list[dict[str, Any]]:
    result = []
    for item in items:
        destination = directory / Path(item["key"]).name
        response = s3.get_object(Bucket=bucket, Key=item["key"])
        digest, size = hash_stream(response["Body"], destination)
        if size != item["size"]:
            raise RuntimeError(f"Landing size changed while reading {item['key']}")
        result.append({
            "key": item["key"],
            "name": destination.name,
            "member": item["member"],
            "size": size,
            "sha256": digest,
        })
    return result


def input_set_digest(inputs: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        [
            {
                "key": item["key"],
                "member": item["member"],
                "size": item["size"],
                "sha256": item["sha256"],
            }
            for item in inputs
        ],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(payload).hexdigest()


def quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def download_current_table_files(s3, bucket: str, table, directory: Path) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
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
        raise RuntimeError("production Iceberg table has no data files")
    return paths


def prove_logical_equivalence(table, local_path: Path, current_paths: list[Path]) -> dict[str, Any]:
    identifier = getattr(table, "_identifier", None)
    if not isinstance(identifier, tuple) or not identifier:
        raise RuntimeError("PyIceberg table identifier is unavailable")
    name = ".".join(identifier)

    local_names = list(pq.read_schema(local_path).names)
    current_names = [field.name for field in table.schema().fields]
    if set(local_names) != set(current_names):
        raise RuntimeError(
            f"schema column mismatch for {name}: "
            f"current={sorted(current_names)} regenerated={sorted(local_names)}"
        )
    business_columns = [column for column in local_names if column not in TECHNICAL_COLUMNS]
    if not business_columns:
        raise RuntimeError(f"no business columns found in {name}")

    local_path_sql = str(local_path).replace("'", "''")
    current_glob = str(current_paths[0].parent / "*.parquet").replace("'", "''")
    cols = ", ".join(quoted(column) for column in business_columns)

    con = duckdb.connect()
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA memory_limit='3GB'")
        local_rows = int(
            con.execute(f"SELECT count(*) FROM read_parquet('{local_path_sql}')").fetchone()[0]
        )
        current_rows = int(
            con.execute(f"SELECT count(*) FROM read_parquet('{current_glob}')").fetchone()[0]
        )
        if local_rows != current_rows:
            raise RuntimeError(
                f"row count mismatch for {name}: current={current_rows} regenerated={local_rows}"
            )
        regenerated_only = int(
            con.execute(
                f"""
                SELECT count(*) FROM (
                    SELECT {cols} FROM read_parquet('{local_path_sql}')
                    EXCEPT ALL
                    SELECT {cols} FROM read_parquet('{current_glob}')
                )
                """
            ).fetchone()[0]
        )
        current_only = int(
            con.execute(
                f"""
                SELECT count(*) FROM (
                    SELECT {cols} FROM read_parquet('{current_glob}')
                    EXCEPT ALL
                    SELECT {cols} FROM read_parquet('{local_path_sql}')
                )
                """
            ).fetchone()[0]
        )
    finally:
        con.close()

    if regenerated_only or current_only:
        raise RuntimeError(
            f"logical content mismatch for {name}: "
            f"regenerated_only={regenerated_only} current_only={current_only}"
        )

    snapshot = table.current_snapshot()
    planned = list(table.scan().plan_files())
    return {
        "table": name,
        "snapshot_id": str(snapshot.snapshot_id) if snapshot is not None else None,
        "rows": local_rows,
        "current_data_files": len(planned),
        "current_data_bytes": sum(int(task.file.file_size_in_bytes) for task in planned),
        "business_columns_proven_equal": business_columns,
        "excluded_technical_columns": sorted(TECHNICAL_COLUMNS & set(current_names)),
    }


def transform(inputs: list[dict[str, Any]], landing_dir: Path, output_dir: Path) -> dict[str, Any]:
    by_member = {item["member"]: landing_dir / item["name"] for item in inputs}
    return {
        "lei2_rows": transform_gleif_lei2_to_bronze(
            by_member["lei2"], output_dir / TOKENS["lei2"]
        ),
        "relationship_rows": transform_gleif_rr_to_bronze(
            by_member["rr"], output_dir / TOKENS["rr"]
        ),
        "reporting_exception_rows": transform_gleif_repex_to_bronze(
            by_member["repex"], output_dir / TOKENS["repex"]
        ),
    }


def copy_and_verify_archive(s3, bucket: str, inputs: list[dict[str, Any]], digest: str) -> list[dict[str, Any]]:
    archived = []
    for item in inputs:
        target = f"{ARCHIVE_PREFIX}{digest}/{item['name']}"
        try:
            head = s3.head_object(Bucket=bucket, Key=target)
            present = int(head.get("ContentLength", -1)) == item["size"]
        except Exception:
            present = False
        if not present:
            s3.copy_object(
                Bucket=bucket,
                Key=target,
                CopySource={"Bucket": bucket, "Key": item["key"]},
            )
        response = s3.get_object(Bucket=bucket, Key=target)
        observed_sha, observed_size = hash_stream(response["Body"])
        if observed_size != item["size"] or observed_sha != item["sha256"]:
            raise RuntimeError(f"Archive readback mismatch for {item['key']}")
        archived.append({
            "source_key": item["key"],
            "archive_key": target,
            "member": item["member"],
            "size": observed_size,
            "sha256": observed_sha,
            "compression": "native_zip",
        })
    return archived


def existing_json(s3, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise RuntimeError(f"existing JSON is not an object: {key}")
    return value


def put_immutable_json(s3, bucket: str, key: str, value: dict[str, Any]) -> None:
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        existing = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception:
        existing = None
    if existing is not None:
        if existing != raw:
            raise RuntimeError(f"immutable lineage receipt conflict: {key}")
        return
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=raw,
        ContentType="application/json",
        Metadata={"sha256": sha256(raw).hexdigest()},
    )


def attach_receipt(cat, receipt_uri: str, digest: str, proofs: list[dict[str, Any]]) -> None:
    by_table = {item["table"]: item for item in proofs}
    for identifier in TABLES.values():
        table = cat.load_table(identifier)
        proof = by_table[".".join(identifier)]
        desired = {
            "zohelo.lineage.mode": "r2-landing-logical-equivalence-v1",
            "zohelo.lineage.source": SOURCE,
            "zohelo.lineage.input-set-sha256": digest,
            "zohelo.lineage.receipt": receipt_uri,
            "zohelo.lineage.snapshot-id": str(proof["snapshot_id"]),
        }
        if all(table.properties.get(key) == value for key, value in desired.items()):
            continue
        with table.transaction() as tx:
            tx.set_properties(**desired)


def control_references(s3, lakehouse_bucket: str, landing_bucket: str, inputs: list[dict[str, Any]]) -> list[str]:
    needles = []
    for item in inputs:
        needles.extend([
            item["key"].encode("utf-8"),
            f"r2://{landing_bucket}/{item['key']}".encode("utf-8"),
        ])
    matches = []
    prefix = f"06_control/source_campaigns/{SOURCE}/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=lakehouse_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = str(obj["Key"])
            size = int(obj.get("Size", 0))
            if size <= 0 or size > 8 * 1024 * 1024:
                continue
            body = s3.get_object(Bucket=lakehouse_bucket, Key=key)["Body"].read()
            if any(needle in body for needle in needles):
                matches.append(key)
    return sorted(matches)


def main() -> int:
    s3 = client()
    cat = catalog()
    landing_bucket = req("R2_LANDING_BUCKET")
    lakehouse_bucket = req("R2_LAKEHOUSE_BUCKET")

    landing_items = list_landing_zips(s3, landing_bucket)

    with tempfile.TemporaryDirectory(prefix="zohelo-gleif-lineage-") as temporary:
        root = Path(temporary)
        landing_dir = root / "landing"
        output_dir = root / "bronze"
        current_dir = root / "current"
        landing_dir.mkdir()
        output_dir.mkdir()
        current_dir.mkdir()

        inputs = download_inputs(s3, landing_bucket, landing_items, landing_dir)
        digest = input_set_digest(inputs)
        transform_result = transform(inputs, landing_dir, output_dir)

        proofs = []
        for filename, identifier in TABLES.items():
            local_path = output_dir / filename
            if not local_path.is_file():
                raise RuntimeError(f"GLEIF transformer did not create {filename}")
            table = cat.load_table(identifier)
            current_paths = download_current_table_files(
                s3, lakehouse_bucket, table, current_dir / identifier[-1]
            )
            proofs.append(prove_logical_equivalence(table, local_path, current_paths))

    archived = copy_and_verify_archive(s3, landing_bucket, inputs, digest)
    references = control_references(s3, lakehouse_bucket, landing_bucket, inputs)

    receipt_key = f"{CONTROL_PREFIX}{digest}.json"
    receipt_uri = f"s3://{lakehouse_bucket}/{receipt_key}"
    prior = existing_json(s3, lakehouse_bucket, receipt_key)
    observed_at = (
        prior.get("observed_at_utc")
        if prior is not None and isinstance(prior.get("observed_at_utc"), str)
        else datetime.now(timezone.utc).isoformat()
    )
    receipt = {
        "format_version": 1,
        "kind": "landing_bronze_lineage_receipt",
        "source_id": SOURCE,
        "proof": "deterministic_logical_equivalence_excluding_technical_timestamp",
        "input_set_sha256": digest,
        "landing_inputs": inputs,
        "bronze_proofs": proofs,
        "archive": archived,
        "archive_readback_verified": True,
        "landing_deleted": False,
        "landing_delete_eligible": len(references) == 0,
        "blocking_control_references": references,
        "transform_result": transform_result,
        "observed_at_utc": observed_at,
    }
    put_immutable_json(s3, lakehouse_bucket, receipt_key, receipt)
    attach_receipt(cat, receipt_uri, digest, proofs)

    for identifier in TABLES.values():
        table = cat.load_table(identifier)
        if table.properties.get("zohelo.lineage.receipt") != receipt_uri:
            raise RuntimeError(f"lineage property was not committed for {'.'.join(identifier)}")

    print(json.dumps({
        "result": "pass",
        "source": SOURCE,
        "input_set_sha256": digest,
        "landing_objects": len(inputs),
        "landing_bytes": sum(item["size"] for item in inputs),
        "bronze_tables_proven": len(proofs),
        "archive_objects_verified": len(archived),
        "landing_deleted": False,
        "landing_delete_eligible": len(references) == 0,
        "blocking_control_references": references,
        "lineage_receipt": receipt_uri,
        "transform_result": transform_result,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
