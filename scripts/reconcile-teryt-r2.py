#!/usr/bin/env python3
"""Prove exact R2 Landing -> Bronze lineage for GUS TERYT.

This job is intentionally conservative:
- downloads the three current TERYT Landing ZIPs from private R2;
- hashes the exact raw bytes;
- reruns the existing deterministic TERYT transformer locally with no Drive/R2 write;
- proves the regenerated Bronze business columns are exactly equal to the current
  production Iceberg tables (technical extracted_at_utc is excluded);
- copies the already-compressed raw ZIPs into Archive and verifies restored SHA-256;
- writes one immutable lineage receipt and attaches its identity to the Bronze tables.

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

from teryt_bronze_loader import transform_teryt_bronze

SOURCE = "gus_teryt"
LANDING_PREFIX = "01_landing/gus_teryt/"
ARCHIVE_PREFIX = "05_archive/gus_teryt/"
CONTROL_PREFIX = "06_control/lineage/gus_teryt/"

TABLES = {
    "br_teryt_terc.parquet": ("bronze", "teryt_terc"),
    "br_teryt_simc.parquet": ("bronze", "teryt_simc"),
    "br_teryt_ulic.parquet": ("bronze", "teryt_ulic"),
}
TECHNICAL_COLUMNS = {"extracted_at_utc"}


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


def list_landing_zips(s3, bucket: str) -> list[dict[str, Any]]:
    items = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=LANDING_PREFIX):
        for obj in page.get("Contents", []):
            key = str(obj["Key"])
            size = int(obj.get("Size", 0))
            if key.lower().endswith(".zip") and size > 0:
                items.append({"key": key, "size": size})
    items.sort(key=lambda item: item["key"])
    if len(items) != 3:
        raise RuntimeError(f"expected exactly 3 TERYT Landing ZIPs, observed {len(items)}")
    names = [Path(item["key"]).name.upper() for item in items]
    for token in ("TERC", "SIMC", "ULIC"):
        if sum(token in name for name in names) != 1:
            raise RuntimeError(f"TERYT Landing does not contain exactly one {token} ZIP")
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
            "size": size,
            "sha256": digest,
        })
    return result


def file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def input_set_digest(inputs: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        [{"key": x["key"], "size": x["size"], "sha256": x["sha256"]} for x in inputs],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(payload).hexdigest()


def quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_name(table) -> str:
    identifier = getattr(table, "_identifier", None)
    if not isinstance(identifier, tuple) or not identifier:
        raise RuntimeError("PyIceberg table identifier is unavailable")
    return ".".join(identifier)


def prove_logical_equivalence(table, local_path: Path) -> dict[str, Any]:
    name = table_name(table)
    current = table.scan().to_arrow()
    local = pq.read_table(local_path)

    if current.num_rows != local.num_rows:
        raise RuntimeError(
            f"row count mismatch for {name}: "
            f"current={current.num_rows} regenerated={local.num_rows}"
        )

    current_names = set(current.column_names)
    local_names = set(local.column_names)
    if current_names != local_names:
        raise RuntimeError(
            f"schema column mismatch for {name}: "
            f"current={sorted(current_names)} regenerated={sorted(local_names)}"
        )

    business_columns = [
        name for name in local.column_names if name not in TECHNICAL_COLUMNS
    ]
    if not business_columns:
        raise RuntimeError(f"no business columns found in {name}")

    con = duckdb.connect()
    try:
        con.register("current_iceberg", current)
        escaped_path = str(local_path).replace("'", "''")
        cols = ", ".join(quoted(name) for name in business_columns)
        forward = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT {cols} FROM read_parquet('{escaped_path}')
                EXCEPT ALL
                SELECT {cols} FROM current_iceberg
            )
            """
        ).fetchone()[0]
        reverse = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT {cols} FROM current_iceberg
                EXCEPT ALL
                SELECT {cols} FROM read_parquet('{escaped_path}')
            )
            """
        ).fetchone()[0]
    finally:
        con.close()

    if forward or reverse:
        raise RuntimeError(
            f"logical content mismatch for {name}: "
            f"regenerated_only={forward} current_only={reverse}"
        )

    snapshot = table.current_snapshot()
    files = list(table.scan().plan_files())
    return {
        "table": name,
        "snapshot_id": str(snapshot.snapshot_id) if snapshot is not None else None,
        "rows": current.num_rows,
        "current_data_files": len(files),
        "current_data_bytes": sum(int(task.file.file_size_in_bytes) for task in files),
        "business_columns_proven_equal": business_columns,
        "excluded_technical_columns": sorted(TECHNICAL_COLUMNS & current_names),
        "regenerated_parquet_bytes": local_path.stat().st_size,
        "regenerated_parquet_sha256": file_sha256(local_path),
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
            "size": observed_size,
            "sha256": observed_sha,
            "compression": "native_zip",
        })
    return archived


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

    with tempfile.TemporaryDirectory(prefix="zohelo-teryt-lineage-") as temporary:
        root = Path(temporary)
        landing_dir = root / "landing"
        output_dir = root / "bronze"
        landing_dir.mkdir()
        output_dir.mkdir()

        inputs = download_inputs(s3, landing_bucket, landing_items, landing_dir)
        digest = input_set_digest(inputs)

        transform_result = transform_teryt_bronze(
            landing_dir=landing_dir,
            output_dir=output_dir,
            skip_upload=True,
        )

        proofs = []
        for filename, identifier in TABLES.items():
            local_path = output_dir / filename
            if not local_path.is_file():
                raise RuntimeError(f"TERYT transformer did not create {filename}")
            proofs.append(prove_logical_equivalence(cat.load_table(identifier), local_path))

    archived = copy_and_verify_archive(s3, landing_bucket, inputs, digest)
    references = control_references(s3, lakehouse_bucket, landing_bucket, inputs)

    receipt_key = f"{CONTROL_PREFIX}{digest}.json"
    receipt_uri = f"s3://{lakehouse_bucket}/{receipt_key}"
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
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    put_immutable_json(s3, lakehouse_bucket, receipt_key, receipt)
    attach_receipt(cat, receipt_uri, digest, proofs)

    # Re-read properties after commit.
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
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
