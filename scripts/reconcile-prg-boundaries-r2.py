#!/usr/bin/env python3
"""Prove and finalize exact R2 Landing -> Bronze lineage for PRG boundaries.

The retained PRG boundary ZIP is independently reconciled from the already
completed surface-area XLSX dataset. This job:
- reads exactly 00_jednostki_administracyjne.zip from private R2 Landing;
- hashes the native ZIP and reruns the existing PRG boundary transformer locally;
- proves all four regenerated Bronze business tables equal current Iceberg data;
- archives the native ZIP and verifies full SHA-256 readback;
- writes an immutable boundary-lineage receipt and attaches it to Bronze tables;
- deletes only the proven boundary ZIP when no live control reference blocks it.

It never touches the archived/current PRG surface-area XLSX dataset.
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
from botocore.exceptions import ClientError
import duckdb
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog

from prg_bronze_loader import transform_prg_bronze

SOURCE = "gugik_prg"
DATASET = "administrative_boundaries"
LANDING_PREFIX = "01_landing/gugik_prg/"
EXPECTED_NAME = "00_jednostki_administracyjne.zip"
TECHNICAL_COLUMNS = {"extracted_at_utc"}
TABLES = {
    "br_prg_country.parquet": ("bronze", "prg_country"),
    "br_prg_voivodeships.parquet": ("bronze", "prg_voivodeships"),
    "br_prg_counties.parquet": ("bronze", "prg_counties"),
    "br_prg_municipalities.parquet": ("bronze", "prg_municipalities"),
}


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


def list_boundary_zip(s3, bucket: str) -> dict[str, Any]:
    items = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=LANDING_PREFIX):
        for obj in page.get("Contents", []):
            key = str(obj["Key"])
            size = int(obj.get("Size", 0))
            if Path(key).name == EXPECTED_NAME and size > 0:
                items.append({"key": key, "size": size})
    if len(items) != 1:
        raise RuntimeError(f"expected exactly one PRG boundary ZIP, observed {len(items)}")
    return items[0]


def quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def download_current_table_files(s3, bucket: str, table, directory: Path) -> list[Path]:
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
        raise RuntimeError("production Iceberg table has no data files")
    return paths


def prove_logical_equivalence(table, regenerated: Path, current_paths: list[Path]) -> dict[str, Any]:
    identifier = getattr(table, "_identifier", None)
    if not isinstance(identifier, tuple) or not identifier:
        raise RuntimeError("PyIceberg table identifier is unavailable")
    name = ".".join(identifier)

    local_names = list(pq.read_schema(regenerated).names)
    current_names = [field.name for field in table.schema().fields]
    if set(local_names) != set(current_names):
        raise RuntimeError(
            f"schema column mismatch for {name}: "
            f"current={sorted(current_names)} regenerated={sorted(local_names)}"
        )
    business_columns = [column for column in local_names if column not in TECHNICAL_COLUMNS]
    if not business_columns:
        raise RuntimeError(f"no business columns found in {name}")

    local_sql = str(regenerated).replace("'", "''")
    current_glob = str(current_paths[0].parent / "*.parquet").replace("'", "''")
    cols = ", ".join(quoted(column) for column in business_columns)
    con = duckdb.connect()
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA memory_limit='3GB'")
        local_rows = int(con.execute(
            f"SELECT count(*) FROM read_parquet('{local_sql}')"
        ).fetchone()[0])
        current_rows = int(con.execute(
            f"SELECT count(*) FROM read_parquet('{current_glob}')"
        ).fetchone()[0])
        if local_rows != current_rows:
            raise RuntimeError(
                f"row count mismatch for {name}: current={current_rows} regenerated={local_rows}"
            )
        local_only = int(con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT {cols} FROM read_parquet('{local_sql}')
                EXCEPT ALL
                SELECT {cols} FROM read_parquet('{current_glob}')
            )
            """
        ).fetchone()[0])
        current_only = int(con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT {cols} FROM read_parquet('{current_glob}')
                EXCEPT ALL
                SELECT {cols} FROM read_parquet('{local_sql}')
            )
            """
        ).fetchone()[0])
    finally:
        con.close()
    if local_only or current_only:
        raise RuntimeError(
            f"logical content mismatch for {name}: "
            f"regenerated_only={local_only} current_only={current_only}"
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


def control_references(s3, lakehouse_bucket: str, landing_bucket: str, key: str) -> list[str]:
    needles = (key.encode("utf-8"), f"r2://{landing_bucket}/{key}".encode("utf-8"))
    matches = []
    prefix = f"06_control/source_campaigns/{SOURCE}/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=lakehouse_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            control_key = str(obj["Key"])
            size = int(obj.get("Size", 0))
            if size <= 0 or size > 8 * 1024 * 1024:
                continue
            body = s3.get_object(Bucket=lakehouse_bucket, Key=control_key)["Body"].read()
            if any(needle in body for needle in needles):
                matches.append(control_key)
    return sorted(matches)


def put_immutable_json(s3, bucket: str, key: str, value: dict[str, Any]) -> None:
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        existing = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception:
        existing = None
    if existing is not None:
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


def attach_receipt(cat, receipt_uri: str, source_sha: str, proofs: list[dict[str, Any]]) -> None:
    by_table = {item["table"]: item for item in proofs}
    for identifier in TABLES.values():
        table = cat.load_table(identifier)
        proof = by_table[".".join(identifier)]
        desired = {
            "zohelo.lineage.mode": "r2-landing-logical-equivalence-v1",
            "zohelo.lineage.source": SOURCE,
            "zohelo.lineage.dataset": DATASET,
            "zohelo.lineage.input-set-sha256": source_sha,
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
    item = list_boundary_zip(s3, landing_bucket)

    with tempfile.TemporaryDirectory(prefix="zohelo-prg-boundary-lineage-") as temporary:
        root = Path(temporary)
        landing_dir = root / "landing"
        output_dir = root / "bronze"
        current_dir = root / "current"
        landing_dir.mkdir()
        output_dir.mkdir()
        current_dir.mkdir()

        source_path = landing_dir / EXPECTED_NAME
        response = s3.get_object(Bucket=landing_bucket, Key=item["key"])
        source_sha, source_size = hash_stream(response["Body"], source_path)
        if source_size != item["size"]:
            raise RuntimeError("PRG boundary Landing size changed while reading")

        transform_result = transform_prg_bronze(
            landing_dir=landing_dir,
            output_dir=output_dir,
            skip_upload=True,
        )

        proofs = []
        for filename, identifier in TABLES.items():
            regenerated = output_dir / filename
            if not regenerated.is_file():
                raise RuntimeError(f"PRG transformer did not create {filename}")
            table = cat.load_table(identifier)
            current_paths = download_current_table_files(
                s3, lakehouse_bucket, table, current_dir / identifier[-1]
            )
            proofs.append(prove_logical_equivalence(table, regenerated, current_paths))

    archive_key = f"05_archive/{SOURCE}/{source_sha}/{EXPECTED_NAME}"
    if not object_exists(s3, landing_bucket, archive_key):
        s3.copy_object(
            Bucket=landing_bucket,
            Key=archive_key,
            CopySource={"Bucket": landing_bucket, "Key": item["key"]},
        )
    archive_response = s3.get_object(Bucket=landing_bucket, Key=archive_key)
    archive_sha, archive_size = hash_stream(archive_response["Body"])
    if archive_sha != source_sha or archive_size != source_size:
        raise RuntimeError("PRG boundary Archive readback mismatch")

    blockers = control_references(s3, lakehouse_bucket, landing_bucket, item["key"])
    receipt_key = f"06_control/lineage/{SOURCE}/boundaries-{source_sha}.json"
    receipt_uri = f"s3://{lakehouse_bucket}/{receipt_key}"
    receipt = {
        "format_version": 1,
        "kind": "landing_bronze_lineage_receipt",
        "source_id": SOURCE,
        "dataset_id": DATASET,
        "proof": "deterministic_logical_equivalence_excluding_technical_timestamp",
        "input_set_sha256": source_sha,
        "landing_inputs": [{
            "key": item["key"],
            "name": EXPECTED_NAME,
            "size": source_size,
            "sha256": source_sha,
        }],
        "bronze_proofs": proofs,
        "archive": [{
            "source_key": item["key"],
            "archive_key": archive_key,
            "size": archive_size,
            "sha256": archive_sha,
            "compression": "native_zip",
        }],
        "archive_readback_verified": True,
        "landing_delete_eligible": len(blockers) == 0,
        "blocking_control_references": blockers,
        "transform_result": transform_result,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    put_immutable_json(s3, lakehouse_bucket, receipt_key, receipt)
    attach_receipt(cat, receipt_uri, source_sha, proofs)

    for identifier in TABLES.values():
        table = cat.load_table(identifier)
        if table.properties.get("zohelo.lineage.receipt") != receipt_uri:
            raise RuntimeError(f"lineage property was not committed for {'.'.join(identifier)}")

    deleted = False
    if not blockers:
        check = s3.get_object(Bucket=landing_bucket, Key=archive_key)
        check_sha, check_size = hash_stream(check["Body"])
        if check_sha != source_sha or check_size != source_size:
            raise RuntimeError("PRG boundary Archive changed before Landing deletion")
        s3.delete_object(Bucket=landing_bucket, Key=item["key"])
        if object_exists(s3, landing_bucket, item["key"]):
            raise RuntimeError("PRG boundary Landing deletion did not complete")
        deleted = True
        lifecycle_key = f"06_control/lifecycle/{SOURCE}/boundaries-{source_sha}.json"
        put_immutable_json(s3, lakehouse_bucket, lifecycle_key, {
            "format_version": 1,
            "kind": "landing_archive_finalization_receipt",
            "source_id": SOURCE,
            "dataset_id": DATASET,
            "lineage_receipt": receipt_uri,
            "bronze_tables": [proof["table"] for proof in proofs],
            "landing_deleted": True,
            "deleted_landing_key": item["key"],
            "archive_key": archive_key,
            "sha256": source_sha,
            "size": source_size,
            "archive_verified_before_delete": True,
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        })

    print(json.dumps({
        "result": "pass",
        "source": SOURCE,
        "dataset": DATASET,
        "source_sha256": source_sha,
        "landing_objects": 1,
        "landing_bytes": source_size,
        "bronze_tables_proven": len(proofs),
        "archive_objects_verified": 1,
        "landing_deleted": deleted,
        "blocking_control_references": blockers,
        "surface_area_dataset_untouched": True,
        "google_drive_accessed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
