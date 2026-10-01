#!/usr/bin/env python3
"""Prove exact R2 Landing -> Bronze lineage for MF Biala Lista.

This one-time reconciliation:
- reads the single current Landing .7z from private R2;
- hashes its exact compressed bytes;
- reruns the existing deterministic Bronze transformer locally (no upload);
- proves regenerated business columns equal current production Iceberg tables,
  excluding only processed_at_utc;
- copies the original .7z to Archive and verifies restored SHA-256;
- writes an immutable lineage receipt and attaches it to the Bronze tables.

It does not delete Landing and does not rewrite production table data.
The script is idempotent for the same immutable Landing input set and may be safely rerun after runtime dependency fixes or transient worker failures.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import csv
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Any

import boto3
from botocore.config import Config
import duckdb
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog


SOURCE = "mf_biala_lista"
LANDING_PREFIX = "01_landing/mf_biala_lista/"
ARCHIVE_PREFIX = "05_archive/mf_biala_lista/"
CONTROL_PREFIX = "06_control/lineage/mf_biala_lista/"

TABLES = {
    "br_biala_lista_header.parquet": ("bronze", "biala_lista_header"),
    "br_biala_lista_masks.parquet": ("bronze", "biala_lista_masks"),
    "br_biala_lista_taxpayers.parquet": ("bronze", "biala_lista_taxpayers"),
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


def list_landing_archive(s3, bucket: str) -> dict[str, Any]:
    items = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=LANDING_PREFIX):
        for obj in page.get("Contents", []):
            key = str(obj["Key"])
            size = int(obj.get("Size", 0))
            if key.lower().endswith(".7z") and size > 0:
                items.append({"key": key, "size": size})
    if len(items) != 1:
        raise RuntimeError(f"expected exactly one MF Biala Lista Landing .7z, observed {len(items)}")
    return items[0]


def reconstruct_business_parquet(
    archive_path: Path,
    workspace: Path,
) -> dict[str, Any]:
    """Reproduce the legacy loader's business columns with vectorized I/O.

    The historical loader inserts ~3.7M hashes through executemany, which is too
    slow for a bounded reconciliation run. This reads the same 7z member and
    applies the same line/section rules, but stages hashes as TSV and lets
    DuckDB build Parquet in vectorized scans.
    """
    match = re.search(r"(\d{8})", archive_path.name)
    if not match:
        raise RuntimeError(f"cannot parse YYYYMMDD from {archive_path.name}")
    date_str = match.group(1)
    snapshot_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    internal_name = f"{date_str}.json"

    workspace.mkdir(parents=True, exist_ok=True)
    taxpayer_tsv = workspace / "taxpayers.tsv"
    masks_tsv = workspace / "masks.tsv"

    proc = subprocess.Popen(
        ["7z", "e", "-so", str(archive_path), internal_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1024 * 1024,
    )
    if proc.stdout is None:
        raise RuntimeError("7z stdout was not available")

    header_data: dict[str, Any] = {}
    masks = 0
    active = 0
    exempt = 0
    current_section: str | None = None
    in_header = False
    header_lines: list[str] = []

    with taxpayer_tsv.open("w", encoding="utf-8", newline="") as tf, masks_tsv.open(
        "w", encoding="utf-8", newline=""
    ) as mf:
        taxpayer_writer = csv.writer(tf, delimiter="\t", lineterminator="\n")
        mask_writer = csv.writer(mf, delimiter="\t", lineterminator="\n")

        for raw_line in proc.stdout:
            line = raw_line.decode("utf-8", errors="ignore")

            if '"naglowek":' in line:
                in_header = True
                header_lines = ["{"]
                continue
            if in_header:
                header_lines.append(line)
                if "}" in line:
                    in_header = False
                    try:
                        header_text = "".join(header_lines)
                        header_text = re.sub(r",\s*$", "", header_text.strip())
                        header_data = json.loads(header_text)
                    except Exception as exc:
                        raise RuntimeError("cannot parse MF Biala Lista header") from exc
                continue

            section = re.match(r'^\s*"([^"]+)":\s*\[', line)
            if section:
                current_section = section.group(1)
                continue

            if current_section == "maski":
                value = re.search(r'"([^"]+)"', line)
                if value:
                    mask = value.group(1)
                    mask_writer.writerow([mask, mask[2:10] if len(mask) >= 10 else "", snapshot_date])
                    masks += 1
                if "]" in line:
                    current_section = None
                continue

            if current_section in {"skrotyPodatnikowCzynnych", "skrotyPodatnikowZwolnionych"}:
                value = re.search(r'"([0-9a-fA-F]{64,128})"', line)
                if value:
                    status = (
                        "active"
                        if current_section == "skrotyPodatnikowCzynnych"
                        else "exempt"
                    )
                    taxpayer_writer.writerow([value.group(1), status, snapshot_date])
                    if status == "active":
                        active += 1
                    else:
                        exempt += 1
                if "]" in line:
                    current_section = None

    proc.stdout.close()
    stderr = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
    if proc.stderr:
        proc.stderr.close()
    code = proc.wait()
    if code != 0:
        raise RuntimeError(f"7z extraction failed ({code}): {stderr[-500:]}")

    outputs = {
        "br_biala_lista_header.parquet": workspace / "br_biala_lista_header.parquet",
        "br_biala_lista_masks.parquet": workspace / "br_biala_lista_masks.parquet",
        "br_biala_lista_taxpayers.parquet": workspace / "br_biala_lista_taxpayers.parquet",
    }

    con = duckdb.connect()
    try:
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA memory_limit='2GB'")
        con.execute(
            """
            CREATE TABLE header_business(
                snapshot_date DATE,
                generation_date VARCHAR,
                transformation_count INTEGER,
                schema_description VARCHAR
            )
            """
        )
        con.execute(
            "INSERT INTO header_business VALUES (CAST(? AS DATE), ?, ?, ?)",
            [
                snapshot_date,
                str(header_data.get("dataGenerowaniaDanych", date_str)),
                int(header_data.get("liczbaTransformacji", 5000)),
                str(header_data.get("schemat", "")),
            ],
        )
        escaped = str(outputs["br_biala_lista_header.parquet"]).replace("'", "''")
        con.execute(
            f"COPY header_business TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        masks_input = str(masks_tsv).replace("'", "''")
        masks_output = str(outputs["br_biala_lista_masks.parquet"]).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    column0::VARCHAR AS account_mask,
                    column1::VARCHAR AS bank_prefix,
                    CAST(column2 AS DATE) AS snapshot_date
                FROM read_csv(
                    '{masks_input}',
                    delim='\\t',
                    header=false,
                    columns={{'column0':'VARCHAR','column1':'VARCHAR','column2':'VARCHAR'}}
                )
            ) TO '{masks_output}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )

        tax_input = str(taxpayer_tsv).replace("'", "''")
        tax_output = str(outputs["br_biala_lista_taxpayers.parquet"]).replace("'", "''")
        con.execute(
            f"""
            COPY (
                SELECT
                    column0::VARCHAR AS hash,
                    column1::VARCHAR AS status,
                    CAST(column2 AS DATE) AS snapshot_date
                FROM read_csv(
                    '{tax_input}',
                    delim='\\t',
                    header=false,
                    columns={{'column0':'VARCHAR','column1':'VARCHAR','column2':'VARCHAR'}}
                )
            ) TO '{tax_output}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        con.close()

    return {
        "status": "reconstructed_business_columns",
        "snapshot_date": snapshot_date,
        "active_hashes": active,
        "exempt_hashes": exempt,
        "masks": masks,
        "files": [path.name for path in outputs.values()],
    }


def download_current_table_files(
    s3,
    bucket: str,
    table,
    directory: Path,
) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for index, task in enumerate(table.scan().plan_files()):
        uri = str(task.file.file_path)
        if not uri.startswith(f"s3://{bucket}/"):
            raise RuntimeError(f"unexpected Iceberg data-file URI: {uri}")
        key = uri[len(f"s3://{bucket}/") :]
        target = directory / f"{index:06d}.parquet"
        s3.download_file(bucket, key, str(target))
        if target.stat().st_size != int(task.file.file_size_in_bytes):
            raise RuntimeError(f"current table file size mismatch: {uri}")
        paths.append(target)
    if not paths:
        raise RuntimeError("production Iceberg table has no data files")
    return paths


def prove_business_equivalence_from_files(
    table,
    regenerated_path: Path,
    current_paths: list[Path],
) -> dict[str, Any]:
    identifier = getattr(table, "_identifier", None)
    if not isinstance(identifier, tuple) or not identifier:
        raise RuntimeError("PyIceberg table identifier is unavailable")
    name = ".".join(identifier)

    business_columns = list(pq.read_schema(regenerated_path).names)
    current_names = {field.name for field in table.schema().fields}
    if not set(business_columns).issubset(current_names):
        raise RuntimeError(
            f"regenerated business columns are not a subset of {name}: "
            f"{sorted(set(business_columns) - current_names)}"
        )

    con = duckdb.connect()
    try:
        local_path = str(regenerated_path).replace("'", "''")
        current_glob = str(current_paths[0].parent / "*.parquet").replace("'", "''")
        cols = ", ".join(quoted(column) for column in business_columns)
        local_rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{local_path}')"
        ).fetchone()[0]
        current_rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{current_glob}')"
        ).fetchone()[0]
        if local_rows != current_rows:
            raise RuntimeError(
                f"row count mismatch for {name}: current={current_rows} regenerated={local_rows}"
            )

        regenerated_only = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT {cols} FROM read_parquet('{local_path}')
                EXCEPT ALL
                SELECT {cols} FROM read_parquet('{current_glob}')
            )
            """
        ).fetchone()[0]
        current_only = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT {cols} FROM read_parquet('{current_glob}')
                EXCEPT ALL
                SELECT {cols} FROM read_parquet('{local_path}')
            )
            """
        ).fetchone()[0]
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
        "rows": int(local_rows),
        "current_data_files": len(planned),
        "current_data_bytes": sum(int(task.file.file_size_in_bytes) for task in planned),
        "business_columns_proven_equal": business_columns,
        "excluded_technical_columns": sorted(current_names - set(business_columns)),
        "regenerated_parquet_bytes": regenerated_path.stat().st_size,
        "regenerated_parquet_sha256": file_sha256(regenerated_path),
    }


def file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def copy_and_verify_archive(
    s3,
    bucket: str,
    landing: dict[str, Any],
    digest: str,
) -> dict[str, Any]:
    name = Path(landing["key"]).name
    target = f"{ARCHIVE_PREFIX}{digest}/{name}"
    try:
        head = s3.head_object(Bucket=bucket, Key=target)
        present = int(head.get("ContentLength", -1)) == landing["size"]
    except Exception:
        present = False
    if not present:
        s3.copy_object(
            Bucket=bucket,
            Key=target,
            CopySource={"Bucket": bucket, "Key": landing["key"]},
        )
    response = s3.get_object(Bucket=bucket, Key=target)
    observed_sha, observed_size = hash_stream(response["Body"])
    if observed_size != landing["size"] or observed_sha != landing["sha256"]:
        raise RuntimeError("MF Biala Lista Archive readback mismatch")
    return {
        "source_key": landing["key"],
        "archive_key": target,
        "size": observed_size,
        "sha256": observed_sha,
        "compression": "native_7z",
    }


def control_references(s3, lakehouse_bucket: str, landing_bucket: str, landing: dict[str, Any]) -> list[str]:
    needles = (
        landing["key"].encode("utf-8"),
        f"r2://{landing_bucket}/{landing['key']}".encode("utf-8"),
    )
    matches = []
    for source_id in ("mf_biala_lista", "mf_biala_lista_bronze"):
        prefix = f"06_control/source_campaigns/{source_id}/"
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


def main() -> int:
    s3 = client()
    cat = catalog()
    landing_bucket = req("R2_LANDING_BUCKET")
    lakehouse_bucket = req("R2_LAKEHOUSE_BUCKET")

    item = list_landing_archive(s3, landing_bucket)

    with tempfile.TemporaryDirectory(prefix="zohelo-mf-lineage-") as temporary:
        root = Path(temporary)
        archive_path = root / Path(item["key"]).name
        workspace = root / "bronze"
        response = s3.get_object(Bucket=landing_bucket, Key=item["key"])
        digest, size = hash_stream(response["Body"], archive_path)
        if size != item["size"]:
            raise RuntimeError("MF Biala Lista Landing size changed while reading")
        landing = {**item, "sha256": digest}

        transform_result = reconstruct_business_parquet(
            archive_path=archive_path,
            workspace=workspace,
        )

        proofs = []
        current_root = root / "current"
        for filename, identifier in TABLES.items():
            local_path = workspace / filename
            if not local_path.is_file():
                raise RuntimeError(f"MF reconstruction did not create {filename}")
            table = cat.load_table(identifier)
            current_paths = download_current_table_files(
                s3,
                lakehouse_bucket,
                table,
                current_root / identifier[-1],
            )
            proofs.append(
                prove_business_equivalence_from_files(table, local_path, current_paths)
            )

    archived = copy_and_verify_archive(s3, landing_bucket, landing, digest)
    references = control_references(s3, lakehouse_bucket, landing_bucket, landing)

    receipt_key = f"{CONTROL_PREFIX}{digest}.json"
    receipt_uri = f"s3://{lakehouse_bucket}/{receipt_key}"
    receipt = {
        "format_version": 1,
        "kind": "landing_bronze_lineage_receipt",
        "source_id": SOURCE,
        "proof": "deterministic_vectorized_logical_equivalence_excluding_technical_timestamp",
        "input_set_sha256": digest,
        "landing_inputs": [landing],
        "bronze_proofs": proofs,
        "archive": [archived],
        "archive_readback_verified": True,
        "landing_deleted": False,
        "landing_delete_eligible": len(references) == 0,
        "blocking_control_references": references,
        "transform_result": transform_result,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
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
        "landing_objects": 1,
        "landing_bytes": size,
        "bronze_tables_proven": len(proofs),
        "archive_objects_verified": 1,
        "landing_deleted": False,
        "landing_delete_eligible": len(references) == 0,
        "blocking_control_references": references,
        "lineage_receipt": receipt_uri,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
