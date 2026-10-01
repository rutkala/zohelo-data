#!/usr/bin/env python3
"""Compare retained WDI API bytes with the existing bulk Bronze data, read-only.

No provider request, catalog write, archive, pointer change or deletion occurs.
The report classifies every retained response SHA separately. A numeric match is
exact Decimal equality, not a floating point tolerance. Matching observations
does not prove that API metadata or status fields are represented by bulk data.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ingestion.sources import world_bank

SOURCE = "world_bank_wdi"
TABLE = ("bronze", "wdi_data")
PREFIX = "01_landing/world_bank_wdi/responses/"
NAME = re.compile(r"response-([0-9a-f]{64})\.bin$")
EXPECTED = ["country_name", "country_code", "indicator_name", "indicator_code",
            "observation_year", "observation_value_text"]


def required(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def inventory(client, bucket):
    result = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        for value in page.get("Contents", []):
            key, size = str(value["Key"]), int(value["Size"])
            if key.endswith("/") and size == 0:
                continue
            match = NAME.fullmatch(key.removeprefix(PREFIX))
            if not match or not 0 < size <= 8 * 1024 * 1024:
                raise RuntimeError(f"unexpected WDI response identity or size: {key}")
            result.append({"key": key, "size": size, "sha256": match[1],
                           "etag": str(value.get("ETag", ""))})
    result.sort(key=lambda item: item["key"])
    if len({item["sha256"] for item in result}) != len(result):
        raise RuntimeError("duplicate WDI response SHA in current Landing")
    return result


def parse_response(raw):
    """Reuse source validation without creating or executing ingestion tasks."""
    header, rows = world_bank._paged_json(raw)
    page = world_bank._page_number(header, "page", minimum=1)
    pages = world_bank._page_number(header, "pages", minimum=0)
    per_page = world_bank._page_number(header, "per_page", minimum=1)
    total = world_bank._page_number(header, "total", minimum=0)
    if ((pages == 0 and total != 0) or (pages and page > pages)
            or len(rows) > per_page or len(rows) > total or (total and not rows)):
        raise ValueError("WDI pagination counts conflict")
    world_bank._optional_text(header, "lastupdated")
    if not rows:
        return "empty_response", []
    if all(isinstance(row.get("indicator"), dict) and "value" in row for row in rows):
        grouped = {}
        for row in rows:
            indicator = world_bank._require_indicator(row["indicator"].get("id"))
            grouped.setdefault(indicator, []).append(row)
        for indicator, group in grouped.items():
            world_bank._validate_observations(group, indicator, 0, 9999)
        # Preserve the exact JSON number spelling through Decimal. The source
        # validator above sees its ordinary int/float types, unchanged.
        exact_rows = json.loads(raw, parse_float=Decimal)[1]
        values = []
        for number, row in enumerate(exact_rows, 1):
            value = row["value"]
            text = None if value is None else str(value)
            if text is not None and not Decimal(text).is_finite():
                raise ValueError("WDI observation number is not finite")
            code = row.get("countryiso3code") or None
            values.append((number, code, row["indicator"]["id"], int(row["date"]), text))
        return "observations", values
    if all("source" in row and "id" in row for row in rows):
        world_bank._validate_indicators(rows)
        return "indicator_catalog", []
    if all("region" in row and "id" in row for row in rows):
        world_bank._validate_countries(rows)
        return "country_catalog", []
    raise ValueError("unrecognized or mixed WDI response rows")


def read_response(client, bucket, item):
    response = client.get_object(Bucket=bucket, Key=item["key"])
    body = response["Body"]
    try:
        raw = body.read(item["size"] + 1)
    finally:
        body.close()
    if len(raw) != item["size"] or sha256(raw).hexdigest() != item["sha256"]:
        raise RuntimeError(f"WDI response hash/size mismatch: {item['key']}")
    try:
        kind, values = parse_response(raw)
        record_count = len(world_bank._paged_json(raw)[1])
        return {**item, "kind": kind, "rows": record_count, "counts": {}}, values
    except (ValueError, KeyError, TypeError) as exc:
        # Invalid retained bytes are evidence, never silently dropped. Do not
        # include response bodies or row values in the diagnostic output.
        return {**item, "kind": "invalid_response", "rows": None,
                "error_type": type(exc).__name__, "counts": {}}, []


def compare_value(api_value, bulk_value):
    if api_value is None:
        return "null_api_value_with_bulk_key"
    if bulk_value is None:
        return "bulk_value_null"
    try:
        bulk = Decimal(bulk_value.strip())
        api = Decimal(api_value)
    except (InvalidOperation, ValueError):
        return "bulk_value_not_numeric"
    if not bulk.is_finite() or not api.is_finite():
        return "nonfinite_value"
    return "exact_numeric_match" if api == bulk else "value_conflict"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="wdi-retained-reconciliation.json")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    if not 1 <= args.workers <= 16:
        parser.error("workers must be between 1 and 16")
    import boto3
    from botocore.config import Config
    import duckdb
    import pyarrow as pa
    from pyiceberg.catalog.rest import RestCatalog

    client = boto3.client("s3", endpoint_url=required("R2_S3_ENDPOINT"), region_name="auto",
        aws_access_key_id=required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", connect_timeout=30, read_timeout=180,
                      retries={"max_attempts": 8, "mode": "standard"}))
    cat = RestCatalog(name="zohelo", warehouse=required("R2_WAREHOUSE"),
        uri=required("R2_CATALOG_URI"), token=required("R2_DATA_CATALOG_TOKEN"),
        **{"s3.access-key-id": required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
           "s3.secret-access-key": required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
           "s3.endpoint": required("R2_S3_ENDPOINT"), "s3.region": "auto"})
    bucket, lakehouse = required("R2_LANDING_BUCKET"), required("R2_LAKEHOUSE_BUCKET")
    inputs = inventory(client, bucket)
    if not inputs:
        raise RuntimeError("no retained WDI API responses found in Landing")
    table = cat.load_table(TABLE)
    if [field.name for field in table.schema().fields] != EXPECTED:
        raise RuntimeError("bronze.wdi_data schema changed; inspect before comparison")
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("bronze.wdi_data has no committed snapshot")
    tasks = list(table.scan().plan_files())
    if not tasks or any(task.delete_files for task in tasks):
        raise RuntimeError("direct retained Parquet comparison requires files without delete files")
    summaries = {}
    with tempfile.TemporaryDirectory(prefix="wdi-retained-") as temporary:
        root = Path(temporary)
        files = []
        for number, task in enumerate(tasks):
            parsed = urlsplit(str(task.file.file_path))
            if parsed.scheme != "s3" or parsed.netloc != lakehouse:
                raise RuntimeError("WDI Bronze file is outside the lakehouse bucket")
            destination = root / f"bronze-{number}.parquet"
            client.download_file(lakehouse, parsed.path.lstrip("/"), str(destination))
            if destination.stat().st_size != int(task.file.file_size_in_bytes):
                raise RuntimeError("WDI Bronze file size changed")
            files.append(str(destination))
        con = duckdb.connect(str(root / "comparison.duckdb"))
        con.execute("SET memory_limit='2GB'")
        con.execute("SET threads=2")
        con.from_parquet(files).create_view("bulk_rows")
        expected_rows = sum(int(task.file.record_count) for task in tasks)
        actual_rows = con.execute("SELECT count(*) FROM bulk_rows").fetchone()[0]
        if actual_rows != expected_rows:
            raise RuntimeError("WDI Bronze row count differs from Iceberg snapshot")
        con.execute("""CREATE TABLE api_rows (raw_sha256 VARCHAR, source_row_number BIGINT,
            country_code VARCHAR, indicator_code VARCHAR, observation_year INTEGER,
            api_value VARCHAR)""")
        arrow_schema = pa.schema([("raw_sha256", pa.string()), ("source_row_number", pa.int64()),
            ("country_code", pa.string()), ("indicator_code", pa.string()),
            ("observation_year", pa.int32()), ("api_value", pa.string())])
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            def responses():
                # Bound queued response bytes as well as active network reads.
                for start in range(0, len(inputs), args.workers * 2):
                    yield from pool.map(lambda item: read_response(client, bucket, item),
                                        inputs[start:start + args.workers * 2])

            for number, (summary, values) in enumerate(responses(), 1):
                digest = summary["sha256"]
                summaries[digest] = summary
                if values:
                    records = [dict(zip(arrow_schema.names, (digest, *value))) for value in values]
                    batch = pa.Table.from_pylist(records, schema=arrow_schema)
                    con.register("incoming", batch)
                    con.execute("INSERT INTO api_rows SELECT * FROM incoming")
                    con.unregister("incoming")
                if number % 250 == 0:
                    print(json.dumps({"operation": "inspect-wdi-retained", "responses_read": number,
                                      "responses_total": len(inputs)}), flush=True)
        # Group only bulk keys actually requested by retained API observations.
        # Ambiguous bulk keys are reported instead of selecting a convenient row.
        con.execute("""CREATE TABLE bulk_index AS
            SELECT b.country_code, b.indicator_code, b.observation_year,
                   count(*) AS key_rows, min(b.observation_value_text) AS bulk_value
            FROM bulk_rows b SEMI JOIN api_rows a
              USING (country_code, indicator_code, observation_year)
            GROUP BY b.country_code, b.indicator_code, b.observation_year""")
        cursor = con.execute("""SELECT a.raw_sha256, a.country_code, a.api_value,
                   b.key_rows, b.bulk_value FROM api_rows a LEFT JOIN bulk_index b
              USING (country_code, indicator_code, observation_year)""")
        while chunk := cursor.fetchmany(10000):
            for digest, code, api, count, bulk in chunk:
                if not code:
                    outcome = "missing_country_iso3code"
                elif count is None:
                    outcome = "null_api_value_absent_from_bulk" if api is None else "missing_bulk_key"
                elif count != 1:
                    outcome = "ambiguous_bulk_key"
                else:
                    outcome = compare_value(api, bulk)
                counts = summaries[digest]["counts"]
                counts[outcome] = counts.get(outcome, 0) + 1
        api_count = con.execute("SELECT count(*) FROM api_rows").fetchone()[0]
        con.close()
    observed = cat.load_table(TABLE).current_snapshot()
    if observed is None or observed.snapshot_id != snapshot.snapshot_id:
        raise RuntimeError("WDI Bronze snapshot changed during comparison")
    if inventory(client, bucket) != inputs:
        raise RuntimeError("WDI Landing inventory changed during comparison")
    kinds, counts = Counter(), Counter()
    for item in summaries.values():
        kinds[item["kind"]] += 1
        counts.update(item["counts"])
    if sum(counts.values()) != api_count:
        raise RuntimeError("WDI comparison lost observation row membership")
    report = {
        "format_version": 1, "operation": "compare-retained-wdi-api-to-bulk-bronze",
        "source_id": SOURCE, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "result": "inspected", "writes_performed": False, "provider_calls": 0,
        "landing_deleted": False, "reconciliation_complete": False,
        "bulk_table": ".".join(TABLE), "bulk_snapshot_id": str(snapshot.snapshot_id),
        "bulk_rows": actual_rows, "bulk_files": len(tasks),
        "retained_responses": len(inputs), "retained_response_bytes": sum(x["size"] for x in inputs),
        "response_kinds": dict(kinds), "observation_rows": api_count,
        "comparison_counts": dict(counts), "responses": list(summaries.values()),
        "limitations": ["Retained Landing scope only; pending provider tasks are not executed.",
            "Source JSON/schema/pagination validated; receipt task date ranges are not revalidated.",
            "Observation equality does not establish API unit/status/footnote/catalog metadata lineage.",
            "Null API observations are explicitly classified; bulk model omits empty values.",
            "No API observation is promoted, overwritten, archived, or declared fully reconciled."]}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "responses"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
