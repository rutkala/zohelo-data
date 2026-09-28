#!/usr/bin/env python3
"""Synthetic Iceberg v2 query-plane contract probe.

This uses only generated data plus the disposable local REST catalog/object store
started by tests/fixtures/iceberg-query-contract/docker-compose.yml. It must not
read production Drive data or create an external service.
"""

from __future__ import annotations

import json
import os
import re
import resource
import subprocess
import time
from pathlib import Path

import duckdb


CATALOG = "iceberg_datalake"
TABLE = f"{CATALOG}.bronze.gus_dbw_observations"
INDICATORS = 8
ROWS_PER_INDICATOR = 20_000
EXPECTED_ROWS = INDICATORS * ROWS_PER_INDICATOR
FILTER_IDS = (2, 7)
EXPECTED_FILTERED = len(FILTER_IDS) * ROWS_PER_INDICATOR
COMPOSE_FILE = Path(
    os.environ.get(
        "ICEBERG_COMPOSE_FILE",
        "tests/fixtures/iceberg-query-contract/docker-compose.yml",
    )
)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def make_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='512MiB'")
    con.execute("SET enable_external_file_cache=true")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    con.execute(
        """
        CREATE SECRET fixture_s3 (
            TYPE S3,
            KEY_ID 'admin',
            SECRET 'password',
            REGION 'us-east-1',
            ENDPOINT '127.0.0.1:5000',
            URL_STYLE 'path',
            USE_SSL false
        )
        """
    )
    return con


def attach_catalog(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        ATTACH '' AS {CATALOG} (
            TYPE iceberg,
            CLIENT_ID 'admin',
            CLIENT_SECRET 'password',
            ENDPOINT 'http://127.0.0.1:8181'
        )
        """
    )


def explain_text(con: duckdb.DuckDBPyConnection, query: str) -> str:
    rows = con.execute("EXPLAIN ANALYZE " + query).fetchall()
    return "\n".join(str(value) for row in rows for value in row if value is not None)


def parse_transfer_bytes(plan: str) -> int | None:
    match = re.search(
        r"Total Data Transferred:\s*([0-9.,]+)\s*([KMGT]?i?B|bytes?)",
        plan,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    amount = float(match.group(1).replace(",", ""))
    unit = match.group(2).lower()
    multipliers = {
        "b": 1,
        "byte": 1,
        "bytes": 1,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
    }
    return int(amount * multipliers[unit])


def parse_requests(plan: str) -> int | None:
    match = re.search(r"Total Requests:\s*([0-9,]+)", plan, flags=re.IGNORECASE)
    if match:
        return int(match.group(1).replace(",", ""))
    method_counts = re.findall(r"#(?:HEAD|GET|POST|PUT):\s*([0-9,]+)", plan)
    if method_counts:
        return sum(int(value.replace(",", "")) for value in method_counts)
    return None


def profile(con: duckdb.DuckDBPyConnection, query: str) -> dict[str, object]:
    started = time.perf_counter()
    plan = explain_text(con, query)
    elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
    return {
        "elapsed_ms": elapsed_ms,
        "requests": parse_requests(plan),
        "bytes": parse_transfer_bytes(plan),
        "plan_excerpt": plan[-4000:],
    }


def cache_stats(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    entries, total_bytes, loaded_bytes = con.execute(
        """
        SELECT
            count(*),
            coalesce(sum(nr_bytes), 0),
            coalesce(sum(CASE WHEN loaded THEN nr_bytes ELSE 0 END), 0)
        FROM duckdb_external_file_cache()
        """
    ).fetchone()
    return {
        "entries": int(entries),
        "bytes": int(total_bytes),
        "loaded_bytes": int(loaded_bytes),
    }


def moto_python(code: str) -> subprocess.CompletedProcess[str]:
    command = [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "exec",
        "-T",
        "moto",
        "python",
        "-c",
        code,
    ]
    return subprocess.run(command, check=True, capture_output=True, text=True)


def object_store_parquet_bytes() -> int:
    result = moto_python(
        "import boto3,json; "
        "c=boto3.client('s3',endpoint_url='http://127.0.0.1:5000',"
        "aws_access_key_id='admin',aws_secret_access_key='password',region_name='us-east-1'); "
        "print(json.dumps(c.list_objects_v2(Bucket='warehouse')))"
    )
    payload = json.loads(result.stdout)
    total = sum(
        int(item.get("Size", 0))
        for item in payload.get("Contents", [])
        if str(item.get("Key", "")).endswith(".parquet")
    )
    if total <= 0:
        raise AssertionError("object_store_parquet_bytes_not_observed")
    return total


def remove_object(s3_path: str) -> None:
    prefix = "s3://warehouse/"
    if not s3_path.startswith(prefix):
        raise AssertionError(f"unexpected_data_path:{s3_path}")
    key = s3_path[len(prefix) :]
    key_literal = json.dumps(key)
    moto_python(
        "import boto3; "
        "c=boto3.client('s3',endpoint_url='http://127.0.0.1:5000',"
        "aws_access_key_id='admin',aws_secret_access_key='password',region_name='us-east-1'); "
        f"c.delete_object(Bucket='warehouse',Key={key_literal})"
    )


def main() -> int:
    receipt: dict[str, object] = {
        "result": "fail",
        "contract": "synthetic-iceberg-v2-query-plane",
        "production_data": False,
        "external_account": False,
        "rows": EXPECTED_ROWS,
        "indicators": INDICATORS,
    }

    writer = make_connection()
    attach_catalog(writer)
    writer.execute(f"CREATE SCHEMA {CATALOG}.bronze")
    writer.execute(
        f"""
        CREATE TABLE {TABLE} (
            indicator_id INTEGER,
            unit_id BIGINT,
            period_year INTEGER,
            value DOUBLE,
            payload VARCHAR
        )
        WITH ('format-version' = '2')
        """
    )
    writer.execute(f"ALTER TABLE {TABLE} SET PARTITIONED BY (indicator_id)")
    writer.execute(
        f"""
        INSERT INTO {TABLE}
        SELECT
            indicator_id::INTEGER,
            (indicator_id * {ROWS_PER_INDICATOR} + row_id)::BIGINT AS unit_id,
            (2000 + row_id % 25)::INTEGER AS period_year,
            (indicator_id * 1000000 + row_id)::DOUBLE AS value,
            md5(indicator_id::VARCHAR || ':' || row_id::VARCHAR) AS payload
        FROM range(1, {INDICATORS + 1}) AS i(indicator_id)
        CROSS JOIN range(0, {ROWS_PER_INDICATOR}) AS r(row_id)
        """
    )

    catalog_count = writer.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    if catalog_count != EXPECTED_ROWS:
        raise AssertionError(f"catalog_count:{catalog_count}")

    load_cursor = writer.execute(
        f"SELECT * FROM iceberg_load_table_response({TABLE})"
    )
    load_columns = [column[0] for column in load_cursor.description]
    load_row = load_cursor.fetchone()
    load_response = dict(zip(load_columns, load_row))
    metadata_location = load_response.get("metadata_location")
    if not isinstance(metadata_location, str) or not metadata_location.startswith("s3://"):
        raise AssertionError("missing_metadata_location")

    snapshots = writer.execute(
        f"SELECT snapshot_id FROM iceberg_snapshots({TABLE}) ORDER BY sequence_number"
    ).fetchall()
    if not snapshots:
        raise AssertionError("missing_catalog_snapshot")
    snapshot_id = int(snapshots[-1][0])

    membership = writer.execute(
        f"""
        SELECT file_path, record_count
        FROM iceberg_metadata({TABLE})
        WHERE content = 'EXISTING' AND status <> 'DELETED'
        ORDER BY file_path
        """
    ).fetchall()
    if not membership:
        membership = writer.execute(
            f"""
            SELECT file_path, record_count
            FROM iceberg_metadata({TABLE})
            WHERE file_format = 'PARQUET' AND status <> 'DELETED'
            ORDER BY file_path
            """
        ).fetchall()
    membership_rows = sum(int(row[1]) for row in membership)
    if membership_rows != EXPECTED_ROWS:
        raise AssertionError(f"manifest_membership_rows:{membership_rows}")

    schema_catalog = writer.execute(f"DESCRIBE SELECT * FROM {TABLE}").fetchall()
    writer.close()

    reader = make_connection()
    source = f"iceberg_scan({sql_string(metadata_location)})"
    direct_snapshots = reader.execute(
        f"SELECT snapshot_id FROM iceberg_snapshots({sql_string(metadata_location)}) "
        "ORDER BY sequence_number"
    ).fetchall()
    if not direct_snapshots or int(direct_snapshots[-1][0]) != snapshot_id:
        raise AssertionError("snapshot_disagreement")

    direct_membership = reader.execute(
        f"""
        SELECT file_path, record_count
        FROM iceberg_metadata({sql_string(metadata_location)})
        WHERE content = 'EXISTING' AND status <> 'DELETED'
        ORDER BY file_path
        """
    ).fetchall()
    if direct_membership != membership:
        raise AssertionError("membership_disagreement")

    schema_direct = reader.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    if schema_direct != schema_catalog:
        raise AssertionError("schema_disagreement")

    queries = {
        "preview": f"SELECT * FROM {source} LIMIT 1000",
        "count": f"SELECT count(*) FROM {source}",
        "multi_indicator": (
            f"SELECT count(*) FROM {source} "
            f"WHERE indicator_id IN ({FILTER_IDS[0]}, {FILTER_IDS[1]})"
        ),
        "join": (
            f"SELECT count(*) FROM {source} AS o "
            f"JOIN (VALUES ({FILTER_IDS[0]}, 'two'), ({FILTER_IDS[1]}, 'seven')) "
            "AS d(indicator_id, label) USING (indicator_id)"
        ),
    }

    profiles = {name: profile(reader, query) for name, query in queries.items()}
    missing_metrics = [
        name
        for name, values in profiles.items()
        if values["requests"] is None or values["bytes"] is None
    ]
    if missing_metrics:
        raise AssertionError("missing_http_metrics:" + ",".join(missing_metrics))

    preview_rows = reader.execute(queries["preview"]).fetchall()
    count = reader.execute(queries["count"]).fetchone()[0]
    filtered = reader.execute(queries["multi_indicator"]).fetchone()[0]
    joined = reader.execute(queries["join"]).fetchone()[0]
    if len(preview_rows) != 1000:
        raise AssertionError(f"preview_rows:{len(preview_rows)}")
    if count != EXPECTED_ROWS:
        raise AssertionError(f"direct_count:{count}")
    if filtered != EXPECTED_FILTERED:
        raise AssertionError(f"filtered_count:{filtered}")
    if joined != EXPECTED_FILTERED:
        raise AssertionError(f"join_count:{joined}")

    cache_after_first = cache_stats(reader)
    reader.execute(queries["preview"]).fetchall()
    cache_after_repeat = cache_stats(reader)
    physical_parquet_bytes = object_store_parquet_bytes()
    preview_bytes = int(profiles["preview"]["bytes"])
    if preview_bytes >= physical_parquet_bytes:
        raise AssertionError(
            f"preview_not_lazy:{preview_bytes}>={physical_parquet_bytes}"
        )
    if cache_after_first["bytes"] <= 0:
        raise AssertionError("external_cache_empty")
    reader.close()

    missing_path = str(membership[0][0])
    remove_object(missing_path)
    fail_reader = make_connection()
    missing_failed_closed = False
    try:
        fail_reader.execute(
            f"SELECT sum(value) FROM iceberg_scan({sql_string(metadata_location)})"
        ).fetchone()
    except duckdb.Error:
        missing_failed_closed = True
    finally:
        fail_reader.close()
    if not missing_failed_closed:
        raise AssertionError("missing_object_did_not_fail_closed")

    max_rss_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    receipt.update(
        {
            "result": "pass",
            "iceberg_format_version": 2,
            "snapshot_id": snapshot_id,
            "metadata_location_scheme": metadata_location.split(":", 1)[0],
            "data_files": len(membership),
            "manifest_rows": membership_rows,
            "physical_parquet_bytes": physical_parquet_bytes,
            "queries": {
                "preview_rows": len(preview_rows),
                "complete_count": int(count),
                "multi_indicator_count": int(filtered),
                "join_count": int(joined),
            },
            "profiles": profiles,
            "cache": {
                "after_first": cache_after_first,
                "after_repeat": cache_after_repeat,
            },
            "max_rss_bytes": max_rss_bytes,
            "missing_object_failed_closed": missing_failed_closed,
        }
    )
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(
            json.dumps(
                {
                    "result": "fail",
                    "category": type(exc).__name__,
                    "detail": str(exc)[:300],
                    "production_data": False,
                    "external_account": False,
                },
                sort_keys=True,
            )
        )
        raise
