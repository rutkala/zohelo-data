#!/usr/bin/env python3
"""Synthetic Iceberg v2 query-plane contract probe.

This uses only generated data plus the disposable local REST catalog/object store
started by tests/fixtures/iceberg-query-contract/docker-compose.yml. It must not
read production Drive data or create an external service.
"""

from __future__ import annotations

import http.client
import json
import os
import resource
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen
from xml.etree import ElementTree

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


def make_connection(endpoint_port: int = 5000) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='512MiB'")
    con.execute("SET enable_external_file_cache=true")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    con.execute(
        f"""
        CREATE SECRET fixture_s3 (
            TYPE S3,
            KEY_ID 'admin',
            SECRET 'password',
            REGION 'us-east-1',
            ENDPOINT '127.0.0.1:{endpoint_port}',
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


class CountingProxy:
    """Local S3 proxy that counts exactly the response bytes DuckDB receives."""

    def __init__(
        self,
        upstream_host: str = "127.0.0.1",
        upstream_port: int = 5000,
    ) -> None:
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self._lock = threading.Lock()
        self.requests = 0
        self.bytes = 0
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _forward(self) -> None:
                request_body = None
                request_length = int(self.headers.get("content-length", "0") or 0)
                if request_length:
                    request_body = self.rfile.read(request_length)

                headers = {key: value for key, value in self.headers.items()}
                connection = http.client.HTTPConnection(
                    proxy.upstream_host,
                    proxy.upstream_port,
                    timeout=30,
                )
                try:
                    connection.request(
                        self.command,
                        self.path,
                        body=request_body,
                        headers=headers,
                    )
                    upstream = connection.getresponse()
                    payload = upstream.read()
                    upstream_length = upstream.getheader("content-length")

                    self.send_response(upstream.status)
                    for key, value in upstream.getheaders():
                        if key.lower() in {
                            "connection",
                            "transfer-encoding",
                            "content-length",
                        }:
                            continue
                        self.send_header(key, value)
                    if self.command == "HEAD" and upstream_length is not None:
                        self.send_header("content-length", upstream_length)
                    else:
                        self.send_header("content-length", str(len(payload)))
                    self.end_headers()
                    if self.command != "HEAD":
                        self.wfile.write(payload)

                    with proxy._lock:
                        proxy.requests += 1
                        if self.command != "HEAD":
                            proxy.bytes += len(payload)
                finally:
                    connection.close()

            def do_GET(self) -> None:
                self._forward()

            def do_HEAD(self) -> None:
                self._forward()

            def log_message(self, format: str, *args: object) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = int(self.server.server_address[1])
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )

    def __enter__(self) -> "CountingProxy":
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def reset(self) -> None:
        with self._lock:
            self.requests = 0
            self.bytes = 0

    def snapshot(self) -> tuple[int, int]:
        with self._lock:
            return self.requests, self.bytes


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


def query_once(
    query: str,
    proxy: CountingProxy,
) -> tuple[list[tuple], dict[str, object], dict[str, int]]:
    con = make_connection(proxy.port)
    proxy.reset()
    started = time.perf_counter()
    try:
        rows = con.execute(query).fetchall()
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        requests, transferred = proxy.snapshot()
        cache = cache_stats(con)
        return rows, {
            "elapsed_ms": elapsed_ms,
            "requests": requests,
            "bytes": transferred,
        }, cache
    finally:
        con.close()


def object_store_listing() -> list[dict[str, object]]:
    request = Request("http://127.0.0.1:5000/warehouse?list-type=2")
    with urlopen(request, timeout=30) as response:
        payload = response.read()
    root = ElementTree.fromstring(payload)
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    items: list[dict[str, object]] = []
    for content in root.findall("s3:Contents", namespace):
        key = content.findtext("s3:Key", default="", namespaces=namespace)
        size_text = content.findtext("s3:Size", default="0", namespaces=namespace)
        items.append({"Key": key, "Size": int(size_text)})
    return items


def object_store_parquet_bytes() -> int:
    total = sum(
        int(item.get("Size", 0))
        for item in object_store_listing()
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
    request = Request(
        "http://127.0.0.1:5000/warehouse/" + quote(key, safe="/"),
        method="DELETE",
    )
    with urlopen(request, timeout=30) as response:
        if response.status not in (200, 204):
            raise AssertionError(f"delete_failed:{response.status}")

def main() -> int:
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
    load_response = dict(zip(load_columns, load_cursor.fetchone()))
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
    membership_rows = sum(int(row[1]) for row in membership)
    if not membership or membership_rows != EXPECTED_ROWS:
        raise AssertionError(f"manifest_membership_rows:{membership_rows}")

    schema_catalog = writer.execute(f"DESCRIBE SELECT * FROM {TABLE}").fetchall()
    writer.close()

    physical_parquet_bytes = object_store_parquet_bytes()

    with CountingProxy() as proxy:
        consistency_reader = make_connection(proxy.port)
        direct_snapshots = consistency_reader.execute(
            f"SELECT snapshot_id FROM iceberg_snapshots({sql_string(metadata_location)}) "
            "ORDER BY sequence_number"
        ).fetchall()
        direct_membership = consistency_reader.execute(
            f"""
            SELECT file_path, record_count
            FROM iceberg_metadata({sql_string(metadata_location)})
            WHERE content = 'EXISTING' AND status <> 'DELETED'
            ORDER BY file_path
            """
        ).fetchall()
        schema_direct = consistency_reader.execute(
            f"DESCRIBE SELECT * FROM iceberg_scan({sql_string(metadata_location)})"
        ).fetchall()
        consistency_reader.close()

        if not direct_snapshots or int(direct_snapshots[-1][0]) != snapshot_id:
            raise AssertionError("snapshot_disagreement")
        if direct_membership != membership:
            raise AssertionError("membership_disagreement")
        if schema_direct != schema_catalog:
            raise AssertionError("schema_disagreement")

        source = f"iceberg_scan({sql_string(metadata_location)})"
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

        preview_rows, preview_profile, preview_cache = query_once(
            queries["preview"], proxy
        )
        count_rows, count_profile, _ = query_once(queries["count"], proxy)
        filtered_rows, filtered_profile, _ = query_once(
            queries["multi_indicator"], proxy
        )
        join_rows, join_profile, _ = query_once(queries["join"], proxy)

        profiles = {
            "preview": preview_profile,
            "count": count_profile,
            "multi_indicator": filtered_profile,
            "join": join_profile,
        }
        if any(int(values["requests"]) <= 0 for values in profiles.values()):
            raise AssertionError("missing_http_requests")
        if any(int(values["bytes"]) <= 0 for values in profiles.values()):
            raise AssertionError("missing_proxy_bytes")

        count = int(count_rows[0][0])
        filtered = int(filtered_rows[0][0])
        joined = int(join_rows[0][0])
        if len(preview_rows) != 1000:
            raise AssertionError(f"preview_rows:{len(preview_rows)}")
        if count != EXPECTED_ROWS:
            raise AssertionError(f"direct_count:{count}")
        if filtered != EXPECTED_FILTERED:
            raise AssertionError(f"filtered_count:{filtered}")
        if joined != EXPECTED_FILTERED:
            raise AssertionError(f"join_count:{joined}")

        preview_bytes = int(preview_profile["bytes"])
        if preview_bytes >= physical_parquet_bytes:
            raise AssertionError(
                f"preview_not_lazy:{preview_bytes}>={physical_parquet_bytes}"
            )

        repeat_reader = make_connection(proxy.port)
        proxy.reset()
        repeat_reader.execute(queries["preview"]).fetchall()
        first_repeat_requests, first_repeat_bytes = proxy.snapshot()
        cache_after_first = cache_stats(repeat_reader)
        proxy.reset()
        repeat_reader.execute(queries["preview"]).fetchall()
        repeat_requests, repeat_bytes = proxy.snapshot()
        cache_after_repeat = cache_stats(repeat_reader)
        repeat_reader.close()

        if first_repeat_requests <= 0:
            raise AssertionError("repeat_reader_first_query_missing_requests")
        if repeat_requests > first_repeat_requests or repeat_bytes > first_repeat_bytes:
            raise AssertionError("repeat_query_cache_regressed")

        missing_path = str(membership[0][0])
        remove_object(missing_path)
        fail_reader = make_connection(proxy.port)
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
    receipt = {
        "result": "pass",
        "contract": "synthetic-iceberg-v2-query-plane",
        "production_data": False,
        "external_account": False,
        "iceberg_format_version": 2,
        "snapshot_id": snapshot_id,
        "metadata_location_scheme": metadata_location.split(":", 1)[0],
        "rows": EXPECTED_ROWS,
        "indicators": INDICATORS,
        "data_files": len(membership),
        "manifest_rows": membership_rows,
        "physical_parquet_bytes": physical_parquet_bytes,
        "queries": {
            "preview_rows": len(preview_rows),
            "complete_count": count,
            "multi_indicator_count": filtered,
            "join_count": joined,
        },
        "profiles": profiles,
        "repeat_preview": {
            "first_requests": first_repeat_requests,
            "first_bytes": first_repeat_bytes,
            "repeat_requests": repeat_requests,
            "repeat_bytes": repeat_bytes,
        },
        "cache": {
            "cold_preview": preview_cache,
            "after_first_repeat_reader_query": cache_after_first,
            "after_second_repeat_reader_query": cache_after_repeat,
        },
        "max_rss_bytes": max_rss_bytes,
        "missing_object_failed_closed": missing_failed_closed,
    }
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
