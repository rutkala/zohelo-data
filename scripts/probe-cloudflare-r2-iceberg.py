#!/usr/bin/env python3
"""Bounded live Cloudflare R2 + Iceberg contract probe.

The probe uses generated data only. It writes one disposable Parquet object to the
configured Landing bucket and one disposable Iceberg namespace/table to the R2
Data Catalog, verifies the query/storage contract, and cleans up its own objects.
It must never read Google Drive or production source payloads.
"""

from __future__ import annotations

import datetime
import hashlib
import hmac
import http.client
import json
import os
import resource
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import duckdb


INDICATORS = 8
ROWS_PER_INDICATOR = 20_000
EXPECTED_ROWS = INDICATORS * ROWS_PER_INDICATOR
FILTER_IDS = (2, 7)
EXPECTED_FILTERED = len(FILTER_IDS) * ROWS_PER_INDICATOR
RANGE_BYTES = 16_384


class PilotError(RuntimeError):
    pass


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise PilotError(f"missing_environment:{name}")
    return value


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def safe_identifier(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    if not value or not (value[0].isalpha() or value[0] == "_"):
        value = "p_" + value
    return value[:120]


def expected_s3_endpoint(account_id: str) -> str:
    return f"https://{account_id}.r2.cloudflarestorage.com"


def validate_configuration(config: dict[str, str]) -> None:
    if not re.fullmatch(r"[0-9a-fA-F]{32}", config["account_id"]):
        raise PilotError("invalid_account_id_shape")
    for field in ("landing_bucket", "lakehouse_bucket"):
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,62}[a-z0-9]", config[field]):
            raise PilotError(f"invalid_bucket_name:{field}")
    endpoint = config["s3_endpoint"].rstrip("/")
    if endpoint != expected_s3_endpoint(config["account_id"]):
        raise PilotError("s3_endpoint_account_mismatch")
    catalog = urlsplit(config["catalog_uri"])
    if catalog.scheme != "https" or not catalog.hostname:
        raise PilotError("invalid_catalog_uri")
    if "cloudflarestorage.com" not in catalog.hostname:
        raise PilotError("unexpected_catalog_host")
    if not config["warehouse"]:
        raise PilotError("empty_warehouse")


def make_r2_connection(config: dict[str, str]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='512MiB'")
    con.execute("SET enable_external_file_cache=true")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(
        f"""
        CREATE SECRET landing_r2 (
            TYPE R2,
            KEY_ID {sql_string(config['access_key_id'])},
            SECRET {sql_string(config['secret_access_key'])},
            ACCOUNT_ID {sql_string(config['account_id'])},
            SCOPE {sql_string('r2://' + config['landing_bucket'])}
        )
        """
    )
    return con


def make_catalog_connection(config: dict[str, str]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='512MiB'")
    con.execute("SET enable_external_file_cache=true")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    con.execute(
        f"""
        CREATE SECRET r2_catalog_token (
            TYPE ICEBERG,
            TOKEN {sql_string(config['catalog_token'])}
        )
        """
    )
    con.execute(
        f"""
        ATTACH {sql_string(config['warehouse'])} AS r2_catalog (
            TYPE ICEBERG,
            SECRET r2_catalog_token,
            ENDPOINT {sql_string(config['catalog_uri'])}
        )
        """
    )
    return con


def sigv4_key(secret: str, date: str, region: str, service: str) -> bytes:
    date_key = hmac.new(("AWS4" + secret).encode(), date.encode(), hashlib.sha256).digest()
    region_key = hmac.new(date_key, region.encode(), hashlib.sha256).digest()
    service_key = hmac.new(region_key, service.encode(), hashlib.sha256).digest()
    return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()


def signed_s3_request(
    config: dict[str, str],
    method: str,
    bucket: str,
    key: str,
    *,
    range_header: str | None = None,
) -> tuple[int, dict[str, str], bytes]:
    endpoint = urlsplit(config["s3_endpoint"])
    host = endpoint.netloc
    canonical_uri = "/" + quote(bucket, safe="-_.~") + "/" + quote(key, safe="/-_.~")
    region = "auto"
    service = "s3"
    now = datetime.datetime.now(datetime.UTC)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()

    signed = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    signed_headers = ";".join(sorted(signed))
    canonical_headers = "".join(f"{name}:{signed[name]}\n" for name in sorted(signed))
    canonical_request = "\n".join(
        [method, canonical_uri, "", canonical_headers, signed_headers, payload_hash]
    )
    scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    signature = hmac.new(
        sigv4_key(config["secret_access_key"], date_stamp, region, service),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={config['access_key_id']}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    headers = {
        "Authorization": authorization,
        "Host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if range_header:
        headers["Range"] = range_header

    connection = http.client.HTTPSConnection(host, timeout=30)
    try:
        connection.request(method, canonical_uri, headers=headers)
        response = connection.getresponse()
        payload = response.read()
        response_headers = {name.lower(): value for name, value in response.getheaders()}
        return int(response.status), response_headers, payload
    finally:
        connection.close()


def header_value(headers: Any, key: str) -> str | None:
    key = key.lower()
    if headers is None:
        return None
    if isinstance(headers, dict):
        for name, value in headers.items():
            if str(name).lower() != key:
                continue
            if isinstance(value, (list, tuple)):
                return str(value[0]) if value else None
            return str(value)
        return None
    if hasattr(headers, "items"):
        return header_value(dict(headers.items()), key)
    if isinstance(headers, (list, tuple)):
        try:
            return header_value(dict(headers), key)
        except (TypeError, ValueError):
            return None
    return None


def http_stats(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = con.execute(
        """
        SELECT request.type, request.url, response.status, response.headers
        FROM duckdb_logs_parsed('HTTP')
        """
    ).fetchall()
    methods: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    categories: Counter[str] = Counter()
    partial_gets = 0
    range_responses = 0
    responses_with_length = 0
    known_response_bytes = 0

    for method, raw_url, status, headers in rows:
        method_text = str(method or "UNKNOWN")
        status_text = str(status or "NONE")
        methods[method_text] += 1
        statuses[status_text] += 1
        hostname = (urlsplit(str(raw_url or "")).hostname or "").lower()
        if hostname == "catalog.cloudflarestorage.com":
            category = "catalog"
        elif hostname.endswith(".r2.cloudflarestorage.com") or hostname.endswith(
            ".cloudflarestorage.com"
        ):
            category = "storage"
        else:
            category = "other"
        categories[category] += 1
        if method_text == "GET" and (
            "206" in status_text or "PartialContent" in status_text
        ):
            partial_gets += 1
        if header_value(headers, "content-range"):
            range_responses += 1
        length = header_value(headers, "content-length")
        if length and length.isdigit():
            responses_with_length += 1
            known_response_bytes += int(length)

    return {
        "requests": len(rows),
        "methods": dict(sorted(methods.items())),
        "statuses": dict(sorted(statuses.items())),
        "categories": dict(sorted(categories.items())),
        "partial_gets": partial_gets,
        "range_responses": range_responses,
        "responses_with_content_length": responses_with_length,
        "known_response_bytes": known_response_bytes,
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


def query_once(
    config: dict[str, str],
    query: str,
) -> tuple[list[tuple], dict[str, Any], dict[str, int]]:
    con = make_catalog_connection(config)
    try:
        con.execute("CALL enable_logging('HTTP', storage_buffer_size = 0)")
        con.execute("CALL truncate_duckdb_logs()")
        started = time.perf_counter()
        rows = con.execute(query).fetchall()
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        stats = http_stats(con)
        stats["elapsed_ms"] = elapsed_ms
        cache = cache_stats(con)
        return rows, stats, cache
    finally:
        con.close()


def write_receipt(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


def main() -> int:
    receipt_path = Path(
        os.environ.get("R2_PILOT_RECEIPT", "cloudflare-r2-pilot-result.json")
    )
    stage = "configuration"
    cleanup = {
        "landing_object_deleted": False,
        "iceberg_table_dropped": False,
        "iceberg_namespace_dropped": False,
    }
    config = {
        "account_id": required_env("CLOUDFLARE_ACCOUNT_ID"),
        "catalog_token": required_env("CLOUDFLARE_R2_CATALOG_TOKEN"),
        "access_key_id": required_env("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        "secret_access_key": required_env("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        "landing_bucket": required_env("R2_LANDING_BUCKET"),
        "lakehouse_bucket": required_env("R2_LAKEHOUSE_BUCKET"),
        "s3_endpoint": required_env("R2_S3_ENDPOINT"),
        "catalog_uri": required_env("R2_CATALOG_URI"),
        "warehouse": required_env("R2_WAREHOUSE"),
    }
    validate_configuration(config)

    run_id = safe_identifier(os.environ.get("GITHUB_RUN_ID", "local"))
    attempt = safe_identifier(os.environ.get("GITHUB_RUN_ATTEMPT", "1"))
    namespace = safe_identifier(f"zohelo_pilot_{run_id}_{attempt}").lower()
    table = f"r2_catalog.{namespace}.dbw_observations"
    landing_key = f"__zohelo_pilot/{run_id}-{attempt}/range-contract.parquet"
    landing_path = f"r2://{config['landing_bucket']}/{landing_key}"

    writer: duckdb.DuckDBPyConnection | None = None
    catalog: duckdb.DuckDBPyConnection | None = None
    primary_error: Exception | None = None
    receipt: dict[str, Any] = {}

    try:
        stage = "landing_write"
        writer = make_r2_connection(config)
        writer.execute(
            f"""
            COPY (
                SELECT
                    i::INTEGER AS indicator_id,
                    row_id::BIGINT AS row_id,
                    (2000 + row_id % 25)::INTEGER AS period_year,
                    md5(i::VARCHAR || ':' || row_id::VARCHAR) AS payload
                FROM range(1, 9) AS indicators(i)
                CROSS JOIN range(0, 31_250) AS rows(row_id)
            ) TO {sql_string(landing_path)} (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE 10_000
            )
            """
        )
        landing_count = int(
            writer.execute(
                f"SELECT count(*) FROM read_parquet({sql_string(landing_path)})"
            ).fetchone()[0]
        )
        if landing_count != 250_000:
            raise PilotError(f"landing_count:{landing_count}")
        writer.close()
        writer = None

        stage = "landing_range_contract"
        head_status, head_headers, _ = signed_s3_request(
            config, "HEAD", config["landing_bucket"], landing_key
        )
        if head_status != 200:
            raise PilotError(f"landing_head_status:{head_status}")
        size_text = head_headers.get("content-length", "")
        if not size_text.isdigit() or int(size_text) <= RANGE_BYTES:
            raise PilotError("landing_object_size_not_observed")
        landing_size = int(size_text)
        range_status, range_headers, range_payload = signed_s3_request(
            config,
            "GET",
            config["landing_bucket"],
            landing_key,
            range_header=f"bytes=0-{RANGE_BYTES - 1}",
        )
        if range_status != 206:
            raise PilotError(f"landing_range_status:{range_status}")
        if len(range_payload) != RANGE_BYTES:
            raise PilotError(f"landing_range_bytes:{len(range_payload)}")
        if not range_headers.get("content-range"):
            raise PilotError("landing_missing_content_range")

        stage = "catalog_write"
        catalog = make_catalog_connection(config)
        catalog.execute(f"CREATE SCHEMA r2_catalog.{namespace}")
        catalog.execute(
            f"""
            CREATE TABLE {table} (
                indicator_id INTEGER,
                unit_id BIGINT,
                period_year INTEGER,
                value DOUBLE,
                payload VARCHAR
            )
            WITH ('format-version' = '2')
            """
        )
        catalog.execute(f"ALTER TABLE {table} SET PARTITIONED BY (indicator_id)")
        catalog.execute(
            f"""
            INSERT INTO {table}
            SELECT
                indicator_id::INTEGER,
                (indicator_id * {ROWS_PER_INDICATOR} + row_id)::BIGINT,
                (2000 + row_id % 25)::INTEGER,
                (indicator_id * 1000000 + row_id)::DOUBLE,
                md5(indicator_id::VARCHAR || ':' || row_id::VARCHAR)
            FROM range(1, {INDICATORS + 1}) AS i(indicator_id)
            CROSS JOIN range(0, {ROWS_PER_INDICATOR}) AS r(row_id)
            """
        )
        catalog_count = int(
            catalog.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        )
        if catalog_count != EXPECTED_ROWS:
            raise PilotError(f"catalog_count:{catalog_count}")

        load_cursor = catalog.execute(
            f"SELECT * FROM iceberg_load_table_response({table})"
        )
        load_columns = [column[0] for column in load_cursor.description]
        load_response = dict(zip(load_columns, load_cursor.fetchone()))
        metadata_location = load_response.get("metadata_location")
        if not isinstance(metadata_location, str) or ":" not in metadata_location:
            raise PilotError("missing_metadata_location")
        metadata_scheme = metadata_location.split(":", 1)[0]

        snapshots = catalog.execute(
            f"SELECT snapshot_id FROM iceberg_snapshots({table}) ORDER BY sequence_number"
        ).fetchall()
        if not snapshots:
            raise PilotError("missing_snapshot")
        snapshot_id = int(snapshots[-1][0])
        membership = catalog.execute(
            f"""
            SELECT file_path, record_count
            FROM iceberg_metadata({table})
            WHERE content = 'EXISTING' AND status <> 'DELETED'
            ORDER BY file_path
            """
        ).fetchall()
        manifest_rows = sum(int(row[1]) for row in membership)
        if not membership or manifest_rows != EXPECTED_ROWS:
            raise PilotError(f"manifest_rows:{manifest_rows}")
        schema_before = catalog.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()
        catalog.close()
        catalog = None

        stage = "catalog_read_contract"
        queries = {
            "preview": f"SELECT * FROM {table} LIMIT 1000",
            "count": f"SELECT count(*) FROM {table}",
            "multi_indicator": (
                f"SELECT count(*) FROM {table} "
                f"WHERE indicator_id IN ({FILTER_IDS[0]}, {FILTER_IDS[1]})"
            ),
            "join": (
                f"SELECT count(*) FROM {table} AS o "
                f"JOIN (VALUES ({FILTER_IDS[0]}, 'two'), ({FILTER_IDS[1]}, 'seven')) "
                "AS d(indicator_id, label) USING (indicator_id)"
            ),
        }
        preview_rows, preview_profile, preview_cache = query_once(
            config, queries["preview"]
        )
        count_rows, count_profile, _ = query_once(config, queries["count"])
        filtered_rows, filtered_profile, _ = query_once(
            config, queries["multi_indicator"]
        )
        join_rows, join_profile, _ = query_once(config, queries["join"])

        count = int(count_rows[0][0])
        filtered = int(filtered_rows[0][0])
        joined = int(join_rows[0][0])
        if len(preview_rows) != 1000:
            raise PilotError(f"preview_rows:{len(preview_rows)}")
        if count != EXPECTED_ROWS:
            raise PilotError(f"complete_count:{count}")
        if filtered != EXPECTED_FILTERED:
            raise PilotError(f"filtered_count:{filtered}")
        if joined != EXPECTED_FILTERED:
            raise PilotError(f"join_count:{joined}")
        if int(preview_profile["partial_gets"]) <= 0:
            raise PilotError("preview_missing_partial_get")
        if int(filtered_profile["partial_gets"]) <= 0:
            raise PilotError("filter_missing_partial_get")

        stage = "snapshot_consistency"
        verify = make_catalog_connection(config)
        try:
            schema_after = verify.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()
            verify_snapshot = verify.execute(
                f"SELECT snapshot_id FROM iceberg_snapshots({table}) ORDER BY sequence_number"
            ).fetchall()
            verify_membership = verify.execute(
                f"""
                SELECT file_path, record_count
                FROM iceberg_metadata({table})
                WHERE content = 'EXISTING' AND status <> 'DELETED'
                ORDER BY file_path
                """
            ).fetchall()
            if schema_after != schema_before:
                raise PilotError("schema_disagreement")
            if not verify_snapshot or int(verify_snapshot[-1][0]) != snapshot_id:
                raise PilotError("snapshot_disagreement")
            if verify_membership != membership:
                raise PilotError("membership_disagreement")
        finally:
            verify.close()

        stage = "cache_contract"
        repeat = make_catalog_connection(config)
        try:
            repeat.execute("CALL enable_logging('HTTP', storage_buffer_size = 0)")
            repeat.execute("CALL truncate_duckdb_logs()")
            repeat.execute(queries["preview"]).fetchall()
            first_http = http_stats(repeat)
            cache_after_first = cache_stats(repeat)
            repeat.execute("CALL truncate_duckdb_logs()")
            repeat.execute(queries["preview"]).fetchall()
            second_http = http_stats(repeat)
            cache_after_second = cache_stats(repeat)
            if int(second_http["known_response_bytes"]) > int(
                first_http["known_response_bytes"]
            ):
                raise PilotError("repeat_query_transfer_regressed")
        finally:
            repeat.close()

        receipt = {
            "result": "pass",
            "contract": "cloudflare-r2-iceberg-v2-live-pilot",
            "production_data": False,
            "google_drive_accessed": False,
            "iceberg_format_version": 2,
            "landing": {
                "bucket": config["landing_bucket"],
                "rows": landing_count,
                "object_bytes": landing_size,
                "head_status": head_status,
                "range_status": range_status,
                "range_bytes": len(range_payload),
                "content_range_present": True,
            },
            "lakehouse": {
                "bucket": config["lakehouse_bucket"],
                "rows": EXPECTED_ROWS,
                "indicators": INDICATORS,
                "data_files": len(membership),
                "manifest_rows": manifest_rows,
                "snapshot_id": str(snapshot_id),
                "metadata_location_scheme": metadata_scheme,
            },
            "queries": {
                "preview_rows": len(preview_rows),
                "complete_count": count,
                "multi_indicator_count": filtered,
                "join_count": joined,
            },
            "http_profiles": {
                "preview": preview_profile,
                "count": count_profile,
                "multi_indicator": filtered_profile,
                "join": join_profile,
                "repeat_first": first_http,
                "repeat_second": second_http,
            },
            "cache": {
                "cold_preview": preview_cache,
                "after_first_repeat": cache_after_first,
                "after_second_repeat": cache_after_second,
            },
            "max_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        }
    except Exception as exc:  # receipt deliberately omits raw provider errors/URLs
        primary_error = exc
        receipt = {
            "result": "fail",
            "contract": "cloudflare-r2-iceberg-v2-live-pilot",
            "production_data": False,
            "google_drive_accessed": False,
            "stage": stage,
            "category": type(exc).__name__,
        }
    finally:
        if writer is not None:
            writer.close()
        if catalog is not None:
            catalog.close()

        try:
            cleanup_con = make_catalog_connection(config)
            try:
                cleanup_con.execute(f"DROP TABLE IF EXISTS {table}")
                cleanup["iceberg_table_dropped"] = True
                cleanup_con.execute(f"DROP SCHEMA IF EXISTS r2_catalog.{namespace}")
                cleanup["iceberg_namespace_dropped"] = True
            finally:
                cleanup_con.close()
        except Exception:
            pass

        try:
            delete_status, _, _ = signed_s3_request(
                config, "DELETE", config["landing_bucket"], landing_key
            )
            cleanup["landing_object_deleted"] = delete_status in (200, 204)
        except Exception:
            pass

        receipt["cleanup"] = cleanup
        cleanup_ok = all(cleanup.values())
        if receipt.get("result") == "pass" and not cleanup_ok:
            receipt["result"] = "fail"
            receipt["stage"] = "cleanup"
            receipt["category"] = "PilotCleanupError"
        write_receipt(receipt_path, receipt)
        print(json.dumps(receipt, sort_keys=True))

    return 0 if receipt.get("result") == "pass" and primary_error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
