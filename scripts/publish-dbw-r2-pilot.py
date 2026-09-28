#!/usr/bin/env python3
"""Publish the exact reviewed retained DBW Bronze observations as an R2 Iceberg pilot.

This is a serving-copy migration pilot. It reads only the already-restored,
byte-verified DBW package prepared by prepare_retained_dbw_release.py. It never
writes Google Drive and it never changes a Drive release/current pointer.

The target is deliberately a non-production pilot namespace. A partial successful
run is resumable: committed Iceberg indicators remain in the pilot table and a
later run inserts only missing indicator partitions after verifying the source
inventory identity again.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import resource
import sys
import time
from typing import Any
from urllib.parse import urlsplit

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from retained_dbw_publication import EXPECTED_SCHEMAS, REVIEWED_INVENTORY_SHA256


EXPECTED_ROWS = 879_999_727
EXPECTED_INDICATORS = 1_550
EXPECTED_REMOTE_BYTES = 4_803_673_234
PILOT_SCHEMA = "zohelo_pilot_dbw"
PILOT_TABLE = "gus_dbw_observations"
BATCH_INDICATORS = 50


class PilotError(RuntimeError):
    pass


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise PilotError(f"missing_environment:{name}")
    return value


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def attach_catalog() -> duckdb.DuckDBPyConnection:
    token = required_env("CLOUDFLARE_R2_CATALOG_TOKEN")
    catalog_uri = required_env("R2_CATALOG_URI")
    warehouse = required_env("R2_WAREHOUSE")
    parsed = urlsplit(catalog_uri)
    if parsed.scheme != "https" or not parsed.hostname or "cloudflarestorage.com" not in parsed.hostname:
        raise PilotError("invalid_catalog_uri")

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
            TOKEN {sql_string(token)}
        )
        """
    )
    con.execute(
        f"""
        ATTACH {sql_string(warehouse)} AS r2_catalog (
            TYPE ICEBERG,
            SECRET r2_catalog_token,
            ENDPOINT {sql_string(catalog_uri)}
        )
        """
    )
    return con


def schema_sql() -> str:
    mapping = {
        "BIGINT": "BIGINT",
        "INTEGER": "INTEGER",
        "VARCHAR": "VARCHAR",
        "DOUBLE": "DOUBLE",
    }
    return ",\n".join(
        f'                "{name}" {mapping[kind]}'
        for name, kind in EXPECTED_SCHEMAS["observations"]
    )


def expected_describe() -> list[tuple[str, str]]:
    return EXPECTED_SCHEMAS["observations"]


def load_restore(output: Path) -> tuple[dict[str, Any], dict[str, Any], Path]:
    receipt_path = output / "restore-evidence.json"
    inventory_path = output / "audit" / "descriptor-inventory.json"
    if not receipt_path.is_file() or not inventory_path.is_file():
        raise PilotError("reviewed_restore_package_missing")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if (
        receipt.get("status") != "verified"
        or receipt.get("read_only") is not True
        or receipt.get("inventory_sha256") != REVIEWED_INVENTORY_SHA256
        or int(receipt.get("verified_objects", -1)) != 3103
        or int(receipt.get("verified_bytes", -1)) != EXPECTED_REMOTE_BYTES
        or int((receipt.get("measured_parquet_rows") or {}).get("observations", -1)) != EXPECTED_ROWS
        or inventory.get("inventory_sha256") != REVIEWED_INVENTORY_SHA256
        or int(inventory.get("object_count", -1)) != 3103
        or int(inventory.get("total_bytes", -1)) != EXPECTED_REMOTE_BYTES
    ):
        raise PilotError("reviewed_restore_identity_mismatch")
    release_root = Path(receipt["release_root"])
    if not release_root.is_dir():
        raise PilotError("reviewed_release_root_missing")
    return receipt, inventory, release_root


def observation_inputs(inventory: dict[str, Any], release_root: Path) -> list[tuple[int, Path, dict[str, Any]]]:
    result: list[tuple[int, Path, dict[str, Any]]] = []
    pattern = re.compile(r"^observations/part_(\d+)\.parquet$")
    for item in inventory["objects"]:
        match = pattern.fullmatch(str(item.get("path", "")))
        if not match:
            continue
        indicator = int(match.group(1))
        path = release_root / item["path"]
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != int(item["size"])
        ):
            raise PilotError(f"restored_observation_changed:{indicator}")
        result.append((indicator, path, item))
    result.sort(key=lambda value: value[0])
    if len(result) != EXPECTED_INDICATORS or len({item[0] for item in result}) != EXPECTED_INDICATORS:
        raise PilotError("observation_membership_incomplete")
    return result


def table_name() -> str:
    return f"r2_catalog.{PILOT_SCHEMA}.{PILOT_TABLE}"


def table_exists(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute(f"DESCRIBE SELECT * FROM {table_name()}").fetchall()
        return True
    except duckdb.Error:
        return False


def validate_table_schema(con: duckdb.DuckDBPyConnection) -> None:
    rows = con.execute(f"DESCRIBE SELECT * FROM {table_name()}").fetchall()
    observed = [(row[0], row[1]) for row in rows]
    if observed != expected_describe():
        raise PilotError("pilot_table_schema_mismatch")


def create_or_open_table(con: duckdb.DuckDBPyConnection) -> bool:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS r2_catalog.{PILOT_SCHEMA}")
    created = False
    if not table_exists(con):
        con.execute(
            f"""
            CREATE TABLE {table_name()} (
{schema_sql()}
            )
            WITH ('format-version' = '2')
            """
        )
        con.execute(f"ALTER TABLE {table_name()} SET PARTITIONED BY (indicator_id)")
        created = True
    validate_table_schema(con)
    return created


def existing_indicator_counts(con: duckdb.DuckDBPyConnection) -> dict[int, int]:
    rows = con.execute(
        f"SELECT indicator_id, count(*) FROM {table_name()} GROUP BY indicator_id ORDER BY indicator_id"
    ).fetchall()
    result: dict[int, int] = {}
    for indicator, count in rows:
        indicator = int(indicator)
        count = int(count)
        if indicator in result or count <= 0:
            raise PilotError("pilot_existing_indicator_invalid")
        result[indicator] = count
    return result


def local_indicator_rows(path: Path, indicator: int) -> int:
    con = duckdb.connect()
    try:
        rows, mismatched = con.execute(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE indicator_id IS DISTINCT FROM $indicator)
            FROM read_parquet($path)
            """,
            {"path": str(path), "indicator": indicator},
        ).fetchone()
    finally:
        con.close()
    if int(rows) <= 0 or int(mismatched) != 0:
        raise PilotError(f"source_indicator_binding_invalid:{indicator}")
    return int(rows)


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
    known_bytes = 0
    for method, raw_url, status, headers in rows:
        method_text = str(method or "UNKNOWN")
        status_text = str(status or "NONE")
        methods[method_text] += 1
        statuses[status_text] += 1
        host = (urlsplit(str(raw_url or "")).hostname or "").lower()
        if host == "catalog.cloudflarestorage.com":
            category = "catalog"
        elif host.endswith(".cloudflarestorage.com"):
            category = "storage"
        else:
            category = "other"
        categories[category] += 1
        if method_text == "GET" and ("206" in status_text or "PartialContent" in status_text):
            partial_gets += 1
        if isinstance(headers, dict):
            length = next(
                (str(value) for key, value in headers.items() if str(key).lower() == "content-length"),
                "",
            )
            if length.isdigit():
                known_bytes += int(length)
    return {
        "requests": len(rows),
        "methods": dict(sorted(methods.items())),
        "statuses": dict(sorted(statuses.items())),
        "categories": dict(sorted(categories.items())),
        "partial_gets": partial_gets,
        "known_response_bytes": known_bytes,
    }


def measured_query(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[list[tuple], dict[str, Any]]:
    con.execute("CALL enable_logging('HTTP', storage_buffer_size = 0)")
    con.execute("CALL truncate_duckdb_logs()")
    started = time.perf_counter()
    rows = con.execute(sql).fetchall()
    stats = http_stats(con)
    stats["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
    return rows, stats


def write_receipt(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restore", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()

    stage = "restore_validation"
    receipt: dict[str, Any] = {
        "result": "running",
        "contract": "dbw-r2-real-serving-copy-pilot",
        "source_inventory_sha256": REVIEWED_INVENTORY_SHA256,
        "drive_write_performed": False,
        "production_pointer_changed": False,
    }
    write_receipt(args.receipt, receipt)

    try:
        restore, inventory, release_root = load_restore(args.restore)
        inputs = observation_inputs(inventory, release_root)
        source_observation_bytes = sum(int(item[2]["size"]) for item in inputs)
        if source_observation_bytes != 4_737_200_817:
            raise PilotError(f"unexpected_observation_bytes:{source_observation_bytes}")

        stage = "catalog_attach"
        con = attach_catalog()
        try:
            created = create_or_open_table(con)
            existing = existing_indicator_counts(con)
            source_rows: dict[int, int] = {}
            for indicator, path, _descriptor in inputs:
                if indicator in existing:
                    continue
                source_rows[indicator] = local_indicator_rows(path, indicator)

            stage = "iceberg_copy"
            missing = [(indicator, path) for indicator, path, _ in inputs if indicator not in existing]
            for offset in range(0, len(missing), BATCH_INDICATORS):
                batch = missing[offset : offset + BATCH_INDICATORS]
                paths = [str(path) for _indicator, path in batch]
                con.execute(
                    f"INSERT INTO {table_name()} SELECT * FROM read_parquet($paths, union_by_name=false)",
                    {"paths": paths},
                )
                completed = min(offset + len(batch), len(missing))
                print(
                    json.dumps(
                        {
                            "operation": "dbw_r2_pilot_copy",
                            "inserted_missing_indicators": completed,
                            "missing_indicators_at_start": len(missing),
                            "batch_size": len(batch),
                        }
                    ),
                    flush=True,
                )

            stage = "table_acceptance"
            final_counts = existing_indicator_counts(con)
            if len(final_counts) != EXPECTED_INDICATORS:
                raise PilotError(f"pilot_indicator_count:{len(final_counts)}")

            complete_count_rows, count_http = measured_query(
                con, f"SELECT count(*) FROM {table_name()}"
            )
            complete_count = int(complete_count_rows[0][0])
            if complete_count != EXPECTED_ROWS:
                raise PilotError(f"pilot_complete_count:{complete_count}")

            filter_ids = (inputs[0][0], inputs[-1][0])
            expected_filter = 0
            for indicator in filter_ids:
                source = next(path for value, path, _ in inputs if value == indicator)
                expected_filter += local_indicator_rows(source, indicator)

            preview_rows, preview_http = measured_query(
                con, f"SELECT * FROM {table_name()} LIMIT 1000"
            )
            filtered_rows, filter_http = measured_query(
                con,
                f"SELECT count(*) FROM {table_name()} "
                f"WHERE indicator_id IN ({filter_ids[0]}, {filter_ids[1]})",
            )
            join_rows, join_http = measured_query(
                con,
                f"SELECT count(*) FROM {table_name()} o "
                f"JOIN (VALUES ({filter_ids[0]}), ({filter_ids[1]})) d(indicator_id) "
                "USING (indicator_id)",
            )
            if len(preview_rows) != 1000:
                raise PilotError(f"pilot_preview_rows:{len(preview_rows)}")
            if int(filtered_rows[0][0]) != expected_filter:
                raise PilotError("pilot_filter_count_mismatch")
            if int(join_rows[0][0]) != expected_filter:
                raise PilotError("pilot_join_count_mismatch")
            if int(preview_http["partial_gets"]) <= 0:
                raise PilotError("pilot_preview_missing_partial_get")
            if int(filter_http["partial_gets"]) <= 0:
                raise PilotError("pilot_filter_missing_partial_get")

            snapshots = con.execute(
                f"SELECT snapshot_id FROM iceberg_snapshots({table_name()}) ORDER BY sequence_number"
            ).fetchall()
            if not snapshots:
                raise PilotError("pilot_snapshot_missing")
            snapshot_id = str(int(snapshots[-1][0]))
            membership = con.execute(
                f"""
                SELECT count(*), coalesce(sum(record_count), 0)
                FROM iceberg_metadata({table_name()})
                WHERE content = 'EXISTING' AND status <> 'DELETED'
                """
            ).fetchone()
            if int(membership[1]) != EXPECTED_ROWS:
                raise PilotError("pilot_manifest_rows_mismatch")

            receipt = {
                "result": "pass",
                "contract": "dbw-r2-real-serving-copy-pilot",
                "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_inventory_sha256": REVIEWED_INVENTORY_SHA256,
                "source_verified_objects": int(restore["verified_objects"]),
                "source_verified_bytes": int(restore["verified_bytes"]),
                "source_observation_bytes": source_observation_bytes,
                "source_observation_rows": EXPECTED_ROWS,
                "source_indicator_partitions": EXPECTED_INDICATORS,
                "source_limitations": [
                    "Reviewed retained inventory is not proof of current DBW provider completeness.",
                    "Legacy receipt membership does not prove native-to-Bronze value lineage.",
                ],
                "target": {
                    "catalog": "r2_data_catalog",
                    "schema": PILOT_SCHEMA,
                    "table": PILOT_TABLE,
                    "iceberg_format_version": 2,
                    "snapshot_id": snapshot_id,
                    "data_files": int(membership[0]),
                    "manifest_rows": int(membership[1]),
                    "created_in_this_run": created,
                    "retained_for_portal_pilot": True,
                },
                "queries": {
                    "preview_rows": len(preview_rows),
                    "complete_count": complete_count,
                    "filter_ids": list(filter_ids),
                    "filtered_count": int(filtered_rows[0][0]),
                    "join_count": int(join_rows[0][0]),
                    "expected_filtered_count": expected_filter,
                },
                "http_profiles": {
                    "preview": preview_http,
                    "complete_count": count_http,
                    "multi_indicator": filter_http,
                    "join": join_http,
                },
                "max_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                "drive_write_performed": False,
                "production_pointer_changed": False,
            }
        finally:
            con.close()
    except Exception as exc:
        receipt = {
            "result": "fail",
            "contract": "dbw-r2-real-serving-copy-pilot",
            "stage": stage,
            "category": type(exc).__name__,
            "source_inventory_sha256": REVIEWED_INVENTORY_SHA256,
            "drive_write_performed": False,
            "production_pointer_changed": False,
        }
        write_receipt(args.receipt, receipt)
        print(json.dumps(receipt, sort_keys=True))
        raise

    write_receipt(args.receipt, receipt)
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
