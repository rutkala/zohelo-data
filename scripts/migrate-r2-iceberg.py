#!/usr/bin/env python3
"""Consolidate the existing Zohelo R2 lakehouse into production Iceberg tables.

This is a non-destructive, idempotent cutover:
- source objects are never deleted or rewritten;
- existing Parquet files are committed as Iceberg DataFiles without rewriting them;
- legacy current/release metadata is used only to select the latest legacy
  release during this one-time consolidation;
- duplicate DBW query_parts/retained copies are deliberately excluded;
- production namespaces are bronze, silver and gold.

After production DBW acceptance succeeds, the two old pilot catalog tables are
dropped from the catalog. Their storage is not purged by this script.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import PurePosixPath, Path
import re
import time
from typing import Any, Iterable
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pyarrow.fs as pafs
from pyarrow.fs import AwsStandardS3RetryStrategy
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import TableProperties
from pyiceberg.typedef import Record
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)

RELEASE_SOURCES = ("bdl", "eurostat", "nbp", "wdi")
PRODUCTION_NAMESPACES = ("bronze", "silver", "gold")
DBW_EXPECTED_FILES = 1550
DBW_EXPECTED_ROWS = 879_999_727
ADD_FILES_BATCH = 200


class MigrationError(RuntimeError):
    pass


@dataclass
class ObjectInfo:
    key: str
    size: int

    @property
    def uri(self) -> str:
        return f"s3://{required('R2_LAKEHOUSE_BUCKET')}/{self.key}"


@dataclass
class Group:
    namespace: str
    table: str
    origin: str
    objects: list[ObjectInfo] = field(default_factory=list)

    @property
    def identifier(self) -> tuple[str, str]:
        return self.namespace, self.table

    @property
    def expected_paths(self) -> set[str]:
        return {item.uri for item in self.objects}

    @property
    def expected_bytes(self) -> int:
        return sum(item.size for item in self.objects)


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise MigrationError(f"missing_environment:{name}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name("." + path.name + ".tmp")
    temp.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def s3_client() -> Any:
    endpoint = required("R2_S3_ENDPOINT").rstrip("/")
    parsed = urlsplit(endpoint)
    if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(
        ".r2.cloudflarestorage.com"
    ):
        raise MigrationError("invalid_r2_endpoint")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="auto",
        aws_access_key_id=required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts": 6, "mode": "standard"}),
    )


def arrow_fs() -> pafs.S3FileSystem:
    endpoint = urlsplit(required("R2_S3_ENDPOINT"))
    if not endpoint.hostname:
        raise MigrationError("invalid_r2_endpoint_host")
    return pafs.S3FileSystem(
        access_key=required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        secret_key=required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        region="auto",
        endpoint_override=endpoint.netloc,
        scheme="https",
        retry_strategy=AwsStandardS3RetryStrategy(max_attempts=8),
    )


def catalog() -> RestCatalog:
    uri = required("R2_CATALOG_URI")
    parsed = urlsplit(uri)
    if parsed.scheme != "https" or not parsed.hostname or "cloudflarestorage.com" not in parsed.hostname:
        raise MigrationError("invalid_catalog_uri")
    return RestCatalog(
        name="zohelo_r2",
        warehouse=required("R2_WAREHOUSE"),
        uri=uri,
        token=required("R2_DATA_CATALOG_TOKEN"),
        **{
            "s3.access-key-id": required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
            "s3.secret-access-key": required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
            "s3.endpoint": required("R2_S3_ENDPOINT"),
            "s3.region": "auto",
        },
    )


def get_json(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = int(exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) or 0)
        if status == 404 or code in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise
    value = json.loads(body)
    if not isinstance(value, dict):
        raise MigrationError(f"json_object_expected:{key}")
    return value


def list_objects(client: Any, bucket: str, prefix: str) -> list[ObjectInfo]:
    result: list[ObjectInfo] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            size = int(item.get("Size", 0))
            if key.endswith("/") and size == 0:
                continue
            result.append(ObjectInfo(key=key, size=size))
    return result


def table_name(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9_]+", "_", value).strip("_")
    value = re.sub(r"_+", "_", value)
    if not value or not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise MigrationError(f"invalid_table_name:{value}")
    return value


def parquet_stem(key: str) -> str:
    name = PurePosixPath(key).name
    if not name.lower().endswith(".parquet"):
        raise MigrationError(f"not_parquet:{key}")
    stem = name[:-8]
    if "--" in stem:
        stem = stem.split("--", 1)[0]
    return stem


def normalize_direct_stem(stem: str) -> str:
    for prefix in ("bronze_", "br_"):
        if stem.startswith(prefix):
            stem = stem[len(prefix) :]
            break
    return table_name(stem)


def classify_release_key(key: str) -> tuple[str, str]:
    stem = parquet_stem(key)
    if stem.startswith("bronze_"):
        return "bronze", table_name(stem[len("bronze_") :])
    if stem.startswith(("dim_", "fact_", "mart_")):
        return "gold", table_name(stem)
    return "silver", table_name(stem)


def add_group(
    groups: dict[tuple[str, str], Group],
    *,
    namespace: str,
    table: str,
    objects: Iterable[ObjectInfo],
    origin: str,
    prefer_existing: bool = True,
) -> None:
    objects = sorted(objects, key=lambda item: item.key)
    if not objects:
        return
    identifier = (namespace, table)
    existing = groups.get(identifier)
    if existing is not None:
        existing_paths = {item.key for item in existing.objects}
        new_paths = {item.key for item in objects}
        if existing_paths == new_paths:
            return
        if prefer_existing:
            print(
                json.dumps(
                    {
                        "operation": "source_preference",
                        "table": f"{namespace}.{table}",
                        "kept": existing.origin,
                        "ignored": origin,
                        "ignored_files": len(objects),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            return
        raise MigrationError(f"conflicting_source_groups:{namespace}.{table}")
    groups[identifier] = Group(
        namespace=namespace,
        table=table,
        origin=origin,
        objects=list(objects),
    )


def discover_release_groups(
    client: Any, bucket: str, groups: dict[tuple[str, str], Group]
) -> dict[str, str]:
    selected: dict[str, str] = {}
    for source in RELEASE_SOURCES:
        pointer_key = f"releases/{source}/current-release.json"
        pointer = get_json(client, bucket, pointer_key)
        if pointer is None:
            continue
        release_id = str(pointer.get("release_id", "")).strip()
        if not re.fullmatch(r"[0-9a-fA-F-]{36}", release_id):
            raise MigrationError(f"invalid_release_id:{source}:{release_id}")
        selected[source] = release_id
        prefix = f"releases/{source}/{release_id}/"
        objects = [item for item in list_objects(client, bucket, prefix) if item.key.lower().endswith(".parquet")]
        if not objects:
            raise MigrationError(f"current_release_has_no_parquet:{source}:{release_id}")
        by_table: dict[tuple[str, str], list[ObjectInfo]] = {}
        for item in objects:
            namespace, name = classify_release_key(item.key)
            by_table.setdefault((namespace, name), []).append(item)
        for (namespace, name), items in sorted(by_table.items()):
            add_group(
                groups,
                namespace=namespace,
                table=name,
                objects=items,
                origin=f"legacy_current_release:{source}:{release_id}",
            )
    return selected


def discover_direct_bronze(
    client: Any, bucket: str, groups: dict[tuple[str, str], Group]
) -> dict[str, int]:
    objects = [
        item
        for item in list_objects(client, bucket, "02_bronze/")
        if item.key.lower().endswith(".parquet")
        and "/current/" not in item.key
        and "/query_parts/" not in item.key
        and "/retained/" not in item.key
    ]
    parents: dict[str, list[ObjectInfo]] = {}
    for item in objects:
        parents.setdefault(str(PurePosixPath(item.key).parent), []).append(item)

    stats = {"objects": len(objects), "bytes": sum(item.size for item in objects), "groups": 0}
    for parent, items in sorted(parents.items()):
        parts = parent.split("/")
        rel = parts[1:]
        if not rel:
            continue

        # DBW reviewed direct Bronze.
        if rel[0] == "gus_dbw":
            subtype = rel[1] if len(rel) > 1 else ""
            if subtype == "observations":
                add_group(
                    groups,
                    namespace="bronze",
                    table="dbw_observations",
                    objects=items,
                    origin=parent,
                )
                stats["groups"] += 1
                continue
            if len(items) == 1:
                add_group(
                    groups,
                    namespace="bronze",
                    table=normalize_direct_stem(parquet_stem(items[0].key)),
                    objects=items,
                    origin=parent,
                )
                stats["groups"] += 1
                continue

        # OpenData has one logical table per nested directory.
        if rel[0] == "opendata_org" and len(rel) >= 2:
            add_group(
                groups,
                namespace="bronze",
                table=table_name("opendata_org_" + rel[-1]),
                objects=items,
                origin=parent,
            )
            stats["groups"] += 1
            continue

        stems = [parquet_stem(item.key) for item in items]
        # Folders such as GLEIF/PRG/TERYT/white-list contain several distinct
        # one-file tables rather than parts of one table.
        if len(items) > 1 and all(stem.startswith("br_") for stem in stems):
            for item in items:
                add_group(
                    groups,
                    namespace="bronze",
                    table=normalize_direct_stem(parquet_stem(item.key)),
                    objects=[item],
                    origin=parent,
                )
                stats["groups"] += 1
            continue

        # Ordinary multi-file tables (for example NBP time chunks). Release
        # based tables were added first and therefore win on name collisions.
        candidate = table_name(rel[-1])
        add_group(
            groups,
            namespace="bronze",
            table=candidate,
            objects=items,
            origin=parent,
        )
        stats["groups"] += 1
    return stats


def discover_direct_layer(
    client: Any,
    bucket: str,
    prefix: str,
    namespace: str,
    groups: dict[tuple[str, str], Group],
) -> dict[str, int]:
    objects = [
        item
        for item in list_objects(client, bucket, prefix)
        if item.key.lower().endswith(".parquet") and "/current/" not in item.key
    ]
    parents: dict[str, list[ObjectInfo]] = {}
    for item in objects:
        parents.setdefault(str(PurePosixPath(item.key).parent), []).append(item)
    for parent, items in sorted(parents.items()):
        if len(items) == 1:
            name = normalize_direct_stem(parquet_stem(items[0].key))
        else:
            name = table_name(PurePosixPath(parent).name)
        add_group(
            groups,
            namespace=namespace,
            table=name,
            objects=items,
            origin=parent,
        )
    return {
        "objects": len(objects),
        "bytes": sum(item.size for item in objects),
        "groups": len(parents),
    }


def discover_non_table_medallion(client: Any, bucket: str) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for prefix in ("02_bronze/", "03_silver/", "04_gold/"):
        counts: dict[str, list[int]] = {}
        for item in list_objects(client, bucket, prefix):
            if item.key.lower().endswith(".parquet") or "/current/" in item.key:
                continue
            suffix = PurePosixPath(item.key).suffix.lower() or "<none>"
            counts.setdefault(suffix, [0, 0])
            counts[suffix][0] += 1
            counts[suffix][1] += item.size
        result[prefix.rstrip("/")] = {
            suffix: {"objects": values[0], "bytes": values[1]}
            for suffix, values in sorted(counts.items())
        }
    return result


def ensure_namespace(cat: RestCatalog, namespace: str) -> None:
    try:
        cat.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass


def read_arrow_schema(fs: pafs.S3FileSystem, bucket: str, key: str):
    return pq.read_schema(f"{bucket}/{key}", filesystem=fs)


def parquet_footer(
    fs: pafs.S3FileSystem, bucket: str, item: ObjectInfo
) -> pq.FileMetaData:
    last_error: Exception | None = None
    for attempt in range(1, 7):
        try:
            metadata = pq.read_metadata(f"{bucket}/{item.key}", filesystem=fs)
            if metadata.num_rows <= 0:
                raise MigrationError(f"parquet_file_empty:{item.key}")
            return metadata
        except OSError as exc:
            last_error = exc
            if attempt >= 6:
                break
            delay = min(2 ** (attempt - 1), 16)
            print(
                json.dumps(
                    {
                        "operation": "parquet_footer_retry",
                        "key": item.key,
                        "attempt": attempt,
                        "next_delay_seconds": delay,
                        "error": str(exc)[:300],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            time.sleep(delay)
    raise MigrationError(
        f"parquet_footer_network_failure:{item.key}:{type(last_error).__name__}:"
        f"{str(last_error)[:300]}"
    )


def minimal_data_file(
    *,
    table: Any,
    item: ObjectInfo,
    metadata: pq.FileMetaData,
) -> DataFile:
    return DataFile.from_args(
        _table_format_version=int(table.metadata.format_version),
        content=DataFileContent.DATA,
        file_path=item.uri,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=int(metadata.num_rows),
        file_size_in_bytes=item.size,
        spec_id=int(table.metadata.default_spec_id),
    )


def append_existing_parquet_batch(
    *,
    table: Any,
    fs: pafs.S3FileSystem,
    bucket: str,
    items: list[ObjectInfo],
    expected_schema: Any,
    snapshot_properties: dict[str, str],
) -> tuple[int, int]:
    data_files: list[DataFile] = []
    rows = 0
    for item in items:
        metadata = parquet_footer(fs, bucket, item)
        schema = metadata.schema.to_arrow_schema()
        if not schema.equals(expected_schema, check_metadata=False):
            raise MigrationError(f"parquet_schema_mismatch:{item.key}")
        rows += int(metadata.num_rows)
        data_files.append(minimal_data_file(table=table, item=item, metadata=metadata))

    with table.transaction() as tx:
        if tx.table_metadata.name_mapping() is None:
            tx.set_properties(
                **{
                    TableProperties.DEFAULT_NAME_MAPPING:
                    tx.table_metadata.schema().name_mapping.model_dump_json()
                }
            )
        with tx.update_snapshot(snapshot_properties=snapshot_properties).fast_append() as append:
            for data_file in data_files:
                append.append_data_file(data_file)
    return len(data_files), rows


def create_or_load_table(
    cat: RestCatalog,
    fs: pafs.S3FileSystem,
    group: Group,
    bucket: str,
):
    ensure_namespace(cat, group.namespace)
    identifier = group.identifier
    try:
        table = cat.load_table(identifier)
    except NoSuchTableError:
        schema = read_arrow_schema(fs, bucket, group.objects[0].key)
        try:
            table = cat.create_table(identifier, schema=schema)
        except TableAlreadyExistsError:
            table = cat.load_table(identifier)

    if int(table.metadata.format_version) < 2:
        with table.transaction() as tx:
            tx.upgrade_table_version(2)
        table = cat.load_table(identifier)
    if int(table.metadata.format_version) != 2:
        raise MigrationError(
            f"unexpected_iceberg_format_version:{group.namespace}.{group.table}:"
            f"{table.metadata.format_version}"
        )

    return table


def table_files(table: Any) -> dict[str, tuple[int, int]]:
    files: dict[str, tuple[int, int]] = {}
    for task in table.scan().plan_files():
        path = str(task.file.file_path)
        if path in files:
            raise MigrationError(f"duplicate_iceberg_data_file:{path}")
        files[path] = (int(task.file.record_count), int(task.file.file_size_in_bytes))
    return files


def migrate_group(
    cat: RestCatalog,
    fs: pafs.S3FileSystem,
    bucket: str,
    group: Group,
) -> dict[str, Any]:
    table = create_or_load_table(cat, fs, group, bucket)
    expected = group.expected_paths
    existing = table_files(table)
    unexpected = set(existing) - expected
    if unexpected:
        raise MigrationError(
            f"production_table_has_unexpected_files:{group.namespace}.{group.table}:"
            f"{len(unexpected)}"
        )

    missing = sorted(expected - set(existing))
    object_by_uri = {item.uri: item for item in group.objects}
    reference_schema = parquet_footer(
        fs, bucket, group.objects[0]
    ).schema.to_arrow_schema()
    for offset in range(0, len(missing), ADD_FILES_BATCH):
        batch_paths = missing[offset : offset + ADD_FILES_BATCH]
        batch_items = [object_by_uri[path] for path in batch_paths]
        appended_files, appended_rows = append_existing_parquet_batch(
            table=table,
            fs=fs,
            bucket=bucket,
            items=batch_items,
            expected_schema=reference_schema,
            snapshot_properties={
                "zohelo.migration": "r2-production-iceberg-cutover",
                "zohelo.source": group.origin,
            },
        )
        print(
            json.dumps(
                {
                    "operation": "iceberg_append_existing_parquet",
                    "table": f"{group.namespace}.{group.table}",
                    "batch_files": appended_files,
                    "batch_rows": appended_rows,
                    "completed_missing_files": min(offset + appended_files, len(missing)),
                    "missing_files_at_start": len(missing),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        table = cat.load_table(group.identifier)

    final = table_files(table)
    if set(final) != expected:
        raise MigrationError(
            f"iceberg_membership_mismatch:{group.namespace}.{group.table}:"
            f"{len(final)}:{len(expected)}"
        )
    total_rows = sum(row_count for row_count, _ in final.values())
    total_bytes = sum(file_size for _, file_size in final.values())
    if len(final) != len(group.objects):
        raise MigrationError(f"iceberg_file_count_mismatch:{group.namespace}.{group.table}")
    if total_bytes != group.expected_bytes:
        raise MigrationError(
            f"iceberg_byte_count_mismatch:{group.namespace}.{group.table}:"
            f"{total_bytes}:{group.expected_bytes}"
        )
    if total_rows <= 0:
        raise MigrationError(f"iceberg_table_empty:{group.namespace}.{group.table}")

    if group.identifier == ("bronze", "dbw_observations"):
        if len(final) != DBW_EXPECTED_FILES or total_rows != DBW_EXPECTED_ROWS:
            raise MigrationError(
                f"dbw_production_acceptance_failed:{len(final)}:{total_rows}"
            )

    snapshot = table.current_snapshot()
    return {
        "namespace": group.namespace,
        "table": group.table,
        "origin": group.origin,
        "data_files": len(final),
        "rows": total_rows,
        "bytes": total_bytes,
        "snapshot_id": str(snapshot.snapshot_id) if snapshot is not None else None,
        "added_files": len(missing),
        "reused_files": len(final) - len(missing),
        "format_version": int(table.metadata.format_version),
    }


def cleanup_pilot_catalogs(cat: RestCatalog, dbw_result: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not dbw_result:
        return []
    if (
        int(dbw_result.get("data_files", -1)) != DBW_EXPECTED_FILES
        or int(dbw_result.get("rows", -1)) != DBW_EXPECTED_ROWS
    ):
        return []

    result: list[dict[str, Any]] = []
    for namespace in ("zohelo_pilot_dbw", "zohelo_pilot_dbw_direct"):
        identifier = (namespace, "gus_dbw_observations")
        dropped_table = False
        dropped_namespace = False
        try:
            cat.drop_table(identifier)
            dropped_table = True
        except NoSuchTableError:
            pass
        try:
            cat.drop_namespace(namespace)
            dropped_namespace = True
        except NoSuchNamespaceError:
            pass
        except Exception as exc:
            # A non-empty namespace is not a production migration failure. Keep
            # the exact status in the receipt instead of deleting anything else.
            result.append(
                {
                    "namespace": namespace,
                    "table_dropped": dropped_table,
                    "namespace_dropped": False,
                    "namespace_drop_error": type(exc).__name__,
                }
            )
            continue
        result.append(
            {
                "namespace": namespace,
                "table_dropped": dropped_table,
                "namespace_dropped": dropped_namespace,
            }
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()

    bucket = required("R2_LAKEHOUSE_BUCKET")
    client = s3_client()
    cat = catalog()
    fs = arrow_fs()

    receipt: dict[str, Any] = {
        "result": "running",
        "operation": "r2-production-iceberg-cutover",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_deletion_performed": False,
        "google_drive_accessed": False,
    }
    write_json(args.receipt, receipt)

    stage = "discover"
    try:
        groups: dict[tuple[str, str], Group] = {}
        selected_releases = discover_release_groups(client, bucket, groups)
        direct_bronze = discover_direct_bronze(client, bucket, groups)
        direct_silver = discover_direct_layer(
            client, bucket, "03_silver/", "silver", groups
        )
        direct_gold = discover_direct_layer(
            client, bucket, "04_gold/", "gold", groups
        )
        non_table = discover_non_table_medallion(client, bucket)

        counts = {
            namespace: sum(1 for group in groups.values() if group.namespace == namespace)
            for namespace in PRODUCTION_NAMESPACES
        }
        if not groups or counts["bronze"] == 0:
            raise MigrationError("no_production_groups_discovered")

        print(
            json.dumps(
                {
                    "operation": "iceberg_plan",
                    "selected_releases": selected_releases,
                    "table_counts": counts,
                    "source_files": sum(len(group.objects) for group in groups.values()),
                    "source_bytes": sum(group.expected_bytes for group in groups.values()),
                    "non_table_medallion": non_table,
                    "tables": [
                        {
                            "table": f"{group.namespace}.{group.table}",
                            "origin": group.origin,
                            "files": len(group.objects),
                            "bytes": group.expected_bytes,
                        }
                        for group in sorted(groups.values(), key=lambda value: value.identifier)
                    ],
                },
                sort_keys=True,
            ),
            flush=True,
        )

        receipt.update(
            {
                "stage": stage,
                "selected_legacy_releases": selected_releases,
                "planned_table_counts": counts,
                "planned_source_files": sum(len(group.objects) for group in groups.values()),
                "planned_source_bytes": sum(group.expected_bytes for group in groups.values()),
                "direct_bronze": direct_bronze,
                "direct_silver": direct_silver,
                "direct_gold": direct_gold,
                "non_table_medallion": non_table,
            }
        )
        write_json(args.receipt, receipt)

        stage = "migrate"
        results: list[dict[str, Any]] = []
        for group in sorted(groups.values(), key=lambda value: value.identifier):
            print(
                json.dumps(
                    {
                        "operation": "iceberg_table_start",
                        "table": f"{group.namespace}.{group.table}",
                        "files": len(group.objects),
                        "bytes": group.expected_bytes,
                        "origin": group.origin,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            result = migrate_group(cat, fs, bucket, group)
            results.append(result)
            receipt["stage"] = stage
            receipt["completed_tables"] = len(results)
            receipt["tables"] = results
            write_json(args.receipt, receipt)

        stage = "production_acceptance"
        observed_counts = {
            namespace: sum(1 for item in results if item["namespace"] == namespace)
            for namespace in PRODUCTION_NAMESPACES
        }
        if observed_counts != counts:
            raise MigrationError(f"production_table_count_mismatch:{observed_counts}:{counts}")

        dbw_result = next(
            (
                item
                for item in results
                if item["namespace"] == "bronze" and item["table"] == "dbw_observations"
            ),
            None,
        )

        stage = "pilot_catalog_cleanup"
        pilot_cleanup = cleanup_pilot_catalogs(cat, dbw_result)

        receipt = {
            "result": "pass",
            "operation": "r2-production-iceberg-cutover",
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "selected_legacy_releases": selected_releases,
            "table_counts": observed_counts,
            "source_files": sum(item["data_files"] for item in results),
            "source_bytes": sum(item["bytes"] for item in results),
            "rows_from_parquet_metadata": sum(item["rows"] for item in results),
            "tables": results,
            "pilot_catalog_cleanup": pilot_cleanup,
            "non_table_medallion": non_table,
            "source_deletion_performed": False,
            "google_drive_accessed": False,
            "legacy_current_release_cleanup_performed": False,
            "notes": [
                "Legacy release/current metadata was read only to select one-time canonical sources.",
                "Existing Parquet files were registered as Iceberg DataFiles without rewriting them.",
                "Legacy R2 current/release objects remain until a separate reconciliation/deletion gate.",
                "Non-Parquet medallion objects are not represented as Iceberg table data.",
            ],
        }
        write_json(args.receipt, receipt)
        print(json.dumps(receipt, sort_keys=True), flush=True)
        return 0
    except Exception as exc:
        receipt.update(
            {
                "result": "fail",
                "stage": stage,
                "category": type(exc).__name__,
                "message": str(exc)[:1000],
                "failed_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_deletion_performed": False,
                "google_drive_accessed": False,
            }
        )
        write_json(args.receipt, receipt)
        print(json.dumps(receipt, sort_keys=True), flush=True)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
