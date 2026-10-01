#!/usr/bin/env python3
"""Read-only reconciliation of live R2 Landing/Archive against Iceberg medallion tables.

This report deliberately separates layer presence from coverage proof.
A source having a Bronze table does not prove that every Landing object is
represented in Bronze. The report never promotes, copies, archives or deletes data.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import os
from pathlib import PurePosixPath, Path
from typing import Any

import boto3
from botocore.config import Config
from pyiceberg.catalog.rest import RestCatalog

KNOWN_SOURCES = (
    "nbp",
    "gus_bdl",
    "world_bank_wdi",
    "eurostat",
    "opendata_org",
    "gus_dbw",
    "gus_teryt",
    "gugik_prg",
    "mf_biala_lista",
    "gleif",
    "imgw_pib",
    "gios_pjp",
)

LANDING_ALIASES = {
    "nbp": "nbp",
    "gold": "nbp",
    "gus_bdl": "gus_bdl",
    "world_bank_wdi": "world_bank_wdi",
    "eurostat": "eurostat",
    "opendata_org": "opendata_org",
    "gus_dbw": "gus_dbw",
    "gus_teryt": "gus_teryt",
    "gugik_prg": "gugik_prg",
    "mf_biala_lista": "mf_biala_lista",
    "gleif": "gleif",
    "imgw_pib": "imgw_pib",
    "gios_pjp": "gios_pjp",
}

LINEAGE_FIELD_NAMES = {
    "raw_sha256",
    "raw_file_id",
    "source_file_id",
    "source_file_sha256",
    "source_sha256",
    "landing_file_id",
    "landing_sha256",
}


def req(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def s3_client():
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


def suffix(key: str) -> str:
    name = PurePosixPath(key).name.lower()
    for ext in (".json.gz", ".csv.gz", ".tsv.gz"):
        if name.endswith(ext):
            return ext
    return PurePosixPath(name).suffix.lower() or "<none>"


def source_from_landing_key(key: str) -> str:
    parts = [part for part in key.split("/") if part]
    if len(parts) < 2:
        return "<unattributed>"
    return LANDING_ALIASES.get(parts[1], parts[1])


def source_from_archive_key(key: str) -> str:
    parts = [part for part in key.split("/") if part]
    for part in parts[1:]:
        if part in LANDING_ALIASES:
            return LANDING_ALIASES[part]
    return "<unattributed>"


def source_from_table(layer: str, name: str) -> str:
    name = name.lower()

    prefix_rules = (
        ("bdl_", "gus_bdl"),
        ("dbw_", "gus_dbw"),
        ("eurostat_", "eurostat"),
        ("wdi_", "world_bank_wdi"),
        ("nbp_", "nbp"),
        ("opendata_org_", "opendata_org"),
        ("gleif_", "gleif"),
        ("teryt_", "gus_teryt"),
        ("prg_", "gugik_prg"),
        ("biala_lista_", "mf_biala_lista"),
        ("imgw_", "imgw_pib"),
        ("gios_", "gios_pjp"),
    )
    for prefix, source in prefix_rules:
        if name.startswith(prefix):
            return source

    token_rules = (
        ("_bdl_", "gus_bdl"),
        ("_dbw_", "gus_dbw"),
        ("_eurostat_", "eurostat"),
        ("_wdi_", "world_bank_wdi"),
        ("opendata_", "opendata_org"),
        ("corporate_", "gleif"),
        ("prg_", "gugik_prg"),
        ("vat_", "mf_biala_lista"),
    )
    for token, source in token_rules:
        if token in name:
            return source

    exact = {
        "fact_fx_quotes": "nbp",
        "fact_gold_prices": "nbp",
        "dim_currency": "nbp",
        "dim_commodity": "nbp",
        "dim_source_table": "nbp",
        "dim_date": "nbp",
        "dim_locality": "gus_teryt",
        "dim_territory": "gus_teryt",
        "dim_street": "gus_teryt",
        "dim_corporate_entity": "gleif",
        "fact_corporate_relationships": "gleif",
        "fact_corporate_reporting_exceptions": "gleif",
        "dim_opendata_location": "opendata_org",
        "dim_opendata_organization": "opendata_org",
        "dim_opendata_person": "opendata_org",
    }
    return exact.get(name, "<unattributed>")


def object_inventory(client, bucket: str, prefix: str, archive: bool = False) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"objects": 0, "bytes": 0, "extensions": Counter(), "extension_bytes": Counter()}
    )
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            size = int(item.get("Size", 0))
            if key.endswith("/") and size == 0:
                continue
            source = source_from_archive_key(key) if archive else source_from_landing_key(key)
            ext = suffix(key)
            group = groups[source]
            group["objects"] += 1
            group["bytes"] += size
            group["extensions"][ext] += 1
            group["extension_bytes"][ext] += size

    result = {}
    for source, group in sorted(groups.items()):
        result[source] = {
            "objects": group["objects"],
            "bytes": group["bytes"],
            "extensions": {
                ext: {
                    "objects": count,
                    "bytes": int(group["extension_bytes"][ext]),
                }
                for ext, count in group["extensions"].most_common()
            },
        }
    return result


def table_inventory(cat, namespace: str) -> tuple[dict[str, Any], dict[str, list[str]]]:
    by_source: dict[str, Any] = defaultdict(lambda: {
        "tables": 0,
        "data_files": 0,
        "rows": 0,
        "bytes": 0,
        "table_names": [],
        "tables_with_lineage_columns": [],
    })
    unattributed: dict[str, list[str]] = defaultdict(list)

    for identifier in sorted(cat.list_tables(namespace)):
        table = cat.load_table(identifier)
        table_name = identifier[-1]
        source = source_from_table(namespace, table_name)
        fields = [field.name for field in table.schema().fields]
        lineage = sorted(set(fields) & LINEAGE_FIELD_NAMES)
        files = list(table.scan().plan_files())
        rows = sum(int(task.file.record_count) for task in files)
        byte_count = sum(int(task.file.file_size_in_bytes) for task in files)

        if source == "<unattributed>":
            unattributed[namespace].append(table_name)

        group = by_source[source]
        group["tables"] += 1
        group["data_files"] += len(files)
        group["rows"] += rows
        group["bytes"] += byte_count
        group["table_names"].append(table_name)
        if lineage:
            group["tables_with_lineage_columns"].append({
                "table": table_name,
                "columns": lineage,
            })

    return dict(by_source), dict(unattributed)


def status_for(source: str, landing: dict[str, Any], layers: dict[str, dict[str, Any]]) -> dict[str, Any]:
    landing_objects = int(landing.get("objects", 0))
    bronze = layers["bronze"].get(source, {})
    silver = layers["silver"].get(source, {})
    gold = layers["gold"].get(source, {})
    bronze_tables = int(bronze.get("tables", 0))
    silver_tables = int(silver.get("tables", 0))
    gold_tables = int(gold.get("tables", 0))
    lineage_tables = len(bronze.get("tables_with_lineage_columns", []))

    if landing_objects and bronze_tables == 0:
        stage = "pending_bronze"
    elif bronze_tables and silver_tables == 0:
        stage = "pending_silver"
    elif silver_tables and gold_tables == 0:
        stage = "pending_gold"
    elif bronze_tables and silver_tables and gold_tables:
        stage = "all_layers_present"
    elif bronze_tables:
        stage = "bronze_present"
    else:
        stage = "raw_only_or_absent"

    if landing_objects == 0:
        coverage = "no_current_landing_objects"
    elif bronze_tables == 0:
        coverage = "not_represented_in_bronze"
    elif lineage_tables:
        coverage = "bronze_has_lineage_columns_but_full_landing_membership_not_proven"
    else:
        coverage = "bronze_present_but_landing_membership_not_proven"

    return {
        "stage_status": stage,
        "coverage_status": coverage,
        "safe_to_remove_landing": False if landing_objects else None,
        "reason": (
            "Layer presence is not file-level lineage proof. Keep Landing until source-specific "
            "Bronze membership and Archive readback are proven."
            if landing_objects
            else "No current Landing object was observed for this source."
        ),
    }


def render_markdown(report: dict[str, Any]) -> str:
    sources = report["sources"]
    lines = [
        "# Live R2 Landing to Iceberg reconciliation",
        "",
        f"Observed at: {report['observed_at_utc']}",
        "",
        "This is a conservative reconciliation. A table existing in Bronze/Silver/Gold "
        "does not by itself prove that every Landing object is represented.",
        "",
        "| Source | Landing objects | Landing GiB | Bronze | Silver | Gold | Coverage | Next gate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for source, item in sorted(sources.items()):
        landing = item["landing"]
        bronze = item["layers"]["bronze"]
        silver = item["layers"]["silver"]
        gold = item["layers"]["gold"]
        coverage = item["status"]["coverage_status"].replace("_", " ")
        stage = item["status"]["stage_status"].replace("_", " ")
        lines.append(
            f"| {source} | {landing.get('objects',0):,} | "
            f"{landing.get('bytes',0)/(1024**3):.2f} | "
            f"{bronze.get('tables',0)} | {silver.get('tables',0)} | {gold.get('tables',0)} | "
            f"{coverage} | {stage} |"
        )

    lines.extend([
        "",
        "## Interpretation",
        "",
        "- all layers present means tables exist in all three Iceberg namespaces; it is not a completeness claim.",
        "- Bronze lineage columns mean at least one Bronze table retains raw/source identity fields, but a full object-by-object match still has to be proven.",
        "- Landing removal remains blocked until source-specific Bronze membership and Archive readback are verified.",
        "",
        "## Unattributed table names",
        "",
    ])
    for layer, names in report["unattributed_tables"].items():
        lines.append(f"- {layer}: " + (", ".join(names) if names else "none"))
    lines.append("")
    return "\\n".join(lines)


def main() -> int:
    client = s3_client()
    cat = catalog()
    landing_bucket = req("R2_LANDING_BUCKET")

    landing = object_inventory(client, landing_bucket, "01_landing/")
    archive = object_inventory(client, landing_bucket, "05_archive/", archive=True)

    layers: dict[str, dict[str, Any]] = {}
    unattributed: dict[str, list[str]] = {}
    for namespace in ("bronze", "silver", "gold"):
        layer, unknown = table_inventory(cat, namespace)
        layers[namespace] = layer
        unattributed[namespace] = unknown.get(namespace, [])

    all_sources = sorted(
        set(KNOWN_SOURCES)
        | set(landing)
        | set(archive)
        | {source for layer in layers.values() for source in layer if source != "<unattributed>"}
    )

    sources: dict[str, Any] = {}
    for source in all_sources:
        source_layers = {
            layer: layers[layer].get(source, {
                "tables": 0,
                "data_files": 0,
                "rows": 0,
                "bytes": 0,
                "table_names": [],
                "tables_with_lineage_columns": [],
            })
            for layer in ("bronze", "silver", "gold")
        }
        landing_group = landing.get(source, {"objects": 0, "bytes": 0, "extensions": {}})
        archive_group = archive.get(source, {"objects": 0, "bytes": 0, "extensions": {}})
        sources[source] = {
            "landing": landing_group,
            "archive": archive_group,
            "layers": source_layers,
            "status": status_for(source, landing_group, source_layers),
        }

    report = {
        "format_version": 1,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "landing_bucket": landing_bucket,
        "lakehouse_bucket": req("R2_LAKEHOUSE_BUCKET"),
        "sources": sources,
        "unattributed_archive": archive.get("<unattributed>", {"objects": 0, "bytes": 0, "extensions": {}}),
        "unattributed_tables": unattributed,
        "rules": {
            "layer_presence_is_not_coverage_proof": True,
            "landing_deletion_requires_source_specific_lineage_and_archive_readback": True,
            "writes_performed": False,
        },
    }

    output = Path(os.environ.get("RECONCILIATION_JSON", "/tmp/r2-reconciliation.json"))
    markdown = Path(os.environ.get("RECONCILIATION_MD", "/tmp/r2-reconciliation.md"))
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
    markdown.write_text(render_markdown(report), encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
