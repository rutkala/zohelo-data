"""Build, validate and publish WDI Bronze, Silver, Gold and semantic coverage."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile

import duckdb
import yaml

from drive_release_store import DriveReleaseStore
from ingestion.bulk_publication import verify_bulk_index
from ingestion.bulk_transport import BulkDriveRawStore
from ingestion.drive_state_store import DriveStateStore
from ingestion.full_source_adapters import inspect_distribution
from ingestion.full_source_campaign import coverage as bulk_coverage
from ingestion.source_campaign_store import _CampaignStore
from layout_resolution import resolve_source_release_root
from medallion_navigation import sync_source_medallion_navigation
from release_protocol import (
    publish_release,
    read_current_release_manifest,
    restore_current_release,
    ReleaseProtocolError,
)
from runtime_metadata import _code_sha
from storage_manager import StorageManager
from wdi_business_catalog import build_wdi_business_catalog
from wdi_platform_contract import (
    WDI_ALLOW_ZERO_ROWS,
    WDI_ARCHIVE_MEMBERS,
    WDI_PLATFORM_DATASETS,
    WDI_PLATFORM_DATE_COLUMNS,
    WDI_PLATFORM_MODEL_NAMES,
    WDI_RELEASE_SCOPE,
    WDI_SOURCE_ID,
)
from wdi_release_validation import validate_staged_wdi_release
from wdi_semantic import METRICS, validate_release_metrics


REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_NAMES = (
    "manifest.json", "catalog.json", "run_results.json",
    "semantic_manifest.json", "metric-validation.json",
)
PARTITION_DATASETS = {
    "bronze_wdi_data": "observation_year",
    "wdi_observations": "observation_year",
    "fact_wdi_observations": "year_key",
}
MAX_RELEASE_PART_BYTES = 120 * 1024 * 1024
MAX_EXTRACTED_BYTES = 8 * 1024 * 1024 * 1024


def _release_root(storage: StorageManager, root_id: str, is_writer: bool = False) -> tuple[str, bool]:
    return resolve_source_release_root(storage, root_id, "wdi", is_writer=is_writer)


def _stores(allow_production_write: bool):
    if not allow_production_write:
        raise PermissionError("Drive WDI publication requires --allow-production-write")
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise PermissionError("Production WDI publication must use the serialized main-branch Actions workflow")
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    storage.authorize_writes()
    root_id = storage.resolve_root(create=False)
    control = storage.get_or_create_nested_folder(
        ["06_control", "source_campaigns", f"{WDI_SOURCE_ID}_bulk"], root_id=root_id
    )
    landing = storage.resolve_zone("landing", create=True)
    raw_root = storage.get_or_create_nested_folder([WDI_SOURCE_ID, "bulk"], root_id=landing)
    transport = DriveStateStore(storage, root_id, control, allow_landing_pointer=True)
    campaign_store = _CampaignStore(
        transport, f"{WDI_SOURCE_ID}_bulk", control, raw_root
    )
    raw_store = BulkDriveRawStore(storage, WDI_SOURCE_ID, responses_root_id=raw_root)
    release_root_id, direct_releases = _release_root(storage, root_id, is_writer=True)
    release_store = DriveReleaseStore(storage, release_root_id)
    return campaign_store, raw_store, release_store, direct_releases, root_id, storage


def _unchanged_release(release_store, code_sha: str, receipt: dict):
    """Return an existing release when code and current archive bytes are unchanged."""
    try:
        manifest = read_current_release_manifest(release_store, release_store.root_id)
    except ReleaseProtocolError as exc:
        if "no current-release pointer exists" in str(exc):
            return None
        raise
    inputs = manifest.get("inputs")
    if (
        manifest.get("release_scope") != WDI_RELEASE_SCOPE
        or manifest.get("code_sha") != code_sha
        or not isinstance(inputs, list)
        or len(inputs) != 1
    ):
        return None
    release_input = inputs[0]
    if (
        release_input.get("source_id") != WDI_SOURCE_ID
        or release_input.get("sha256") != receipt["raw"]["sha256"]
        or release_input.get("size") != receipt["raw"]["size_bytes"]
    ):
        return None
    return manifest


def _current_archive(campaign_store, raw_store, workspace: Path):
    index = verify_bulk_index(campaign_store, require_current=True)
    if index is None:
        raise RuntimeError("No current WDI full-distribution index is available")
    state = campaign_store.load()
    if state is None:
        raise RuntimeError("No WDI full-distribution state is available")
    coverage = bulk_coverage(state)
    if coverage.get("coverage_status") != "complete_current_catalogue":
        raise RuntimeError("WDI current official archive coverage is incomplete")
    receipts = state.get("receipts")
    if not isinstance(receipts, list) or not receipts:
        raise RuntimeError("WDI full-distribution state has no accepted archive")
    receipt = campaign_store.read_receipt(receipts[-1])
    if (
        not isinstance(receipt, dict) or receipt.get("accepted") is not True
        or receipt.get("source_id") != WDI_SOURCE_ID
        or receipt.get("distribution", {}).get("dataset_id") != "WDI"
        or receipt.get("distribution", {}).get("kind") != "wdi_zip"
        or receipt.get("inspection", {}).get("status") != "complete"
    ):
        raise RuntimeError("Latest accepted WDI receipt is not a complete official CSV archive")
    archive = workspace / "WDI_CSV.zip"
    raw_store.read_to_file(receipt["raw"], archive)
    observed = inspect_distribution(archive, "wdi_zip")
    if observed != receipt["inspection"]:
        raise RuntimeError("Fresh WDI archive inspection differs from its accepted receipt")
    return state, coverage, receipt, archive, observed, index


def _extract_archive(archive: Path, inspection: dict, output: Path) -> dict[str, Path]:
    members = inspection.get("members")
    if not isinstance(members, list):
        raise RuntimeError("WDI archive inspection has no member inventory")
    expected = set(WDI_ARCHIVE_MEMBERS)
    by_basename = {}
    total = 0
    for member in members:
        name = member.get("name") if isinstance(member, dict) else None
        size = member.get("byte_size") if isinstance(member, dict) else None
        if not isinstance(name, str) or type(size) is not int or size < 0:
            raise RuntimeError("WDI archive member evidence is invalid")
        basename = Path(name).name
        if basename in by_basename:
            raise RuntimeError(f"WDI archive repeats member basename {basename}")
        by_basename[basename] = name
        total += size
    if set(by_basename) != expected:
        raise RuntimeError(
            f"WDI archive member contract changed; missing={sorted(expected - set(by_basename))}, "
            f"extra={sorted(set(by_basename) - expected)}"
        )
    if total > MAX_EXTRACTED_BYTES:
        raise RuntimeError("WDI archive exceeds the reviewed extracted-size bound")
    free = shutil.disk_usage(output.parent).free
    if free < total + 2 * 1024 * 1024 * 1024:
        raise OSError("Runner disk headroom is insufficient for complete WDI modeling")
    output.mkdir(parents=True, exist_ok=False)
    paths = {}
    with zipfile.ZipFile(archive) as bundle:
        info_by_name = {item.filename: item for item in bundle.infolist()}
        for basename in sorted(expected):
            member_name = by_basename[basename]
            info = info_by_name.get(member_name)
            if info is None or info.is_dir():
                raise RuntimeError(f"Inspected WDI member is missing from archive: {member_name}")
            destination = output / basename
            with bundle.open(info, "r") as source, destination.open("xb") as target:
                shutil.copyfileobj(source, target, length=8 * 1024 * 1024)
            if destination.stat().st_size != info.file_size:
                raise RuntimeError(f"Extracted WDI member size changed: {basename}")
            paths[basename] = destination
    return paths


def _copy_relation(connection, relation: str, output: Path, dataset_id: str) -> list[Path]:
    partition_column = PARTITION_DATASETS.get(dataset_id)
    output.parent.mkdir(parents=True, exist_ok=True)
    if partition_column is None:
        target = output.with_suffix(".parquet")
        escaped = str(target).replace("'", "''")
        connection.execute(f"COPY (SELECT * FROM {relation}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        if target.stat().st_size > MAX_RELEASE_PART_BYTES:
            raise RuntimeError(f"{dataset_id} exceeds the release part bound and needs a source-key partition")
        return [target]
    minimum, maximum = connection.execute(
        f'SELECT min("{partition_column}"), max("{partition_column}") FROM {relation}'
    ).fetchone()
    if minimum is None or maximum is None:
        raise RuntimeError(f"{dataset_id} has no partition range")
    paths = []

    def write(start: int, end: int) -> None:
        target = output.with_name(f"{output.name}-{start}-{end}.parquet")
        escaped = str(target).replace("'", "''")
        connection.execute(
            f'COPY (SELECT * FROM {relation} WHERE "{partition_column}" BETWEEN {start} AND {end}) '
            f"TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        if target.stat().st_size > MAX_RELEASE_PART_BYTES:
            target.unlink()
            if start == end:
                raise RuntimeError(f"{dataset_id} single-year partition exceeds the release part bound")
            middle = (start + end) // 2
            write(start, middle)
            write(middle + 1, end)
        else:
            paths.append(target)

    first = (int(minimum) // 10) * 10
    for start in range(first, int(maximum) + 1, 10):
        write(max(start, int(minimum)), min(start + 9, int(maximum)))
    return paths


def build_platform(workspace: Path, member_paths: dict[str, Path], evidence_path: Path):
    database = workspace / "build.duckdb"
    target = workspace / "target"
    env = dict(os.environ)
    for basename, variable in WDI_ARCHIVE_MEMBERS.items():
        env[variable] = str(member_paths[basename])
    env.update(
        ZOHELO_WDI_ARCHIVE_EVIDENCE=str(evidence_path),
        ZOHELO_DUCKDB_PATH=str(database),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false", DO_NOT_TRACK="1",
    )
    env = {
        key: value for key, value in env.items()
        if not key.startswith(("GOOGLE_", "GCP_", "AWS_", "AZURE_"))
        and key not in {"CLOUDSDK_CONFIG", "ZOHELO_DRIVE_ROOT_ID", "ZOHELO_DRIVE_ROOT_NAME"}
    }
    common = [
        "--profiles-dir", str(REPO_ROOT), "--target-path", str(target),
        "--log-path", str(workspace / "logs"), "--threads", "1", "--no-partial-parse",
        "--vars", "{enable_wdi: true}",
    ]
    cli = [sys.executable, "-c", "from dbt.cli.main import cli; cli()"]
    model_names = list(WDI_PLATFORM_MODEL_NAMES.values())
    subprocess.run([*cli, "build", "--select", *model_names, *common], cwd=REPO_ROOT, env=env, check=True, timeout=3600)
    results = (target / "run_results.json").read_bytes()
    subprocess.run([*cli, "docs", "generate", "--no-compile", *common], cwd=REPO_ROOT, env=env, check=True, timeout=600)
    (target / "run_results.json").write_bytes(results)
    datasets = []
    with duckdb.connect(str(database), read_only=True) as connection:
        for dataset_id, (layer, model_id) in WDI_PLATFORM_DATASETS.items():
            table_name = dataset_id.removeprefix("bronze_")
            relation = f'"{layer}"."{table_name}"'
            base = workspace / layer / dataset_id
            paths = _copy_relation(connection, relation, base, dataset_id)
            date_column = WDI_PLATFORM_DATE_COLUMNS[dataset_id]
            date_sql = f'min("{date_column}"), max("{date_column}")' if date_column else "NULL, NULL"
            count, first, last = connection.execute(f"SELECT count(*), {date_sql} FROM {relation}").fetchone()
            if count <= 0 and dataset_id not in WDI_ALLOW_ZERO_ROWS:
                raise ValueError(f"Required WDI platform dataset is empty: {dataset_id}")
            datasets.append({
                "dataset_id": dataset_id, "table_name": table_name,
                "model_name": model_id.rsplit('.', 1)[-1], "model_id": model_id,
                "layer": layer, "path": str(paths[0]), "paths": [str(path) for path in paths],
                "row_count": count, "date_column": date_column,
                "min_date": first.isoformat() if first is not None else None,
                "max_date": last.isoformat() if last is not None else None,
                "columns": [{"name": row[0], "type": row[1]} for row in connection.execute(f"DESCRIBE {relation}").fetchall()],
            })
        semantic_path = target / "semantic_manifest.json"
        raw_semantic = json.loads(semantic_path.read_text(encoding="utf-8"))
        semantic_path.write_text(json.dumps({
            **raw_semantic,
            "semantic_models": [item for item in raw_semantic.get("semantic_models", []) if item.get("name") == "wdi_coverage"],
            "metrics": [item for item in raw_semantic.get("metrics", []) if item.get("name") in METRICS],
        }, sort_keys=True), encoding="utf-8")
        semantic_report = validate_release_metrics(database, semantic_path, workspace / "metric-validation")
        row = connection.execute('SELECT * FROM "04_gold"."mart_wdi_coverage"').fetchone()
    (target / "metric-validation.json").write_text(json.dumps(semantic_report, sort_keys=True), encoding="utf-8")
    coverage = {
        "snapshot_date": row[0].isoformat(),
        "latest_retrieved_at_utc": row[1].isoformat(),
        "current_archive_total": row[2], "archive_member_total": row[3],
        "source_geography_total": row[4], "source_indicator_total": row[5],
        "source_value_total": row[6], "modeled_geography_total": row[7],
        "modeled_indicator_total": row[8], "modeled_observation_total": row[9],
        "first_observation_date": row[10].isoformat(), "latest_observation_date": row[11].isoformat(),
        "modeled_value_coverage_ratio": row[12],
    }
    database.unlink(missing_ok=True)
    artifacts = [{"name": name, "path": str(target / name)} for name in ARTIFACT_NAMES]
    return datasets, artifacts, coverage


def run_platform(*, allow_production_write: bool = False, verify_current: bool = False):
    started = time.monotonic()
    campaign_store, raw_store, release_store, direct_releases, root_id, storage = _stores(allow_production_write)
    if verify_current:
        manifest = restore_current_release(release_store, release_store.root_id)
        if manifest.get("release_scope") != WDI_RELEASE_SCOPE:
            raise RuntimeError("Current WDI pointer does not identify a WDI modeled release")
        return {"status": "fresh_wdi_release_verified", "release_id": manifest["release_id"], "datasets": len(manifest["datasets"])}

    code_sha = _code_sha()
    with tempfile.TemporaryDirectory(prefix="zohelo-wdi-platform-") as temporary:
        workspace = Path(temporary)
        state, full_coverage, receipt, archive, inspection, index = _current_archive(campaign_store, raw_store, workspace)
        existing = _unchanged_release(release_store, code_sha, receipt)
        if existing is not None:
            if direct_releases:
                sync_source_medallion_navigation(storage, root_id, "wdi", existing)
            report = {
                "status": "wdi_platform_unchanged",
                "release_id": existing["release_id"],
                "source_id": WDI_SOURCE_ID,
                "archive_sha256": receipt["raw"]["sha256"],
                "reason": "current archive bytes and modeled code SHA already have a published release",
            }
            print(json.dumps(report, indent=2, sort_keys=True), flush=True)
            return report
        members = _extract_archive(archive, inspection, workspace / "members")
        evidence = {
            "source_id": WDI_SOURCE_ID,
            "retrieved_at_utc": receipt["retrieved_at_utc"],
            "raw_sha256": receipt["raw"]["sha256"],
            "raw_size_bytes": receipt["raw"]["size_bytes"],
            "coverage_status": full_coverage["coverage_status"],
            "current_distribution_total": full_coverage["validated_current_distributions"],
            "member_total": inspection["member_count"],
            "members": sorted(WDI_ARCHIVE_MEMBERS),
            "year_range": inspection.get("year_range"),
            "index_snapshot_id": index["snapshot_id"],
        }
        evidence_path = workspace / "archive-evidence.json"
        evidence_path.write_text(json.dumps(evidence, sort_keys=True), encoding="utf-8")
        datasets, artifacts, modeled_coverage = build_platform(workspace, members, evidence_path)
        archive.unlink(missing_ok=True)
        shutil.rmtree(workspace / "members")

        source_config = yaml.safe_load((REPO_ROOT / "config/sources.yaml").read_text(encoding="utf-8"))
        source_state = {
            "checked_through": full_coverage.get("catalogue_checked_on"),
            "latest_observation_date": modeled_coverage["latest_observation_date"],
            "last_successful_ingestion_at": receipt["retrieved_at_utc"],
            "last_attempt_at": receipt["retrieved_at_utc"],
            "raw_response_count": full_coverage["accepted_distributions"],
            "coverage_complete": True,
            "coverage": modeled_coverage,
        }
        ingestion_state = {
            "format_version": 1, "source_id": WDI_SOURCE_ID, "code_sha": code_sha,
            "coverage_status": full_coverage["coverage_status"],
            "current_distribution_total": full_coverage["validated_current_distributions"],
            "accepted_distribution_total": full_coverage["accepted_distributions"],
            "archive_member_total": inspection["member_count"],
            "archive_sha256": receipt["raw"]["sha256"],
            "archive_size_bytes": receipt["raw"]["size_bytes"],
            "archive_retrieved_at_utc": receipt["retrieved_at_utc"],
            "bulk_index_snapshot_id": index["snapshot_id"],
            "coverage": modeled_coverage,
            "sources": {WDI_SOURCE_ID: source_state},
        }
        manifest = json.loads((workspace / "target/manifest.json").read_text(encoding="utf-8"))
        catalogue = build_wdi_business_catalog(
            code_sha=code_sha, source_config=source_config, ingestion_state={"sources": {WDI_SOURCE_ID: source_state}},
            dbt_manifest=manifest, dataset_metadata=datasets,
        )
        for name, document in (("business-catalog.json", catalogue), ("ingestion-state.json", ingestion_state)):
            path = workspace / name
            path.write_text(json.dumps(document, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            artifacts.append({"name": name, "path": str(path)})
        result = publish_release(
            release_store, release_store.root_id, datasets=datasets, artifacts=artifacts,
            inputs=[{
                "source_id": WDI_SOURCE_ID, "dataset_id": "WDI", "id": receipt["raw"]["id"],
                "size": receipt["raw"]["size_bytes"], "sha256": receipt["raw"]["sha256"],
                "retrieved_at_utc": receipt["retrieved_at_utc"],
            }],
            code_sha=code_sha,
            measurements={
                "duration_seconds": round(time.monotonic() - started, 3),
                "archive_bytes": receipt["raw"]["size_bytes"],
                "output_bytes": sum(Path(path).stat().st_size for item in datasets for path in item["paths"]),
                "output_files": sum(len(item["paths"]) for item in datasets),
                "coverage": modeled_coverage,
            },
            release_scope=WDI_RELEASE_SCOPE,
            pre_promote_validator=validate_staged_wdi_release,
            direct_releases=direct_releases,
        )
        if direct_releases:
            sync_source_medallion_navigation(storage, root_id, "wdi", result["manifest"])
        report = {
            "status": "wdi_platform_published", "release_id": result["release_id"],
            "source_id": WDI_SOURCE_ID, "coverage": modeled_coverage,
            "archive_sha256": receipt["raw"]["sha256"],
            "limitations": [
                "WDI indicator values are heterogeneous and are not a generic additive semantic measure.",
                "The modeled release covers the complete current WDI CSV archive, not every other World Bank product.",
                "The independent WDI API reconciliation campaign remains resumable and incomplete.",
            ],
        }
        rendered = json.dumps(report, indent=2, sort_keys=True)
        print(rendered, flush=True)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as handle:
                handle.write("## WDI modeled publication\n\n```json\n" + rendered + "\n```\n")
        if os.environ.get("GITHUB_OUTPUT"):
            with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as handle:
                handle.write(f"release_id={result['release_id']}\n")
        return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--verify-current", action="store_true")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        run_platform(allow_production_write=args.allow_production_write, verify_current=args.verify_current)
    except Exception as exc:
        print(json.dumps({
            "status": "wdi_platform_failed", "error_type": type(exc).__name__,
            "detail": str(exc), "occurred_at_utc": datetime.now(timezone.utc).isoformat(),
        }), file=sys.stderr, flush=True)
        raise SystemExit(1) from exc
