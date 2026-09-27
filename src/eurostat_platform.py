"""Build, validate, and publish a modeled EUROSTAT release from the current Landing snapshot."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import duckdb
import yaml

from eurostat_business_catalog import build_eurostat_business_catalog
from eurostat_platform_contract import (
    EUROSTAT_PLATFORM_DATASETS,
    EUROSTAT_PLATFORM_DATE_COLUMNS,
    EUROSTAT_PLATFORM_MODEL_NAMES,
    EUROSTAT_SOURCE_ID,
    EUROSTAT_RELEASE_SCOPE,
)
from eurostat_decode import decode_landing_files
from eurostat_release_validation import validate_staged_eurostat_release
from eurostat_semantic import METRICS, validate_release_metrics
from drive_release_store import DriveReleaseStore
from ingestion.landing_publication import verify_landing
from ingestion.source_campaign_store import DriveCampaignStore, _CampaignStore
from ingestion.drive_state_store import DriveStateStore
from ingestion.full_source_campaign import coverage as full_source_coverage
from layout_resolution import resolve_source_release_root
from medallion_navigation import (finalize_source_medallion_navigation, sync_source_medallion_navigation)
from release_protocol import publish_release, read_release_manifest, restore_current_release
from runtime_metadata import _code_sha
from storage_manager import StorageManager


REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_NAMES = (
    "manifest.json",
    "catalog.json",
    "run_results.json",
    "semantic_manifest.json",
    "metric-validation.json",
)


def _release_root(storage: StorageManager, root_id: str, is_writer: bool = False) -> str:
    root, _ = resolve_source_release_root(storage, root_id, "eurostat", is_writer=is_writer)
    return root


def _campaign_store(backend: str, local_root: Path | None, allow_production_write: bool):
    if backend == "drive":
        if not allow_production_write:
            raise PermissionError("Drive EUROSTAT publication requires --allow-production-write")
        if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
            raise PermissionError("Production EUROSTAT publication must use the serialized main-branch Actions workflow")
        storage = StorageManager(allow_interactive_auth=False)
        storage.resolve_root(create=False)
        storage.authorize_writes()
        root_id = storage.resolve_root(create=False)
        release_root, _ = resolve_source_release_root(storage, root_id, "eurostat", is_writer=True)
        return storage, root_id, DriveCampaignStore(storage, EUROSTAT_SOURCE_ID), DriveReleaseStore(storage, release_root)
    raise ValueError("EUROSTAT modeled publication currently supports only the drive backend")


def _download_landing_snapshot(store, workspace: Path, manifest: dict) -> list[Path]:
    """Download the pinned manifest, never re-read the mutable current pointer.

    read_landing_object verifies each descriptor's size and checksum. The same
    descriptors are recorded in release inputs and revalidated before promotion.
    """
    if not manifest or not manifest.get("snapshot_id") or not manifest.get("files"):
        raise RuntimeError("No verified EUROSTAT Landing snapshot was supplied")
    paths = []
    for index, descriptor in enumerate(manifest["files"]):
        path = workspace / f"landing-{index}.parquet"
        path.write_bytes(store.read_landing_object(descriptor))
        paths.append(path)
    return paths


def _load_full_source_coverage(storage, root_id: str) -> dict:
    campaign_id = f"{EUROSTAT_SOURCE_ID}_bulk"
    control = storage.get_or_create_nested_folder(
        ["06_control", "source_campaigns", campaign_id], root_id=root_id
    )
    landing = storage.resolve_zone("landing", create=True)
    raw_root = storage.get_or_create_nested_folder(
        [EUROSTAT_SOURCE_ID, "bulk"], root_id=landing
    )
    transport = DriveStateStore(
        storage, root_id, control, allow_landing_pointer=True
    )
    state = _CampaignStore(transport, campaign_id, control, raw_root).load()
    if state is None:
        raise RuntimeError("No Eurostat full-distribution campaign state is available")
    return full_source_coverage(state)


def build_platform(workspace: Path):
    database = workspace / "build.duckdb"
    target = workspace / "target"
    env = dict(os.environ)
    env.update(
        ZOHELO_EUROSTAT_DECODED_PATH=str(workspace / "decoded-observations.parquet"),
        ZOHELO_EUROSTAT_FULL_COVERAGE_PATH=str(workspace / "full-source-coverage.json"),
        ZOHELO_DUCKDB_PATH=str(database),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        DO_NOT_TRACK="1",
    )
    env = {
        key: value
        for key, value in env.items()
        if not key.startswith(("GOOGLE_", "GCP_", "AWS_", "AZURE_"))
        and key not in {"CLOUDSDK_CONFIG", "ZOHELO_DRIVE_ROOT_ID", "ZOHELO_DRIVE_ROOT_NAME"}
    }
    common = ["--profiles-dir", str(REPO_ROOT), "--target-path", str(target), "--log-path", str(workspace / "logs"), "--threads", "1", "--no-partial-parse", "--vars", "{enable_eurostat: true}"]
    cli = [sys.executable, "-c", "from dbt.cli.main import cli; cli()"]
    model_names = list(EUROSTAT_PLATFORM_MODEL_NAMES.values())
    subprocess.run([*cli, "build", "--select", *model_names, *common], cwd=REPO_ROOT, env=env, check=True, timeout=900)
    results = (target / "run_results.json").read_bytes()
    subprocess.run([*cli, "docs", "generate", "--no-compile", *common], cwd=REPO_ROOT, env=env, check=True, timeout=300)
    (target / "run_results.json").write_bytes(results)
    datasets = []
    with duckdb.connect(str(database), read_only=True) as connection:
        for dataset_id, (layer, model_id) in EUROSTAT_PLATFORM_DATASETS.items():
            table_name = dataset_id.removeprefix("bronze_")
            relation = f'"{layer}"."{table_name}"'
            output = workspace / layer / f"{dataset_id}.parquet"
            output.parent.mkdir(parents=True, exist_ok=True)
            output_sql = str(output).replace("'", "''")
            connection.execute(
                f"COPY (SELECT * FROM {relation}) TO '{output_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            date_column = EUROSTAT_PLATFORM_DATE_COLUMNS[dataset_id]
            if date_column is None:
                count = connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
                first = last = None
            else:
                count, first, last = connection.execute(f'SELECT count(*), min("{date_column}"), max("{date_column}") FROM {relation}').fetchone()
            if count <= 0:
                raise ValueError(f"Required EUROSTAT platform dataset is empty: {dataset_id}")
            datasets.append({
                "dataset_id": dataset_id,
                "table_name": table_name,
                "model_name": model_id.rsplit('.', 1)[-1],
                "model_id": model_id,
                "layer": layer,
                "path": str(output),
                "row_count": count,
                "date_column": date_column,
                "min_date": first.isoformat() if first is not None else None,
                "max_date": last.isoformat() if last is not None else None,
                "columns": [{"name": row[0], "type": row[1]} for row in connection.execute(f"DESCRIBE {relation}").fetchall()],
            })
        semantic_manifest_path = target / "semantic_manifest.json"
        if semantic_manifest_path.exists():
            raw_semantic = json.loads(semantic_manifest_path.read_text(encoding="utf-8"))
            filtered_semantic = {
                **raw_semantic,
                "semantic_models": [
                    sm for sm in raw_semantic.get("semantic_models", [])
                    if sm.get("name") == "eurostat_coverage"
                ],
                "metrics": [
                    m for m in raw_semantic.get("metrics", [])
                    if m.get("name") in METRICS
                ],
            }
            semantic_manifest_path.write_text(json.dumps(filtered_semantic, sort_keys=True), encoding="utf-8")
        semantic_report = validate_release_metrics(database, semantic_manifest_path, workspace / "metric-validation")
        coverage = connection.execute('SELECT * FROM "04_gold"."mart_eurostat_coverage"').fetchone()
    (target / "metric-validation.json").write_text(json.dumps(semantic_report, sort_keys=True), encoding="utf-8")
    artifacts = [{"name": name, "path": str(target / name)} for name in ARTIFACT_NAMES]
    return datasets, artifacts, {
        "snapshot_date": coverage[0].isoformat(),
        "latest_retrieved_at_utc": coverage[1].isoformat() if coverage[1] is not None else None,
        "admitted_dataset_total": coverage[2],
        "admitted_series_total": coverage[3],
        "modeled_dataset_total": coverage[4],
        "modeled_series_total": coverage[5],
        "modeled_response_total": coverage[6],
        "modeled_cell_total": coverage[7],
        "modeled_value_total": coverage[8],
        "admitted_dataset_coverage_ratio": coverage[9],
        "admitted_series_coverage_ratio": coverage[10],
        "catalogue_distributions": coverage[11],
        "validated_current_distributions": coverage[12],
        "pending_distribution_tasks": coverage[13],
        "failed_pending_distribution_tasks": coverage[14],
        "accepted_distributions": coverage[15],
        "received_raw_bytes": coverage[16],
        "full_distribution_coverage_ratio": coverage[17],
        "inventories_current": coverage[18],
        "catalogue_checked_on": coverage[19].isoformat() if coverage[19] is not None else None,
        "latest_observation_date": coverage[20].isoformat() if coverage[20] is not None else None,
        "raw_catalogue_complete": coverage[21],
        "complete_official_catalogue": coverage[22],
    }


def run_platform(backend: str = "drive", local_root: Path | None = None, allow_production_write: bool = False, verify_current: bool = False):
    started = time.monotonic()
    storage, root_id, campaign_store, release_store = _campaign_store(backend, local_root, allow_production_write)
    if verify_current:
        if release_store is None:
            raise ValueError("Fresh release verification is only supported for drive-backed publication")
        manifest = restore_current_release(release_store, release_store.root_id)
        return {"status": "fresh_eurostat_release_verified", "release_id": manifest["release_id"], "datasets": len(manifest["datasets"])}

    code_sha = _code_sha()
    with tempfile.TemporaryDirectory(prefix="zohelo-eurostat-platform-") as temporary:
        workspace = Path(temporary)
        landing_manifest = deepcopy(verify_landing(campaign_store))
        if landing_manifest is None:
            raise RuntimeError("No published EUROSTAT Landing snapshot is available")
        campaign_state = campaign_store.load()
        if campaign_state is None:
            raise RuntimeError("No EUROSTAT campaign state is available")
        landing_paths = _download_landing_snapshot(campaign_store, workspace, landing_manifest)
        landing_inputs = [
            {
                "source_id": EUROSTAT_SOURCE_ID,
                "id": descriptor["id"],
                "size": descriptor["size"],
                "sha256": descriptor["sha256"],
                "ingestion_sequence": index + 1,
            }
            for index, descriptor in enumerate(landing_manifest["files"])
        ]
        decode_measurements = decode_landing_files(
            landing_paths, workspace / "decoded-observations.parquet"
        )
        full_coverage = _load_full_source_coverage(storage, root_id)
        (workspace / "full-source-coverage.json").write_text(
            json.dumps(full_coverage, sort_keys=True), encoding="utf-8"
        )
        datasets, artifacts, coverage = build_platform(workspace)
        source_config = yaml.safe_load((REPO_ROOT / "config/sources.yaml").read_text(encoding="utf-8"))
        ingestion_state = {
            "sources": {
                EUROSTAT_SOURCE_ID: {
                    "checked_through": coverage["snapshot_date"],
                    "latest_observation_date": coverage["latest_observation_date"],
                    "last_successful_ingestion_at": campaign_state.get("last_success_utc"),
                    "last_attempt_at": campaign_state.get("last_attempt_utc"),
                    "raw_response_count": campaign_state.get("accepted_responses", 0),
                    "coverage_complete": False,
                    "coverage": coverage,
                }
            }
        }
        catalogue = build_eurostat_business_catalog(code_sha=code_sha, source_config=source_config, ingestion_state=ingestion_state, dbt_manifest=json.loads((workspace / "target/manifest.json").read_text(encoding="utf-8")), dataset_metadata=datasets)
        state_artifact = {
            "format_version": 1,
            "source_id": EUROSTAT_SOURCE_ID,
            "code_sha": code_sha,
            "landing_snapshot_id": landing_manifest["snapshot_id"],
            "accepted_response_count": landing_manifest["accepted_response_count"],
            "published_response_count": landing_manifest["published_response_count"],
            "pending_publication_count": landing_manifest["pending_publication_count"],
            "coverage_status": landing_manifest["coverage_status"],
            "modeling_scope": EUROSTAT_RELEASE_SCOPE,
            "landing_inputs": landing_inputs,
            "raw_response_count": campaign_state.get("accepted_responses", 0),
            "pending_tasks": len(campaign_state.get("pending", [])),
            "catalogue_totals": campaign_state.get("catalogue_totals", {}),
            "last_successful_ingestion_at": campaign_state.get("last_success_utc"),
            "last_attempt_at": campaign_state.get("last_attempt_utc"),
            "coverage": coverage,
            "sources": ingestion_state["sources"],
        }
        for name, document in (("business-catalog.json", catalogue), ("ingestion-state.json", state_artifact)):
            path = workspace / name
            path.write_text(json.dumps(document, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            artifacts.append({"name": name, "path": str(path)})
        measurements = {
            "landing_fragments": len(landing_paths),
            "input_bytes": sum(path.stat().st_size for path in landing_paths),
            "output_bytes": sum(Path(item["path"]).stat().st_size for item in datasets),
            "duration_seconds": round(time.monotonic() - started, 3),
            **decode_measurements,
            "coverage": coverage,
        }
        if release_store is None:
            raise ValueError("EUROSTAT release publication currently requires drive backend")
        _, direct_releases = resolve_source_release_root(storage, root_id, "eurostat", is_writer=True)
        result = publish_release(
            release_store, release_store.root_id, datasets=datasets, artifacts=artifacts,
            inputs=landing_inputs,
            code_sha=code_sha, measurements=measurements, release_scope=EUROSTAT_RELEASE_SCOPE,
            pre_promote_validator=validate_staged_eurostat_release,
            before_pointer_write=(
                lambda release_store, pointer: sync_source_medallion_navigation(
                    storage, root_id, "eurostat",
                    read_release_manifest(release_store, pointer), finalize=False,
                )
            ) if direct_releases else None,
            after_pointer_write=(
                lambda release_store, pointer: finalize_source_medallion_navigation(
                    storage, root_id, "eurostat",
                    read_release_manifest(release_store, pointer),
                )
            ) if direct_releases else None,
            direct_releases=direct_releases,
        )
        report = {
            "status": "eurostat_platform_published",
            "release_id": result["release_id"],
            "source_id": EUROSTAT_SOURCE_ID,
            "coverage": coverage,
            "landing_snapshot_id": landing_manifest["snapshot_id"],
            "pending_tasks": len(campaign_state.get("pending", [])),
            "limitations": [
                "This progressive release models the complete reviewed three-dataset, 81-country-series API contract only; it is not complete Eurostat catalogue coverage.",
                "Full-distribution raw backfill continues independently and is not counted as modeled delivery until source-shaped full-distribution models are accepted.",
                "Observation values remain source-shaped and heterogeneous; only coverage/accountability metrics are semantically approved.",
            ],
        }
        rendered = json.dumps(report, indent=2, sort_keys=True)
        print(rendered, flush=True)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as handle:
                handle.write("## EUROSTAT modeled publication\n\n```json\n" + rendered + "\n```\n")
        if os.environ.get("GITHUB_OUTPUT"):
            with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as handle:
                handle.write(f"release_id={result['release_id']}\n")
        return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("drive", "local"), default="drive")
    parser.add_argument("--local-root", type=Path)
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--verify-current", action="store_true")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        run_platform(args.backend, args.local_root, args.allow_production_write, args.verify_current)
    except Exception as exc:  # pragma: no cover - CLI exit wrapper
        print(json.dumps({"status": "eurostat_platform_failed", "error_type": type(exc).__name__, "detail": str(exc)}), flush=True)
        raise
