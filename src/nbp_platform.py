"""One commit, one resumable NBP ingestion, one validated platform publication."""
import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

import duckdb
import requests
import yaml

from business_catalog import build_business_catalog
from drive_release_store import DriveReleaseStore
from ingestion.drive_state_store import DriveStateStore
from ingestion.nbp_http import fetch_response
from ingestion.nbp_state import (LoadedState, commit_response, list_successful_response_descriptors,
                                 load_state, plan_requests, response_envelope,
                                 source_specs_from_config)
from release_protocol import publish_release
from storage_manager import StorageManager
from transformation.silver_builder import _code_sha

REPO_ROOT = Path(__file__).resolve().parents[1]
MAX_RAW_BYTES = 256 * 1024 * 1024
MAX_OBSERVATION_BATCHES = 2048
# Measured Drive checkpoint latency makes the remaining bootstrap about 50
# minutes. Keep intake bounded and reserve 30 minutes of the 90-minute job for
# publication, fresh reads and cold replay.
MAX_RUN_SECONDS = 60 * 60
DATASET_MODELS = {}
for suffix in ("a", "b", "c"):
    source = f"nbp_exchange_rates_table_{suffix}"
    DATASET_MODELS[f"bronze_{source}"] = ("02_bronze", source, f"br_nbp_table_{suffix}", "effective_date")
    DATASET_MODELS[source] = ("03_silver", source, f"stg_nbp_table_{suffix}", "effectiveDate")
DATASET_MODELS.update({
    "bronze_nbp_gold_prices": ("02_bronze", "nbp_gold_prices", "br_nbp_gold_prices", "effective_date"),
    "nbp_gold_prices": ("03_silver", "nbp_gold_prices", "stg_nbp_gold_prices", "effectiveDate"),
    "nbp_change_events": ("03_silver", "nbp_change_events", "nbp_change_events", "effective_date"),
    "dim_date": ("04_gold", "dim_date", "dim_date", "date_key"),
    "dim_currency": ("04_gold", "dim_currency", "dim_currency", None),
    "dim_source_table": ("04_gold", "dim_source_table", "dim_source_table", None),
    "dim_commodity": ("04_gold", "dim_commodity", "dim_commodity", None),
    "fact_fx_quotes": ("04_gold", "fact_fx_quotes", "fact_fx_quotes", "effective_date"),
    "fact_gold_prices": ("04_gold", "fact_gold_prices", "fact_gold_prices", "effective_date"),
})


def coverage_complete(state, cutoff):
    return all(source.get("last_checked_through_date") is not None
               and source["last_checked_through_date"] >= cutoff.isoformat()
               for source in state["sources"].values())


def ingest(storage, *, specs, cutoff, mode, code_sha, max_requests=512):
    """Persist after each dated response; a failed/capped run never publishes."""
    root = storage.resolve_root(create=False)
    control = storage.get_or_create_nested_folder(["ingestion-control"], root_id=root)
    store = DriveStateStore(storage, root, control)
    loaded = load_state(store, control, specs)
    if mode == "rebuild":
        if not coverage_complete(loaded.state, cutoff):
            raise ValueError("Raw replay requires complete verified date coverage through the selected cutoff")
        return store, loaded, {"requests": 0, "retrieved_bytes": 0, "network_retries": 0}
    landing = storage.resolve_zone("01_landing", create=True)
    folders = {source: storage.get_or_create_nested_folder([source], root_id=landing) for source in specs}
    started = time.monotonic()
    completed = set()
    historical_sources = set()
    run_id = str(uuid4())
    totals = {"requests": 0, "retrieved_bytes": 0, "network_retries": 0}
    with requests.Session() as session:
        while True:
            plans = plan_requests(loaded.state, specs, cutoff, max_catch_up_chunks=512,
                                  recent_recheck_days=93, max_historical_recheck_chunks=1)
            pending = [plan for plan in plans if (plan.source_id, plan.mode, plan.requested_start_date,
                                                  plan.requested_end_date) not in completed
                       and not (plan.mode == "historical_recheck" and plan.source_id in historical_sources)]
            if not pending:
                break
            for plan in pending:
                if totals["requests"] >= max_requests or time.monotonic() - started >= MAX_RUN_SECONDS:
                    print(json.dumps({"status": "nbp_ingestion_checkpointed", "reason": "run_budget_reached",
                                      **totals, "elapsed_seconds": round(time.monotonic() - started, 3),
                                      "checked_through": {key: value.get("last_checked_through_date")
                                                          for key, value in loaded.state["sources"].items()}}), flush=True)
                    raise RuntimeError("Run budget reached; verified raw progress is saved for the next run")
                requested_at = datetime.now(timezone.utc)
                try:
                    status, body, retries = fetch_response(plan, session=session)
                except (RuntimeError, ValueError, requests.RequestException):
                    # Persist the failed request as an attempt without advancing date coverage.
                    commit_response(store, control, loaded, specs, plan, http_status=0, body=b"",
                                    retrieved_at_utc=datetime.now(timezone.utc), started_at_utc=requested_at,
                                    run_id=run_id, code_sha=code_sha)
                    raise
                result = commit_response(
                    store, control, loaded, specs, plan, http_status=status, body=body,
                    retrieved_at_utc=datetime.now(timezone.utc), started_at_utc=requested_at,
                    landing_source_folder_id=folders[plan.source_id], run_id=run_id,
                    retry_count=retries, code_sha=code_sha,
                )
                if not result.advanced:
                    print(json.dumps({"status": "nbp_request_rejected", "source_id": plan.source_id,
                                      "start_date": plan.requested_start_date.isoformat(),
                                      "end_date": plan.requested_end_date.isoformat(), "http_status": status,
                                      "outcome": result.outcome, "attempt_file_id": result.attempt_file_id,
                                      "validation_error": result.validation_error}), flush=True)
                    raise ValueError("NBP response failed validation; attempt retained and coverage unchanged")
                # The commit already read back and verified both immutable state
                # snapshot and mutable pointer. Keep those exact pointer bytes
                # for the next commit's fresh drift comparison instead of
                # downloading the same objects again.
                loaded = LoadedState(
                    result.state, result.pointer_file_id, result.pointer_raw,
                    result.snapshot_file_id,
                )
                totals["requests"] += 1
                totals["retrieved_bytes"] += len(body)
                totals["network_retries"] += retries
                completed.add((plan.source_id, plan.mode, plan.requested_start_date, plan.requested_end_date))
                if plan.mode == "historical_recheck":
                    historical_sources.add(plan.source_id)
                print(json.dumps({"stage": "ingestion", "source_id": plan.source_id,
                                  "mode": plan.mode, "checked_interval_end": plan.requested_end_date.isoformat(),
                                  "outcome": result.outcome, "requests_completed": totals["requests"]}), flush=True)
    if not coverage_complete(loaded.state, cutoff):
        raise ValueError("Verified raw coverage is incomplete; previous consumer release retained")
    return store, loaded, totals


def download_envelopes(store, state, workspace):
    """Inventory bound before download; transfer and verify raw bytes, no data shaping."""
    descriptors = list_successful_response_descriptors(state)
    if not descriptors or len(descriptors) > MAX_OBSERVATION_BATCHES:
        raise ValueError("Raw observation batch count is empty or exceeds the configured limit")
    if any(not isinstance(item.get("size_bytes"), int) or item["size_bytes"] <= 0 for item in descriptors):
        raise ValueError("Raw inventory has invalid sizes")
    if sum(item["size_bytes"] for item in descriptors) > MAX_RAW_BYTES:
        raise ValueError("Raw working set exceeds the configured build limit")
    path = workspace / "batches.jsonl"
    cache = {}
    inputs = []
    with path.open("w", encoding="utf-8") as handle:
        for item in descriptors:
            file_id = item["raw_file_id"]
            if file_id not in cache:
                cache[file_id] = store.read(file_id)
            raw = cache[file_id]
            if len(raw) != item["size_bytes"]:
                raise ValueError("Raw response size differs from ingestion state")
            envelope = response_envelope(item, raw)
            handle.write(json.dumps(envelope, ensure_ascii=False) + "\n")
            inputs.append({"source_id": item["source_id"], "id": file_id, "size": len(raw),
                           "sha256": item["response_sha256"], "ingestion_sequence": item["ingestion_sequence"],
                           "provenance": "verified_exact_nbp_response"})
    return path, inputs, sum(len(value) for value in cache.values())


def build_platform(workspace, envelopes):
    workspace.mkdir(parents=True, exist_ok=True)
    database = workspace / "build.duckdb"
    target = workspace / "target"
    env = dict(os.environ, ZOHELO_NBP_BATCHES_PATH=str(envelopes), ZOHELO_DUCKDB_PATH=str(database),
               DBT_SEND_ANONYMOUS_USAGE_STATS="false", DO_NOT_TRACK="1")
    # dbt only needs local data; keep cloud credentials in the transport process.
    env = {key: value for key, value in env.items() if not key.startswith(("GOOGLE_", "GCP_"))}
    common = ["--profiles-dir", str(REPO_ROOT), "--target-path", str(target),
              "--log-path", str(workspace / "logs"), "--no-partial-parse",
              "--vars", '{"nbp_verified_batches": true}']
    cli = [sys.executable, "-c", "from dbt.cli.main import cli; cli()"]
    subprocess.run([*cli, "build", "--select", *[entry[2] for entry in DATASET_MODELS.values()], *common],
                   cwd=REPO_ROOT, env=env, check=True, timeout=900)
    results = (target / "run_results.json").read_bytes()
    subprocess.run([*cli, "docs", "generate", "--no-compile", *common],
                   cwd=REPO_ROOT, env=env, check=True, timeout=300)
    (target / "run_results.json").write_bytes(results)
    datasets = []
    with duckdb.connect(str(database), read_only=True) as connection:
        for dataset_id, (layer, table, model, date_column) in DATASET_MODELS.items():
            output = workspace / layer / f"{dataset_id}.parquet"
            output.parent.mkdir(exist_ok=True)
            escaped = str(output).replace("'", "''")
            connection.execute(f"COPY (SELECT * FROM {model}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            date_sql = f'min("{date_column}"), max("{date_column}")' if date_column else "NULL, NULL"
            count, first, last = connection.execute(f"SELECT count(*), {date_sql} FROM {model}").fetchone()
            if count == 0 and dataset_id != "nbp_change_events":
                raise ValueError(f"Required platform dataset is empty: {dataset_id}")
            datasets.append({"dataset_id": dataset_id, "table_name": table, "model_name": model,
                             "model_id": f"model.zohelo_data.{model}", "layer": layer, "path": str(output),
                             "row_count": count, "date_column": date_column,
                             "min_date": first.isoformat() if first is not None else None,
                             "max_date": last.isoformat() if last is not None else None,
                             "columns": [{"name": row[0], "type": row[1]} for row in
                                         connection.execute(f"DESCRIBE {model}").fetchall()]})
    return datasets, [{"name": name, "path": str(target / name)}
                      for name in ("manifest.json", "catalog.json", "run_results.json")]


def check_portal_compatibility():
    with requests.get("https://data.zohelo.com/portal-build.json", timeout=(10, 30),
                      stream=True, allow_redirects=False) as response:
        if response.status_code != 200:
            raise ValueError("The portal build could not be verified before v2 publication")
        data = b""
        for chunk in response.iter_content(4096):
            data += chunk
            if len(data) > 16384:
                raise ValueError("Portal build metadata exceeds the limit")
        if 2 not in json.loads(data).get("supported_release_formats", []):
            raise ValueError("Deploy the portal v2 reader before publishing the platform release")


def run_platform(mode="incremental", cutoff=None, max_requests=512):
    started = time.monotonic()
    sha = _code_sha()
    cutoff = cutoff or (datetime.now(timezone.utc).date() - timedelta(days=1))
    if cutoff >= datetime.now(timezone.utc).date():
        raise ValueError("Only completed UTC days can be marked checked")
    config = yaml.safe_load((REPO_ROOT / "config/sources.yaml").read_text())
    specs = source_specs_from_config(config)
    storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
    storage.authorize_writes()
    print(json.dumps({"stage": "ingestion", "status": "started", "cutoff": cutoff.isoformat(), "code_sha": sha}), flush=True)
    store, loaded, ingestion_totals = ingest(storage, specs=specs, cutoff=cutoff, mode=mode,
                                            code_sha=sha, max_requests=max_requests)
    ingested = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="zohelo-platform-") as temporary:
        workspace = Path(temporary)
        print(json.dumps({"stage": "raw_download", "status": "started"}), flush=True)
        envelopes, inputs, transferred = download_envelopes(store, loaded.state, workspace)
        downloaded = time.monotonic()
        print(json.dumps({"stage": "dbt_build", "status": "started", "raw_bytes": transferred,
                          "input_batches": len(inputs)}), flush=True)
        datasets, artifacts = build_platform(workspace, envelopes)
        built = time.monotonic()
        catalogue_state = {"sources": {
            source_id: {"checked_through": item["last_checked_through_date"],
                        "latest_observation_date": item["latest_observation_date"],
                        "last_successful_ingestion_at": item["last_successful_ingestion_at_utc"],
                        "last_attempt_at": item["last_attempt_at_utc"],
                        "raw_response_count": len(item["successful_responses"])}
            for source_id, item in loaded.state["sources"].items()}}
        catalogue = build_business_catalog(code_sha=sha, source_config=config, ingestion_state=catalogue_state,
                                          dbt_manifest=json.loads((workspace / "target/manifest.json").read_text()),
                                          dataset_metadata=datasets)
        evidence = dict(loaded.state, code_sha=sha, cutoff=cutoff.isoformat(),
                        state_file_id=loaded.snapshot_file_id, state_pointer_id=loaded.pointer_file_id)
        for name, document in (("business-catalog.json", catalogue), ("ingestion-state.json", evidence)):
            path = workspace / name
            path.write_text(json.dumps(document, ensure_ascii=False, sort_keys=True), encoding="utf-8")
            artifacts.append({"name": name, "path": str(path)})
        measurements = {**ingestion_totals, "input_observation_batches": len(inputs),
                        "raw_unique_transfer_bytes": transferred, "input_bytes": sum(item["size"] for item in inputs),
                        "output_bytes": sum(Path(item["path"]).stat().st_size for item in datasets),
                        "ingestion_seconds": round(ingested - started, 3),
                        "download_seconds": round(downloaded - ingested, 3),
                        "dbt_and_export_seconds": round(built - downloaded, 3),
                        "working_directory_bytes": sum(path.stat().st_size for path in workspace.rglob("*") if path.is_file())}
        check_portal_compatibility()
        print(json.dumps({"stage": "publication", "status": "started", "datasets": len(datasets)}), flush=True)
        result = publish_release(DriveReleaseStore(storage, store.root_id), store.root_id, datasets=datasets,
                                 artifacts=artifacts, inputs=inputs, code_sha=sha, measurements=measurements,
                                 release_scope="nbp_platform")
        report = {"status": "nbp_platform_published", "release_id": result["release_id"], "code_sha": sha,
                  "cutoff": cutoff.isoformat(), "coverage": catalogue_state["sources"],
                  "datasets": [{key: item[key] for key in ("dataset_id", "row_count", "min_date", "max_date")}
                               for item in datasets],
                  "measurements": {**measurements, "publication_seconds": round(time.monotonic() - built, 3)},
                  "limitations": ["Metric definitions await business approval", "Older corrections are detected by a rotating recheck, not an NBP correction feed"]}
        rendered = json.dumps(report, indent=2, sort_keys=True)
        print(rendered, flush=True)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            Path(os.environ["GITHUB_STEP_SUMMARY"]).write_text("## NBP platform publication\n\n" + rendered + "\n")
        if os.environ.get("GITHUB_OUTPUT"):
            with Path(os.environ["GITHUB_OUTPUT"]).open("a") as handle:
                handle.write(f"release_id={result['release_id']}\n")
        return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["incremental", "full", "rebuild"], default="incremental")
    parser.add_argument("--cutoff", type=date.fromisoformat)
    parser.add_argument("--max-requests", type=int, default=512)
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        if not 1 <= args.max_requests <= 512:
            raise ValueError("Request budget must be between 1 and 512")
        run_platform(args.mode, args.cutoff, args.max_requests)
    except Exception as exc:
        print(json.dumps({"status": "nbp_platform_failed", "error_type": type(exc).__name__,
                          "message": "Publication not confirmed. Verified ingestion progress and previous releases are retained."}), flush=True)
        raise SystemExit(1)
