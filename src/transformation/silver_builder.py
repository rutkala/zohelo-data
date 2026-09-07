"""Build all four NBP silver tables, then publish a verified immutable snapshot."""
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import duckdb
from googleapiclient.http import MediaIoBaseDownload

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from drive_release_store import DriveReleaseStore
from release_protocol import publish_release
from storage_manager import StorageManager

REPO_ROOT = Path(__file__).resolve().parents[2]
DATASET_MODELS = {
    "nbp_exchange_rates_table_a": "stg_nbp_table_a",
    "nbp_exchange_rates_table_b": "stg_nbp_table_b",
    "nbp_exchange_rates_table_c": "stg_nbp_table_c",
    "nbp_gold_prices": "stg_nbp_gold_prices",
}
MAX_INPUT_BYTES = 256 * 1024 * 1024
MAX_INPUT_FILES = 2048


def _dataset_to_model_name(dataset_name):
    return DATASET_MODELS.get(dataset_name)


def _model_to_dataset_name(model_name):
    return next((key for key, value in DATASET_MODELS.items() if value == model_name), None)


def _discover_staging_models(dataset_names):
    missing = set(DATASET_MODELS) - set(dataset_names)
    if missing:
        raise ValueError("Missing NBP bronze datasets: " + ", ".join(sorted(missing)))
    return list(DATASET_MODELS.values())


def _children(drive, parent_id, *, folders_only=False):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", parent_id):
        raise ValueError("Invalid Drive folder identity")
    query = f"'{parent_id}' in parents and trashed=false"
    if folders_only:
        query += " and mimeType='application/vnd.google-apps.folder'"
    token = None
    while True:
        response = drive.files().list(
            q=query, spaces="drive", pageSize=1000, pageToken=token,
            fields="nextPageToken,files(id,name,mimeType,size,modifiedTime)",
        ).execute(num_retries=2)
        yield from response.get("files", [])
        token = response.get("nextPageToken")
        if not token:
            break


def _parquet_files(drive, parent_id, seen=None):
    seen = set() if seen is None else seen
    if parent_id in seen:
        raise ValueError("Repeated folder in bronze input inventory")
    seen.add(parent_id)
    for item in _children(drive, parent_id):
        if item.get("mimeType") == "application/vnd.google-apps.folder":
            yield from _parquet_files(drive, item["id"], seen)
        elif item.get("name", "").lower().endswith(".parquet"):
            yield item


def download_bronze(storage, workspace):
    """Inventory before transfer; use fixed local names, not remote paths."""
    bronze_id = storage.resolve_zone("02_bronze", create=False)
    folders = list(_children(storage.drive_service, bronze_id, folders_only=True))
    inventory = []
    for dataset_id in DATASET_MODELS:
        matches = [folder for folder in folders if folder["name"] == dataset_id]
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one bronze folder for {dataset_id}")
        files = list(_parquet_files(storage.drive_service, matches[0]["id"]))
        if not files:
            raise ValueError(f"No bronze Parquet inputs for {dataset_id}")
        for item in sorted(files, key=lambda value: value["id"]):
            size = int(item.get("size", -1))
            if size <= 0:
                raise ValueError(f"Missing or invalid input size for {dataset_id}")
            inventory.append({**item, "dataset_id": dataset_id, "size": size})
    expected_bytes = sum(item["size"] for item in inventory)
    if expected_bytes > MAX_INPUT_BYTES or len(inventory) > MAX_INPUT_FILES:
        raise ValueError("NBP bronze working set exceeds the configured build limit")
    inputs = []
    for index, item in enumerate(inventory):
        destination = workspace / "02_bronze" / item["dataset_id"] / f"input-{index:06d}.parquet"
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("wb") as handle:
            downloader = MediaIoBaseDownload(
                handle, storage.drive_service.files().get_media(fileId=item["id"]),
                chunksize=1024 * 1024,
            )
            done = False
            while not done:
                _, done = downloader.next_chunk(num_retries=2)
                if handle.tell() > item["size"]:
                    raise ValueError("Bronze input grew during transfer")
        data = destination.read_bytes()
        if len(data) != item["size"]:
            raise ValueError("Bronze input size changed during transfer")
        inputs.append({
            "dataset_id": item["dataset_id"], "id": item["id"], "name": item["name"],
            "size": len(data), "sha256": hashlib.sha256(data).hexdigest(),
            "modified_at_utc": item.get("modifiedTime"),
            "provenance": "legacy_bronze_raw_batch_link_unverified",
        })
    return inputs


def build_silver(workspace):
    database = workspace / "build.duckdb"
    target = workspace / "target"
    env = dict(os.environ, ZOHELO_DATA_ROOT=str(workspace), ZOHELO_DUCKDB_PATH=str(database),
               DBT_SEND_ANONYMOUS_USAGE_STATS="false", DO_NOT_TRACK="1")
    executable = Path(sys.executable).parent / "dbt"
    if not executable.is_file():
        raise RuntimeError("Install the pinned repository dependencies before building silver")
    common = ["--profiles-dir", str(REPO_ROOT), "--target-path", str(target),
              "--log-path", str(workspace / "logs"), "--no-partial-parse"]
    subprocess.run([executable, "build", "--select", *DATASET_MODELS.values(), *common],
                   cwd=REPO_ROOT, env=env, check=True, timeout=600)
    # Preserve build/test evidence across the later documentation invocation.
    build_results = (target / "run_results.json").read_bytes()
    subprocess.run([executable, "docs", "generate", "--no-compile", *common],
                   cwd=REPO_ROOT, env=env, check=True, timeout=300)
    (target / "run_results.json").write_bytes(build_results)
    datasets = []
    con = duckdb.connect(str(database), read_only=True)
    try:
        for dataset_id, model in DATASET_MODELS.items():
            output = workspace / "03_silver" / f"{dataset_id}.parquet"
            output.parent.mkdir(exist_ok=True)
            escaped = str(output).replace("'", "''")
            con.execute(f"COPY (SELECT * FROM {model}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            count, start, end = con.execute(
                f"SELECT count(*), min(effectiveDate), max(effectiveDate) FROM {model}"
            ).fetchone()
            if not count or start is None or end is None:
                raise ValueError(f"Silver dataset is empty: {dataset_id}")
            datasets.append({
                "dataset_id": dataset_id, "layer": "03_silver", "table_name": dataset_id,
                "path": str(output), "row_count": count, "min_date": start.isoformat(),
                "max_date": end.isoformat(),
                "columns": [{"name": row[0], "type": row[1]}
                            for row in con.execute(f"DESCRIBE {model}").fetchall()],
            })
    finally:
        con.close()
    return datasets, [{"name": name, "path": str(target / name)}
                      for name in ("manifest.json", "catalog.json", "run_results.json")]


def _code_sha():
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("Build code must identify a Git commit")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO_ROOT, text=True).strip():
        raise ValueError("Commit the working tree before publishing a data release")
    if os.environ.get("GITHUB_SHA", sha) != sha:
        raise ValueError("Build checkout does not match the workflow commit")
    return sha


def process_silver():
    started = time.monotonic()
    sha = _code_sha()
    storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
    root_id = storage.resolve_root(create=False)
    if DriveReleaseStore(storage, root_id).find("ingestion-control", root_id):
        raise RuntimeError("Verified ingestion is active. Use src/nbp_platform.py; legacy publication is disabled.")
    storage.authorize_writes()
    with tempfile.TemporaryDirectory(prefix="zohelo-silver-") as temporary:
        workspace = Path(temporary)
        inputs = download_bronze(storage, workspace)
        downloaded = time.monotonic()
        datasets, artifacts = build_silver(workspace)
        built = time.monotonic()
        measurements = {
            "input_files": len(inputs), "input_bytes": sum(item["size"] for item in inputs),
            "output_bytes": sum(Path(item["path"]).stat().st_size for item in datasets),
            "download_seconds": round(downloaded - started, 3),
            "dbt_and_export_seconds": round(built - downloaded, 3),
            "working_directory_bytes": sum(path.stat().st_size for path in workspace.rglob("*") if path.is_file()),
        }
        result = publish_release(DriveReleaseStore(storage, root_id), root_id,
                                 datasets=datasets, artifacts=artifacts, inputs=inputs,
                                 code_sha=sha, measurements=measurements)
        report = {
            "status": "silver_release_published", "release_scope": "nbp_silver",
            "release_id": result["release_id"], "code_sha": sha,
            "measurements": {**measurements, "publish_seconds": round(time.monotonic() - built, 3)},
            "datasets": [{key: item[key] for key in ("dataset_id", "row_count", "min_date", "max_date")}
                         for item in datasets],
            "limitations": ["Gold and MetricFlow remain pending", "Legacy raw batch links are unverified"],
        }
        rendered = json.dumps(report, indent=2, sort_keys=True)
        print(rendered)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            Path(os.environ["GITHUB_STEP_SUMMARY"]).write_text("## NBP silver release\n\n" + rendered + "\n")
        if os.environ.get("GITHUB_OUTPUT"):
            with Path(os.environ["GITHUB_OUTPUT"]).open("a") as handle:
                handle.write(f"release_id={result['release_id']}\n")
        return report


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        process_silver()
    except Exception as exc:
        print(json.dumps({"status": "silver_release_failed", "error_type": type(exc).__name__,
                          "message": "The candidate was not confirmed. Inspect build checks; prior releases are retained."}))
        raise SystemExit(1)
