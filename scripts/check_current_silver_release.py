"""Restore the selected release into a fresh local SQL consumer; read only."""
import hashlib
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from drive_release_store import DriveReleaseStore
from local_release import CachedReadStore, quoted_identifier, restore_local_release
from release_protocol import restore_current_release
from semantic_query import validate_release_metrics
from storage_manager import StorageManager


def _dataset_result(connection, dataset, relation):
    started_query = time.monotonic()
    date_column = dataset.get("date_column", "effectiveDate")
    date_sql = (
        f"min({quoted_identifier(date_column)}), max({quoted_identifier(date_column)})"
        if date_column
        else "NULL, NULL"
    )
    count, first, last = connection.execute(
        f"SELECT count(*), {date_sql} FROM {relation}"
    ).fetchone()
    first = first.isoformat() if first is not None else None
    last = last.isoformat() if last is not None else None
    columns = [
        {"name": row[0], "type": row[1]}
        for row in connection.execute(f"DESCRIBE {relation}").fetchall()
    ]
    if (
        count != dataset["row_count"]
        or first != dataset["min_date"]
        or last != dataset["max_date"]
        or columns != dataset["columns"]
    ):
        raise ValueError("Restored SQL results differ from release metadata")
    return {
        "dataset_id": dataset["dataset_id"],
        "rows": count,
        "min_date": first,
        "max_date": last,
        "query_seconds": round(time.monotonic() - started_query, 4),
    }


def _verify_named_database(manifest, database):
    results = []
    with duckdb.connect(str(database), read_only=True) as connection:
        for dataset in manifest["datasets"]:
            relation = (
                f"{quoted_identifier(dataset['layer'])}."
                f"{quoted_identifier(dataset['table_name'])}"
            )
            results.append(_dataset_result(connection, dataset, relation))
    return results


def _verify_legacy_release(manifest, store, directory):
    """Retain read-only SQL proof for format-v1 silver releases."""
    results = []
    with duckdb.connect() as connection:
        try:
            for index, dataset in enumerate(manifest["datasets"]):
                local_files = []
                for file_index, entry in enumerate(dataset["files"]):
                    destination = Path(directory) / f"dataset-{index}-{file_index}.parquet"
                    data = store.read(entry["id"])
                    if (
                        len(data) != entry["size"]
                        or hashlib.sha256(data).hexdigest() != entry["sha256"]
                    ):
                        raise ValueError("Released file changed during consumer restoration")
                    destination.write_bytes(data)
                    local_files.append(str(destination))
                connection.read_parquet(local_files).create_view("verified_dataset", replace=True)
                results.append(_dataset_result(connection, dataset, "verified_dataset"))
        finally:
            connection.execute("DROP VIEW IF EXISTS verified_dataset")
    return results


def _build_report(manifest, datasets, elapsed_seconds, download_bytes, semantic_report=None):
    metrics = semantic_report.get("metrics", []) if semantic_report else []
    report = {
        "status": "fresh_consumer_verified",
        "release_scope": manifest["release_scope"],
        "release_id": manifest["release_id"],
        "code_sha": manifest["code_sha"],
        "read_only": True,
        "dataset_count": len(datasets),
        "datasets": datasets,
        "query_metrics": {
            "metric_count": len(metrics),
            "row_count": sum(item["row_count"] for item in metrics),
        },
        "download_bytes": download_bytes,
        "restore_and_check_seconds": round(elapsed_seconds, 3),
    }
    if semantic_report is not None:
        report["semantic_validation"] = semantic_report
    return report


def check_current():
    started = time.monotonic()
    storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
    root = storage.resolve_root(create=False)
    store = CachedReadStore(DriveReleaseStore(storage, root))
    manifest = restore_current_release(store, root)
    expected_release = os.environ.get("ZOHELO_EXPECTED_RELEASE_ID")
    if expected_release and manifest["release_id"] != expected_release:
        raise ValueError("Current release differs from the expected publication")
    semantic_report = None
    with tempfile.TemporaryDirectory(prefix="zohelo-release-reader-") as temporary:
        directory = Path(temporary)
        if manifest["release_scope"] == "nbp_platform":
            restored_directory = directory / "release"
            restored = restore_local_release(store, root, restored_directory)
            restored_manifest = json.loads(
                (restored_directory / "release.json").read_text(encoding="utf-8")
            )
            if restored_manifest["release_id"] != manifest["release_id"]:
                raise ValueError("Pinned local release differs from the selected publication")
            if expected_release and restored_manifest["release_id"] != expected_release:
                raise ValueError("Restored release differs from the expected publication")
            results = _verify_named_database(restored_manifest, restored["database"])
            semantic_manifest = restored_directory / "semantic_manifest.json"
            if semantic_manifest.is_file():
                semantic_report = validate_release_metrics(
                    restored["database"],
                    semantic_manifest,
                    restored_directory / "fresh-metric-validation",
                )
            manifest = restored_manifest
        else:
            results = _verify_legacy_release(manifest, store, directory)
    if expected_release and manifest["release_id"] != expected_release:
        raise ValueError("Verified release differs from the expected publication")
    report = _build_report(
        manifest,
        results,
        time.monotonic() - started,
        store.bytes_read,
        semantic_report,
    )
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a") as handle:
            handle.write("\n\n## Fresh release consumer\n\n" + rendered + "\n")
    return report


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        check_current()
    except Exception as exc:
        print(json.dumps({"status": "fresh_silver_consumer_failed", "error_type": type(exc).__name__}))
        raise SystemExit(1)
