"""Restore the selected silver release into a fresh local SQL consumer; read only."""
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
from release_protocol import restore_current_release
from storage_manager import StorageManager


def check_current():
    started = time.monotonic()
    storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
    root = storage.resolve_root(create=False)
    store = DriveReleaseStore(storage, root)
    manifest = restore_current_release(store, root)
    expected_release = os.environ.get("ZOHELO_EXPECTED_RELEASE_ID")
    if expected_release and manifest["release_id"] != expected_release:
        raise ValueError("Current release differs from the expected publication")
    results = []
    with tempfile.TemporaryDirectory(prefix="zohelo-silver-reader-") as temporary:
        connection = duckdb.connect()
        try:
            for index, dataset in enumerate(manifest["datasets"]):
                local_files = []
                for file_index, entry in enumerate(dataset["files"]):
                    destination = Path(temporary) / f"dataset-{index}-{file_index}.parquet"
                    data = store.read(entry["id"])
                    if len(data) != entry["size"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
                        raise ValueError("Released file changed during consumer restoration")
                    destination.write_bytes(data)
                    local_files.append(str(destination))
                started_query = time.monotonic()
                connection.read_parquet(local_files).create_view("verified_dataset", replace=True)
                count, first, last = connection.execute(
                    "SELECT count(*), min(effectiveDate), max(effectiveDate) FROM verified_dataset"
                ).fetchone()
                columns = [{"name": row[0], "type": row[1]}
                           for row in connection.execute("DESCRIBE verified_dataset").fetchall()]
                if (count != dataset["row_count"] or first.isoformat() != dataset["min_date"]
                        or last.isoformat() != dataset["max_date"] or columns != dataset["columns"]):
                    raise ValueError("Restored SQL results differ from release metadata")
                results.append({"dataset_id": dataset["dataset_id"], "rows": count,
                                "min_date": first.isoformat(), "max_date": last.isoformat(),
                                "query_seconds": round(time.monotonic() - started_query, 4)})
        finally:
            connection.close()
    report = {"status": "fresh_silver_consumer_verified", "release_scope": "nbp_silver",
              "release_id": manifest["release_id"], "code_sha": manifest["code_sha"],
              "read_only": True, "datasets": results,
              "restore_and_check_seconds": round(time.monotonic() - started, 3)}
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a") as handle:
            handle.write("\n\n## Fresh silver consumer\n\n" + rendered + "\n")
    return report


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        check_current()
    except Exception as exc:
        print(json.dumps({"status": "fresh_silver_consumer_failed", "error_type": type(exc).__name__}))
        raise SystemExit(1)
