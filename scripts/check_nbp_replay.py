"""Read-only fresh-process replay check for the selected NBP platform release."""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from drive_release_store import DriveReleaseStore  # noqa: E402
from ingestion.nbp_state import list_successful_response_descriptors  # noqa: E402
from nbp_platform import DATASET_MODELS, MAX_RAW_BYTES, build_platform, download_envelopes  # noqa: E402
from release_protocol import restore_current_release  # noqa: E402
from storage_manager import StorageManager  # noqa: E402

_EXPECTED_SOURCES = frozenset({
    "nbp_exchange_rates_table_a",
    "nbp_exchange_rates_table_b",
    "nbp_exchange_rates_table_c",
    "nbp_gold_prices",
})
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class _BoundedReleaseReader:
    """Only expose bounded binary reads needed by ``download_envelopes``."""

    def __init__(self, store: DriveReleaseStore):
        self._store = store

    def read(self, file_id: str) -> bytes:
        data = self._store.read(file_id)
        if not isinstance(data, bytes) or len(data) > MAX_RAW_BYTES:
            raise ValueError("Released raw response is not a bounded byte stream")
        return data


def _git_head() -> str:
    value = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
    ).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("Current checkout does not identify a Git commit")
    return value


def _released_artifact(store: DriveReleaseStore, manifest: dict[str, Any], name: str) -> bytes:
    matches = [item for item in manifest.get("artifacts", []) if isinstance(item, dict) and item.get("name") == name]
    if len(matches) != 1:
        raise ValueError("Selected release has an ambiguous required artifact")
    entry = matches[0]
    if not isinstance(entry.get("id"), str) or not isinstance(entry.get("size"), int) or not isinstance(entry.get("sha256"), str) or not _SHA_RE.fullmatch(entry["sha256"]):
        raise ValueError("Selected release has invalid artifact fingerprint metadata")
    data = store.read(entry["id"])
    if len(data) != entry["size"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
        raise ValueError("Selected release artifact does not match its fingerprint")
    return data


def _input_key(item: dict[str, Any]) -> tuple[str, str, int, str, int]:
    source_id = item.get("source_id")
    file_id = item.get("id", item.get("raw_file_id"))
    size = item.get("size", item.get("size_bytes"))
    digest = item.get("sha256", item.get("response_sha256"))
    sequence = item.get("ingestion_sequence")
    if (source_id not in _EXPECTED_SOURCES or not isinstance(file_id, str)
            or not isinstance(size, int) or size <= 0
            or not isinstance(digest, str) or not _SHA_RE.fullmatch(digest)
            or not isinstance(sequence, int) or sequence < 1):
        raise ValueError("Release input inventory is incomplete")
    return source_id, file_id, size, digest, sequence


def _validate_selected_inputs(manifest: dict[str, Any], state: dict[str, Any]) -> None:
    if state.get("format_version") != 1 or not isinstance(state.get("sources"), dict) or set(state["sources"]) != _EXPECTED_SOURCES:
        raise ValueError("Selected release ingestion evidence has unexpected sources")
    descriptors = list_successful_response_descriptors(state)
    state_keys = {_input_key(item) for item in descriptors}
    manifest_inputs = manifest.get("inputs")
    if not isinstance(manifest_inputs, list):
        raise ValueError("Selected release has no input inventory")
    manifest_keys = {_input_key(item) for item in manifest_inputs if isinstance(item, dict)}
    if len(manifest_keys) != len(manifest_inputs) or not manifest_keys or manifest_keys != state_keys:
        raise ValueError("Selected release input inventory differs from its ingestion evidence")
    if {key[0] for key in manifest_keys} != _EXPECTED_SOURCES:
        raise ValueError("Selected release input inventory omits an NBP source")


def _read_published_dataset(store: DriveReleaseStore, dataset: dict[str, Any], destination: Path) -> None:
    files = dataset.get("files")
    if not isinstance(files, list) or len(files) != 1 or not isinstance(files[0], dict):
        raise ValueError("Selected release has invalid dataset files")
    entry = files[0]
    if not isinstance(entry.get("id"), str) or not isinstance(entry.get("size"), int) or not isinstance(entry.get("sha256"), str) or not _SHA_RE.fullmatch(entry["sha256"]):
        raise ValueError("Selected release has invalid dataset fingerprint metadata")
    data = store.read(entry["id"])
    if len(data) != entry["size"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
        raise ValueError("Selected release dataset does not match its fingerprint")
    destination.write_bytes(data)


def _quoted_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _different_rows(connection: duckdb.DuckDBPyConnection, left: Path, right: Path) -> bool:
    left_sql, right_sql = _quoted_path(left), _quoted_path(right)
    for first, second in ((left_sql, right_sql), (right_sql, left_sql)):
        query = (
            f"SELECT * FROM read_parquet('{first}') "
            f"EXCEPT ALL SELECT * FROM read_parquet('{second}') LIMIT 1"
        )
        if connection.execute(query).fetchone() is not None:
            return True
    return False


def check_replay() -> dict[str, Any]:
    started = time.monotonic()
    head = _git_head()
    storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
    root = storage.resolve_root(create=False)
    store = DriveReleaseStore(storage, root)
    manifest = restore_current_release(store, root)
    if manifest.get("format_version") != 2 or manifest.get("release_scope") != "nbp_platform":
        raise ValueError("Selected current release is not an NBP platform format-version-2 release")
    if manifest.get("code_sha") != head:
        raise ValueError("Selected release code SHA differs from the current checkout")
    expected_release = os.environ.get("ZOHELO_EXPECTED_RELEASE_ID")
    if expected_release and manifest.get("release_id") != expected_release:
        raise ValueError("Selected release differs from the expected release ID")

    state_raw = _released_artifact(store, manifest, "ingestion-state.json")
    try:
        state = json.loads(state_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Selected release ingestion evidence is not JSON") from exc
    if not isinstance(state, dict):
        raise ValueError("Selected release ingestion evidence is not an object")
    _validate_selected_inputs(manifest, state)

    with tempfile.TemporaryDirectory(prefix="zohelo-nbp-replay-") as temporary:
        workspace = Path(temporary)
        envelopes, inputs, transferred = download_envelopes(_BoundedReleaseReader(store), state, workspace)
        downloaded = time.monotonic()
        rebuilt, _artifacts = build_platform(workspace / "rebuilt", envelopes)
        built = time.monotonic()
        expected = {item.get("dataset_id"): item for item in manifest.get("datasets", []) if isinstance(item, dict)}
        actual = {item.get("dataset_id"): item for item in rebuilt if isinstance(item, dict)}
        if set(expected) != set(DATASET_MODELS) or set(actual) != set(DATASET_MODELS):
            raise ValueError("Selected or rebuilt release does not contain the 15 required datasets")

        exact_rows = 0
        with duckdb.connect() as connection:
            for index, dataset_id in enumerate(sorted(DATASET_MODELS)):
                published, rebuilt_dataset = expected[dataset_id], actual[dataset_id]
                for field in ("layer", "table_name", "model_name", "model_id", "date_column", "row_count", "min_date", "max_date", "columns"):
                    if published.get(field) != rebuilt_dataset.get(field):
                        raise ValueError("Rebuilt dataset metadata differs from selected release")
                published_path = workspace / "published" / f"dataset-{index}.parquet"
                published_path.parent.mkdir(exist_ok=True)
                _read_published_dataset(store, published, published_path)
                rebuilt_path = Path(rebuilt_dataset["path"])
                if _different_rows(connection, published_path, rebuilt_path):
                    raise ValueError("Rebuilt dataset rows differ from selected release")
                exact_rows += rebuilt_dataset["row_count"]
        compared = time.monotonic()

    report = {
        "status": "fresh_raw_replay_verified",
        "release_id": manifest["release_id"],
        "code_sha": head,
        "read_only": True,
        "tables_compared": len(DATASET_MODELS),
        "exact_rows_matched": exact_rows,
        "input_observation_batches": len(inputs),
        "raw_unique_transfer_bytes": transferred,
        "download_seconds": round(downloaded - started, 3),
        "dbt_seconds": round(built - downloaded, 3),
        "compare_seconds": round(compared - built, 3),
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered, flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as handle:
            handle.write("\n\n## Fresh NBP raw replay\n\n" + rendered + "\n")
    return report


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        check_replay()
    except Exception as exc:
        print(json.dumps({"status": "fresh_raw_replay_failed", "error_type": type(exc).__name__}), flush=True)
        raise SystemExit(1)
