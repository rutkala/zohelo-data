"""Read-only key-coverage audit from a retained NBP silver release to v2."""
from __future__ import annotations

import argparse
from datetime import date
from hashlib import sha256
import json
import logging
import os
from pathlib import Path
import sys
import tempfile
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from drive_release_store import DriveReleaseStore  # noqa: E402
from release_protocol import ReleaseProtocolError, restore_current_release, restore_release  # noqa: E402
from storage_manager import StorageManager  # noqa: E402

SILVER_DATASETS = (
    "nbp_exchange_rates_table_a",
    "nbp_exchange_rates_table_b",
    "nbp_exchange_rates_table_c",
    "nbp_gold_prices",
)
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_TOTAL_BYTES = 256 * 1024 * 1024
MAX_MISSING_SAMPLES = 20


class MigrationAuditError(RuntimeError):
    """A retained baseline or current migration candidate is unsafe or incomplete."""


class MigrationCoverageError(MigrationAuditError):
    """A complete, bounded coverage report contains missing retained keys."""

    def __init__(self, message: str, comparison: dict[str, Any]):
        super().__init__(message)
        self.comparison = comparison


def _json_object(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationAuditError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise MigrationAuditError(f"{label} must be an object")
    return value


def _exact_one(values: list[str], label: str) -> str:
    if len(values) != 1:
        raise MigrationAuditError(f"{label} is missing or ambiguous")
    return values[0]


def load_baseline_release(
    store: Any, root_id: str, baseline_release_id: str, baseline_code_sha: str
) -> dict[str, Any]:
    """Restore one immutable v1 manifest by its retained release folder identity."""
    releases_id = _exact_one(store.find("releases", root_id), "releases folder")
    baseline_folder_id = _exact_one(
        store.find(baseline_release_id, releases_id), "baseline release folder"
    )
    manifest_file_id = _exact_one(
        store.find("release.json", baseline_folder_id), "baseline release manifest"
    )
    raw = store.read(manifest_file_id)
    if not isinstance(raw, bytes):
        raise MigrationAuditError("baseline release manifest did not return bytes")
    manifest = _json_object(raw, "baseline release manifest")
    if manifest.get("release_id") != baseline_release_id:
        raise MigrationAuditError("baseline release manifest ID does not match requested release")
    if manifest.get("code_sha") != baseline_code_sha:
        raise MigrationAuditError("baseline release code SHA does not match the expected baseline")
    pointer = {
        "format_version": 1,
        "release_id": baseline_release_id,
        "manifest_file_id": manifest_file_id,
        "manifest_sha256": sha256(raw).hexdigest(),
        "updated_at_utc": "migration-audit",
    }
    try:
        restored = restore_release(store, pointer)
    except ReleaseProtocolError as exc:
        raise MigrationAuditError("baseline release did not restore and verify") from exc
    if restored.get("format_version") != 1 or restored.get("release_scope") != "nbp_silver":
        raise MigrationAuditError("requested baseline is not a v1 NBP silver release")
    return restored


def _silver_dataset(manifest: dict[str, Any], dataset_id: str, label: str) -> dict[str, Any]:
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list):
        raise MigrationAuditError(f"{label} release has invalid datasets")
    matches = [item for item in datasets if isinstance(item, dict) and item.get("dataset_id") == dataset_id]
    if len(matches) != 1:
        raise MigrationAuditError(f"{label} release has missing or duplicate silver dataset")
    return matches[0]


def _read_dataset(store: Any, dataset: dict[str, Any], destination: Path, downloaded: int) -> int:
    files = dataset.get("files")
    if not isinstance(files, list) or len(files) != 1 or not isinstance(files[0], dict):
        raise MigrationAuditError("release dataset has invalid file metadata")
    entry = files[0]
    size, digest, file_id = entry.get("size"), entry.get("sha256"), entry.get("id")
    if (not isinstance(size, int) or isinstance(size, bool) or size <= 0 or size > MAX_FILE_BYTES
            or not isinstance(digest, str) or len(digest) != 64
            or not isinstance(file_id, str)):
        raise MigrationAuditError("release dataset exceeds bounded fingerprint metadata")
    if downloaded + size > MAX_TOTAL_BYTES:
        raise MigrationAuditError("migration audit downloads exceed the safety limit")
    data = store.read(file_id)
    if not isinstance(data, bytes) or len(data) != size or sha256(data).hexdigest() != digest:
        raise MigrationAuditError("release dataset did not match its fingerprint")
    destination.write_bytes(data)
    return downloaded + size


def _date(value: Any, label: str) -> date:
    if not isinstance(value, str):
        raise MigrationAuditError(f"{label} has no ISO date bound")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise MigrationAuditError(f"{label} has no ISO date bound") from exc


def _quoted(path: Path) -> str:
    return str(path).replace("'", "''")


def _key_columns(dataset_id: str) -> tuple[str, ...]:
    return ("effectiveDate",) if dataset_id == "nbp_gold_prices" else ("effectiveDate", "code")


def _report_value(value: Any) -> Any:
    return value.isoformat() if isinstance(value, date) else value


def _key_query(path: Path, columns: tuple[str, ...], cutoff: str) -> str:
    selected = ", ".join(f'"{column}"' for column in columns)
    return f"SELECT DISTINCT {selected} FROM read_parquet('{_quoted(path)}') WHERE \"effectiveDate\" <= DATE '{cutoff}'"


def _parquet_date_bounds(connection: Any, path: Path, dataset_id: str, label: str) -> tuple[date, date]:
    row = connection.execute(
        f"SELECT min(\"effectiveDate\"), max(\"effectiveDate\") FROM read_parquet('{_quoted(path)}')"
    ).fetchone()
    if not isinstance(row, tuple) or len(row) != 2 or row[0] is None or row[1] is None:
        raise MigrationAuditError(f"{label} {dataset_id} parquet has no effectiveDate values")
    try:
        return date.fromisoformat(str(row[0])), date.fromisoformat(str(row[1]))
    except ValueError as exc:
        raise MigrationAuditError(f"{label} {dataset_id} parquet has invalid effectiveDate values") from exc


def compare_silver_key_coverage(
    store: Any, baseline: dict[str, Any], current: dict[str, Any], directory: Path
) -> dict[str, Any]:
    """Verify retained v1 analytical keys are present in v2 through the old cutoff."""
    if baseline.get("format_version") != 1 or baseline.get("release_scope") != "nbp_silver":
        raise MigrationAuditError("baseline release must be v1 NBP silver")
    if current.get("format_version") != 2 or current.get("release_scope") != "nbp_platform":
        raise MigrationAuditError("current release must be v2 NBP platform")
    directory.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    reports: list[dict[str, Any]] = []
    violations: list[dict[str, str]] = []
    with duckdb.connect() as connection:
        for index, dataset_id in enumerate(SILVER_DATASETS):
            old_dataset = _silver_dataset(baseline, dataset_id, "baseline")
            new_dataset = _silver_dataset(current, dataset_id, "current")
            old_min = _date(old_dataset.get("min_date"), f"baseline {dataset_id}")
            old_max = _date(old_dataset.get("max_date"), f"baseline {dataset_id}")
            new_min = _date(new_dataset.get("min_date"), f"current {dataset_id}")
            new_max = _date(new_dataset.get("max_date"), f"current {dataset_id}")
            old_path = directory / f"baseline-{index}.parquet"
            new_path = directory / f"current-{index}.parquet"
            downloaded = _read_dataset(store, old_dataset, old_path, downloaded)
            downloaded = _read_dataset(store, new_dataset, new_path, downloaded)
            old_data_min, old_data_max = _parquet_date_bounds(connection, old_path, dataset_id, "baseline")
            new_data_min, new_data_max = _parquet_date_bounds(connection, new_path, dataset_id, "current")
            metadata_matches_parquet = (
                (old_min, old_max) == (old_data_min, old_data_max)
                and (new_min, new_max) == (new_data_min, new_data_max)
            )
            if not metadata_matches_parquet:
                violations.append({"dataset_id": dataset_id, "reason": "date_bounds_metadata_mismatch"})
            max_date_regressed = new_data_max < old_data_max
            if max_date_regressed:
                violations.append({"dataset_id": dataset_id, "reason": "max_date_regressed"})
            columns = _key_columns(dataset_id)
            old_keys = _key_query(old_path, columns, old_data_max.isoformat())
            new_keys = _key_query(new_path, columns, old_data_max.isoformat())
            missing_query = f"{old_keys} EXCEPT {new_keys}"
            missing_count = connection.execute(
                f"SELECT count(*) FROM ({missing_query}) AS missing_keys"
            ).fetchone()[0]
            sample_columns = ", ".join(f'"{column}"' for column in columns)
            samples = connection.execute(
                f"SELECT {sample_columns} FROM ({missing_query}) AS missing_keys "
                f"ORDER BY {sample_columns} LIMIT {MAX_MISSING_SAMPLES}"
            ).fetchall()
            report = {
                "dataset_id": dataset_id,
                "baseline_min_date": old_min.isoformat(),
                "baseline_max_date": old_max.isoformat(),
                "baseline_parquet_min_date": old_data_min.isoformat(),
                "baseline_parquet_max_date": old_data_max.isoformat(),
                "current_min_date": new_min.isoformat(),
                "current_max_date": new_max.isoformat(),
                "current_parquet_min_date": new_data_min.isoformat(),
                "current_parquet_max_date": new_data_max.isoformat(),
                "date_bounds_metadata_match": metadata_matches_parquet,
                "max_date_regressed": max_date_regressed,
                "baseline_distinct_keys": connection.execute(
                    f"SELECT count(*) FROM ({old_keys}) AS old_keys"
                ).fetchone()[0],
                "current_distinct_keys_through_baseline_cutoff": connection.execute(
                    f"SELECT count(*) FROM ({new_keys}) AS new_keys"
                ).fetchone()[0],
                "missing_key_count": missing_count,
                "missing_key_samples": [[_report_value(value) for value in row] for row in samples],
            }
            reports.append(report)
    comparison = {
        "datasets": reports,
        "downloaded_bytes": downloaded,
        "missing_key_count": sum(item["missing_key_count"] for item in reports),
        "violations": violations,
    }
    if comparison["missing_key_count"] or violations:
        raise MigrationCoverageError(
            "current release does not cover the retained baseline", comparison
        )
    return comparison


def _write_report(report: dict[str, Any]) -> None:
    rendered = json.dumps(report, sort_keys=True)
    print(rendered, flush=True)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as handle:
            handle.write("\n\n## NBP v1 to v2 key coverage\n\n```json\n" + rendered + "\n```\n")


def check_migration(
    *, baseline_release_id: str, baseline_code_sha: str, expected_current_release_id: str | None = None
) -> dict[str, Any]:
    storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
    root = storage.resolve_root(create=False)
    store = DriveReleaseStore(storage, root)
    baseline = load_baseline_release(store, root, baseline_release_id, baseline_code_sha)
    current = restore_current_release(store, root)
    if current.get("format_version") != 2 or current.get("release_scope") != "nbp_platform":
        raise MigrationAuditError("current release is not v2 NBP platform")
    if expected_current_release_id and current.get("release_id") != expected_current_release_id:
        raise MigrationAuditError("current release differs from the expected release ID")
    try:
        with tempfile.TemporaryDirectory(prefix="zohelo-nbp-migration-") as temporary:
            comparison = compare_silver_key_coverage(store, baseline, current, Path(temporary))
    except MigrationCoverageError as exc:
        _write_report(
            {
                "status": "nbp_migration_key_coverage_failed",
                "read_only": True,
                "baseline_release_id": baseline["release_id"],
                "baseline_code_sha": baseline["code_sha"],
                "current_release_id": current["release_id"],
                "current_code_sha": current["code_sha"],
                **exc.comparison,
            }
        )
        raise
    report = {
        "status": "nbp_migration_key_coverage_verified",
        "read_only": True,
        "baseline_release_id": baseline["release_id"],
        "baseline_code_sha": baseline["code_sha"],
        "current_release_id": current["release_id"],
        "current_code_sha": current["code_sha"],
        **comparison,
    }
    _write_report(report)
    return report


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-release-id", required=True)
    parser.add_argument("--baseline-code-sha", required=True)
    parser.add_argument("--expected-current-release-id", default=os.environ.get("ZOHELO_EXPECTED_RELEASE_ID"))
    return parser.parse_args()


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        arguments = _arguments()
        check_migration(
            baseline_release_id=arguments.baseline_release_id,
            baseline_code_sha=arguments.baseline_code_sha,
            expected_current_release_id=arguments.expected_current_release_id,
        )
    except MigrationCoverageError:
        raise SystemExit(1)
    except Exception as exc:
        print(json.dumps({"status": "nbp_migration_key_coverage_failed", "error_type": type(exc).__name__}), flush=True)
        raise SystemExit(1)
