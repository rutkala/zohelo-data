"""Build a validated GUS DBW Bronze/Silver/Gold platform candidate offline."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import duckdb

from dbw_platform_contract import (
    DBW_PLATFORM_DATASETS,
    DBW_PLATFORM_DATE_COLUMNS,
    DBW_PLATFORM_MODEL_NAMES,
)
from dbw_retained_source import (
    load_retained_source_descriptor,
    validate_native_bronze_tree,
)
from runtime_metadata import _code_sha

REPO_ROOT = Path(__file__).resolve().parents[1]
MAX_RELEASE_PART_BYTES = 120 * 1024 * 1024
PARTITION_DATASETS = {
    "bronze_dbw_observations": "indicator_id",
    "dbw_observations": "indicator_id",
    "fact_dbw_observations": "indicator_key",
}


def _quoted_path(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _copy_query(connection, query: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"COPY ({query}) TO {_quoted_path(target)} "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def _split_oversized_parquet(
    connection, source: Path, stem: str, *, sequence: str = "0"
) -> list[Path]:
    """Split one oversized Parquet without dropping, sorting or deduplicating rows."""
    if source.stat().st_size <= MAX_RELEASE_PART_BYTES:
        return [source]
    rows = int(connection.execute(
        f"SELECT count(*) FROM read_parquet({_quoted_path(source)})"
    ).fetchone()[0])
    if rows < 2:
        raise RuntimeError(
            f"{source.name} exceeds the release part bound with only one row"
        )
    left_rows = rows // 2
    left = source.with_name(f"{stem}-{sequence}0.parquet")
    right = source.with_name(f"{stem}-{sequence}1.parquet")
    try:
        _copy_query(
            connection,
            f"SELECT * FROM read_parquet({_quoted_path(source)}) LIMIT {left_rows}",
            left,
        )
        _copy_query(
            connection,
            f"SELECT * FROM read_parquet({_quoted_path(source)}) "
            f"LIMIT {rows - left_rows} OFFSET {left_rows}",
            right,
        )
    except BaseException:
        left.unlink(missing_ok=True)
        right.unlink(missing_ok=True)
        raise
    source.unlink()
    return [
        *_split_oversized_parquet(
            connection, left, stem, sequence=sequence + "0"
        ),
        *_split_oversized_parquet(
            connection, right, stem, sequence=sequence + "1"
        ),
    ]


def _copy_relation(
    connection,
    relation: str,
    output: Path,
    dataset_id: str,
    expected_rows: int,
) -> list[Path]:
    """Export one relation into unique, size-bounded Parquet parts."""
    partition_column = PARTITION_DATASETS.get(dataset_id)
    if partition_column is None:
        target = output.with_suffix(".parquet")
        _copy_query(connection, f"SELECT * FROM {relation}", target)
        if target.stat().st_size > MAX_RELEASE_PART_BYTES:
            target.unlink()
            raise RuntimeError(
                f"{dataset_id} exceeds the release part bound and needs a reviewed partition key"
            )
        paths = [target]
    else:
        staging = output.parent / f".{output.name}-partitioned"
        if staging.exists() or staging.is_symlink():
            raise RuntimeError(f"Fresh partition staging required for {dataset_id}")
        staging.mkdir(parents=True)
        produced: list[Path] = []
        try:
            # Partition on a duplicate key so the released Parquet keeps the
            # original indicator column in every file.
            connection.execute(
                f'COPY (SELECT *, "{partition_column}" AS _zohelo_partition_key '
                f"FROM {relation}) TO {_quoted_path(staging)} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, "
                "PARTITION_BY (_zohelo_partition_key))"
            )
            raw_parts = sorted(staging.rglob("*.parquet"))
            if not raw_parts:
                raise RuntimeError(f"{dataset_id} export produced no Parquet files")
            for number, source in enumerate(raw_parts):
                match = re.fullmatch(
                    r"_zohelo_partition_key=(-?[0-9]+)", source.parent.name
                )
                if match is None:
                    raise RuntimeError(
                        f"{dataset_id} produced an invalid partition identity"
                    )
                stem = f"{output.name}-{match.group(1)}-{number:04d}"
                target = output.with_name(stem + ".parquet")
                os.replace(source, target)
                produced.extend(
                    _split_oversized_parquet(connection, target, stem)
                )
            paths = sorted(produced)
        except BaseException:
            for path in output.parent.glob(f"{output.name}-*.parquet"):
                path.unlink(missing_ok=True)
            raise
        finally:
            shutil.rmtree(staging, ignore_errors=True)
    if any(path.stat().st_size > MAX_RELEASE_PART_BYTES for path in paths):
        raise RuntimeError(f"{dataset_id} export exceeded the release part bound")
    files_sql = ",".join(_quoted_path(path) for path in paths)
    copied_rows = int(connection.execute(
        f"SELECT count(*) FROM read_parquet([{files_sql}])"
    ).fetchone()[0])
    if copied_rows != expected_rows:
        raise RuntimeError(
            f"{dataset_id} export row count changed: {copied_rows} != {expected_rows}"
        )
    return paths


def build_candidate(
    data_root: Path,
    release_id: str,
    workspace: Path,
    *,
    retained_pointer: Path,
    retained_manifest: Path,
    retained_pointer_file_id: str,
    retained_audit_dir: Path,
) -> dict:
    """Build all eleven DBW relations from one verified retained snapshot."""
    retained_source, retained_document = load_retained_source_descriptor(
        retained_pointer,
        retained_manifest,
        pointer_file_id=retained_pointer_file_id,
        expected_inventory_sha256=release_id,
    )
    native_validation = validate_native_bronze_tree(
        data_root,
        release_id,
        retained_audit_dir,
        retained_manifest=retained_document,
    )
    database = workspace / "dbw.duckdb"
    target = workspace / "target"
    env = dict(os.environ)
    env.update(
        ZOHELO_DATA_ROOT=str(data_root),
        ZOHELO_DBW_BRONZE_RELEASE_ID=release_id,
        ZOHELO_DUCKDB_PATH=str(database),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        DO_NOT_TRACK="1",
    )
    cli = [sys.executable, "-c", "from dbt.cli.main import cli; cli()"]
    models = list(DBW_PLATFORM_MODEL_NAMES.values())
    common = [
        "--profiles-dir", str(REPO_ROOT), "--target-path", str(target),
        "--log-path", str(workspace / "logs"), "--threads", "1",
        "--no-partial-parse", "--vars",
        '{"enable_gus_dbw": true, "dbw_observations_materialization": "view", '
        '"dbw_fact_materialization": "view"}',
    ]
    subprocess.run([*cli, "build", "--select", *models, *common], cwd=REPO_ROOT, env=env, check=True, timeout=1800)
    subprocess.run([*cli, "docs", "generate", "--no-compile", *common], cwd=REPO_ROOT, env=env, check=True, timeout=600)
    code_sha = _code_sha()
    (target / "business-catalog.json").write_text(json.dumps({
        "format_version": 1, "code_sha": code_sha,
        "sources": [{
            "source_id": "gus_dbw", "name": "GUS DBW",
            "description": "Statistics Poland domain knowledge databases.",
            "status": "published_snapshot",
            "retained_snapshot_id": retained_source["snapshot_id"],
            "coverage_status": retained_source["coverage_status"],
            "lineage_status": retained_source["lineage_status"],
            "checked_through": None,
            "latest_observation_date": None, "last_successful_ingestion_at": None,
            "last_attempt_at": None, "raw_response_count": 0,
        }],
        "datasets": [], "lineage": {"nodes": [], "edges": []},
        "metrics": [], "metrics_status": "awaiting_business_approval",
    }, sort_keys=True), encoding="utf-8")
    (target / "ingestion-state.json").write_text(json.dumps({
        "format_version": 1, "source_id": "gus_dbw",
        "code_sha": code_sha,
        "release_id": retained_source["snapshot_id"],
        "status": "published_snapshot",
        "sources": {"gus_dbw": {
            "status": "published_snapshot",
            "release_id": retained_source["snapshot_id"],
            "native_inventory_sha256": retained_source["inventory_sha256"],
            "retained_manifest_file_id": retained_source["manifest_file_id"],
            "retained_manifest_sha256": retained_source["manifest_sha256"],
            "coverage_status": retained_source["coverage_status"],
            "lineage_status": retained_source["lineage_status"],
        }},
    }, sort_keys=True), encoding="utf-8")
    datasets = []
    with duckdb.connect(str(database), read_only=True) as connection:
        for dataset_id, (layer, model_id) in DBW_PLATFORM_DATASETS.items():
            table_name = dataset_id.removeprefix("bronze_")
            relation = f'"{layer}"."{table_name}"'
            output = workspace / layer / dataset_id
            output.parent.mkdir(parents=True, exist_ok=True)
            date_column = DBW_PLATFORM_DATE_COLUMNS[dataset_id]
            if date_column:
                rows, minimum, maximum = connection.execute(
                    f'SELECT count(*), min("{date_column}"), max("{date_column}") FROM {relation}'
                ).fetchone()
                if date_column == "period_year":
                    min_date = (
                        f"{int(minimum):04d}-01-01"
                        if minimum is not None
                        else None
                    )
                    max_date = (
                        f"{int(maximum):04d}-01-01"
                        if maximum is not None
                        else None
                    )
                else:
                    min_date = str(minimum) if minimum is not None else None
                    max_date = str(maximum) if maximum is not None else None
            else:
                rows = connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
                min_date = max_date = None
            if rows <= 0:
                raise ValueError(f"Required DBW platform dataset is empty: {dataset_id}")
            paths = _copy_relation(
                connection, relation, output, dataset_id, int(rows)
            )
            datasets.append({
                "dataset_id": dataset_id, "layer": layer,
                "table_name": table_name, "model_name": model_id.rsplit(".", 1)[-1],
                "model_id": model_id, "path": str(paths[0]),
                "paths": [str(path) for path in paths], "row_count": rows,
                "date_column": date_column, "min_date": min_date, "max_date": max_date,
                "columns": [{"name": row[0], "type": row[1]} for row in connection.execute(f"DESCRIBE {relation}").fetchall()],
            })
    catalogue = json.loads((target / "business-catalog.json").read_text(encoding="utf-8"))
    catalogue["datasets"] = [
        {key: item[key] for key in (
            "dataset_id", "table_name", "layer", "model_name", "row_count",
            "min_date", "max_date", "date_column", "columns"
        )}
        for item in datasets
    ]
    (target / "business-catalog.json").write_text(json.dumps(catalogue, sort_keys=True), encoding="utf-8")
    return {
        "datasets": datasets,
        "artifacts": [
            {"name": name, "path": str(target / name)}
            for name in ("manifest.json", "catalog.json", "run_results.json",
                         "business-catalog.json", "ingestion-state.json")
        ],
        "inputs": [retained_source],
        "measurements": {
            "retained_snapshot_id": retained_source["snapshot_id"],
            "native_inventory_sha256": retained_source["inventory_sha256"],
            "native_audit_report_sha256": native_validation[
                "audit_report_sha256"
            ],
            "native_verified_files": native_validation["verified_files"],
            "native_verified_bytes": native_validation["verified_bytes"],
            "dataset_count": len(datasets),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--release-id",
        required=True,
        help="Audited 64-character native inventory SHA-256",
    )
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--retained-pointer", type=Path, required=True)
    parser.add_argument("--retained-manifest", type=Path, required=True)
    parser.add_argument("--retained-pointer-file-id", required=True)
    parser.add_argument("--retained-audit-dir", type=Path, required=True)
    args = parser.parse_args()
    args.workspace.mkdir(parents=True, exist_ok=True)
    print(json.dumps(build_candidate(
        args.data_root,
        args.release_id,
        args.workspace,
        retained_pointer=args.retained_pointer,
        retained_manifest=args.retained_manifest,
        retained_pointer_file_id=args.retained_pointer_file_id,
        retained_audit_dir=args.retained_audit_dir,
    ), indent=2))


if __name__ == "__main__":
    main()
