"""Build a validated GUS DBW Bronze/Silver/Gold platform candidate offline."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import duckdb

from dbw_platform_contract import (
    DBW_PLATFORM_DATASETS,
    DBW_PLATFORM_DATE_COLUMNS,
    DBW_PLATFORM_MODEL_NAMES,
)
from runtime_metadata import _code_sha

REPO_ROOT = Path(__file__).resolve().parents[1]


def build_candidate(data_root: Path, release_id: str, workspace: Path) -> dict:
    """Build all eleven DBW relations and return release-protocol inputs."""
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
        "--no-partial-parse", "--vars", '{"enable_gus_dbw": true}',
    ]
    subprocess.run([*cli, "build", "--select", *models, *common], cwd=REPO_ROOT, env=env, check=True, timeout=1800)
    subprocess.run([*cli, "docs", "generate", "--no-compile", *common], cwd=REPO_ROOT, env=env, check=True, timeout=600)
    code_sha = _code_sha()
    (target / "business-catalog.json").write_text(json.dumps({
        "format_version": 1, "code_sha": code_sha,
        "sources": [{
            "source_id": "gus_dbw", "name": "GUS DBW",
            "description": "Statistics Poland domain knowledge databases.",
            "status": "published_snapshot", "checked_through": None,
            "latest_observation_date": None, "last_successful_ingestion_at": None,
            "last_attempt_at": None, "raw_response_count": 0,
        }],
        "datasets": [], "lineage": {"nodes": [], "edges": []},
        "metrics": [], "metrics_status": "awaiting_business_approval",
    }, sort_keys=True), encoding="utf-8")
    (target / "ingestion-state.json").write_text(json.dumps({
        "format_version": 1, "source_id": "gus_dbw",
        "code_sha": code_sha,
        "release_id": release_id, "status": "published_snapshot",
        "sources": {"gus_dbw": {
            "status": "published_snapshot",
            "release_id": release_id,
            "coverage_status": "complete_retained_inventory",
        }},
    }, sort_keys=True), encoding="utf-8")
    datasets = []
    with duckdb.connect(str(database), read_only=True) as connection:
        for dataset_id, (layer, model_id) in DBW_PLATFORM_DATASETS.items():
            table_name = dataset_id.removeprefix("bronze_")
            relation = f'"{layer}"."{table_name}"'
            output = workspace / layer / f"{dataset_id}.parquet"
            output.parent.mkdir(parents=True, exist_ok=True)
            connection.execute(f"COPY (SELECT * FROM {relation}) TO '{str(output).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            date_column = DBW_PLATFORM_DATE_COLUMNS[dataset_id]
            if date_column:
                rows, minimum, maximum = connection.execute(
                    f'SELECT count(*), min("{date_column}"), max("{date_column}") FROM {relation}'
                ).fetchone()
                min_date = str(minimum) if minimum is not None else None
                max_date = str(maximum) if maximum is not None else None
            else:
                rows = connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
                min_date = max_date = None
            if rows <= 0:
                raise ValueError(f"Required DBW platform dataset is empty: {dataset_id}")
            datasets.append({
                "dataset_id": dataset_id, "layer": layer,
                "table_name": table_name, "model_name": model_id.rsplit(".", 1)[-1],
                "model_id": model_id, "path": str(output), "row_count": rows,
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
        "inputs": [{"source_id": "gus_dbw", "release_id": release_id}],
        "measurements": {"release_id": release_id, "dataset_count": len(datasets)},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    args = parser.parse_args()
    args.workspace.mkdir(parents=True, exist_ok=True)
    print(json.dumps(build_candidate(args.data_root, args.release_id, args.workspace), indent=2))


if __name__ == "__main__":
    main()
