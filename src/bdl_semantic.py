"""Supported, grain-preserving BDL coverage metric validation and queries."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

import duckdb
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
OFFLINE_RUNNER = Path(__file__).with_name("offline_process.py")
MF_CLI = "from dbt_metricflow.cli.main import cli; cli()"
BDL_GRAIN = ("metric_time",)


@dataclass(frozen=True)
class MetricSpec:
    name: str
    column: str


METRICS = {
    item.name: item
    for item in (
        MetricSpec("bdl_source_universe_total", "source_universe_total"),
        MetricSpec("bdl_discovered_total", "discovered_total"),
        MetricSpec("bdl_landed_accepted_total", "landed_accepted_total"),
        MetricSpec("bdl_modeled_total", "modeled_total"),
        MetricSpec("bdl_modeled_observation_total", "modeled_observation_total"),
        MetricSpec("bdl_discovery_coverage_ratio", "discovery_coverage_ratio"),
        MetricSpec("bdl_modeled_coverage_ratio", "modeled_coverage_ratio"),
    )
}
_RATIO_COLUMNS = {"discovery_coverage_ratio", "modeled_coverage_ratio"}


def _spec(metric: str, group_by=None) -> MetricSpec:
    if metric not in METRICS:
        raise ValueError(f"Unsupported metric {metric!r}; choose one of {', '.join(METRICS)}")
    if group_by is not None and tuple(group_by) != BDL_GRAIN:
        raise ValueError(f"{metric} requires the exact snapshot grain {','.join(BDL_GRAIN)}")
    return METRICS[metric]


def _date(value):
    if isinstance(value, date):
        value = value.isoformat()
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("Metric dates must use YYYY-MM-DD")
    return parsed


def _read_manifest(path):
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    names = {item["name"] for item in document.get("metrics", [])}
    if set(METRICS) - names:
        raise ValueError("The semantic manifest does not contain the supported BDL coverage metrics")
    models = {item["name"]: item for item in document.get("semantic_models", [])}
    model = models.get("bdl_coverage", {})
    node = model.get("node_relation", {})
    if node.get("schema_name") != "04_gold" or node.get("alias") != "mart_bdl_coverage":
        raise ValueError("The semantic manifest has an unsupported BDL coverage relation")
    alias = node.get("database")
    if not isinstance(alias, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", alias):
        raise ValueError("BDL semantic relations must use one simple local database alias")
    return document, alias


def validate_metric_grain(database):
    with duckdb.connect(str(Path(database).resolve()), read_only=True) as connection:
        relation = '"04_gold"."mart_bdl_coverage"'
        if connection.execute(f"select count(*) from {relation} where snapshot_date is null").fetchone()[0]:
            raise ValueError("Invalid BDL coverage metric grain")
        if connection.execute(
            f"select count(*) from (select snapshot_date from {relation} group by snapshot_date having count(*) <> 1)"
        ).fetchone()[0]:
            raise ValueError("Duplicate BDL coverage metric grain")
        for column in ("source_universe_total", "discovered_total", "landed_accepted_total", "modeled_total", "modeled_observation_total"):
            if connection.execute(f"select count(*) from {relation} where {column} is null or {column} < 0").fetchone()[0]:
                raise ValueError(f"Invalid BDL coverage metric value: {column}")
        for column in _RATIO_COLUMNS:
            if connection.execute(f"select count(*) from {relation} where {column} is null or {column} < 0 or {column} > 1").fetchone()[0]:
                raise ValueError(f"Invalid BDL coverage metric ratio: {column}")


def _run_cli(project, arguments):
    env = dict(os.environ)
    for name in tuple(env):
        if name.startswith(("GOOGLE_", "GCP_", "AWS_", "AZURE_")) or name in {"CLOUDSDK_CONFIG", "ZOHELO_DRIVE_ROOT_ID", "ZOHELO_DRIVE_ROOT_NAME"}:
            env.pop(name, None)
    env.update(DBT_PROFILES_DIR=str(project), DBT_PROJECT_DIR=str(project), DBT_SEND_ANONYMOUS_USAGE_STATS="false", DO_NOT_TRACK="1", TMPDIR=str(project))
    result = subprocess.run(
        [sys.executable, str(OFFLINE_RUNNER), sys.executable, "-c", MF_CLI, *arguments],
        cwd=project,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=180,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"Native offline MetricFlow query failed (exit {result.returncode}):\n{result.stdout}")
    return result.stdout


def _prepare_project(project, database, semantic_manifest, alias):
    (project / "target").mkdir()
    (project / "dbt_project.yml").write_text(yaml.safe_dump({"name": "zohelo_bdl_query", "version": "1.0.0", "config-version": 2, "profile": "zohelo_bdl_query", "model-paths": []}), encoding="utf-8")
    (project / "profiles.yml").write_text(yaml.safe_dump({
        "zohelo_bdl_query": {"target": "local", "outputs": {"local": {
            "type": "duckdb", "path": str(project / "query.duckdb"), "database": alias, "schema": "main", "threads": 1, "keep_open": False,
            "attach": [{"path": str(database), "alias": alias, "read_only": True}],
        }}}}), encoding="utf-8")
    shutil.copyfile(semantic_manifest, project / "target/semantic_manifest.json")


def _query(database, semantic_manifest, spec, start, end, output_csv, alias):
    output_csv = Path(output_csv).resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="zohelo-bdl-metricflow-", dir=output_csv.parent) as temporary:
        project = Path(temporary)
        _prepare_project(project, database, semantic_manifest, alias)
        candidate = project / "result.csv"
        execution = _run_cli(project, ["query", "--metrics", spec.name, "--group-by", ",".join(BDL_GRAIN), "--start-time", start.isoformat(), "--end-time", end.isoformat(), "--csv", str(candidate)])
        if not candidate.exists():
            with duckdb.connect(str(database), read_only=True) as connection:
                count = connection.execute('select count(*) from "04_gold"."mart_bdl_coverage" where snapshot_date between ? and ?', [start, end]).fetchone()[0]
            if count or "Query returned an empty result set" not in execution:
                raise ValueError(f"MetricFlow did not produce the expected CSV for {spec.name}")
            with candidate.open("w", newline="", encoding="utf-8") as handle:
                csv.writer(handle).writerow(["metric_time__day", spec.name])
        with candidate.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        candidate.replace(output_csv)
    return rows


def query_metric(database, semantic_manifest, metric, start_date, end_date, output_csv, *, group_by=None):
    spec = _spec(metric, group_by)
    start, end = _date(start_date), _date(end_date)
    if end < start:
        raise ValueError("Metric end date must be on or after start date")
    _, alias = _read_manifest(semantic_manifest)
    validate_metric_grain(database)
    return _query(Path(database).resolve(), Path(semantic_manifest).resolve(), spec, start, end, output_csv, alias)


def validate_release_metrics(database, semantic_manifest, output_dir):
    started = time.monotonic()
    database, semantic_manifest = Path(database).resolve(), Path(semantic_manifest).resolve()
    output_dir = Path(output_dir)
    _, alias = _read_manifest(semantic_manifest)
    validate_metric_grain(database)
    report = {"status": "verified", "engine": "native_metricflow", "query_interface": "src/bdl_semantic.py", "grain_validation": "all_gold_rows", "comparison_window": "latest_snapshot_date", "metrics": []}
    with duckdb.connect(str(database), read_only=True) as connection:
        dates = [row[0] for row in connection.execute('select distinct snapshot_date from "04_gold"."mart_bdl_coverage" order by snapshot_date desc limit 1').fetchall()]
    if not dates:
        raise ValueError("No published BDL coverage snapshot is available")
    start = end = dates[0]
    for spec in METRICS.values():
        with duckdb.connect(str(database), read_only=True) as connection:
            expected = [
                (row[0].isoformat(), Decimal(str(row[1])))
                for row in connection.execute(
                    f'select snapshot_date, {spec.column} from "04_gold"."mart_bdl_coverage" where snapshot_date between ? and ?',
                    [start, end],
                ).fetchall()
            ]
        rows = _query(database, semantic_manifest, spec, start, end, output_dir / f"{spec.name}.csv", alias)
        actual = [(row["metric_time__day"].split(" ", 1)[0], Decimal(row[spec.name])) for row in rows]
        if actual != expected:
            raise ValueError(f"MetricFlow result does not match published gold values for {spec.name}")
        report["metrics"].append({"name": spec.name, "start_date": start.isoformat(), "end_date": end.isoformat(), "row_count": len(rows), "group_by": list(BDL_GRAIN), "matches_gold": True})
    report["duration_seconds"] = round(time.monotonic() - started, 3)
    return report
