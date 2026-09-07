"""Supported, grain-preserving NBP queries using the native MetricFlow CLI.

The definitions use MAX only as an identity at a checked unique daily grain.
Arbitrary raw ``mf`` requests do not enforce that contract; this interface does.
The input database and semantic manifest must belong to the same release.
"""
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
FX_GRAIN = ("metric_time", "fx_quote__currency", "fx_quote__source_table")
GOLD_GRAIN = ("metric_time", "gold_price__commodity")


@dataclass(frozen=True)
class MetricSpec:
    name: str
    model: str
    column: str
    source_table: str | None
    group_by: tuple[str, ...]


METRICS = {
    item.name: item for item in (
        MetricSpec("nbp_table_a_mid", "fact_fx_quotes", "mid", "A", FX_GRAIN),
        MetricSpec("nbp_table_b_mid", "fact_fx_quotes", "mid", "B", FX_GRAIN),
        MetricSpec("nbp_table_c_bid", "fact_fx_quotes", "bid", "C", FX_GRAIN),
        MetricSpec("nbp_table_c_ask", "fact_fx_quotes", "ask", "C", FX_GRAIN),
        MetricSpec("nbp_gold_price_pln_per_gram_1000", "fact_gold_prices",
                   "price_pln_per_gram_1000", None, GOLD_GRAIN),
    )
}


def _spec(metric, group_by=None):
    if metric not in METRICS:
        raise ValueError(f"Unsupported metric {metric!r}; choose one of {', '.join(METRICS)}")
    spec = METRICS[metric]
    if group_by is not None and tuple(group_by) != spec.group_by:
        raise ValueError(
            f"{metric} requires the exact daily grain {','.join(spec.group_by)}; "
            "omitted dimensions, coarser time grains and aggregation are unsupported"
        )
    return spec


def _date(value):
    if isinstance(value, date):
        value = value.isoformat()
    try:
        result = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Metric dates must use YYYY-MM-DD") from exc
    if result.isoformat() != value:
        raise ValueError("Metric dates must use YYYY-MM-DD")
    return result


def _read_manifest(path):
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    names = {item["name"] for item in document.get("metrics", [])}
    if not set(METRICS).issubset(names):
        raise ValueError("The semantic manifest does not contain the five supported NBP metrics")
    models = {item["name"]: item for item in document.get("semantic_models", [])}
    aliases = set()
    for semantic_name, relation in (("nbp_fx_quotes", "fact_fx_quotes"),
                                    ("nbp_gold_prices", "fact_gold_prices")):
        model = models.get(semantic_name, {})
        node = model.get("node_relation", {})
        if node.get("schema_name") != "04_gold" or node.get("alias") != relation:
            raise ValueError(f"The semantic manifest has an unsupported {semantic_name} relation")
        if node.get("relation_name") != f'"{node.get("database")}"."04_gold"."{relation}"':
            raise ValueError(f"The semantic manifest has an unsupported {semantic_name} relation expression")
        if model.get("defaults", {}).get("agg_time_dimension") != "effective_date":
            raise ValueError(f"The semantic manifest must use publication date for {semantic_name}")
        expected_dimensions = {"effective_date": ("time", None)}
        expected_dimensions.update({"currency": ("categorical", "currency_key"),
                                    "source_table": ("categorical", "source_table_key")}
                                   if semantic_name == "nbp_fx_quotes" else
                                   {"commodity": ("categorical", "commodity_key")})
        dimensions = {item["name"]: item for item in model.get("dimensions", [])}
        for dimension, (kind, expr) in expected_dimensions.items():
            actual = dimensions.get(dimension, {})
            if actual.get("type") != kind or actual.get("expr") != expr:
                raise ValueError(f"Unsupported {semantic_name} dimension definition: {dimension}")
        if dimensions["effective_date"].get("type_params", {}).get("time_granularity") != "day":
            raise ValueError(f"The semantic manifest must retain daily dates for {semantic_name}")
        aliases.add(node.get("database"))
    if len(aliases) != 1 or not all(isinstance(x, str) and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", x)
                                    for x in aliases):
        raise ValueError("NBP semantic relations must use one simple local database alias")
    # A different artifact must not quietly change the supported source-value
    # contract. The release reader verifies artifact bytes; these checks bind the
    # executable meaning of the five allowed names to this interface version.
    metrics = {item["name"]: item for item in document["metrics"]}
    for spec in METRICS.values():
        model = models["nbp_fx_quotes" if spec.source_table else "nbp_gold_prices"]
        measure_name = (f"nbp_{spec.column}_at_publication" if spec.source_table
                        else "nbp_gold_price_at_publication")
        measures = {item["name"]: item for item in model.get("measures", [])}
        measure = measures.get(measure_name, {})
        metric = metrics[spec.name]
        params = metric.get("type_params", {})
        actual_input = params.get("measure", {})
        expected_filter = ({"where_filters": [{"where_sql_template":
                           "{{ Dimension('fx_quote__source_table') }} = '" + spec.source_table + "'"}]}
                           if spec.source_table else None)
        if (measure.get("agg") != "max" or measure.get("expr") != spec.column
                or measure.get("non_additive_dimension") is not None
                or measure.get("agg_time_dimension") is not None
                or metric.get("type") != "simple" or actual_input.get("name") != measure_name
                or metric.get("filter") != expected_filter or actual_input.get("filter") is not None
                or actual_input.get("join_to_timespine") or params.get("join_to_timespine")
                or actual_input.get("fill_nulls_with") is not None or params.get("fill_nulls_with") is not None):
            raise ValueError(f"Unsupported executable definition for {spec.name}; use its matching release runtime")
    return document, aliases.pop()


def validate_metric_grain(database):
    """Reject a broken gold contract before an identity MAX can hide bad rows."""
    with duckdb.connect(str(Path(database).resolve()), read_only=True) as connection:
        checks = (
            ("FX", '"04_gold"."fact_fx_quotes"',
             "effective_date, source_table_key, currency_key",
             "effective_date is null or source_table_key is null or currency_key is null "
             "or currency_key = '' or source_table_key not in ('A', 'B', 'C') "
             "or quote_currency_key is distinct from 'PLN' "
             "or (source_table_key in ('A', 'B') and (mid is null or not isfinite(mid) or mid < 0)) "
             "or (source_table_key = 'C' and (bid is null or ask is null or not isfinite(bid) "
             "or not isfinite(ask) or bid < 0 or ask < 0))"),
            ("gold", '"04_gold"."fact_gold_prices"', "effective_date, commodity_key",
             "effective_date is null or commodity_key is distinct from 'nbp_gold_1000_gram' "
             "or quote_currency_key is distinct from 'PLN' or price_pln_per_gram_1000 is null "
             "or not isfinite(price_pln_per_gram_1000) or price_pln_per_gram_1000 < 0"),
        )
        for label, relation, keys, invalid in checks:
            if connection.execute(f"select count(*) from {relation} where {invalid}").fetchone()[0]:
                raise ValueError(f"Invalid {label} metric grain, quote unit or source value")
            if connection.execute(
                f"select count(*) from (select {keys} from {relation} group by {keys} having count(*) <> 1)"
            ).fetchone()[0]:
                raise ValueError(f"Duplicate {label} daily metric grain; refusing to hide rows with MAX")
            columns = dict((row[0], row[1]) for row in connection.execute(f"describe {relation}").fetchall())
            if columns.get("effective_date") != "DATE":
                raise ValueError(f"The {label} publication date must be a DATE")


def _run_cli(project, arguments):
    env = dict(os.environ)
    for name in tuple(env):
        if name.startswith(("GOOGLE_", "GCP_", "AWS_", "AZURE_")) or name in {
            "CLOUDSDK_CONFIG", "ZOHELO_DRIVE_ROOT_ID", "ZOHELO_DRIVE_ROOT_NAME",
        }:
            env.pop(name, None)
    env.update(DBT_PROFILES_DIR=str(project), DBT_PROJECT_DIR=str(project),
               DBT_SEND_ANONYMOUS_USAGE_STATS="false", DO_NOT_TRACK="1",
               TMPDIR=str(project))
    result = subprocess.run(
        [sys.executable, str(OFFLINE_RUNNER), sys.executable, "-c", MF_CLI, *arguments],
        cwd=project, env=env, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, timeout=180, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"Native offline MetricFlow query failed (exit {result.returncode}):\n{result.stdout}")
    return result.stdout


def _prepare_project(project, database, semantic_manifest, alias):
    (project / "target").mkdir()
    (project / "dbt_project.yml").write_text(yaml.safe_dump({
        "name": "zohelo_nbp_query", "version": "1.0.0", "config-version": 2,
        "profile": "zohelo_nbp_query", "model-paths": [],
    }), encoding="utf-8")
    (project / "profiles.yml").write_text(yaml.safe_dump({
        "zohelo_nbp_query": {"target": "local", "outputs": {"local": {
            "type": "duckdb", "path": str(project / "query.duckdb"),
            "database": alias, "schema": "main", "threads": 1, "keep_open": False,
            "attach": [{"path": str(database), "alias": alias, "read_only": True}],
        }}},
    }), encoding="utf-8")
    shutil.copyfile(semantic_manifest, project / "target/semantic_manifest.json")


def _query(database, semantic_manifest, spec, start, end, output_csv, alias):
    output_csv = Path(output_csv).resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if output_csv in {database, semantic_manifest}:
        raise ValueError("Metric output must not overwrite the database or semantic manifest")
    with tempfile.TemporaryDirectory(prefix="zohelo-metricflow-", dir=output_csv.parent) as temporary:
        project = Path(temporary)
        _prepare_project(project, database, semantic_manifest, alias)
        candidate = project / "result.csv"
        execution = _run_cli(project, ["query", "--metrics", spec.name, "--group-by", ",".join(spec.group_by),
                                      "--start-time", start.isoformat(), "--end-time", end.isoformat(),
                                      "--csv", str(candidate)])
        fields = ["metric_time__day", *spec.group_by[1:], spec.name]
        if not candidate.exists():
            # Pinned dbt-metricflow 0.14 skips CSV creation for an empty result.
            # Confirm both its explicit empty-result outcome and absence of
            # source rows; missing output is otherwise a failed query.
            predicate = "source_table_key = ? and " if spec.source_table else ""
            parameters = [spec.source_table] if spec.source_table else []
            with duckdb.connect(str(database), read_only=True) as connection:
                count = connection.execute(
                    f'select count(*) from "04_gold"."{spec.model}" where {predicate}'
                    "effective_date between ? and ?", [*parameters, start, end],
                ).fetchone()[0]
            if count or "Query returned an empty result set" not in execution:
                raise ValueError(f"MetricFlow did not produce the expected CSV for {spec.name}")
            with candidate.open("w", newline="", encoding="utf-8") as handle:
                csv.writer(handle).writerow(fields)
        with candidate.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if set(reader.fieldnames or []) != set(fields):
                raise ValueError(f"MetricFlow returned unexpected columns for {spec.name}")
            rows = list(reader)
        # A local rename publishes the complete CSV only after a successful query.
        candidate.replace(output_csv)
    return rows


def query_metric(database, semantic_manifest, metric, start_date, end_date, output_csv, *, group_by=None):
    """Query one supported daily metric. Return CSV rows; never write the input DB.

    ``group_by`` is optional only because the required grain is supplied by this
    interface. Supplying an empty or different grain is an error. There is no
    aggregate, SQL, arbitrary filter, coarser-time or custom-expression option.
    """
    spec = _spec(metric, group_by)
    start, end = _date(start_date), _date(end_date)
    if end < start:
        raise ValueError("Metric end date must be on or after start date")
    database, semantic_manifest = Path(database).resolve(), Path(semantic_manifest).resolve()
    _, alias = _read_manifest(semantic_manifest)
    validate_metric_grain(database)
    return _query(database, semantic_manifest, spec, start, end, output_csv, alias)


def _canonical_rows(rows, spec):
    return sorted((row["metric_time__day"].split(" ", 1)[0],
                   *(row[dimension] for dimension in spec.group_by[1:]),
                   Decimal(row[spec.name])) for row in rows)


def validate_release_metrics(database, semantic_manifest, output_dir):
    """Compare real MF results with gold for the latest two dates per source.

    Grain validation covers every gold row. Query comparisons cover each source's
    latest two distinct publication dates (one if it has only one). This is a
    bounded execution gate, not a claim to have rerun every historical metric.
    """
    started = time.monotonic()
    database, semantic_manifest = Path(database).resolve(), Path(semantic_manifest).resolve()
    output_dir = Path(output_dir)
    _, alias = _read_manifest(semantic_manifest)
    validate_metric_grain(database)
    report = {"status": "verified", "engine": "native_metricflow",
              "query_interface": "scripts/query_metrics.py", "grain_validation": "all_gold_rows",
              "comparison_window": "latest_two_publication_dates_per_source", "metrics": []}
    for spec in METRICS.values():
        relation = f'"04_gold"."{spec.model}"'
        predicate = "source_table_key = ?" if spec.source_table else "true"
        parameters = [spec.source_table] if spec.source_table else []
        with duckdb.connect(str(database), read_only=True) as connection:
            dates = [row[0] for row in connection.execute(
                f"select distinct effective_date from {relation} where {predicate} "
                "order by effective_date desc limit 2", parameters,
            ).fetchall()]
            if not dates:
                raise ValueError(f"No published source observations are available for {spec.name}")
            start, end = min(dates), max(dates)
            keys = "currency_key, source_table_key" if spec.source_table else "commodity_key"
            expected = sorted((row[0].isoformat(), *row[1:-1], Decimal(str(row[-1])))
                              for row in connection.execute(
                                  f"select effective_date, {keys}, {spec.column} from {relation} "
                                  f"where {predicate} and effective_date between ? and ?",
                                  [*parameters, start, end],
                              ).fetchall())
        rows = _query(database, semantic_manifest, spec, start, end,
                      output_dir / f"{spec.name}.csv", alias)
        if _canonical_rows(rows, spec) != expected:
            raise ValueError(f"MetricFlow result does not match published gold values for {spec.name}")
        report["metrics"].append({"name": spec.name, "start_date": start.isoformat(),
                                  "end_date": end.isoformat(), "row_count": len(rows),
                                  "group_by": list(spec.group_by), "matches_gold": True})
    report["duration_seconds"] = round(time.monotonic() - started, 3)
    return report
