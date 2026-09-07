# dbt Core, DuckDB and MetricFlow compatibility

Checked 7 September 2026 from this repository and the released upstream
metadata. This is a technical compatibility note; it does not approve NBP
business metrics or choose a serving product.

## Current pinned state

| Layer | Current value | Where it is pinned | Finding |
| --- | --- | --- | --- |
| Python | 3.12 (CI/devcontainer target) | repository setup instructions | Supported by the current dbt MetricFlow documentation (Python 3.8–3.12). |
| dbt Core | `1.12.3` | `requirements.in`, `requirements.txt` | Direct dependency. |
| dbt DuckDB adapter | `1.11.0` | `requirements.in`, `requirements.txt` | Direct dependency; `profiles.yml` uses `type: duckdb`. |
| DuckDB | `1.5.5` | `requirements.in`, `requirements.txt` | Direct dependency. |
| MetricFlow library | `0.212.0` | `requirements.txt` only | Resolved entry, but not a direct input. `requirements.txt` does not record its parent dependency edge. |

The active shell used for this check has no installed `dbt`, `dbt-core`,
`dbt-duckdb`, `duckdb`, or `metricflow` package metadata. Its `/usr/bin/mf`
command is TeX Metafont, not MetricFlow. No install or query was run. The
existing lock entry therefore does not establish that an executable MetricFlow
CLI is available.

## Released compatibility evidence

The official [MetricFlow 0.212.0 release](https://github.com/dbt-labs/metricflow/releases/tag/v0.212.0)
identifies tag `v0.212.0` at commit `e4b9b7b`. Its dependency notes state
`dbt-core >=1.11, <1.13` and DuckDB development dependency `>=1.5`; the
repository's `1.12.3` and `1.5.5` satisfy those stated constraints. The release
also contains the `dbt-metricflow` dependency-flow changes. This is release
metadata, not a successful local integration test.

The tag-pinned [`dbt-metricflow` package metadata](https://raw.githubusercontent.com/dbt-labs/metricflow/v0.212.0/dbt-metricflow/pyproject.toml)
declares the distribution name `dbt-metricflow`, Python `>=3.10,<3.15`, the
`mf = dbt_metricflow.cli.main:cli` console entry point, and a `dbt-duckdb`
optional dependency. Its [tag-pinned CLI requirements](https://raw.githubusercontent.com/dbt-labs/metricflow/v0.212.0/dbt-metricflow/requirements-files/requirements-cli.txt)
declare `dbt-core>=1.11,<1.13`; the [DuckDB extra requirements](https://raw.githubusercontent.com/dbt-labs/metricflow/v0.212.0/dbt-metricflow/requirements-files/requirements-dbt-duckdb.txt)
select `dbt-duckdb`. This verifies that the released package supplies the local
CLI and an explicit DuckDB adapter extra. It does not verify a working local
installation in this checkout.

The official [MetricFlow command documentation](https://docs.getdbt.com/docs/build/metricflow-commands)
identifies MetricFlow as a Python library usable with local dbt Core, instructs
local users to install `dbt-metricflow`, and specifies the local `mf` commands.
It also documents the generic adapter extra form
`dbt-metricflow[adapter_package_name]`. The official [DuckDB setup
documentation](https://docs.getdbt.com/docs/local/connect-data-platform/duckdb-setup)
documents local dbt execution with DuckDB. Together these support the proposed
local architecture (dbt Core + `dbt-duckdb` + native DuckDB + local MetricFlow),
subject to the fixture check below.

### Proposed minimal dependency addition

Add this as a **direct** requirement in the next dependency update, then
regenerate the lock in a clean Python 3.12 environment:

```text
dbt-metricflow[dbt-duckdb]==0.212.0
```

Keep the existing direct pins `dbt-core==1.12.3`, `dbt-duckdb==1.11.0`, and
`duckdb==1.5.5`. The adapter extra follows the official installation syntax;
the already direct `dbt-duckdb` pin may make the extra a no-op at resolution
time. Whether `dbt-metricflow==0.212.0` without the extra is sufficient for
this profile must be confirmed by the clean resolver and fixture run. Do not
infer an `mf` executable from the existing `metricflow==0.212.0` lock entry.

After installation, the technical checks are:

```text
dbt --version
mf --version
python -c "import importlib.metadata as m; print(m.version('dbt-metricflow')); print(m.version('metricflow'))"
dbt parse --profiles-dir .
mf validate-configs
```

`dbt parse` is required after semantic-definition changes to produce the
matching `semantic_manifest.json`; parsing alone does not prove a query
executes against DuckDB.

## First technical fixture query

Use a synthetic relation, with no NBP interpretation:

| Item | Fixture definition |
| --- | --- |
| Relation | `fixture_daily_values` |
| Grain | One row per `metric_date`, `category` |
| Columns | `metric_date DATE`, `category VARCHAR`, `value DECIMAL(18,2)` |
| Semantic measure | `fixture_value_sum`, `SUM(value)` |
| Technical metric | `fixture_value_total` built from that measure; **fixture only, not an approved business metric** |
| Dimensions | `metric_time` from `metric_date`; `fixture__category` from `category` |

Populate a few known rows (for example, 2024-01-01/A = 2, 2024-01-01/B = 3,
2024-01-02/A = 5), then run the documented local query shape:

```text
mf query --metrics fixture_value_total --group-by metric_time,fixture__category \
  --start-time 2024-01-01 --end-time 2024-01-03
```

The expected grouped values are independently specified fixture expectations,
not a proposal for FX or gold aggregation. The compatibility acceptance check
is that a fresh Python process can restore the fixture DuckDB file, run
`dbt parse`, `mf validate-configs`, and this query, and return those values
without Metafont shadowing `mf`. An optional `--explain` run can inspect generated
SQL, but does not substitute for executing the query and checking returned values.

## Unresolved choices

Technical support does not settle the serving decision: **must queries and BI
refreshes work while development environments are stopped?** Dynamic MetricFlow
queries need an available native runtime. Alternatively, batch-published metric
snapshots can be explored without keeping that runtime running; they have a fixed
set of previously computed results. Codespaces and a static portal alone do not
provide continuously available native metric execution. The related external-consumer
choice—whether v1 needs a BI or research integration, and whether it uses
snapshot imports or live metric queries—also remains open. Approved NBP metric
names, formulas, units, and aggregation rules remain business decisions.
