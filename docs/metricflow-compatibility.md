# Verified MetricFlow runtime compatibility

Checked 7 September 2026. This is a synthetic technical compatibility check,
not approval of NBP metrics or a deployed semantic service.

**Production status updated 8 September 2026:** the later audit delivery published
and verified five source-defined daily NBP metrics, modeled gold and release-pinned
semantic artifacts. The synthetic proof below remains runtime evidence. See
[live acceptance](releases/2026-09-07-platform-readiness.md) and the
[current delivery record](deliverables.md) for production evidence and explicit holds.

## Package identity

| Component | Version |
| --- | --- |
| Python | 3.12 |
| dbt Core | 1.12.3 |
| dbt DuckDB adapter | 1.11.0 |
| DuckDB | 1.5.5 |
| MetricFlow engine distribution, `metricflow` | 0.212.0 |
| Local CLI distribution, `dbt-metricflow` | 0.14.0 |

**The CLI and engine distributions have different version numbers.** The earlier
proposal incorrectly used the engine version for the CLI requirement. Installed
wheel metadata for `dbt-metricflow==0.14.0` requires Python `>=3.10,<3.15`,
`dbt-core>=1.11,<1.13`, and `metricflow==0.212.0`; its DuckDB extra selects
`dbt-duckdb`. The console entry point is `mf = dbt_metricflow.cli.main:cli`.
The existing dbt, adapter and engine pins resolved unchanged in an isolated
Python 3.12 environment.

The direct requirement is:

```text
dbt-metricflow[dbt-duckdb]==0.14.0
```

The [official command guide](https://docs.getdbt.com/docs/build/metricflow-commands)
describes local dbt Core execution and `dbt-metricflow` installation. Package
identity was additionally checked against the installed 0.14.0 wheel metadata;
the [engine release tag](https://github.com/dbt-labs/metricflow/releases/tag/v0.212.0)
alone does not establish the CLI distribution version. A machine's generic
`mf` executable can be unrelated TeX Metafont, so tests invoke the installed
Python entry point using the selected interpreter.

## What the check establishes

A standalone synthetic dbt project materializes daily category/value rows and
produces its semantic manifest. The real unmodified MetricFlow CLI queries
native DuckDB and writes CSV; independent expected rows verify daily grouping
and inclusive date filtering. Separate subprocesses reopen the database and
semantic definitions. `dbt parse`, config validation or `--explain` alone would
not establish query execution.

The sum metric is deliberately named `fixture_value_total` and belongs only
to the synthetic test project. The root dbt project excludes `tests/fixtures/`
through `.dbtignore`; a regression checks that those resources never enter its
production manifest. It does not approve summing exchange rates or
gold prices and is not published in the production NBP catalogue.

## Offline verification

The supported `DBT_SEND_ANONYMOUS_USAGE_STATS=false` setting disables dbt usage
tracking. The installed CLI also performs an automatic public package-version
lookup without a CLI opt-out. A local approval cancellation blocked that
lookup during investigation. The verification therefore runs the unmodified
CLI with a Linux kernel syscall filter that denies network sockets and sends,
while retaining local file and DuckDB access. This does not monkeypatch
packages or substitute a fake remote response. The helper fails closed if
`libseccomp` or the filter cannot be activated; it must never silently run the
query with networking restored. This fixture targets the Linux CI/Codespaces
environment, not arbitrary operating systems.

An earlier temporary updater-patched experiment is excluded from accepted
proof. The accepted local query ran after kernel network denial was independently
confirmed in a child process. The repository fixture and its CI result are the
repeatable acceptance evidence for the integrated dependency set.

## Production use and serving boundary

This fixture proves the technical route to self-managed native MetricFlow. The
production NBP definitions, gold models and matching semantic artifacts were
subsequently delivered and verified separately. Restore a verified release and
use `scripts/query_metrics.py` through the [operating guide](nbp-platform-operations.md)
to enforce the daily metric grain.

Dynamic queries need an available native runtime. Batch-published metric
snapshots can instead be explored without an always-on runtime, with a fixed
set of previously computed results. Codespaces and a static portal alone do
not provide continuously available native execution. The five source-defined
daily metrics need no further methodology decision. Optional derived metrics
and always-on serving remain H-DERIVED and H-SERVE in the delivery record;
resolve their intended use and availability before expanding that scope.
