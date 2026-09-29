# Current status

## Owner priority — 29 September 2026

Remove repository tests and retain only a few operational Actions. Keep the
architecture and existing capabilities; simplify before adding new machinery.

## Cleanup

Four workflow definitions remain: ingestion, transformations, portal deployment
and the temporary Drive-to-R2 copy. The old 12-workflow surface is replaced by
these four. No automatic CI/check/test Action remains.

Python and portal test suites and fixtures, browser test configuration/commands,
dbt test declarations and the generic test macro are removed. The publisher no
longer requires dbt test results or coverage. Model success and ordinary runtime
integrity/ownership/error handling remain. Agent guides prohibit restoring tests
or historical workflow queues without an explicit owner request.

No ingestion, migration, transformation, deployment or test execution is part of
this cleanup. A temporary branch-only maintenance Action was attempted but did
not start; its YAML and helper script were deleted. Changes were applied through
GitHub edits instead. The final merge must not retain that temporary Action.

## Boundaries and remaining work

The source runners and portal application remain; this is not a full internal
rewrite. World Bank/Eurostat downstream modeling is now manually selected rather
than chained after intake. Existing NBP/WDI/Eurostat intake schedules remain.
BDL keeps ten VPN workers and checkpoints, without automatic self-dispatch.

BDL still needs its browser automation dependency. The npm lockfile has not been
regenerated, so historical test-package records may remain there. Old reports are
historical evidence, not instructions to restore tests. Legacy `tests` metadata
is retained for compatibility with the deployed readers, not to claim suite runs.

R2/Iceberg remains the target; no storage cutover, retained-data deletion or release
pointer change occurred. Copy completion is not certified. Earlier execution holds,
including the held R2 observer publication, are not resolved or retried by cleanup.
Separately configured agents/automations are not cancelled by repository edits.

See [operating commands](audits/2026-09-07-workflows.md) and [AGENTS.md](../AGENTS.md).
Historical status remains in Git history and deliverables-before-r2-20260929.md.
