# Operating commands

Updated 29 September 2026. This guide supersedes older workflow/test instructions.

## Four Actions

| Action | File | What it runs |
| --- | --- | --- |
| Ingestion | `ingestion.yml` | Select NBP, BDL Web, DBW Web/API, World Bank or Eurostat intake |
| Transformations | `transform.yml` | Select WDI/Eurostat modeling, Eurostat Bronze, DBW modeled/Bronze publication or OpenData Bronze |
| Deploy portal | `deploy-portal.yml` | Build and deploy Pages; retain build identity/output checks |
| Copy Drive files to R2 | `drive-to-r2-migration.yml` | Existing non-destructive copy command; temporary migration operation |

All are main-only. There is no PR, CI, test, preflight, probe or generic task-runner
Action. The temporary branch cleanup Action was removed without executing.
Removed Actions may remain visible in GitHub's historical runs; their YAML is gone.

Ingestion preserves the existing NBP daily, World Bank six-hour and Eurostat hourly
intake cron offsets. BDL uses ten WireGuard routes and resumes checkpoints; it no
longer automatically dispatches another run. DBW Web/API and BDL are manual.
All data Actions share the production writer concurrency group.

World Bank and Eurostat ingestion no longer automatically launches downstream
modeling, post-publication consumer queries or platform-wide diagnostics. Their
API campaign and full native catalogue retention commands remain. The bounded
`--verify-state` operation between them retains the accepted provider ledger.
NBP still uses its existing combined Python platform runner, not a new framework.

Transformations is manual. It runs only the selected existing command(s). DBW
retained Bronze preparation/publication and OpenData load/publication each retain
their two actual processing stages. There is no separate reader-acceptance job,
repeated environment install, broad suite or live browser validation.

## No repository tests

Python and portal suites, test fixtures, browser tests/configurations, dbt test
declarations and the generic test macro were deleted. Test commands, workflow
check scripts and the devcontainer verification wrapper were removed. Publication
requires successful model builds, not successful test counts or model-test coverage.
No replacement test framework or renamed acceptance suite was added.

Existing data integrity checks, checkpoints, writer locks and source/model code
remain. Historical release formats still contain a `tests` compatibility marker;
this does not indicate that a deleted suite executed. BDL's browser dependency is
retained because real ingestion imports it. Dependency lock metadata is unchanged.

Repository cleanup does not copy data, publish a release, deploy the portal or
establish complete source coverage. Internal source runners have not been broadly
rewritten. Use their existing CLI directly for advanced operations not in the menus.
