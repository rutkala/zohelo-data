# Current GitHub Actions inventory

Updated 29 September 2026. This is the current operating guide despite its
historical filename. The previous version and removed workflows remain in Git
history; do not recreate them from old audit or deployment instructions.

## Available workflows

| Workflow file | Purpose / existing command | Trigger |
| --- | --- | --- |
| `data-validation.yml` | Python syntax and existing workflow-safety checker; optional one selected test file | PR or manual |
| `daily-ingestion.yml` | NBP publication (`src/nbp_platform.py`) or explicit retained-release promotion | Existing daily schedule or manual |
| `bdl-web-bootstrap.yml` | Checkpointed BDL Web ingestion with ten WireGuard workers | Manual; existing explicit continuation option |
| `dbw-web-bootstrap.yml` | DBW native Web bulk extraction (`src/dbw_web_extractor.py`) | Manual |
| `source-dbw.yml` | DBW API campaign (`src/source_campaign.py --source gus_dbw`) | Manual |
| `source-world-bank.yml` | Existing WDI intake and modeled pipeline | Existing triggers unchanged |
| `source-eurostat.yml` | Existing Eurostat intake, Bronze and modeled pipeline | Existing triggers unchanged |
| `source-opendata.yml` | Existing OpenData source campaign | Existing triggers unchanged |
| `dbw-bronze-release.yml` | Publish retained DBW Bronze | Existing triggers unchanged |
| `dbw-modeled-release.yml` | Build/publish DBW modeled layers | Existing triggers unchanged |
| `drive-to-r2-migration.yml` | Byte-preserving copy using `scripts/migrate-drive-to-r2.py` | Manual, main only |
| `deploy-portal.yml` | Build portal and dbt Docs viewer, deploy Pages, check build identity | Portal-related main push or manual |

Choose one operation intentionally. A workflow name is not evidence that its
internal source runner has already been made minimal. In particular, the WDI,
Eurostat and DBW release paths still need stage-by-stage simplification. Their
checkpoints, production ownership and schedules are unchanged in this increment.

## What was removed

Fourteen standalone Actions entrypoints were removed: BDL Web preflight, Bronze
naming live checks, Cloudflare synthetic Iceberg live checks, DBW Bronze release
validation, DBW R2 direct copy, DBW R2 pilot, DBW release preflight, Drive folder
reconciliation (`deploy.yml`), devcontainer validation, Drive lazy-HTTP live
checks, synthetic DuckDB HTTP probe, Iceberg query-contract probe, native Landing
live checks and portal validation. Their implementation scripts/tests and Git
history remain available for an explicitly requested diagnostic; they are not
routine operational buttons. DBW R2 copies use the retained all-project migration
path rather than competing pilot workflows. Folder reconciliation remains
`python src/storage_manager.py`, not an automatically scheduled operation.

## Validation is separate from execution

PRs run only Python syntax and the existing inexpensive workflow-safety check.
`Code checks` accepts one existing `test_*.py` file under `tests/` when manually
dispatched. An empty field does not run tests or install platform dependencies.
For local targeted Python tests:

```sh
python -m unittest discover -s tests -p 'test_migration_plan_provenance.py'
```

Select an actual test file relevant to the change. Broad diagnostics remain
available as `bash scripts/check-data.sh` and the commands in `portal/package.json`;
run them only for an explicitly selected purpose. Production input/output checks,
byte integrity and safe publication remain inside the actual data code.

NBP no longer adds full-release restore queries, optional raw replay, platform
health scans or health artifacts after normal publication. These commands remain
available under `scripts/` when needed. Portal deployment no longer runs lint and
unit/engine regressions; its build and deployed-identity check remain. It installs
only the constrained dbt-core dependency tree for the Docs viewer, not the entire
data/semantic environment. Migration no longer has commit-message launch tags or
manual SHA/confirmation fields: selecting the main-only copy Action is the explicit
operation; the transfer's existing non-destructive checks remain unchanged.

No retained data, source checkpoint, release pointer, source schedule or portal
runtime was changed by this repository cleanup. Removed workflow histories may
still be visible in GitHub's historical runs; they are not active YAML definitions.
