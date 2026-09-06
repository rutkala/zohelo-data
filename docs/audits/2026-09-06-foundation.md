# Foundation audit — 6 September 2026

Baseline: [`32f875c1`](https://github.com/rutkala/zohelo-data/tree/32f875c1cb3e0191582487a18d7fe8879697d66f). This is a bounded repository/workflow audit and the first foundation delivery, not approval of a complete data-platform release. The lead reviewed architecture, dependencies, development setup and integration; a lighter agent inventoried and updated the existing workflows. The associated pull request records actual verification results.

## Findings and immediate fixes

| Finding at baseline | First delivery | Remaining work |
| --- | --- | --- |
| Python 3.11/3.12 mixed; unpinned packages | Shared Python 3.12; direct and transitive versions pinned from a known successful build | Validate future MetricFlow combination; dependency/image/action update policy |
| Node not declared in Codespaces; Node 20 in CI | Explicit Node 24.20.0 in container and CI | Maintain tested runtime updates |
| Startup fetches an installer and launches `agy --dangerously-skip-permissions` | Remove automatic installer/agent startup; isolated Python environment; preview only | Optional tools installed explicitly from verified official instructions |
| Only mocked authentication tests for Python | Credential-free A/B/C dbt fixture execution, SQL results, duplicate replay, docs artifact and missing-input checks; PR job | Actual NBP coverage; ingestion, gold-price, publication and MetricFlow tests |
| A merge to config/src/models can create Drive folders | Rename to **Reconcile Drive folders**, manual-only | Enforced development/production root selection in storage code |
| Manual source input embedded directly in shell | Actual source choices and quoted environment-bound arguments | Validate operations against a durable source/run ledger |
| No single approved work programme in Git | Delivery plan, current-scope update, shared AI entrypoints | Record business decisions and accepted ADRs as they are resolved |

The first delivery leaves production data contents and existing ingestion/bronze/silver execution logic unchanged. Existing cron and success-only stage chaining remain. It does not repair authorization, add gold-price silver, implement gold facts or publish metrics yet.

## Workflow map after the foundation change

All Python jobs use `.python-version` and `requirements.txt`; supported portal jobs use `.node-version` and `portal/package-lock.json`.

| Workflow / file | Trigger | Actual work | Identity / recovery limitation |
| --- | --- | --- | --- |
| Validate data platform / `data-validation.yml` | Relevant PR changes or manual | Dependency check, mocked auth, local dbt fixtures/docs | Read-only Git permissions; no Drive secrets |
| Validate development container / `devcontainer-validation.yml` | Setup/runtime/dependency PR changes or manual | Build actual container, run fixtures, check local preview | No image push or production credentials |
| Validate portal / `portal-validation.yml` | Portal/runtime/deploy PR changes or manual | Lint, build/types, unit/engine and browser fixture checks at two base paths | Synthetic Drive responses, not production completeness |
| Deploy Unified Data Portal / `deploy-portal.yml` | Relevant main push or manual | Build static portal and dbt docs; publish Pages | `portal-build.json` records commit; does not identify a data release |
| Reconcile Drive folders / `deploy.yml` | Manual only | Create/find configured runtime folders | Hardcoded storage root still applies; creates folders rather than deploying a platform |
| Data Ingestion Pipeline / `daily-ingestion.yml` | Daily 02:00 UTC or manual | Request latest observations for all four configured datasets | No durable watermark; may miss observations during outages |
| Historical Backfill Pipeline / `historical-backfill.yml` | Manual source/mode choice | Fetch configured date ranges or latest observations | No durable per-range resume state |
| Bronze Layer Transformation / `transform-bronze.yml` | Successful ingestion/backfill completion or manual | Convert landing JSON to Parquet; archive processed input | Lists current folders; no explicit immutable batch handoff |
| Silver Layer Transformation / `transform-silver.yml` | Successful bronze completion or manual | Download bronze; dbt A/B/C; export/upload silver | Gold-price dataset skipped; old matching files deleted before upload |

The `workflow_run` stages still check out their default revision independently. An upstream success is not a guarantee of a consistent code revision or complete set of inputs. Fix this with one orchestrated production run and explicit input/release manifests. Merely adding a checkout SHA or separate concurrency groups does not establish that protocol. Scheduled, backfill and manual writers also need one enforced publication boundary; current timeouts limit execution duration, not shared-write races or total spend.

## Business-goal coverage

| Goal | Evidence | Gap |
| --- | --- | --- |
| Owner can query existing NBP in a web interface | Owner demonstrated a silver Table A query; [portal validation](https://github.com/rutkala/zohelo-data/actions/runs/34023404227) and [deployment](https://github.com/rutkala/zohelo-data/actions/runs/34023732492) succeeded | All-four historical completeness and representative real-data correctness are unproven |
| All NBP data reaches silver/gold/semantics | Four sources in `config/sources.yaml`; A/B/C staging exists | Gold prices skipped; gold mart is only a Table A projection; no executable business metric definitions/tests |
| Automatic, recoverable updates | Daily and manual extraction paths exist | [6 September ingestion failure](https://github.com/rutkala/zohelo-data/actions/runs/34005869087) reports OAuth `invalid_grant`; latest-only extraction cannot catch up |
| Drive is durable authority | Python writes raw/Parquet to Drive; local DuckDB supports processing | No immutable release manifest/pointer, rollback proof or enforced development root |
| Business catalogue and lineage | Portal embeds dbt docs | No source/run status registry, Python-to-dbt lineage records or approved metrics catalogue |
| Facts, dimensions and conformed modeling | Proposed architecture and approved delivery scope | Compare/record gold modeling choice, bus matrix, grains, dimensions and aggregation rules |
| Minimal technical burden on owner | Shared handoff rules and planned catalogue/status | Owner-facing status still lacks last attempt/success/observation/publication distinctions |

The pinned dbt dependency graph includes a MetricFlow library transitively. That alone is not an implemented self-managed metrics service, a valid semantic model or a tested metric result.

## Priority order for the NBP work

1. **Protect storage and publication.** Enforce selected Drive roots in runtime code; reconcile `05_archive` versus `config/storage.yaml`'s `01_landing/archive`; replace delete-before-upload with validated, immutable candidates and a stable release pointer. Prove failure preserves the prior complete release before broadening remote writes.
2. **Repair ingestion and restore authorization.** Prepare a documented OAuth recovery path, then obtain the necessary account authorization securely. Browser authorization is separate from Actions' refresh credentials. Implement dated catch-up, durable checkpoints, retries and replay; do not repeatedly rerun full history as the recovery mechanism. Table B's configured weekly frequency is metadata, not evidence that the all-source daily job respects it.
3. **Complete the data contract and NBP graph.** Include gold prices. Define keys, NBP date roles and rate/commodity units; add proper dbt source dependencies. Record the owner's correction-history choice before assigning conflicting revised observations a business meaning. Filename sorting is the current deduplication mechanism, not an accepted correction policy.
4. **Build gold and governed metrics.** Record dimensional design and agreed business examples. Check actual metric execution and a fresh consumer reading a published release. Keep data, code, dbt artifacts and semantic definitions together.
5. **Expose the business catalogue.** Generate source/dataset status and lineage from actual ingestion/build records and matching semantic definitions. Make this visible alongside SQL in the existing portal before considering broad UI redesign.

## Measurements and cost evidence

An earlier read-only Drive inventory in this work session recorded 365 bronze Parquet files totaling 3,585,652 bytes (A/B/C: 102 each; gold prices: 59) and three silver files totaling 3,016,462 bytes. These are file-inventory observations, not newly repeated measurements or proof of row counts, historical coverage, absence of gaps or future capacity. No complete ingestion/transfer/dbt/publication benchmark was recorded.

The recorded portal production build/deploy took approximately 2 minutes 20 seconds; this is a web deployment duration, not a data build benchmark. Python fixture checks and container validation should record their CI durations in the delivery evidence. Do not extrapolate from a 5 TB Drive allocation to browser, runner or query capacity.

Smallest useful measurement set for the NBP release: per-dataset rows/bytes/date range/gaps; selected working-set bytes and peak disk/RAM; download, dbt and publish durations; representative SQL/metric timings; attempt/success/latest-observation/publication timestamps; and recovery after a deliberately failed fixture publication. Record consumed runner minutes and actual account allowances before adding schedules or an always-on service. Current free-tier feasibility is a boundary to verify, not a guarantee of unlimited free compute.

## Scope and security limits

The earlier obfuscated payload in the portal lint configuration was removed in [PR 47](https://github.com/rutkala/zohelo-data/pull/47). This foundation audit is not a full forensic assessment; removal does not establish whether earlier execution exposed information. No payload or remote AI installer was executed for this audit.

Image/action digest pinning, dependency-update automation, the inherited alternative Bun/Docker path and broader portal code review remain follow-ups. They must be prioritized against release risk rather than becoming an unlimited cleanup project. No production pipeline, backfill, Drive write or credential change is used as a validation step for this foundation delivery.
