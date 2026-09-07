# Zohelo-data delivery plan

Approved work programme, 6 September 2026. The owner approved starting this scope after reviewing the deliverables. It combines the nine requested points with the business data catalogue. Approval to start does not settle the open business or architecture choices identified below. See [foundation audit](audits/2026-09-06-foundation.md) for verified implementation status.

The sole initial user is the owner. Scope includes all already-ingested NBP history: Tables A, B, C and gold prices. The owner demonstrated a silver Table A query in the live portal. All four existing bronze datasets now have a verified silver snapshot and fresh native SQL reads; historical gap checks and catch-up remain unfinished. PR57 adds resumable ingestion repair/catch-up, silver gold prices, modeled gold tables, and release-bound business catalogue metadata while retaining SQL. Executable approved metrics remain open.

The architecture remains proposed. Drive is durable authority for data and release metadata; Git stores code and definitions; Python handles transport/publication; dbt owns transformations; native DuckDB and MetricFlow are disposable compute; and the existing React/DuckDB WASM portal is the consumer. Respect the free-cost boundary and existing subscriptions, verify actual allowances, and do not promise free unlimited or always-on compute. The assistant owns technical work; the owner supplies business decisions one at a time.

## Delivery status

Updated 7 September 2026. **The complete data-platform release is not finished.** PR57 is merged on `main` at [`a2400c5`](https://github.com/rutkala/zohelo-data/commit/a2400c570b3ee55b42c629dc13ca4b0bb815a5de), including the consolidated runner, 15-table platform build, catalogue, and portal v2 guard. Data CI, devcontainer, portal validation/deploy checks passed; live bootstrap [run 34089760722](https://github.com/rutkala/zohelo-data/actions/runs/34089760722) is still running, with no final coverage or publication proof. The current live consumer remains on v1 silver.

| Deliverable | Status | Completed / implemented | Remaining for acceptance |
| --- | --- | --- | --- |
| D1. Repository, architecture and Actions audit | In progress | Foundation audit; consistent runtimes/CI; credential repair; shared production-writer concurrency; consolidated platform workflow merged on `main` | New orchestrated run, durable handoff/checkpoints, complete recovery and measured free-allowance evidence |
| D2. Development environment and AI instructions | Foundation delivered | Declared Python/Node, pinned dependencies, tested container definition, fixture command, shared AGENTS entrypoints and executable synthetic MetricFlow proof | Keep owner Codespace checkout current; preserve CI verification of the newly added native MetricFlow runtime |
| D3. Contracts, ingestion rules and gold design | In progress | Accepted correction policy; [NBP source-contract research](nbp-data-contracts.md); merged resumable state, raw replay boundaries, correction events, and 15-table gold build | Complete historical FX unit validation and end-to-end recovery proof; keep the [dimensional gold design and bus matrix](nbp-gold-design.md) explicit about open business choices |
| D4. Complete NBP flow | In progress | SQL portal; OAuth read checks in Actions/Codespaces and real Actions upload/readback/cleanup; merged runner for all four sources, raw replay, 15 datasets, and v2 publication guard | Live proof; current v1 live release remains unchanged until v2 restore, query, and replay evidence is confirmed; approved and executed metrics |
| D5. Business catalogue | Merged implementation; live proof pending | Prior foundation plus merged four-source catalogue, null-safe ingestion status, dbt ancestor lineage, portal views, and approval-gated empty metrics | Fresh-process/live proof; business metric definitions and approval state remain open |
| D6. Additional sources | Candidate research prepared | [Eurostat and WDI access/reuse inventory](source-candidates.md), with dataset-specific exceptions | Owner priorities and selected-dataset commercial reuse evidence before onboarding |
| D7. Tailored UI | Deferred | Existing SQL portal retained | Evaluate after the essential NBP/catalogue work; redesign scope requires an owner decision |

Verified authorization evidence: [OAuth preference and read check, PR 53](https://github.com/rutkala/zohelo-data/pull/53); [56-byte upload, matching download and confirmed removal](https://github.com/rutkala/zohelo-data/actions/runs/34067541199). These checks did not publish NBP data or finish D4.

[Verified v1 delivery evidence](releases/2026-09-07-nbp-silver.md): PR 55 is on `main`; 429,449 silver rows are published, all-four fresh reads passed, and the matching portal deployed. Existing observations end on 26–28 August 2026. This is not evidence of current freshness, complete historical coverage, or finished gold/metrics.

The current live implementation is described in [NBP silver publication](nbp-silver-publication.md). Its `nbp_silver` release remains the consumer baseline. The merged platform entrypoint and operating limits are documented in [NBP platform operations](nbp-platform-operations.md); bootstrap proof and v2 live publication remain pending.

## D1. Platform, repository and GitHub Actions audit

**Outcome:** A bounded audit and repair plan assesses business goals, architecture, code quality, security, workflows and cost feasibility. Findings at commit `32f875c1` are source-inspection evidence, not new test results. The current configuration covers four NBP sources ([sources.yaml](https://github.com/rutkala/zohelo-data/blob/32f875c1cb3e0191582487a18d7fe8879697d66f/config/sources.yaml)); the gold mart is only a Table A projection ([mart](https://github.com/rutkala/zohelo-data/blob/32f875c1cb3e0191582487a18d7fe8879697d66f/models/marts/mart_exchange_rates_daily.sql)).

**Completion criteria:**

- A workflow map identifies triggers, Python versions, dependencies, stage inputs/outputs, commit identity, and retries.
- A requirements matrix links each business goal to implementation, evidence and gaps; fixes are ranked by impact and necessity for release.
- Effective Drive paths and development/production boundaries are demonstrated, including archive behavior.
- A repair list covers OAuth recovery, workflow version consistency, stage handoff, and immutable publication without claiming repairs prematurely.

## D2. Reproducible Codespaces/dev environment and shared AI instructions

**Outcome:** A fresh Codespace runs bounded local checks and the portal with consistent dependencies, while GitHub/Copilot/ChatGPT-style agents start from shared instructions. Side effects remain visible.

**Completion criteria:**

- Fresh setup uses explicit Python/Node versions and a locked, tested dependency set shared with CI; any environment differences have a documented reason.
- One fixture-only command exercises ingestion-shaped inputs through dbt and query checks without credentials or remote writes.
- `AGENTS.md` and linked entrypoints require evidence, distinguish proposed from accepted architecture, require economical explicit model choice for bounded tasks, and forbid invented completion.
- Startup and optional AI tools are explicitly defined. Opening the environment does not automatically run production pipelines or start an unrestricted AI agent.

## D3. Data contracts, full/incremental rules and gold dimensional design

**Outcome:** Each dataset states one row’s meaning, keys, units, date roles, corrections, and deduplication. The plan separates full source extraction, incremental catch-up/revisions, and rebuilding from retained raw. It defines facts, dimensions, conformed dimensions, and a bus matrix. Kimball-style gold is the recommended starting point, pending a recorded comparison and decision against Inmon or hybrid ([bus architecture](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/kimball-data-warehouse-bus-architecture/)).

**Completion criteria:**

- Contracts cover A/B/C rates and gold prices, including currency/commodity keys, rate units, effective/publication dates, source batch identity, and revised values.
- Rules specify watermarks, overlap, checkpoints, retries, duplicates, schema changes, catch-up, and full rebuild from retained raw.
- A fact/dimension design and bus matrix declare grain and aggregation rules, and reuse dimensions where definitions match; date and currency are initial candidates.
- [NBP revision policy](decisions/0001-nbp-corrections.md): normal analysis uses current corrected values, with detected changes and raw versions retained for traceability. A historical-comparison interface is outside the current release scope; the pipeline implementation remains pending.

## D4. Complete NBP end-to-end

**Outcome:** One reviewed release makes all four NBP datasets, their existing history and agreed catch-up range available through bronze, silver, modeled gold, semantic definitions and the portal. Source inputs are recorded for replay; missing legacy raw inputs are identified explicitly. MetricFlow and SQL use matching released data and definitions.

**Completion criteria:**

- Ingestion authorization and catch-up work; all four datasets have verified coverage, keys, units and representative values. Full and incremental results agree for the same inputs and cutoff under the chosen revision policy.
- All four datasets have tested gold and semantic coverage. Agreed business metrics execute with checked values, filters and time aggregation. An initial small compatibility test precedes the complete build; parsing alone is insufficient.
- A fresh process restores the release and runs SQL and MetricFlow, returning its release ID and clear unavailable-input errors.
- Failed publication leaves the prior complete release usable; recovery rebuilds from raw and publishes a new version.

## D5. Business data catalogue

**Outcome:** The catalogue is core to the NBP release and lets the owner trace and assess a metric without GitHub or code. It combines actual ingestion records with dbt and semantic artifacts; dbt does not become presumed live ingestion monitoring. Metadata matches the queried release. It has three linked views:

1. **Sources and datasets:** description, licence/reuse terms, coverage, frequency, status, and identity.
2. **Lineage:** source, landing, bronze, silver, gold, semantic models, and metrics, release-specific.
3. **Metrics:** definition, formula, unit, dimensions, time aggregation, approval state, and version.

Status distinguishes last attempt, last successful ingestion, latest observation, and latest validated publication. Editing descriptions or approving definitions inside the UI is a separate business decision, not a silently assumed feature.

**Completion criteria:**

- All four NBP datasets expose coverage and status distinctions, with each metric linked to source and model lineage.
- Published metadata combines ingestion records with matching [dbt artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts) and semantic definitions. Data, lineage, quality results and metric definitions identify the release they describe.
- The portal supports discovery, lineage, metric review and SQL navigation without requiring the owner to use GitHub or read code.
- An owner query traces metric to physical data and back to validated publication.

## D6. Source expansion research and onboarding

**Outcome:** A business-led, prioritized source catalogue and onboarding plan. Sources must be public, free to obtain and permitted for the intended commercial use, with any attribution or redistribution conditions recorded.

**Completion criteria:**

- Candidate profiles record business value, coverage, update behavior, access limits and licensing evidence with a review date. Public access alone does not establish commercial reuse permission.
- Selected sources have an explicit priority and onboarding requirements for contracts, fixtures, extraction, lineage and recovery.
- Onboarding preserves NBP’s release, catalogue, testing, and cost boundaries.

## D7. Optional tailored UI evaluation

**Outcome:** A focused evaluation decides whether to adapt or redesign the existing portal for catalogue, SQL, and metric work. It follows data contracts and release work.

**Completion criteria:**

- Review covers mobile usability, discovery, lineage, results, provenance/release visibility and failures using real NBP examples.
- Any redesign proposal has a bounded scope and demonstrated benefit over the current layout.
- No rebuild is required until the owner accepts scope and resource implications.

## Cross-cutting acceptance requirements

These apply across D1–D5, not as extra projects:

- **Correctness:** business examples and representative fixtures verify results; full/incremental equivalence uses the same inputs, cutoff and revision policy.
- **Safe versioned publication:** rollback and rebuild from raw are proven; failed publish leaves the previous release usable.
- **Operational clarity:** status, provenance, errors, source batches, release IDs, and code versions are understandable.
- **Measured limits:** download size, disk/RAM, runtime, and actual allowances are measured within the free-cost boundary; no exact SLA, numeric cost, or date is assumed.

The next release should complete essential D1–D5 work. Optional cleanup need not delay it. D6 source additions and D7 redesign follow afterward; source discovery can inform the earlier modeling discussion.

Business decisions to resolve during design, one at a time: metric use cases, metric availability/serving mode, and source priorities or commercial reuse needs. Historical revisions now have the policy linked above. These remaining choices are not implementation assumptions.
