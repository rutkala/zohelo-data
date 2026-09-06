# Zohelo-data delivery plan

Approved work programme, 6 September 2026. The owner approved starting this scope after reviewing the deliverables. It combines the nine requested points with the business data catalogue. Approval to start does not settle the open business or architecture choices identified below. See [foundation audit](audits/2026-09-06-foundation.md) for verified implementation status.

The sole initial user is the owner. Scope includes all already-ingested NBP history: Tables A, B, C and gold prices. The owner has demonstrated a silver Table A query in the live portal; completeness and correctness across all four datasets are not yet established. The next release adds ingestion repair/catch-up, silver gold prices, modeled gold, and executable, tested MetricFlow while retaining SQL.

The architecture remains proposed. Drive is durable authority for data and release metadata; Git stores code and definitions; Python handles transport/publication; dbt owns transformations; native DuckDB and MetricFlow are disposable compute; and the existing React/DuckDB WASM portal is the consumer. Respect the free-cost boundary and existing subscriptions, verify actual allowances, and do not promise free unlimited or always-on compute. The assistant owns technical work; the owner supplies business decisions one at a time.

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
