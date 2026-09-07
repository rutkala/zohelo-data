# Zohelo-data delivery plan

Approved work programme, 6 September 2026. The owner approved starting this scope after reviewing the deliverables. It combines the nine requested points with the business data catalogue. Approval to start does not settle the open business or architecture choices identified below. See [foundation audit](audits/2026-09-06-foundation.md) for verified implementation status.

The sole initial user is the owner. Scope includes all already-ingested NBP history: Tables A, B, C and gold prices. The owner demonstrated a silver Table A query in the earlier live portal. The new v2 data release has verified request coverage through 6 September 2026, all four sources through modeled gold, matching source/lineage catalogue metadata, fresh native SQL reads and exact raw replay. Every key from the preceding silver release is retained. Executable approved metrics remain open.

The architecture remains proposed. Drive is durable authority for data and release metadata; Git stores code and definitions; Python handles transport/publication; dbt owns transformations; native DuckDB and MetricFlow are disposable compute; and the existing React/DuckDB WASM portal is the consumer. Respect the free-cost boundary and existing subscriptions, verify actual allowances, and do not promise free unlimited or always-on compute. The assistant owns technical work; the owner supplies business decisions one at a time.

## Delivery status

Updated 7 September 2026 after the successful 08:45–08:46 UTC checks. **The NBP data foundation is live; the complete platform scope remains open for governed metrics.** Release `5f356b1b-97cd-470d-9d5e-b46ebb37a7d1` was produced by [`f071669`](https://github.com/rutkala/zohelo-data/commit/f071669937829a3d0775a13146b3170e1b283b4e). Its 15 tables passed publication, fresh native SQL reads, exact raw replay and comparison against the prior live silver key set. The matching portal deployed successfully. See [dated release evidence and measurements](releases/2026-09-07-nbp-platform.md); this is not a completed semantic layer or a published GitHub Release.

| Deliverable | Status | Completed / implemented | Remaining for acceptance |
| --- | --- | --- | --- |
| D1. Repository, architecture and Actions audit | Audit and major repairs delivered | [Workflow map and findings](audits/2026-09-07-platform.md); consistent runtimes/CI; credential repair; consolidated writer; live checkpoint resume, publication, recovery and measured NBP workload | Operational follow-ups: routine incremental-run measurement, retained-state growth, peak RAM, representative queries and account-specific allowances; no free-unlimited claim |
| D2. Development environment and AI instructions | Delivered | Declared Python/Node, pinned dependencies, tested actual container, fixture command, shared AGENTS entrypoints and executable synthetic MetricFlow proof | Ongoing maintenance as dependencies and implementation change |
| D3. Contracts, ingestion rules and gold design | Implemented; unit follow-up open | Accepted correction policy; [source contracts](nbp-data-contracts.md); resumable full/incremental rules; exact raw replay; [facts, dimensions and bus matrix](nbp-gold-design.md) | Historical FX unit normalization requires provider evidence; values remain as published. Cross-source mappings and business aggregation rules remain open |
| D4. Complete NBP flow | Data through gold live; semantics open | All four sources; 429,795 silver observations; every prior key preserved; 15 published tables; fresh SQL and exact raw recovery; matching portal deployed | Approve and execute NBP business metrics on the same release; decide the native metric-serving experience |
| D5. Business catalogue | Sources and lineage live; metrics open | Release-bound four-source catalogue, coverage/status distinctions, dbt lineage and portal views | Business metric definitions, approval and executable semantic lineage; owner usability feedback on the new catalogue |
| D6. Additional sources | Candidate research prepared | [Eurostat and WDI access/reuse inventory](source-candidates.md), with dataset-specific exceptions | Owner priorities and selected-dataset commercial reuse evidence before onboarding |
| D7. Tailored UI | Deferred | Existing SQL portal retained | Evaluate after the essential NBP/catalogue work; redesign scope requires an owner decision |

Verified authorization evidence: [OAuth preference and read check, PR 53](https://github.com/rutkala/zohelo-data/pull/53); [56-byte upload, matching download and confirmed removal](https://github.com/rutkala/zohelo-data/actions/runs/34067541199). These checks did not publish NBP data or finish D4.

[Earlier v1 delivery evidence](releases/2026-09-07-nbp-silver.md) records the initial 429,449-row snapshot. A subsequent v1 release reached 429,611 rows. The new v2 preservation audit retained every one of those later keys and recovered 184 additional historical records; current silver totals 429,795. The earlier evidence is historical, not the current freshness statement.

The current live implementation and limits are described in [NBP platform operations](nbp-platform-operations.md). [NBP silver publication](nbp-silver-publication.md) describes the retained v1 compatibility path. The [short portal guide](using-the-portal.md) covers source/lineage navigation and gold queries. No technical action is required from the owner to finish these completed jobs.

## D1. Platform, repository and GitHub Actions audit

**Outcome:** A bounded audit and repair plan assesses business goals, architecture, code quality, security, workflows and cost feasibility. The historical findings at commit `32f875c1` came from source inspection: four configured NBP sources and only a Table A gold projection. The [current audit supplement](audits/2026-09-07-platform.md) records the implemented repairs, measured live NBP release and remaining limitations.

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

**Outcome:** Each dataset states one row’s meaning, keys, units, date roles, corrections, and deduplication. The implementation separates resumable historical bootstrap, incremental catch-up/rechecks, and rebuilding from retained raw. The [gold design](nbp-gold-design.md) records the comparison with Inmon/hybrid and implements Kimball-style facts, conformed dimensions and a bus matrix for NBP. Business aggregation rules remain unapproved.

**Completion criteria:**

- Contracts cover A/B/C rates and gold prices, including currency/commodity keys, rate units, effective/publication dates, source batch identity, and revised values.
- Rules specify watermarks, overlap, checkpoints, retries, duplicates, schema changes, catch-up, and full rebuild from retained raw.
- A fact/dimension design and bus matrix declare grain and aggregation rules, and reuse dimensions where definitions match; date and currency are initial candidates.
- [NBP revision policy](decisions/0001-nbp-corrections.md): normal analysis uses current validated values, with detected changes and raw versions retained for traceability. A historical-comparison interface is outside the current scope. Detection is implemented by recent and rotating historical rechecks; it is not an official correction notification feed.

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
