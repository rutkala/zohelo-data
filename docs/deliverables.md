# Zohelo-data delivery plan

Approved work programme, 6 September 2026. The owner approved this scope; that approval did not settle the business or architecture choices recorded below. The initial user is the owner. The NBP scope is all already-ingested history for Tables A, B, C and gold prices.

## Current status

Updated 8 September 2026. **The audit and selected NBP product acceptance are complete; full coverage of all four selected sources is not.** NBP has 15 published tables and five verified daily metrics; its latest previously checked daily release contains 429,841 cleaned observations through 7 September. Daily ingestion runs at **02:00 UTC**. The owner has delegated full feasible onboarding across the [182 researched products/families](source-research/README.md), with no source-by-source review gate. [ADR 0006](decisions/0006-complete-selected-source-coverage.md) replaces starter-only scope with complete selected-product coverage. PRs 80–82 and the distribution-index portal are deployed. The fresh [successful run 34252801846](https://github.com/rutkala/zohelo-data/actions/runs/34252801846) verifies the complete WDI archive, 53 of 21,233 current Eurostat distributions, and 278 published BDL responses; the detailed checkpoint and remaining acceptance programme are below. Full Eurostat/BDL coverage and new-source Bronze/Silver/Gold/semantic delivery remain open. Serialized half-hour Actions resume saved ingestion progress, and an enabled daily engineering review now provides separate follow-through. The [secure BDL key setup](source-accounts.md) can activate the free registered quota.

This is the single project-status and owner-question record. Research, architecture, runbooks and dated release evidence support it; they are not additional task boards. The portal remains a data tool.

### Fresh-context completion agreement — 8 September 2026

The owner reaffirmed autonomous delivery within the initial boundaries and rejected stopping at
partial results as though the complete goal were delivered. The [working agreement](collaboration.md)
and [shared agent instructions](../AGENTS.md) apply this to every component, including the portal.
For ingestion the target remains every available dataset, dimension, geography, historical period
and accompanying metadata in each selected product. Execution limits produce resumable checkpoints;
they do not reduce that target. A source-access, provider, capacity or implementation blocker must
remain visible with its next action. Partial data can be useful while the complete goal stays open.

The fresh check of [run 34252801846](https://github.com/rutkala/zohelo-data/actions/runs/34252801846)
found all three jobs successful and the following measured progress. These figures are a dated
checkpoint, not an all-source completion claim or a claim of new-source modeled data.

| Selected product | Verified checkpoint | Remaining acceptance |
| --- | --- | --- |
| NBP REST A/B/C and gold | Existing accepted 15-table/five-metric release; latest previously recorded daily check is through 7 September | Continue daily freshness/recovery checks; other NBP statistical products stay separate inventory work. |
| World Bank WDI | Complete 282,845,220-byte official archive freshly restored at 16:51 UTC; 218 API response rows published | Full source-shaped/typed/modelled access, revision reconciliation and release acceptance; API reconciliation remains separate from archive coverage. |
| Eurostat | All three inventories current; 53 of 21,233 catalogue distributions accepted at 17:20 UTC; 21,180 pending, zero failed pending tasks; 304 API responses published | Exhaust the current catalogue, recover oversized/asynchronous distributions, validate published coverage and deliver typed/modelled access. |
| GUS BDL | 278 accepted/published responses at 17:00 UTC; 559 pending tasks, zero pending retries; variable catalogue reports 172,573 entries | Exhaust catalogue/history, reconcile page membership and completeness, refresh catalogue generations, then deliver typed/modelled access. Response count is not variable or observation coverage. |

Half-hour serialized Actions jobs were actually active during this check. The enabled daily
**Advance Zohelo-data delivery** task was also created on 8 September to review current Actions,
investigate stalls and advance feasible engineering work. It has not yet supplied a completed
review run. These are specific continuation mechanisms, not a promise of continuous AI execution.

Priority is to close correctness gaps before claiming completeness, then remove measured throughput
bottlenecks while retaining quotas and recovery guarantees. This correction rejects incomplete BDL response pages, binds fresh distribution-index verification
to the current campaign checkpoint, and coalesces index publication while keeping every raw/state
checkpoint durable. It does not close the remaining acceptance items. The remaining completion programme is:

1. **Catalogue and extraction:** version BDL catalogue discovery, reconcile expected/observed unique
   IDs and page totals, and track unavailable/failed scope explicitly. Complete Eurostat backfill
   using its durable queue, asynchronous preparation and adaptive partitions. Catalogue totals must
   be scoped to their endpoint/parent/generation; a child-subject total is not a global total.
2. **Operational convergence:** measure accepted useful work per runner minute and retained bytes,
   prioritize bulk over redundant starter reconciliation, detect progress stalls outside provider
   cooldowns and replace repeated full-state work where measurement warrants it. Keep Actions as the
   current human-operable orchestrator; revisit a dedicated orchestrator against these measurements.
3. **Completion proof:** reconcile current catalogue membership, accepted raw, published indexes,
   missing/failed work and revisions. Add a resumable full-current-raw audit before final full-source
   acceptance; existing fresh bulk restore samples only the latest raw object. A successful sample
   or a complete archive is not proof of all-source end-to-end delivery.
4. **Usable data delivery:** continue source-specific dbt Bronze/Silver, dimensional Gold and
   appropriate semantics, with replay/release/catalogue/SQL acceptance. Raw responses and archive
   indexes are intermediate access, not a substitute for this already-authorized outcome.

These items remain open until their actual acceptance evidence is recorded. Future improvements
are reprioritized from observed usefulness, correctness, reliability and cost in this same record.

### Ingestion Action failures checked on 8 September

The owner's follow-up asked to check several hours of failed ingestion before other work.
The fresh run/job logs establish distinct causes:

| Failure evidence | Actual cause | Resolution / current evidence |
| --- | --- | --- |
| [34240148821](https://github.com/rutkala/zohelo-data/actions/runs/34240148821), [34241326444](https://github.com/rutkala/zohelo-data/actions/runs/34241326444), BDL | A valid parent-subject listing returned all 21 children despite `pageSize=20`; the parser rejected it. | Fixed by `1f841d6`; the later all-source successful run accepted BDL pages with zero failed requests. |
| 34241326444, WDI | Drive reported a ZIP media MIME type, and immutable raw verification rejected it. | Fixed by `75c1afd`; the complete 282,845,220-byte archive is accepted and freshly restored. |
| 34241326444, Eurostat | `RemoteDisconnected` on one API history request. | Retained progress was published; later runs resumed with zero pending retries. |
| [34245632263](https://github.com/rutkala/zohelo-data/actions/runs/34245632263), [34246624216](https://github.com/rutkala/zohelo-data/actions/runs/34246624216), WDI | Historical API reads timed out at 90 seconds (`BX.GSR.CCIS.CD` and `BX.GSR.INSF.ZS`). The bulk archive and other provider jobs succeeded. | Subsequent run 34252801846 succeeded for all three providers. This correction adds one bounded, quota-reserved in-run transport retry with retained failure receipts and separate recovered/unrecovered counts. |

The source workflow runs at minutes **7 and 37 UTC** with separate serialized WDI/BDL/Eurostat
jobs and a **60-minute job timeout**. Normal API collection uses three cycles, up to twelve
requests/240 seconds per cycle and a 900-second aggregate between-operation budget. WDI/Eurostat
run the first API cycle, then up to 24 full-distribution requests/1,200 seconds, then remaining
API cycles after successful collection/verification. Provider quotas and cooldowns apply to
every path. Work can span the next schedule; an older pending job may be superseded while the
active source writer finishes its checkpoint. A red workflow can contain successful provider
jobs and durable published increments. It is not an instruction to restart all data.

NBP is separate: its daily **02:00 UTC** [run 34179015910](https://github.com/rutkala/zohelo-data/actions/runs/34179015910)
succeeded on 8 September. Manual/configuration guidance remains in [campaign operations](source-campaign-operations.md).

**Status meanings:** **Done** = delivered and evidenced; **In progress** = active work or verification; **On hold** = explicitly excluded for now with a reason and reopening condition; **Waiting for input** = an owner choice; **Not started** = future implementation; **Cancelled** = intentionally retired.

| Deliverable | Status | What this means |
| --- | --- | --- |
| D1. Repository, architecture and all Actions audit | **Done** | All nine original workflows and the repository/data architecture were audited. Repairs are merged and tested; live acceptance passed. [Audit](audits/2026-09-07-readiness.md), [all workflow decisions](audits/2026-09-07-workflows.md). |
| D2. Reproducible environment and shared instructions | **Done** | Pinned Python/Node dependencies, fresh-container checks and standard human/agent instructions exist. Audit updates preserve the supported npm/Python path. |
| D3. Source contracts, correction evidence and gold design | **Done** | Official definitions, declarative ingestion policy and correction evidence are documented, implemented and tested. The new detection rules ran successfully in the verified live release. |
| D4. NBP end-to-end including semantic queries | **Done** | All 15 tables and five daily metrics are published. Fresh native queries matched gold; raw replay matched 1,329,363 rows exactly. The complete capacity inventory is below current warning thresholds. |
| D5. One data catalogue with source and metric lineage | **Done** | Five real metric definitions and source methodology/frequency/reuse metadata are in the matching release artifacts. The native viewer passes discovery and semantic-to-physical-lineage browser checks. |
| D6. Define and onboard additional sources | **In progress** | Research and the [implementation plan](source-expansion-plan.md) cover 182 families and 231 categories. ADR 0006 requires complete selected products. PRs 80–82 implement full WDI/Eurostat distributions and scalable BDL paging; the portal supports response tables and archive indexes. The complete WDI CSV archive is accepted, indexed and freshly verified. Eurostat and BDL backfills remain incomplete. Broader onboarding and new-source Bronze/Silver/Gold/semantic models remain open. [Measured coverage and validation](releases/2026-09-08-complete-source-correction.md). |
| D7. Portal UX and shared desktop/mobile theme | **Done** | Narrow palette alignment and identity/toolchain cleanup passed both deployment builds and all browser checks. Deployment evidence is linked below. |
| Working agreement for the Zohelo-data subproject | **Done** | [Collaboration instructions](collaboration.md) define focused chats, one GitHub status record, Drive data artifacts and human-operable handoffs. |
| Completed one-time migration workflow / old executable builders | **Cancelled** | Migration evidence and read-only comparison script remain; obsolete workflow and direct mutating CLI paths are retired. |

## Explicit holds and reopening conditions

These are visible limits, not claims of completed functionality. None requires inventing an owner methodology answer to finish the five daily source metrics.

| ID | Held work | Why / when to reopen |
| --- | --- | --- |
| H-CAP | Reference-safe state compaction and automatic archive/release deletion | Current policy retains all evidence. Capacity reporting warns at 70% of any build/state bound. Design/test compaction before that threshold or before onboarding a source that would cross it; never raise caps or delete old files blindly. |
| H-SERVE | Always-on/multi-user semantic API | On-demand native MetricFlow and CSV are the present supported experience. Reopen for a named BI integration, availability need or shared-service use case with hosting/authentication requirements. |
| H-DERIVED | Spread, period average, return, conversion and other derived metrics | Optional business calculations need an intended use and explicit aggregation/missing-date rules. Source-defined daily values proceed without them. |
| H-FX | Exhaustive historic currency identity/unit validation | Sampled official comparisons support API normalization; values remain unchanged. Validate the specific currency/history before governed conversion/accounting use. No PLN redenomination transform applies to the post-2002 API. |
| H-REUSE | Commercial redistribution of NBP data | Official material establishes public read access but did not establish the complete commercial redistribution/attribution grant. Resolve terms for the intended sharing product before releasing that feature. |
| H-HOST | Commercial hosted service on GitHub Pages | Pages has commercial-hosting restrictions. Select suitable hosting before turning the owner project portal into a commercial data/SaaS service. |
| H-GIT | Server-side branch protection/rulesets | The branch API reports `main` unprotected; the connector cannot administer this setting. Maintain reviewed PR/check discipline and configure enforcement before adding collaborators or depending on automatic protection. |
| H-REV | Whole-publication disappearance and transformation-only row-delta ledger | Comparable currency omissions are detected; absent publications are ambiguous. Immutable raw/releases and code/contract versions remain evidence. Reopen for a real anomaly or a model migration requiring explicit cross-release deltas. |
| H-SCALE | Broad portal feature pruning, module/package restructuring or orchestration/storage migration | The owner asked to keep the portal stable. Reopen against measured performance/maintenance needs or a selected source benchmark; no speculative framework replacement. |

## Owner decisions

**Q-LIVE / H-LIVE are resolved:** after the explicit production scope was presented, the owner replied **“Yes, I approve”** on 7 September 2026. This authorizes publishing and verifying the new NBP release in Google Drive, retaining previous releases and resuming daily ingestion. The approved live run passed and the daily schedule is restored; no further approval is pending for this delivery.

**Q-M1 is resolved for the baseline:** the 7 September instruction authorizes researching and implementing source-defined daily NBP observations. Five definitions are documented in [NBP methodology](nbp-business-definitions.md). It is no longer a blocker. Optional derived metrics are H-DERIVED.

**Q-S1 is resolved:** on 8 September 2026 the owner delegated source selection, implementation,
connection tests and production intake, targeting all feasible scope and recoverable history.
The owner requested stable domain/category coverage, granular FMCG/markets, media and sport,
and discovery of additional sources for gaps. [ADR 0004](decisions/0004-autonomous-source-onboarding.md)
records this authorization. There is no source-by-source review gate. New paid services,
paid-account access and unresolved consequential business definitions remain concrete future prerequisites. [ADR 0005](decisions/0005-agile-landing-and-source-access.md) adds free-account onboarding and a secure key setup path; free opportunities must be researched rather than deferred without action.

**Q-ACCESS-001 is resolved:** on 11 September 2026 the owner supplied and configured `GUS_BDL_API_KEY` in Actions secrets. The registered BDL profile is active with expanded 4x rate limits (400 requests/15m, 40,000/week) and X-ClientId transport headers.

**Decoupled per-source pipelines & ADF-style orchestration, 11 September:** Decomposed monolithic matrix into independent source-tailored pipelines:
- `GUS BDL` (`.github/workflows/source-gus-bdl.yml`): Runs every 15 minutes (`*/15 * * * *`) with up to 360 requests/run (6 cycles of 60 requests) to maximize collection under the registered 400 req/15m window. Features ADF-style chained stages (`ingest_landing` -> `platform_transform_and_release`), with support for on-demand `transform_only` execution without re-ingesting.
- `Eurostat` (`.github/workflows/source-eurostat.yml`): Hourly schedule (`12 * * * *`) for bulk distributions and API collection.
- `World Bank WDI` (`.github/workflows/source-world-bank.yml`): 6-hour schedule (`25 */6 * * *`) for bulk CSV and API indicators.
- `OpenData.org` (`.github/workflows/source-opendata.yml`): Dedicated streaming loader workflow (`src/ingestion/sources/opendata_bronze_loader.py`) that reads the 21.38 GB Senzing archive via seekable HTTP Range streams on Google Drive, flattens entity features into typed Parquet (`br_opendata_organizations`, `br_opendata_locations`, `br_opendata_people`), and writes to `02_bronze/opendata_org/` with zero disk extraction in Codespaces/CI. Checkpointing is tracked in `06_control/source_campaigns/opendata_org_bronze/checkpoint.json`.
- All four decoupled pipelines launched concurrently in production via serialized execution.

## Evidence

**Complete-source correction deployed; backfills in progress, 8 September:** the owner rejected the partial
production scope. Inspection found permanent WDI/BDL discovery ceilings, Eurostat's three
filtered starter datasets, repeated whole-state writes and Drive user-rate-limit failures.
[ADR 0006](decisions/0006-complete-selected-source-coverage.md) records full selected-product
acceptance. [PR 80](https://github.com/rutkala/zohelo-data/pull/80) merged as `1287f65`,
[PR 81](https://github.com/rutkala/zohelo-data/pull/81) as `1f841d6`, and
[PR 82](https://github.com/rutkala/zohelo-data/pull/82) as `75c1afd`.
Final [data CI](https://github.com/rutkala/zohelo-data/actions/runs/34245075797) passed 347 tests;
[portal CI](https://github.com/rutkala/zohelo-data/actions/runs/34240810496) passed both builds,
686 unit tests and 14 browser flows per base path. The portal deployed at `1287f65`;
the later fixes affect backend collection, storage verification and workflow ordering.
The [corrected production run](https://github.com/rutkala/zohelo-data/actions/runs/34245632263)
accepted and published the 282,845,220-byte WDI archive and passed fresh full-distribution
verification. Its six CSV members include 396,970 country–indicator rows with 1960–2025
year columns. BDL has 228 accepted/published responses with fresh verification passed and
a provider-issued rate-limit delay. Eurostat's data and codelist inventories now identify
16,964 distributions; the first complete dataset `AACT_ALI01` and its structure are accepted
and indexed alongside those two inventories. Its full collector remains active. These are dated
observations, not a claim of complete Eurostat/BDL or new modeled coverage.
[Detailed production evidence](releases/2026-09-08-complete-source-correction.md).

Agile source delivery, 8 September: [PR 77](https://github.com/rutkala/zohelo-data/pull/77) merged after [252 data tests](https://github.com/rutkala/zohelo-data/actions/runs/34221334104). An immediate earlier batch added 29 responses; the improved [production run](https://github.com/rutkala/zohelo-data/actions/runs/34221713083) then added 81 through three collect/publish cycles per source, leaving 178 accepted responses fully published and freshly verified. Half-hour triggers continue resumable collection. [PR 78](https://github.com/rutkala/zohelo-data/pull/78) passed two production builds, 685 unit tests and 14 browser flows per base path in [CI 34223422928](https://github.com/rutkala/zohelo-data/actions/runs/34223422928); [deployment 34223857331](https://github.com/rutkala/zohelo-data/actions/runs/34223857331) succeeded at `40844c3bd30aa84a3cec7b4e6fdb5a5995c05975`. Production data was verified through fresh Actions processes and portal SQL through browser fixtures; the cloud browser had no owner Drive session for an authenticated live data query. [Detailed snapshot evidence](releases/2026-09-08-agile-landing.md) separates response rows from facts, pending collection from pending publication, and registered support from an acquired key.

Audit acceptance release: `96b14b78-dc36-4422-952c-5fb3af726ac8`, producer [`8f29a0b`](https://github.com/rutkala/zohelo-data/commit/8f29a0b98876f6ae6b161aae3ec5be5cbeb247cb). [Successful publication, fresh metrics, raw replay and health run](https://github.com/rutkala/zohelo-data/actions/runs/34167068188). It contains 15 tables and 429,795 cleaned observations. Previous releases remain retained.

Latest scheduled release checked on 8 September: `8d08c7b4-f9e3-485c-acfc-b194fc180b08`, producer [`b0eb886`](https://github.com/rutkala/zohelo-data/commit/b0eb88698c74687147764c0496938a90dd0dfd7b). [Scheduled run 34179015910](https://github.com/rutkala/zohelo-data/actions/runs/34179015910) passed publication, fresh SQL/native MetricFlow and health checks, with 15 tables and 429,841 cleaned observations. All four feeds were checked through 7 September. No capacity warnings were emitted. Routine scheduled runs do not repeat exact raw replay; the audit acceptance release above retains that separate proof. Later daily runs may supersede this dated operational observation.

Desktop-menu follow-up, 8 September: [PR 70](https://github.com/rutkala/zohelo-data/pull/70) removed hover-only hiding from the shared table ellipsis, so published tables and Browser workspace relations expose the same visible menu on desktop and mobile. [Validation run 34196021287](https://github.com/rutkala/zohelo-data/actions/runs/34196021287) passed both production builds, 678 unit tests and 14 browser flows per deployment base, including non-hover visibility at desktop, phone and 768px widths. [Portal deployment 34196368805](https://github.com/rutkala/zohelo-data/actions/runs/34196368805) succeeded at commit `dc9f1283dd4153e40122e99c15fdff5568c52af8`. This portal correction did not run production ingestion or publish data. The resumed-chat audit found no unfinished essential deliverables, open PRs or active/queued Actions before this follow-up began; no separate AI monitoring task was started.

[Audit delivery evidence](releases/2026-09-07-platform-readiness.md) records the live results plus 138 data tests, 678 portal unit tests and 14 browser flows for each deployment configuration. The full project inventory is 156,260,745 bytes; the highest used build/state bound is 17.92%, below the 70% review threshold. These are measured observations, not future capacity guarantees.

[Current architecture](architecture.md) explains tool choices and commercial/service boundaries. [Operations](nbp-platform-operations.md) gives the Actions and command-line path without AI. [Working agreement](collaboration.md) explains how to start a focused new chat and resume from this record.

## Approved scope by deliverable

### D1. Platform, repository and GitHub Actions audit

Audit business goals, architecture, code quality, security, workflows, effective Drive boundaries, and cost feasibility. Keep a workflow and requirements map, repair OAuth/workflow/stage-handoff/publication failures, and base claims on evidence.

### D2. Reproducible development environment and AI instructions

Provide a fresh Codespaces path for bounded local data and portal checks with explicit Python and Node versions, locked dependencies, shared agent instructions, and visible side effects. Opening the environment must not run production pipelines or an unrestricted agent.

### D3. Data contracts, ingestion rules and gold dimensional design

For A/B/C FX rates and gold prices, document row grain, keys, units, date roles, corrections, deduplication, source batches, watermarks, overlaps, retries, schema handling, catch-up, and rebuild from retained raw. Use current validated values for normal analysis while retaining raw versions and detected changes; a historical-comparison UI is outside scope. The gold design must state fact/dimension grain and business aggregation rules rather than invent them.

### D4. Complete NBP end-to-end

Make all four NBP datasets, existing history, and agreed catch-up range available through bronze, silver, modeled gold, semantic definitions, and the portal. Record inputs for replay, identify unavailable legacy raw inputs, keep SQL and any semantic service on the matching release, restore a release in a fresh process, and preserve the prior complete release if publication fails.

### D5. Business data catalogue

The catalogue covers the connected release: sources and datasets (description, licence/reuse terms, coverage, frequency, status, and identity); release-specific lineage from source through gold and any approved semantic models; and approved metrics with definition, formula, unit, dimensions, time aggregation, approval state, and version. It distinguishes last attempt, last successful ingestion, latest observation, and latest validated publication. It must let the owner discover data and trace an approved metric to physical data and validated publication without GitHub or code. It must not contain project-management or owner-review content.

### D6. Source expansion research and onboarding

Maintain a business-led, prioritized onboarding path. A source must be public, free to obtain, and permitted for the intended commercial use; record attribution, redistribution, and dataset-specific conditions. For a chosen source, add contracts, fixtures, extraction, lineage, recovery, release controls, and testing without weakening the NBP platform boundary.

**Scope clarified 8 September 2026:** [ADR 0003](decisions/0003-source-expansion-scope.md)
records worldwide multicountry coverage, detailed Polish sources, accepted overlaps,
English documentation, recent collection alongside available historical backfill, and an
appropriate silver/gold/semantic role for each admitted source. Research also includes
paid/restricted candidates; initial production eligibility above is unchanged. No subscription
or implementation is authorized merely by a research row.

**Research delivered:** [the source landscape](source-research/README.md) has 182 stable
product/family IDs, including NBP, with 312 source/reference links to 285 official URLs.
134 entries have reviewed access documentation; 48 retain catalogue-only evidence. The
JSON/CSV inventory records access, cost, rights, history, updates, keys, proposed loading
and modeling, overlap, unresolved conditions and reference depth. Six sector reports,
a coverage matrix, a reviewed ingestion/modeling proposal and bounded future-test cards
support selection. Schema, IDs, evidence labels, exports and documentation links were
checked. Documentation research is not a successful connection or production acceptance.

**Implementation now authorized:** [ADR 0004](decisions/0004-autonomous-source-onboarding.md)
supersedes the owner-selection gate. The versioned taxonomy has 15 domains, 45 subdomains,
231 categories and 17 analytical dimensions, independent of official classification editions.
All 182 inventory IDs have candidate mappings; these do not establish dataset or ingested coverage.
The plan records 15 thematic gaps and seven additional candidates. Across all candidates, 177 of 231 categories have a research lead and 54 have none; neither number measures ingested coverage.

**First delivery boundary:** independent WDI/BDL/Eurostat Landing campaigns with exact raw
responses, durable recent/history queues, persisted quotas and source-scoped Drive state.
[Operations](source-campaign-operations.md) documents the bounded schedule, controls and capacity
limits. [Production run 34217059540](https://github.com/rutkala/zohelo-data/actions/runs/34217059540)
passed for all three providers and their fresh-process restore/replay steps at producer
`7e43ccf1de25fdc8207f21f29861e6e3eb5402a5`. Cumulative accepted responses were WDI 13,
BDL 22 and Eurostat 33; these include metadata and repeated representations, not unique facts.
Each source advanced recent and historical queues, with zero pending retries at this checkpoint.
The corrected implementation passed [222 full-suite tests](https://github.com/rutkala/zohelo-data/actions/runs/34216699560).
[The dated evidence record](releases/2026-09-08-source-campaigns.md) preserves initial failures,
repairs, run measurements and the bounded replay samples.

**Agile delivery correction:** the owner rejected inaccessible Landing and four-hour gaps between
small batches. A new immediate run of the existing workflow completed successfully, advancing
cumulative accepted responses to WDI 21, BDL 31 and Eurostat 45.
[ADR 0005](decisions/0005-agile-landing-and-source-access.md) implements consecutive collection
and publication, independently verified/queryable Landing snapshots, and the
[free-account and secure-key setup](source-accounts.md). Production collection and fresh
Landing verification passed; [portal deployment](https://github.com/rutkala/zohelo-data/actions/runs/34223857331)
also passed. Browser SQL behavior was verified with fixtures; an authenticated owner-session
browser query was not performed. The complete-source correction and current coverage are
recorded above.

**Following increments:** source-specific dbt Bronze/Silver and then Gold/semantics using only
the references needed for each slice. Broader source waves can progress alongside those models.
Scalable state, physical retained-byte accounting and reference editions are implemented when
needed for the next admitted scope, with measured limits preserved. Full-scope completion
remains open; only configured ingestion jobs continue automatically after a delivery turn.

### D7. Data-first portal and SQL experience

Deliver a focused data-first portal update: one native dbt catalogue for published data, lineage, and metric definitions; no project-management or owner-review UI; and SQL that automatically loads every released table referenced by a statement, including joins, unions, and CTEs. Keep published data-file downloads within 64 MiB per engine session and explain a limit failure before execution. Catalogue artifacts have a separate 16 MiB limit; these download limits are not guarantees about query memory use. A modern palette and layout may support this bounded experience. Portal validation, CI, and deployment are required before any of this new behavior is described as live.

## Cross-cutting acceptance requirements

Across D1–D5: verify business examples and representative fixtures; use the same inputs, cutoff, and revision policy for full/incremental equivalence; prove safe versioned publication and raw rebuild; expose release, provenance, status, errors, and source batches clearly; and measure operating limits without assuming a date, cost, SLA, or unlimited free capacity.
