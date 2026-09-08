# Zohelo-data delivery plan

Approved work programme, 6 September 2026. The owner approved this scope; that approval did not settle the business or architecture choices recorded below. The initial user is the owner. The NBP scope is all already-ingested history for Tables A, B, C and gold prices.

## Current status

Updated 8 September 2026. **The audit and live NBP acceptance are complete.** Audit acceptance release `96b14b78-dc36-4422-952c-5fb3af726ac8` has 15 tables, five verified daily MetricFlow metrics and 429,795 cleaned observations. Fresh SQL, exact raw replay and the full project-capacity check passed. Daily ingestion is active at **02:00 UTC** and the 8 September scheduled run also passed, publishing a newer release as recorded below. The [expanded source research](source-research/README.md) contains 182 products/families, including NBP. The owner has now delegated full feasible source onboarding without a source-by-source review gate. The [implementation programme](source-expansion-plan.md) and domain coverage ledger are defined; first WDI/BDL/Eurostat Landing campaigns are in implementation and production acceptance. New-source Silver/Gold/semantic publication remains to be implemented.

This is the single project-status and owner-question record. Research, architecture, runbooks and dated release evidence support it; they are not additional task boards. The portal remains a data tool.

**Status meanings:** **Done** = delivered and evidenced; **In progress** = active work or verification; **On hold** = explicitly excluded for now with a reason and reopening condition; **Waiting for input** = an owner choice; **Not started** = future implementation; **Cancelled** = intentionally retired.

| Deliverable | Status | What this means |
| --- | --- | --- |
| D1. Repository, architecture and all Actions audit | **Done** | All nine original workflows and the repository/data architecture were audited. Repairs are merged and tested; live acceptance passed. [Audit](audits/2026-09-07-readiness.md), [all workflow decisions](audits/2026-09-07-workflows.md). |
| D2. Reproducible environment and shared instructions | **Done** | Pinned Python/Node dependencies, fresh-container checks and standard human/agent instructions exist. Audit updates preserve the supported npm/Python path. |
| D3. Source contracts, correction evidence and gold design | **Done** | Official definitions, declarative ingestion policy and correction evidence are documented, implemented and tested. The new detection rules ran successfully in the verified live release. |
| D4. NBP end-to-end including semantic queries | **Done** | All 15 tables and five daily metrics are published. Fresh native queries matched gold; raw replay matched 1,329,363 rows exactly. The complete capacity inventory is below current warning thresholds. |
| D5. One data catalogue with source and metric lineage | **Done** | Five real metric definitions and source methodology/frequency/reuse metadata are in the matching release artifacts. The native viewer passes discovery and semantic-to-physical-lineage browser checks. |
| D6. Define and onboard additional sources | **In progress** | Research and the [full implementation plan](source-expansion-plan.md) cover 182 families and 231 categories. Autonomous onboarding is authorized. WDI/BDL/Eurostat Landing collection is undergoing production acceptance; broad source expansion and new-source Silver/Gold/semantic delivery remain open. |
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
account access and unresolved consequential business definitions remain concrete future prerequisites.

## Evidence

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
The plan records 15 gaps and seven additional candidates.

**First delivery boundary:** independent WDI/BDL/Eurostat Landing campaigns with exact raw
responses, durable recent/history queues, persisted quotas and source-scoped Drive state.
[Operations](source-campaign-operations.md) documents the bounded schedule, controls and capacity
limits. Production evidence is recorded here after acceptance. Bronze/Silver, domain Gold,
semantic models, new-source publication, broad dataset completion and scalable state compaction
are subsequent implementation milestones, not completed capabilities.

### D7. Data-first portal and SQL experience

Deliver a focused data-first portal update: one native dbt catalogue for published data, lineage, and metric definitions; no project-management or owner-review UI; and SQL that automatically loads every released table referenced by a statement, including joins, unions, and CTEs. Keep published data-file downloads within 64 MiB per engine session and explain a limit failure before execution. Catalogue artifacts have a separate 16 MiB limit; these download limits are not guarantees about query memory use. A modern palette and layout may support this bounded experience. Portal validation, CI, and deployment are required before any of this new behavior is described as live.

## Cross-cutting acceptance requirements

Across D1–D5: verify business examples and representative fixtures; use the same inputs, cutoff, and revision policy for full/incremental equivalence; prove safe versioned publication and raw rebuild; expose release, provenance, status, errors, and source batches clearly; and measure operating limits without assuming a date, cost, SLA, or unlimited free capacity.
