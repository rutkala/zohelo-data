# Zohelo-data delivery plan

Approved work programme, 6 September 2026. The owner approved this scope; that approval did not settle the business or architecture choices recorded below. The initial user is the owner. The NBP scope is all already-ingested history for Tables A, B, C and gold prices.

## Current status

Updated 7 September 2026. The verified NBP v2 data foundation is live. It contains all four NBP sources through modeled gold, source and lineage metadata, fresh native SQL reads, exact raw replay, and preservation of every key from the preceding silver release. Governed metrics and a semantic serving layer are **not** complete.

The portal and matching catalogue update in [PR 64](https://github.com/rutkala/zohelo-data/pull/64) is **Done**, deployed at [`71f79fc`](https://github.com/rutkala/zohelo-data/commit/71f79fc4fbaa7f8c1f2b2133b4f62106f13fcf02). It aligns medallion dbt folders/schemas/aliases, makes the single native catalogue usable on phones, and adds table menus, drag-and-drop and session views. Ordinary SQL still loads referenced published tables automatically.

[Delivery evidence](releases/2026-09-07-medallion-portal.md) records 678 portal tests, 94 data tests, 24 browser executions across both deployment paths, successful deployment, and a live query/view check. The matching 15-table data release passed fresh SQL restore and exact raw replay. Views last for the browser database session; shared/durable user-authored views are not part of this delivery.

This page is the single project-status and owner-question record. Answer a question ID in chat and the assistant will maintain it. The portal contains data exploration and catalogue features; project planning stays here.

**Status meanings:** **Done** is delivered and evidenced. **In progress** is active technical work. **Not started** is technical work that has not begun. **Waiting for your input** needs one owner business decision. **Deferred** is intentionally postponed.

| Deliverable | Work item | Status | Current position |
| --- | --- | --- | --- |
| D1. Platform, repository and GitHub Actions audit | Audit and essential platform repairs | **Done** | [Audit evidence](audits/2026-09-07-platform.md) records the workflow, environment, authorization, publication, recovery, and release repairs. |
| D1 | Routine incremental-run, retained-state growth, peak-memory, browser/query, and account-allowance measurements | **Not started** | Technical follow-up; no owner action is needed. |
| D2. Reproducible development environment and AI instructions | Pinned environment, fixture checks, and shared instructions | **Done** | Python/Node versions, locked dependencies, container validation, and the fixture path are in place. |
| D3. Contracts, ingestion rules and gold design | Source contracts, revision policy, full/incremental rules, and NBP gold design | **Done** | See [NBP contracts](nbp-data-contracts.md), [revision policy](decisions/0001-nbp-corrections.md), and [gold design](nbp-gold-design.md). |
| D3 | Historical FX quote-unit normalization from provider evidence | **Not started** | Technical research is required; API values remain published as received. |
| D3 | Business aggregation rules | **Waiting for your input** | See **Q-M1**. |
| D4. Complete NBP end-to-end | All four NBP datasets through bronze, silver, modeled gold, release publication, SQL, portal, and replay | **Done** | [Verified v2 release evidence](releases/2026-09-07-nbp-platform.md) records 15 tables, 429,795 silver observations, fresh SQL, exact raw replay, and prior-key preservation. |
| D4 | First governed NBP metric definition and acceptance examples | **Waiting for your input** | See **Q-M1**. |
| D4 | Executable semantic definitions, checked metric values, and a native metric-serving experience | **Not started** | Technical implementation starts after the metric scope is agreed. The synthetic MetricFlow check is runtime evidence only. |
| D5. Business data catalogue | Released source, dataset-status, and lineage metadata | **Done** | The v2 release binds four source records and dbt lineage to published data. |
| D5 | One native dbt catalogue for published data, lineage, and metric definitions; no project-management or owner-review UI | **Done** | One native dbt viewer is deployed for the connected release. The current release has no published metric definitions. |
| D5 | Complete provider licence/reuse, frequency and coverage metadata in the catalogue | **Not started** | The native viewer and released ingestion status are delivered; these further approved catalogue fields still require technical metadata work. |
| D5 | Approved metric definitions and executable semantic lineage in the catalogue | **Not started** | Depends on question Q-M1 and D4 implementation. |
| D6. Source expansion research and onboarding | Candidate-source research | **Done** | [Eurostat and WDI inventory](source-candidates.md) records access, reuse evidence, attribution, and exceptions. |
| D6 | Select the next source and intended business use | **Waiting for your input** | See **Q-S1**. |
| D6 | Onboard a selected source | **Not started** | Technical work begins only after selection and dataset-specific reuse review. |
| D7. Data-first portal and SQL experience | Automatically load released tables referenced by schema-qualified SELECT queries, including joins, unions, and CTEs, within the browser download limit | **Done** | Normal Run loads the referenced published tables; browser tests pass. |
| D7 | Medallion catalogue navigation, desktop/touch table actions, drag-and-drop, session views, and responsive UX regressions | **Done** | [Verified delivery](releases/2026-09-07-medallion-portal.md): portal deployed, matching catalogue published, browser checks passed, and all 15 tables restored and rebuilt exactly. |
| D7 | Focused modern palette and layout that supports the catalogue and SQL experience | **Done** | Focused Home/navigation, slate/teal light and dark palettes, and the clearer Browser workspace label are deployed. |

Items marked **In progress** or **Not started** are technical work. Only rows marked **Waiting for your input** need an owner decision.

## Questions for your input

These are the only open owner questions. Reply in chat with an ID and your answer; the assistant will update this page. The linked documents are research, not decisions or approvals.

| ID | Decision needed | Research |
| --- | --- | --- |
| Q-M1 | What is the first NBP business use case? Choose published daily values, daily values plus a defined Table C spread, or describe a different decision/comparison. | [NBP business-definition proposals](nbp-business-definitions.md) |
| Q-S1 | What source or topic should be considered next: Eurostat, World Bank WDI, or another source? State the intended business use. | [Candidate-source research](source-candidates.md) |

## Evidence and current limits

The current verified v2 Drive release is `84784104-e014-4550-bb0c-095d967230b5`, produced by [`71f79fc`](https://github.com/rutkala/zohelo-data/commit/71f79fc4fbaa7f8c1f2b2133b4f62106f13fcf02). [Current release evidence](releases/2026-09-07-medallion-portal.md) records 15 tables, 429,795 clean Silver observations, all-four-source coverage checked through 6 September, fresh queries, and 1,322,051 exact rows matched across all layers during raw replay. The earlier release `5f356b1b-97cd-470d-9d5e-b46ebb37a7d1` is retained as [historical verification evidence](releases/2026-09-07-nbp-platform.md).

Production builds and browser checks passed in GitHub before merge; the local Workbox/Chromium limitations were not used to waive those gates. The public portal's new query/view flow was also exercised after deployment. The full Drive-backed UI used release fixtures in browser CI, while the actual published data passed independent live restore and replay checks. This verifies a Drive data snapshot, not a completed semantic layer.

[Foundation audit](audits/2026-09-06-foundation.md) and the [current platform audit](audits/2026-09-07-platform.md) record the verified repair history and the limits still to measure. [NBP platform operations](nbp-platform-operations.md) describes the live implementation and bounds; [silver publication](nbp-silver-publication.md) describes the retained v1 compatibility path. [Earlier v1 release evidence](releases/2026-09-07-nbp-silver.md) is historical context, not a current freshness statement. Verified authorization evidence is available in [PR 53](https://github.com/rutkala/zohelo-data/pull/53) and the [upload verification run](https://github.com/rutkala/zohelo-data/actions/runs/34067541199); neither of those checks published the v2 release.

Historical FX unit normalization remains unproven. No source is approved for expansion, and public access does not establish commercial reuse permission. The release does not establish unlimited free compute, a service-level commitment, or measured performance for larger sources.

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

### D7. Data-first portal and SQL experience

Deliver a focused data-first portal update: one native dbt catalogue for published data, lineage, and metric definitions; no project-management or owner-review UI; and SQL that automatically loads every released table referenced by a statement, including joins, unions, and CTEs. Keep published data-file downloads within 64 MiB per engine session and explain a limit failure before execution. Catalogue artifacts have a separate 16 MiB limit; these download limits are not guarantees about query memory use. A modern palette and layout may support this bounded experience. Portal validation, CI, and deployment are required before any of this new behavior is described as live.

## Cross-cutting acceptance requirements

Across D1–D5: verify business examples and representative fixtures; use the same inputs, cutoff, and revision policy for full/incremental equivalence; prove safe versioned publication and raw rebuild; expose release, provenance, status, errors, and source batches clearly; and measure operating limits without assuming a date, cost, SLA, or unlimited free capacity.
