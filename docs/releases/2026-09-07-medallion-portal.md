# Medallion catalogue and SQL portal delivery

Verification date: 7 September 2026.

[PR 64](https://github.com/rutkala/zohelo-data/pull/64) merged as
[`71f79fc`](https://github.com/rutkala/zohelo-data/commit/71f79fc4fbaa7f8c1f2b2133b4f62106f13fcf02).
The owner explicitly approved source publication, merge, portal deployment and publication of the matching catalogue after checks passed.

## Delivered behaviour

- The dbt project uses Bronze, Silver and Gold model folders and physical schemas `02_bronze`, `03_silver` and `04_gold`. Physical aliases match the published portal relations. Landing is the real verified raw source in `01_landing`; Archive is documented retained-file storage. Stable dbt model IDs and the 15-table publication contract are preserved.
- The single native dbt catalogue has phone navigation through **Browse**, with Project/Database trees and model details. Project tracking stays in the canonical delivery record.
- Table ellipsis actions offer **Query as SELECT**, **Insert in SQL editor** and **Copy quoted name**. Desktop drag-and-drop inserts SQL. Existing drafts are preserved.
- **Create view** creates a local Browser workspace view from the current SELECT/WITH query and automatically loads referenced published tables. Existing relations are not replaced. Views last for the browser database session; saving their SQL allows recreation.
- One SQL editor remains mounted across desktop/mobile changes. The phone Tables control no longer overlaps the catalogue header.

## Validation and deployment

| Evidence | Result |
| --- | --- |
| [Final portal CI](https://github.com/rutkala/zohelo-data/actions/runs/34142601927) | Both production builds passed, with 678 unit/engine tests and 12 browser flows per deployment path: 24 browser executions total. |
| [Final data CI](https://github.com/rutkala/zohelo-data/actions/runs/34142601968) | 94 Python/data tests passed. |
| [Development container CI](https://github.com/rutkala/zohelo-data/actions/runs/34142601948) | Actual environment build/startup and its data checks passed. |
| [Pages deployment](https://github.com/rutkala/zohelo-data/actions/runs/34142976977) | Build and deployment succeeded for the merged code. |
| Live public portal interaction | `SELECT 1 AS portal_check` returned 1. Created `main.portal_ux_check`, used its ellipsis **Query as SELECT**, and queried the view successfully. |
| [Matching data publication](https://github.com/rutkala/zohelo-data/actions/runs/34142977073) | Publication, fresh restore/query of all 15 tables, and exact raw replay passed. |

Browser regression coverage includes native catalogue details, mobile Project/Database navigation, artifact failures, edited SQL with automatic loading, desktop drag-and-drop, touch insertion, view creation and name collisions, and draft preservation at phone/tablet breakpoints. Successful phone and desktop screenshots were inspected. The new mobile test exposed a real drawer backdrop stacking bug, which was fixed before merge.

The live cloud-browser check used its local database session. That browser was not connected to Google Drive, so the authenticated Drive-backed UI was covered by the production-build browser fixtures and the separate live data restore/replay checks. No live Google sign-in success is claimed from this check.

## Matching data release

The verified current release is `84784104-e014-4550-bb0c-095d967230b5`, produced by the same merged code `71f79fc4fbaa7f8c1f2b2133b4f62106f13fcf02`. Its four clean Silver datasets contain **429,795 observations**.

| Layer | Published tables | Rows across those tables |
| --- | ---: | ---: |
| Bronze | 4 | 453,250 |
| Silver, including the empty change-events table | 5 | 429,795 |
| Gold facts and dimensions | 6 | 439,006 |
| Total compared during replay | 15 | 1,322,051 |

The replay total counts each layer's representation and dimension rows; it is not a count of unique source observations.

All four sources were checked through **6 September 2026**. Latest observations remain 4 September for Tables A/C and gold prices, and 2 September for Table B.

The fresh consumer restored and queried all 15 tables in **17.747 seconds**. A separate read-only process rebuilt the release from **359 recorded input batches** and matched metadata and all **1,322,051 rows** exactly. It transferred **28,851,931 unique raw bytes**; download took 152.805 seconds, dbt 22.094 seconds and comparison 9.924 seconds, for 184.823 seconds total. The incremental publisher made eight source requests with zero network retries and retrieved 503,851 new response bytes. These are measurements of this run, not performance or availability guarantees.

The preceding verified release `5f356b1b-97cd-470d-9d5e-b46ebb37a7d1` remains historical evidence in [the earlier release record](2026-09-07-nbp-platform.md). The new publication preserves the immutable-release mechanism and the existing table contract.

Governed NBP metrics, durable/shared user-authored views, and further catalogue metadata are outside this delivery. See [the canonical delivery record](../deliverables.md) for remaining work and owner questions.
