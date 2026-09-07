# Platform audit delivery — 7 September 2026

**Live verification passed.** Release `96b14b78-dc36-4422-952c-5fb3af726ac8` contains 15 tables and five source-defined daily metrics. [Publication, fresh consumer, raw replay and capacity run](https://github.com/rutkala/zohelo-data/actions/runs/34167068188). Daily ingestion is active at 02:00 UTC.

## Delivered code and portal

[PR #66](https://github.com/rutkala/zohelo-data/pull/66) is merged as
`2bfca0b78b6bc0bc3da1586856f38d34f3af49d9`. The merged tree
`c99dbce0cd6f8d37ad48ca7c1a5bd8d5dbb0f603` matches the reviewed PR head
`27cc957fe5183775475fb23f25249eb9c26a6841` exactly.

| Verification | Result / evidence |
| --- | --- |
| Python, data and real dbt/MetricFlow fixtures | **138 tests passed**; genuine dbt model/test builds, native daily metric queries, invalid-grain rejection, staged publication and portable restore. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165196539). |
| Portal production builds, lint and unit/engine tests | Both deployment layouts passed; **678 tests per configuration**. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165196537). |
| Browser verification | **14 flows per deployment layout**, including real dbt metric search, metadata, semantic-to-physical lineage, SQL editing/views and matching light/dark desktop/mobile colours. Same portal run above; screenshots were also inspected. |
| Fresh development container | Passed the actual environment build and startup/fixture checks. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165196579). |
| Portal deployment | Build and Pages deployment succeeded for the merged commit. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165675576). |
| Google connection | Read-only access check passed after merge. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165675596). |
| Live audit NBP publication | **Passed** at producer `8f29a0b98876f6ae6b161aae3ec5be5cbeb247cb`. Publication, independent SQL/MetricFlow consumption, exact raw replay and health inventory all succeeded. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34167068188). |

GitHub confirms deployment. Direct public-site retrieval of the portal and
`portal-build.json` was blocked by this session's web-access service, so this
record does not claim a separate public-site HTTP check succeeded.

The first browser attempt caught a theme-transition timing issue in the new
test. The next caught a selector treating native search cards as anchors. Both
were corrected before merge; the checks on the final PR commit above passed. The
independent recovery review also fixed a real blocker: damage to current data
files no longer prevents promotion of a healthy retained release. Current
manifest integrity and full target verification remain mandatory. Recovery
failure tests use isolated stores; no live rollback drill was performed.

## Verified live release

[PR #68](https://github.com/rutkala/zohelo-data/pull/68) recorded the owner's explicit **“Yes, I approve”**, restored the 02:00 UTC schedule and started the approved run. Producer commit: `8f29a0b98876f6ae6b161aae3ec5be5cbeb247cb`. The current release is **`96b14b78-dc36-4422-952c-5fb3af726ac8`**. Existing releases and raw evidence remain retained; no deletion or live rollback drill was performed.

The run completed all 72 dbt model/test nodes successfully both during publication and during independent raw replay. Uploaded candidate content and provenance were validated before the current-release reference changed.

The fresh consumer restored a portable DuckDB database and verified all **15 tables**, their row counts, schema and date ranges. It downloaded **11,639,987 bytes** and completed SQL plus native MetricFlow acceptance in **25.306 seconds**.

| Cleaned dataset | Rows | Latest observation |
| --- | ---: | --- |
| NBP Table A | 198,842 | 2026-09-04 |
| NBP Table B | 143,767 | 2026-09-02 |
| NBP Table C | 83,737 | 2026-09-04 |
| NBP gold prices | 3,449 | 2026-09-04 |

There are **429,795 cleaned observations**. All four sources were checked completely through the deliberate cutoff **6 September 2026**; this does not assert that an observation exists on every date. No source-change events were detected in this build. Bronze increased because successful re-observations are retained; the cleaned observation count stayed the same.

## Native MetricFlow acceptance

All five metrics matched independent SQL against the restored gold tables. The query comparison used the **latest two publication dates per source**; grain validation checked **all gold rows**. It returned **350 rows** in total. These are daily source observations; period aggregation, returns and currency conversion remain outside this baseline.

| Metric | Compared publication dates | Rows | Match |
| --- | --- | ---: | --- |
| `nbp_table_a_mid` | 2026-09-03 to 2026-09-04 | 64 | Passed |
| `nbp_table_b_mid` | 2026-08-26 to 2026-09-02 | 232 | Passed |
| `nbp_table_c_bid` | 2026-09-03 to 2026-09-04 | 26 | Passed |
| `nbp_table_c_ask` | 2026-09-03 to 2026-09-04 | 26 | Passed |
| `nbp_gold_price_pln_per_gram_1000` | 2026-09-03 to 2026-09-04 | 2 | Passed |

MetricFlow query/validation time within the fresh-consumer check was **15.872 seconds**. The supported interface is `scripts/query_metrics.py`; the matching release includes its semantic manifest. The existing dbt catalogue exposes the published definitions and lineage; it is not an always-on MetricFlow API.

## Exact recovery and operating measurements

A separate process rebuilt the release from **367 observation batches**, transferring **28,851,931 unique raw bytes**. It compared all 15 tables and matched **1,329,363 rows exactly**. Replay timings: download 127.884 s, dbt/model build 39.456 s, comparison 7.984 s. This establishes recovery from retained raw for this release; it is separate from changing the current pointer back to an older release.

The publication run made **8 source requests**, with **0 retries**. Ingestion took 93.758 s; raw download 182.013 s; dbt/export 40.019 s; staged upload/validation/promotion 79.977 s. Drive transfer dominates this run; source expansion should consider file/request volume as well as bytes.

The Linux memory report observed a **416,776,192-byte parent high-water value** and **2,198,511,616 bytes for the largest completed child**. These are individual process measurements; no concurrent process-tree peak was measured.

## Capacity and retention

The native read-only health report completed at **2026-09-07T22:44:14.851245+00:00**. It found no capacity warnings or unknown descriptor sizes.

| Build/state bound | Used | Limit | Used % |
| --- | ---: | ---: | ---: |
| Observation batches | 367 | 2,048 | 17.92% |
| Declared raw input bytes | 30,344,451 | 268,435,456 | 11.3% |
| Current canonical state bytes | 265,005 | 8,000,000 | 3.31% |

The warning threshold is **70%**, with a review required before expanding across a bound. The current maximum is **17.92%**; no future growth rate or unlimited retention is claimed.

The selected project inventory was **complete**: **2,252 files**, **65 folders**, **156,260,745 bytes** (about 156 MB), **0 unknown sizes** and no incomplete reasons. It used 66 metadata requests over 17.426 seconds. This is a multi-request observation, not a transactional filesystem snapshot. Physical release storage is under retained release folders; a top-level Gold folder size is not a measure of published gold data.

Retention remains **retain all, no automatic deletion**. Current state history occupies 49,968,834 bytes and retained release files 41,551,451 bytes. Reference-safe compaction remains an explicit future requirement before capacity thresholds or new-source projections require it. The account quota check returned available capacity; account-wide totals are not project allocations.

[Machine-readable health artifact](https://github.com/rutkala/zohelo-data/actions/runs/34167068188/artifacts/10034677999) is retained by Actions for 14 days. The durable measurements needed for the audit are recorded above. The earlier connector-only download limitation was superseded by this successful native health report.

## Approval history

Automatic approval review initially rejected the production write, and the schedule was paused in PR #67. The owner's subsequent explicit approval resolved H-LIVE. PR #68 restored the schedule, and the live results above close the acceptance work. No further approval is pending for this release or the restored daily schedule.

## Current project record

[Deliverables and owner decisions](../deliverables.md) distinguish completed
audit/data/semantic/portal work from the longer-term scope holds. The
[architecture](../architecture.md), [workflow audit](../audits/2026-09-07-workflows.md),
[source definitions](../nbp-business-definitions.md),
[operating guide](../nbp-platform-operations.md) and
[collaboration guide](../collaboration.md) are the supporting records.

No additional source or paid service was introduced. Commercial data reuse,
commercial hosting, automatic retention compaction and always-on semantic
serving have explicit reasons and reopening conditions in the delivery record.
