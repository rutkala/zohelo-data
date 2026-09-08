# NBP data release verified — 7 September 2026

**Historical release evidence.** This records the earlier foundation snapshot.
The subsequent [audit acceptance release](2026-09-07-platform-readiness.md)
completed the five daily NBP metrics and full scoped acceptance. Current work
and later operational checks are recorded in [the delivery record](../deliverables.md).

**The NBP data foundation is live. The complete platform scope remains open for governed metrics.** This is evidence for a Drive data snapshot, not a published GitHub Release or a claim that the semantic layer is finished.

## Release and verification

| Item | Verified value |
| --- | --- |
| Drive release ID | `5f356b1b-97cd-470d-9d5e-b46ebb37a7d1` |
| Producer code | [`f071669937829a3d0775a13146b3170e1b283b4e`](https://github.com/rutkala/zohelo-data/commit/f071669937829a3d0775a13146b3170e1b283b4e) |
| Format and scope | v2, `nbp_platform` |
| Published tables | 15: four bronze, four silver, change events, two facts and four dimensions |
| Source coverage checked through | 6 September 2026, all four NBP sources |
| Data job | [34096483209](https://github.com/rutkala/zohelo-data/actions/runs/34096483209), passed at 08:45:24 UTC |
| Preservation audit | [34098420713](https://github.com/rutkala/zohelo-data/actions/runs/34098420713), passed at 08:46:16 UTC |
| Portal deployment | [34096483163](https://github.com/rutkala/zohelo-data/actions/runs/34096483163), passed with the same producer code |

The [data job log](https://github.com/rutkala/zohelo-data/actions/runs/34096483209/job/101661167522) records a passing dbt build of 15 models and 52 data tests (67 successful nodes), publication, fresh native SQL reads of every table, and a separate rebuild from the exact retained raw responses. The raw rebuild matched schemas and all **1,314,774 rows across the 15 tables**, using bidirectional `EXCEPT ALL`. That cross-layer total includes bronze versions, silver records, facts and dimensions; it is not a unique-observation count.

The [preservation log](https://github.com/rutkala/zohelo-data/actions/runs/34098420713/job/101679232066) compared the new four-table silver key set against immutable v1 release `1ab2f2f0-4325-42fc-bc92-cf3d9e9d9eea`, produced by `474bbb61a2bb9d88266808e872f8a7613aca23d6`. **All 429,611 prior keys remain present, with zero missing keys or date regressions.** Value equality was not required: the accepted policy allows corrected source values.

## Silver and gold coverage

| Dataset | Silver rows | First observation | Latest observation | Additional keys over v1 |
| --- | ---: | --- | --- | ---: |
| NBP Table A | 198,842 | 2002-01-02 | 2026-09-04 | 128 |
| NBP Table B | 143,767 | 2002-01-02 | 2026-09-02 | 0 |
| NBP Table C | 83,737 | 2002-01-02 | 2026-09-04 | 52 |
| NBP gold prices | 3,449 | 2013-01-02 | 2026-09-04 | 4 |
| **Total** | **429,795** | | | **184** |

Gold contains `fact_fx_quotes` with 426,346 rows and `fact_gold_prices` with 3,449 rows, plus `dim_date`, `dim_currency`, `dim_source_table` and `dim_commodity`. The date dimension begins on 2001-12-31 because it includes the distinct Table C trading-date role. It does not assert that an observation exists on every calendar date.

“Checked through 6 September” describes validated request coverage, not a quotation on that date. The release includes four source catalogue records and dbt lineage bound to the same data/code. Its change-event table has zero rows: no changes were detected among the retained new ingestion versions in this release; this does not prove that NBP has never revised historical data. Original legacy raw versions that were never retained cannot be recreated.

## Recorded measurements

These are measurements of the successful resumed bootstrap, not a daily-run or service-level forecast. Earlier failed attempts also used compute and are excluded from these stage timings.

| Measurement | Recorded result |
| --- | ---: |
| Successful job elapsed time, including setup and recovery checks | 66 min 37 sec |
| Requests during this resumed run | 226; zero recorded network retries |
| Retained observation batches used | 351 |
| Intake | 3,492.142 sec |
| Unique raw bytes downloaded for the build | 28,851,931 bytes |
| Build input transfer | 207.717 sec |
| dbt build and Parquet export | 20.541 sec |
| Published Parquet output, all 15 tables | 10,077,163 bytes |
| Local working-directory files | 75,267,018 bytes |
| Publication, including compatibility check | 62.997 sec |
| Fresh restore and SQL metadata checks | 13.748 sec |
| Individual native count/date/schema checks | 0.0026–0.0040 sec |
| Raw replay download / dbt / comparison | 139.713 / 20.462 / 7.478 sec |

The slow part of this run was ingestion and transfer, not SQL transformation. The small Parquet and working-directory sizes support retaining the current architecture for this measured NBP workload. These observations do not establish performance for larger sources. Count/date checks are not representative interactive-query benchmarks, and peak RAM, browser query latency, account-specific quotas and total billing were not measured.

## Acceptance and next work

- The latest [data CI](https://github.com/rutkala/zohelo-data/actions/runs/34098125869) passed 93 tests. The actual development-container and portal fixture/browser checks are linked in the [audit](../audits/2026-09-07-platform.md). A successful deployment and native consumer check do not substitute for the owner's first authenticated browser experience with this new snapshot.
- Ingestion resumes from durable checkpoints, all four datasets reach modeled gold, prior keys are preserved, and the published tables rebuild exactly from retained raw. Production failure/rollback boundaries have fixture coverage; no destructive live rollback drill was performed.
- Governed NBP metrics and native metric serving are **not delivered**. The catalogue contains `metrics=[]` and `metrics_status=awaiting_business_approval`; the existing MetricFlow test proves runtime compatibility on synthetic data only.
- Historical FX unit normalization remains unproven. Preserve API values as published; do not silently rescale or sum quotation levels. New-source commercial licensing and business priorities remain open.
- Smallest useful operational follow-ups: record one routine incremental run; measure retained-state growth and peak memory; time representative browser/SQL queries. Address the documented batch/working-set limits before growth reaches them. These follow-ups do not invalidate the completed snapshot checks.

See the [delivery plan](../deliverables.md) for status and the [short portal guide](../using-the-portal.md) for catalogue navigation and example queries.
