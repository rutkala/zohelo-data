# Verified NBP silver snapshot, 7 September 2026

**Intermediate silver milestone; the complete data-platform release remains unfinished.**

[PR 55](https://github.com/rutkala/zohelo-data/pull/55) is merged at
`3c92782b4451ff1c7d7f995421122fe1ac3c45e6`. The
[production workflow](https://github.com/rutkala/zohelo-data/actions/runs/34069787468)
published Drive snapshot `a839a389-d737-498a-86af-c092559a5979`, scope
`nbp_silver`, and a separate fresh process restored and queried every table.
This processed existing bronze inputs; it did not run NBP ingestion.

| Dataset | Rows | First observation | Latest observation |
| --- | ---: | --- | --- |
| Exchange rates A | 198,682 | 2002-01-02 | 2026-08-28 |
| Exchange rates B | 143,651 | 2002-01-02 | 2026-08-26 |
| Exchange rates C | 83,672 | 2002-01-02 | 2026-08-28 |
| Gold prices | 3,444 | 2013-01-02 | 2026-08-28 |

Total: **429,449 rows**. These are measured date bounds and counts, not a
claim that no historical observations are missing. Catch-up remains necessary.

## Validation

- [All 60 Python/dbt fixture tests passed](https://github.com/rutkala/zohelo-data/actions/runs/34069548005), including mixed historical schemas, identical replays, conflicting values, publication failures and fresh consumers.
- [Both portal CI jobs passed](https://github.com/rutkala/zohelo-data/actions/runs/34069547996): 634 unit/engine tests and 3 browser tests at each base path, plus lint and production builds.
- The live build passed 4 dbt models and 12 data tests. Publication read back and checked each file before selecting the snapshot; the fresh reader separately verified hashes, schema, row counts and date bounds.
- [Portal deployment succeeded](https://github.com/rutkala/zohelo-data/actions/runs/34069787447). The public `portal-build.json` returned the matching commit above. Signed-in interaction with the new snapshot on the owner's phone has not been tested by the assistant.

## Recorded measurements

| Measurement | Result |
| --- | ---: |
| Bronze input files | 365 |
| Input bytes | 3,585,652 |
| Silver output bytes | 2,448,810 |
| Recorded working-directory bytes | 10,464,229 |
| Download phase, including initialization/inventory | 174.725 seconds |
| dbt build, docs and export | 14.358 seconds |
| Publication and verification | 21.154 seconds |
| Fresh restore and checks | 5.022 seconds |
| Native per-table count/date check | 0.0026–0.0030 seconds |

The transfer phase dominates this small working set; file/request overhead is
an investigation candidate, not a measured causal breakdown. These timings
are one run. They do not measure browser latency, arbitrary SQL performance,
peak RAM/disk, future growth, or exact billed compute/free allowance usage.

## Remaining boundaries

Prior/legacy files were retained. Failure/recovery behavior was exercised with
fixtures, not by deliberately interrupting this live publication. Raw-to-bronze
links for legacy inputs remain unverified. Durable ingestion checkpoints,
correction history, historical gap checks, rebuilding from retained raw,
modeled gold, approved executable NBP metrics and the business catalogue remain
unfinished. No GitHub Release was created for this intermediate milestone.
