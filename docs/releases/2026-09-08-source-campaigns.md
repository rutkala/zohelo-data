# Source campaign live evidence — 8 September 2026

The first bounded WDI, GUS BDL and Eurostat campaigns passed production collection and fresh-process replay in [run 34217059540](https://github.com/rutkala/zohelo-data/actions/runs/34217059540), finishing at approximately **10:50 UTC**. Each source has accepted recent and historical responses in durable **Landing** storage. Earlier failures and their repairs remain recorded below. This accepts the first bounded campaigns, not full source completion or analytical publication.

## Delivered framework

[PR #73](https://github.com/rutkala/zohelo-data/pull/73) merged the campaign implementation as [`5b66ed2eb1059b1a8a376ad1c7b6c77c04fb4883`](https://github.com/rutkala/zohelo-data/commit/5b66ed2eb1059b1a8a376ad1c7b6c77c04fb4883). Its [CI run 34212414191](https://github.com/rutkala/zohelo-data/actions/runs/34212414191) passed **208 tests**.

[PR #74](https://github.com/rutkala/zohelo-data/pull/74) added retained rejection diagnostics, a 90-second WDI timeout and BDL metadata-cursor repair. It merged as [`168d6783ae8d18de2f3933148c8d5d4a92e132d5`](https://github.com/rutkala/zohelo-data/commit/168d6783ae8d18de2f3933148c8d5d4a92e132d5), and [CI run 34214081132](https://github.com/rutkala/zohelo-data/actions/runs/34214081132) succeeded.

The framework runs at `17 */4 * * *` UTC. Each provider has an independent quota and a maximum of 12 requests per run, with a 240-second run budget checked between attempts. An in-flight request and checkpoint save can extend elapsed time beyond that budget. The default HTTP timeout is 45 seconds; WDI uses 90 seconds after the measured earlier timeout. Recent, history and discovery work advance through independent, fairly scheduled lanes. Exact raw responses, receipts and checkpoints are retained in Drive, and a fresh process verifies selected retained responses after collection. These collection checkpoints do not change an NBP release pointer.

## Production run 34214627594

Producer: [`168d6783ae8d18de2f3933148c8d5d4a92e132d5`](https://github.com/rutkala/zohelo-data/commit/168d6783ae8d18de2f3933148c8d5d4a92e132d5).

| Provider | Job result | Accepted this run | Representations reported | New raw response bytes | Cumulative state after run |
| --- | --- | ---: | ---: | ---: | --- |
| World Bank WDI | [Job 102023565678](https://github.com/rutkala/zohelo-data/actions/runs/34214627594/job/102023565678) **succeeded** | 3: history 2, recent 1 | 3,000 | 703,347 | 7 accepted; 1,422,521 raw-response bytes; 58 pending tasks; 28 recent roots; 1 old pending retry |
| Eurostat | [Job 102023565657](https://github.com/rutkala/zohelo-data/actions/runs/34214627594/job/102023565657) **succeeded** | 12: history 6, recent 6 | 210 | 49,014 | 21 accepted; 2,050,708 raw-response bytes; 152 pending tasks; 81 roots; 0 retries |
| GUS BDL | [Job 102023565664](https://github.com/rutkala/zohelo-data/actions/runs/34214627594/job/102023565664) **failed** | 6 metadata responses | — | — | 13 accepted; 307,672 raw-response bytes; 94 pending tasks; 1 root; 3 retries |

The WDI fresh-process restore verified retained history, recent and discovery evidence at **10:22:51 UTC**. Its discovery response reported a catalogue of **1,498 indicators**.

The Eurostat fresh-process restore verified retained history and recent evidence at **10:22:51 UTC**. Catalogue evidence had already been verified in [the first production run, 34212811919](https://github.com/rutkala/zohelo-data/actions/runs/34212811919), where Eurostat accepted 9 responses, including a catalogue response reporting **12,234 datasets**.

The BDL post-failure verification successfully replayed retained metadata evidence. Its population recent and history requests returned HTTP 200 responses that failed validation because the page was missing or invalid; the root-locality request returned HTTP 400. Successful metadata responses and their checkpoints remain retained, but this job does **not** establish live BDL acceptance.

Reported representations are adapter transport metadata and can include metadata, repeated representations and explicit missing observations. They are not counts of unique analytical facts. Raw-response byte counters are cumulative received response-body bytes, not physical Drive storage.

## Corrected production contract

[PR #75](https://github.com/rutkala/zohelo-data/pull/75) merged as [`7e43ccf1de25fdc8207f21f29861e6e3eb5402a5`](https://github.com/rutkala/zohelo-data/commit/7e43ccf1de25fdc8207f21f29861e6e3eb5402a5) after [CI run 34216699560](https://github.com/rutkala/zohelo-data/actions/runs/34216699560) passed **222 tests** in 151.668 seconds. It implements an observation-pagination fallback, municipality-scoped locality discovery, audited retirement of the exact obsolete root requests, and an explicit retry path for older HTTP 200 `ValueError` failures with different code, without resetting quota reservations or provider cooldowns.

Local validation passed 59 focused recovery/store/adapter/integration tests plus source-coverage and workflow-policy checks. The full local data check stalled in an existing sandbox subprocess test and was stopped; the successful full GitHub CI run is the full-suite evidence.

## Accepted production run 34217059540

Producer: [`7e43ccf1de25fdc8207f21f29861e6e3eb5402a5`](https://github.com/rutkala/zohelo-data/commit/7e43ccf1de25fdc8207f21f29861e6e3eb5402a5). The preparation job verified available account headroom and prepared independent source paths beneath the configured production `zohelo-data` root. All three collection and fresh-process verification jobs succeeded.

| Provider | Successful job | Accepted this run | Representations reported | New response bytes | Cumulative accepted / received bytes | Pending tasks / recent roots |
| --- | --- | --- | ---: | ---: | --- | --- |
| World Bank WDI | [102031393685](https://github.com/rutkala/zohelo-data/actions/runs/34217059540/job/102031393685) | 6: recent 2, history 2, discovery 2 | 3,380 | 904,143 | 13 / 2,326,664 | 106 / 53 |
| GUS BDL | [102031393603](https://github.com/rutkala/zohelo-data/actions/runs/34217059540/job/102031393603) | 9: recent 4, history 4, discovery 1 | 12,827 | 548,495 | 22 / 856,167 | 152 / 21 |
| Eurostat | [102031393679](https://github.com/rutkala/zohelo-data/actions/runs/34217059540/job/102031393679) | 12: recent 6, history 6 | 176 | 48,782 | 33 / 2,099,490 | 146 / 81 |

All three reported zero failed requests, zero pending retry tasks and `coverage_status: incomplete`. WDI and BDL stopped at the between-attempt time budget (241.09 and 244.93 seconds); Eurostat stopped after its 12-request budget (247.00 seconds). Discovery increased pending work and recent roots; an increased queue is not lost progress. Retained catalogue metadata reports 1,498 WDI indicators, 295 World Bank economy/group entities and 172,573 BDL variables; none of those counts means their data are all ingested.

The explicit recovery control reset exactly the two older BDL HTTP-200 parser rejections. WDI and Eurostat reset zero tasks. The BDL migration recorded exactly two superseded root-locality tasks; they are not counted as completed data. Eight BDL observation pages and one variable-catalogue page then passed. Municipality-scoped locality planning is fixture-tested, but this run did not execute locality tasks; complete locality acceptance remains unproven.

Fresh-process replay restored the latest state and verified retained raw-response hashes, sizes and adapter decoding:

| Source | Verified at UTC | Sampled lanes and response evidence |
| --- | --- | --- |
| WDI | 10:50:20 | discovery: 21,773 bytes / 25 records; history: 276,084 / 1,000; recent: 13,476 / 60 |
| BDL | 10:50:32 | history: 114,999 bytes / 2,836 records; recent: 21,621 / 394; discovery: 2,836 / 20 |
| Eurostat | 10:50:20 | history: 2,902 bytes / 10 records; recent: 2,629 / 2; catalogue replay was evidenced by the earlier first run |

These are bounded replay samples, not a full data audit. Historical rejected receipts remain visible as evidence even after the affected tasks recover. The NBP push workflow was skipped because this change did not request an NBP run; its daily schedule and published release remain separate. The four-hour source schedule continues after this acceptance record; it is an ingestion workflow, not a background AI development task.

## Scope and remaining gates

The current work ends at exact Landing intake, durable receipts and resumable checkpoints. It has not created new-source Bronze, Silver, Gold, semantic models, dbt catalogue entries or portal publication. Cold replay is bounded to selected accepted receipts; it is not a full-history replay.

The taxonomy contains **15 domains, 45 subdomains, 231 categories, 17 analytical dimensions and 11 classifications**. The research inventory contains **182 product/family records**. Candidate mappings currently give **177 categories** at least one research lead and leave **54 without one**. These are candidate-discovery counts, not connected, ingested, historically complete or published coverage.

Full source completion is not done. Current WDI, Eurostat and BDL queues remain incomplete. The next scaling gates are sharded state and physical retained-byte accounting, followed by reference geographies and classification editions, dbt medallion models, independent source releases and semantics, then broader source waves. See the [source expansion plan](../source-expansion-plan.md) and [campaign operations guide](../source-campaign-operations.md) for the accepted boundaries and controls.
