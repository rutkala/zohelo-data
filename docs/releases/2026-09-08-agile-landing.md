# Agile Landing production acceptance — 8 September 2026

**Backend production acceptance passed.** [Source campaign run 34221713083](https://github.com/rutkala/zohelo-data/actions/runs/34221713083), produced by [`a58da2e61f4e1093636ef598f8c7e8ff1e0975fe`](https://github.com/rutkala/zohelo-data/commit/a58da2e61f4e1093636ef598f8c7e8ff1e0975fe), completed three consecutive collect-and-publish cycles for WDI, GUS BDL and Eurostat. All three provider jobs passed collection, fresh raw-response replay and fresh published-Landing restore/query.

This run added **81 accepted responses** and left **178 accepted responses published** as queryable Parquet envelope rows: WDI 47, BDL 61 and Eurostat 70. The three final snapshots had zero pending publication. Source-history collection remains incomplete.

## Delivered backend

[PR #77](https://github.com/rutkala/zohelo-data/pull/77) merged the consecutive-batch and Landing-publication backend as [`a58da2e61f4e1093636ef598f8c7e8ff1e0975fe`](https://github.com/rutkala/zohelo-data/commit/a58da2e61f4e1093636ef598f8c7e8ff1e0975fe). [Full CI run 34221334104](https://github.com/rutkala/zohelo-data/actions/runs/34221334104) passed **252 tests** at reviewed head [`03b83caf8ce72c1d9e78b808823895097d34609f`](https://github.com/rutkala/zohelo-data/commit/03b83caf8ce72c1d9e78b808823895097d34609f).

The scheduled trigger is `7,37 * * * *`, every 30 minutes. A normal scheduled source session runs up to three consecutive bounded collection cycles within a 15-minute session target, publishing a validated immutable Landing snapshot between cycles. Provider quotas, retries, source serialization and fail-closed storage checks remain in force. An in-flight request or publication can finish after the session target within the separate job timeout.

The preceding immediate [run 34217059540](https://github.com/rutkala/zohelo-data/actions/runs/34217059540) used the earlier workflow, added 29 accepted responses and reached WDI 21, BDL 31 and Eurostat 45. Run 34221713083 is the first live proof of the merged consecutive-batch and queryable-Landing backend.

## Consecutive production batches

Preparation [job 102046136182](https://github.com/rutkala/zohelo-data/actions/runs/34221713083/job/102046136182) passed before the provider jobs began. Every batch below reported zero failed requests and zero pending retries.

| Provider and access mode | Provider job | Cycle 1 | Cycle 2 | Cycle 3 | Run / cumulative accepted | Final collection queue |
| --- | --- | --- | --- | --- | ---: | ---: |
| World Bank WDI — public | [102046344166](https://github.com/rutkala/zohelo-data/actions/runs/34221713083/job/102046344166) | 9: discovery 2, history 4, recent 3 | 8: discovery 2, history 3, recent 3 | 9: discovery 1, history 4, recent 4 | 26 / 47 | 406 pending |
| GUS BDL — anonymous | [102046344237](https://github.com/rutkala/zohelo-data/actions/runs/34221713083/job/102046344237) | 10: discovery 2, history 4, recent 4 | 10: discovery 2, history 4, recent 4 | 10: discovery 2, history 4, recent 4 | 30 / 61 | 203 pending |
| Eurostat — public | [102046344168](https://github.com/rutkala/zohelo-data/actions/runs/34221713083/job/102046344168) | 8: history 4, recent 4 | 8: history 4, recent 4 | 9: history 4, recent 5 | 25 / 70 | 127 pending |

BDL used the anonymous access profile in this run. This record does not claim that a registered BDL account or credential is configured.

## Incremental Landing snapshots

Each cycle promoted a separately validated per-source snapshot. `Rows` is the number of accepted response-envelope rows present in that snapshot; `pending` is accepted evidence awaiting the next bounded publication chunk at that point.

| Provider | Cycle | Snapshot ID | Rows | Pending publication |
| --- | ---: | --- | ---: | ---: |
| WDI | 1 | `73ccddfd-8318-4b97-824b-b3cecebd36d5` | 24 | 6 |
| WDI | 2 | `74b918da-737c-4852-98d6-8a0d1d6afbcb` | 38 | 0 |
| WDI | 3 | `42fcde98-3f86-4d46-ad28-438424ad85d5` | 47 | 0 |
| BDL | 1 | `6a892d3f-3c60-4235-9bc8-95781beef89f` | 24 | 17 |
| BDL | 2 | `649a6c99-c076-4fb9-a8d9-b4051f289d9b` | 48 | 3 |
| BDL | 3 | `f930e056-c512-45bc-9b41-7f1717aaa0c7` | 61 | 0 |
| Eurostat | 1 | `8a9b815c-d4ed-49b4-8194-44f5af78099a` | 24 | 29 |
| Eurostat | 2 | `a11915bc-1f83-442c-a5df-cf8c91de35a2` | 48 | 13 |
| Eurostat | 3 | `a50a060f-c5c9-4932-b370-d56515e2f217` | 70 | 0 |

## Fresh-process recovery and query proof

After collection, each provider job started new processes for two independent checks. The raw-response check restored current durable campaign state and replayed one retained response for each represented lane. The Landing check restored the final Parquet snapshot, verified its manifest and content hashes, and queried the source response table.

| Provider | Raw-response replay lanes and reported records | Final Landing table | Verified rows | Snapshot |
| --- | --- | --- | ---: | --- |
| WDI | history 1,000; recent 1,000; discovery 25 | `world_bank_wdi_responses` | 47 | `42fcde98-3f86-4d46-ad28-438424ad85d5` |
| BDL | history 2,836; recent 0; discovery 2 | `gus_bdl_responses` | 61 | `f930e056-c512-45bc-9b41-7f1717aaa0c7` |
| Eurostat | history 10; recent 2 | `eurostat_responses` | 70 | `a50a060f-c5c9-4932-b370-d56515e2f217` |

The replay record counts are adapter-reported transport metadata for the selected retained responses. A Landing row is one accepted response envelope containing its exact decoded payload and provenance, not one source observation or unique business fact.

## Portal acceptance

[PR #78](https://github.com/rutkala/zohelo-data/pull/78) merged as [`40844c3bd30aa84a3cec7b4e6fdb5a5995c05975`](https://github.com/rutkala/zohelo-data/commit/40844c3bd30aa84a3cec7b4e6fdb5a5995c05975). [Accepted CI 34223422928](https://github.com/rutkala/zohelo-data/actions/runs/34223422928) passed both deployment base paths (`./` and `/zohelo-data/`), production builds, **685 unit tests** and **14 browser flows per base path**. The mixed-source browser flow queried a real Parquet Landing fixture together with an NBP gold fixture, and checked the three-dot query action and source-access links. Earlier CI attempts exposed test-selector mistakes, which were corrected before the accepted run.

[Portal deployment 34223857331](https://github.com/rutkala/zohelo-data/actions/runs/34223857331) passed its build and GitHub Pages deployment at the merged producer above. A fresh browser visit to `https://data.zohelo.com/` confirmed the deployed Source access setup-guide and encrypted-secrets links. The cloud browser has no owner Google Drive session (its Google popup did not open); fixture-based portal SQL proof and fresh production Drive/Parquet proof are separate evidence. No authenticated owner-browser data query is claimed.

## Remaining boundary

All three provider summaries retain `coverage_status: incomplete`, and their collection queues remain open. These snapshots do not add new-source Bronze, Silver, Gold, semantic metrics, dbt business-catalogue claims or cross-source comparability. They do not establish completion of all researched sources, recoverable history, classifications, geographies, accounts or access opportunities. The existing NBP release and its current pointer remain separate.

See [ADR 0005](../decisions/0005-agile-landing-and-source-access.md), the [source campaign operations guide](../source-campaign-operations.md) and the [source expansion plan](../source-expansion-plan.md) for the accepted design and subsequent modeling gates.
