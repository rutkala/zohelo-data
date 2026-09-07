# NBP platform operations

**Status: The v2 data release has verified live proof dated 7 September 2026.** PR57 remains merged on `main`; the repaired release passed coverage, fresh-process restore/query, raw replay, migration preservation, and portal deployment checks. The full platform remains incomplete because metrics are still `[]` awaiting business approval and no native query backend is available. The earlier v1 silver release is retained as a historical baseline. See the [verified v2 release evidence](releases/2026-09-07-nbp-platform.md).

## Verified live release

Release `5f356b1b-97cd-470d-9d5e-b46ebb37a7d1` was produced by code `f071669937829a3d0775a13146b3170e1b283b4e`. The [NBP data platform run](https://github.com/rutkala/zohelo-data/actions/runs/34096483209) passed at 08:45:24 UTC on 7 September 2026 with 15 physical tables, 52 dbt tests (67 nodes passed), fresh SQL reads across all 15 tables, and exact raw replay of all 15 tables with 1,314,774 rows matched. Sources were checked through 6 September; the latest observed dates are 4 September for Tables A, C and gold, and 2 September for Table B. The release counts are A 198,842, B 143,767, C 83,737, and gold 3,449.

The [migration check](https://github.com/rutkala/zohelo-data/actions/runs/34098420713) passed at 08:46:16 UTC. All 429,611 previously published four-silver keys were retained with zero missing keys; the current silver total is 429,795, an increase of 184. The [portal deployment](https://github.com/rutkala/zohelo-data/actions/runs/34096483163) also passed for producer code `f071669937829a3d0775a13146b3170e1b283b4e` and supports release formats 1 and 2. These checks establish live data and portal availability; they do not establish approved semantics or a native query service.

## Entry point and workflow

Run the platform from the repository root:

```bash
python src/nbp_platform.py --mode incremental
python src/nbp_platform.py --mode full
python src/nbp_platform.py --mode rebuild
```

`incremental` fills missing intervals, rechecks the most recent 93 days, and performs one rotating historical chunk per source and run. `full` uses the same resumable planner while bootstrapping missing historical coverage. It does not force a complete historical refresh in one invocation. `rebuild` performs a native cold replay from retained raw responses only and requires verified coverage through the selected cutoff; it does not call the NBP API.

The scheduled and manual production entrypoint is [`daily-ingestion.yml`](../.github/workflows/daily-ingestion.yml), named **NBP data platform**. It runs at 02:00 UTC and exposes `incremental`, `full`, and `rebuild` as manual choices. A push on `main` runs only with the explicit commit marker `[run-nbp-platform]`. The former separate bronze, silver, and historical-backfill workflows are removed. The workflow serializes production writers and keeps a failed publication from replacing the prior validated release.

Before publication, the runner checks the deployed portal’s `portal-build.json` and requires support for release formats 1 and 2. The portal build writes that marker in [`deploy-portal.yml`](../.github/workflows/deploy-portal.yml).

Outside GitHub Actions, the runner still requires an explicitly selected Drive root. For a development root, set `ZOHELO_DRIVE_ROOT_NAME` or `ZOHELO_DRIVE_ROOT_ID`. Writes to the production root named `zohelo-data` require `ZOHELO_ALLOW_PRODUCTION_WRITES=true`; the code rejects an unapproved outside-Actions write. Google OAuth client and refresh-token variables must be present in the selected runtime. No manual file staging or path-based publication is part of this entrypoint.

## Durable state and release contents

Each successful response is retained as immutable raw bytes with its Drive file ID and SHA-256. Append-only attempts, immutable state snapshots, and the current-state pointer record request progress and provenance. Pointer updates use readback and drift detection; Drive does not provide compare-and-swap here. Raw and state history has no garbage collection, so retention growth must be measured before a long-running deployment.

The verified v2 release defines 15 datasets: four bronze source-aligned tables, four silver tables, `nbp_change_events`, `fact_fx_quotes`, `fact_gold_prices`, `dim_date`, `dim_currency`, `dim_source_table`, and `dim_commodity`. The release includes the four-source business catalogue and dbt ancestor lineage. Its metrics list is empty with `metrics_status=awaiting_business_approval`; no aggregation, return, spread, or other business metric is invented. Production NBP MetricFlow definitions/execution and a served query backend remain open work; native runtime compatibility already has a passing synthetic fixture.

Catalogue dataset metadata contains business fields only: `dataset_id`, `table_name`, `layer`, `model_name`, `row_count`, `min_date`, `max_date`, `date_column`, and `columns`. Scratch paths and model IDs are excluded before the catalogue artifact is written.

## Safety bounds and limitations

The current runner bounds one invocation to 2,048 observation batches total across the four sources plus catch-up, 512 requests, 256 MiB of raw working data, 12 MB per response, and 60 minutes of intake. These are engineering bounds rather than service-level promises. A capped or failed run leaves verified ingestion progress and the previous validated release available for the next attempt. GitHub documents standard Actions runner minutes as free for public repositories in its [product billing guidance](https://docs.github.com/en/billing/concepts/product-billing/github-actions); Drive service limits are documented in the [Drive API quota policy](https://developers.google.com/workspace/drive/api/guides/limits). Account-specific quotas and billing were not inspected, and no free-unlimited promise is made.

Long-term growth of raw/state history, latency of the rotating historical recheck, and missing original legacy history that was never retained before migration remain explicit limitations. A latest-date observation does not prove complete historical coverage. Provider change events are observations of different returned values; they are not claims of an officially announced NBP correction.

Open decisions remain business metric definitions and approval, additional sources and their commercial reuse licences, and a future served-backend or availability requirement. The merged implementation does not close those decisions.

## Preservation of previously published history

Raw replay proves that a new release can be rebuilt from its recorded inputs. The historical v1 migration baseline is release `1ab2f2f0-4325-42fc-bc92-cf3d9e9d9eea`, produced from code `474bbb61a2bb9d88266808e872f8a7613aca23d6` on 7 September 2026. The read-only migration check against that baseline passed for the verified v2 release; its exact result is recorded above and in the [release evidence](releases/2026-09-07-nbp-platform.md).

The [migration workflow](../.github/workflows/nbp-migration-check.yml) runs manually or after a relevant reviewed merge marked `[verify-nbp-migration]`. It shares production concurrency so it waits for publication and recovery checks. It reads the immutable baseline by release ID, verifies both releases, and checks that every previously published date/currency key (date for gold prices) still exists. It also rejects a regression in the latest observation date. Value changes are allowed under the accepted correction policy. The check writes no Drive objects and does not publish data; a failed check is an acceptance failure, not an automatic rollback.

```bash
python scripts/check_nbp_migration.py \
  --baseline-release-id 1ab2f2f0-4325-42fc-bc92-cf3d9e9d9eea \
  --baseline-code-sha 474bbb61a2bb9d88266808e872f8a7613aca23d6
```

The result records the compared release identities and per-table missing-key counts. A pass covers the previously published key set; it does not establish that NBP's historical API contains every original version or that no source-side revision occurred.
