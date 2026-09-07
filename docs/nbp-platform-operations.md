# NBP platform operations

**Status: Feature branch implementation; end-to-end and live v2 proof pending.** The current live consumer remains on the validated v1 silver release. A v2 platform release is not claimed until the new workflow has completed its restore, query, and raw-replay checks.

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

The feature branch builds 15 datasets: four bronze source-aligned tables, four silver tables, `nbp_change_events`, `fact_fx_quotes`, `fact_gold_prices`, `dim_date`, `dim_currency`, `dim_source_table`, and `dim_commodity`. The release includes the four-source business catalogue and dbt ancestor lineage. Its metrics list is empty with `metrics_status=awaiting_business_approval`; no aggregation, return, spread, or other business metric is invented.

Catalogue dataset metadata contains business fields only: `dataset_id`, `table_name`, `layer`, `model_name`, `row_count`, `min_date`, `max_date`, `date_column`, and `columns`. Scratch paths and model IDs are excluded before the catalogue artifact is written.

## Safety bounds and limitations

The current runner bounds one invocation to 2,048 observation batches, 512 requests, 256 MiB of raw working data, 12 MB per response, and 45 minutes of intake. These are engineering bounds rather than service-level promises. A capped or failed run leaves verified ingestion progress and the previous validated release available for the next attempt.

Long-term growth of raw/state history, latency of the rotating historical recheck, and missing original legacy history that was never retained before migration remain explicit limitations. A latest-date observation does not prove complete historical coverage. Provider change events are observations of different returned values; they are not claims of an officially announced NBP correction.

Open decisions remain business metric definitions and approval, additional sources and their commercial reuse licences, and a future served-backend or availability requirement. The feature branch implementation does not close those decisions.
