# GUS BDL web-only historical bulk extraction followed by incremental common tables

Status: Accepted on 14 September 2026. BDL reset prerequisite prepared under branch `agy/bdl-reset-20260914`; live production reset operations remain blocked pending operational approval. Delivery and verification status belongs in [the delivery record](../deliverables.md).

## Context

GUS Bank Danych Lokalnych (BDL) provides Polish regional and national statistical indicators across 172,576 variables.
Collection under the REST API (`source-gus-bdl.yml`) is rate-limited (400 requests/15m under the registered key) and modeled only 41 variables across 4,331,822 modeled observations in release `b0e1a4b6-a9bc-4d63-9ff0-38e43fe0aea7` (with latest Landing at 3,996 responses). REST API pagination cannot deliver full historical coverage across the entire catalogue in reasonable time, and starter samples are not interchangeable with full coverage.

The official BDL Web UI portal allows exporting full subgroup historical distributions as compressed bulk archives.

The user objective mandates:
1. Complete removal of all old BDL data from all layers (`01_landing`, `06_control`, `releases/bdl`, `05_archive/bdl-platform`, and medallion navigation under `02_bronze`, `03_silver`, `04_gold`).
2. Subsequent ingestion of all historical data via BDL Web UI ONLY.
3. Subsequent streaming of API increments into the SAME common medallion tables.

## Decision

1. **Two-Stage Ingestion Model for BDL:**
   - **Historical Baseline via Authenticated Web Bulk ONLY:** All historical BDL observations across admitted subgroups must be extracted via the BDL Web UI bulk export mechanism, landed as immutable raw archives, and flattened into the canonical medallion schemas.
   - **Incremental Refresh via REST API:** Once historical bulk baseline is established for a subgroup, subsequent ongoing updates and current-year increments will be collected via the BDL REST API.
   - **Unified Common Tables:** Both historical web bulk and API increments must populate the *exact same* medallion relations (`br_bdl_*`, `stg_bdl_*`, `dim_bdl_*`, `fact_bdl_*`, `mart_bdl_*`) with shared grains, schema contracts, and coverage metrics.

2. **Safe, Reviewable BDL-Only Reset Mechanism:**
   - **Recoverable Drive Trash Only:** Irreversible permanent `.delete()` is strictly forbidden. All removed objects are moved to Google Drive trash (`trashed: true`), preserving 30-day recovery windows.
   - **Exact Discovery and Ambiguity Rejection:** Targets are discovered strictly from known BDL roots:
     - `01_landing/gus_bdl`
     - `06_control/source_campaigns/gus_bdl`
     - `releases/bdl`
     - `05_archive/bdl-platform` (if present)
     - Root `bdl-platform` (legacy wrapper if present)
     - Medallion navigation under `02_bronze`, `03_silver`, `04_gold` (`current/bdl` and `current/gus_bdl`)
     Discovery fails closed on duplicate folders, foreign assets, or mixed contents. Broad substring matching is prohibited.
   - **Preservation of Non-BDL Assets:** Non-BDL sources (NBP, WDI, Eurostat, OpenData) and shared infrastructure are baselined and verified before and after apply.
   - **Durable Quota Retention:** External GUS BDL API quotas cannot be reset. Durable quota attempt timestamps and cooldown evidence from `06_control/source_campaigns/gus_bdl` are extracted and retained outside the wiped scope so future API operations do not trigger provider rate violations.
   - **Durable Journaling & Drift Checks:** Plan is read-only with immutable SHA-256 digest. Apply verifies target drift, main Actions runtime, serialized `zohelo-production-data` concurrency, requires disabled producer workflow with zero active runs, and supports idempotent resume.

3. **Operational Boundary:**
   - This task implements the reset prerequisite and tooling only.
   - Automatic approval review blocked disabling `source-gus-bdl.yml` and cancelling queued run `34883169715`. Lead will handle reviewed live execution.
   - Overall delivery goal remains incomplete until full historical web bulk collection and incremental ingestion are evidenced.
