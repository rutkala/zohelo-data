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
   - **Root-Only Recoverable Drive Trash:** Irreversible permanent `.delete()` is strictly forbidden. The reset targets ONLY the 7 known non-overlapping BDL root folders (`01_landing/gus_bdl`, `06_control/source_campaigns/gus_bdl`, `releases/bdl`, `05_archive/bdl-platform` if present, and `current/bdl` under `02_bronze`, `03_silver`, `04_gold`). Google Drive v3 inherits `trashed=true` down the entire 36,329-object subtree, while `explicitlyTrashed=true` distinguishes roots.
   - **30-Day Google Drive Trash Retention Policy:** Per official Drive v3 documentation, trashed items are permanently purged by Google after 30 days. Subtree recovery is possible within this 30-day window by setting `trashed=false` on the 7 roots. Beyond 30 days, Drive permanently removes the items; no retention guarantee exists after 30 days without an external backup.
   - **Exact Discovery and Ambiguity Rejection:** Targets are discovered strictly from known BDL roots. Discovery fails closed on duplicate folders, wrong root MIME, foreign assets, foreign appProperties, or shortcuts targeting foreign sources. Broad substring matching is prohibited.
   - **Preservation of Non-BDL Assets:** Non-BDL sources (NBP, WDI, Eurostat, OpenData) and shared infrastructure are baselined and verified (including SHA-256 hashes of key release pointers and navigation indexes) before and after apply.
   - **Durable Quota Retention & Clean Fresh Ledger:** External GUS BDL API quotas cannot be reset. Durable quota attempt timestamps (15m, 12h, 7d windows) and cooldown evidence from `06_control/source_campaigns/gus_bdl` are extracted via a read-only object-store adapter and `_CampaignStore.load()`, and saved in `06_control/bdl_resets/<plan_id>/retained-quota-ledger.json` outside the wiped scope with `coverage_status='awaiting_web_bulk'`, `gate='awaiting_web_bulk'`, and zero prior tasks/rows.
   - **Write-Ahead Remote Journaling & Drift Checks:** Plan is read-only with immutable SHA-256 digest. Immutable plan, identity, and journal are saved under `06_control/bdl_resets/<plan_id>/` before the first mutation. Subtree re-enumeration guarantees zero drift immediately before each root mutation. Each root records write-ahead intent and verified completion, supporting idempotent resume.

3. **Operational Boundary:**
   - This task implements the reset prerequisite and tooling only.
   - Verified current production coverage is 41 / 172,576 variables (0.02376%) and 4,331,822 observations from 3,862 inputs in release `b0e1a4b6-a9bc-4d63-9ff0-38e43fe0aea7` vs 3,996 Landing responses at 18:39 UTC (run 34879372531 cancelled at 25 min timeout with bulk skipped).
   - Automatic approval review blocked disabling `source-gus-bdl.yml` and cancelling queued run `34883169715`. Neither was disabled or cancelled by the assistant; lead will handle reviewed live execution.
   - Overall delivery goal remains incomplete until full historical web bulk collection and incremental ingestion are evidenced.
