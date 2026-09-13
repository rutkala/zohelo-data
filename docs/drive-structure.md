# Google Drive Structure and Publication Guide

This guide explains the Google Drive storage layout for `zohelo-data` (`data.zohelo.com`), how current data is addressed, and why folders appear as they do.

> [!NOTE]
> **Dated Baseline Evidence (13 September 2026):** Structural counts (11 NBP and 79 BDL release folders) and one observed BDL release size (435,547,793 bytes across 26 files) are dated baseline observations from commit `0f68ad2`. They do not represent every release size or verified waste. Live checksum and metadata validation remain pending external controller execution.

---

## 1. Owner FAQ: Understanding the Layout

### Why is there a separate `bdl-platform/` folder?
When GUS BDL modeled releases were implemented, `src/bdl_platform.py:_release_root` routed BDL to `bdl-platform/releases/<release-id>/` and `bdl-platform/current-release.json`, while NBP was already publishing directly at the platform root. The portal (`portal/src/services/googleDrive/releaseCatalog.ts`) explicitly reads both roots. This is implemented code behavior, not storage corruption, though it creates a divergent namespace.

### What do the `releases/` folders mean?
Each folder named with a UUID under `releases/` and `bdl-platform/releases/` is an **immutable release package**. The platform does not overwrite remote files in place. Instead, each completed publication job uploads all Parquet tables and dbt artifacts into a dedicated release folder, then updates a pointer. This avoids partial-read windows for active portal queries and preserves an auditable history.

### Why does a release contain multiple medallion layers?
A release folder physically co-locates all tables for that release, but each table is tagged with a **logical layer** in `release.json` (e.g. `fact_fx_quotes` is `04_gold`, `stg_nbp_table_a` is `03_silver`, `br_nbp_table_a` is `02_bronze`). Packaging them together guarantees atomic consistency across layers from the same pipeline run.

### Why do old releases exist?
The repository retention policy is `retain_all_no_automatic_deletion` (`src/capacity_report.py`). Prior releases are retained for analytical replay, historical verification against `code_sha`, and rollback support. Automated deletion or compaction is on hold (**H-CAP**) until safe pruning logic is reviewed and approved. Rollback requires validating target manifests before pointing; it is not instantaneous.

---

## 2. Physical Storage Map

The table below reconciles top-level paths with codebase producers and consumers:

| Path | Purpose | Primary Producer | Consumers | Status / Classification |
|---|---|---|---|---|
| `01_landing/` | Exact raw HTTP responses and streamed bulk archives | `src/source_campaign.py`, `src/full_source_campaign.py`, `nbp_platform.py` | Ingestion loaders, raw replay | **Active** |
| `02_bronze/` | Streaming Bronze entity Parquets (OpenData) and legacy NBP files | `src/ingestion/sources/opendata_bronze_loader.py`; legacy runners | Portal Lakehouse Explorer, dbt | **Active & Historical** |
| `03_silver/` | Historical prototype path; active silver resides in releases | Retired prototype scripts | Unresolved until live audit | **Unresolved Historical** |
| `04_gold/` | Historical prototype path; active gold resides in releases | Retired prototype scripts | Unresolved until live audit | **Unresolved Historical** |
| `05_archive/` | Retained historical lifecycle files and migration material | Maintenance scripts | Historical audit | **Retained Historical** |
| `06_control/` | Source campaign state shards, quota ledgers, Landing pointers | `src/source_campaign.py`, `opendata_bronze_loader.py` | Campaign runners, portal | **Active** |
| `ingestion-control/` | NBP state snapshots, attempts, and raw references | `src/nbp_platform.py` via `DriveStateStore` | NBP runner | **Active (NBP convention)** |
| `releases/` | NBP publication root: immutable release directories and pointer | `src/nbp_platform.py` via `release_protocol.py` | Portal, `restore_release.py` | **Active** |
| `bdl-platform/` | BDL publication namespace: release directories and pointer | `src/bdl_platform.py` via `release_protocol.py` | Portal release catalog | **Active (asymmetric root)** |
| `promotion-audits/` | Code-supported receipts path (not observed at root in baseline) | `scripts/promote_release.py` | Release audit | **Optional Code Path** |

---

## 3. How to Find Current Data

Always follow publication pointers rather than looking for the latest modified folder:

1. **Current NBP Platform Data:** Read root `current-release.json`. Its `manifest_file_id` points to `release.json` inside `releases/<release-id>/`, listing all 15 active tables and file IDs.
2. **Current GUS BDL Data:** Read `bdl-platform/current-release.json`. Its `manifest_file_id` points to `release.json` inside `bdl-platform/releases/<release-id>/`, listing active BDL tables.
3. **Current Landing Snapshots:** Read `06_control/source_campaigns/<source_id>/current-landing.json` for verified response envelopes under `01_landing/<source_id>/`.
4. **Current OpenData Bronze:** Read `06_control/source_campaigns/opendata_org_bronze/current-landing.json`, which points to entity Parquets in `02_bronze/opendata_org/`.

---

## 4. BDL Cadence and Release Directory Observations

- **Baseline Observation:** 79 directories were observed in `bdl-platform/releases/`, compared to 11 in NBP `releases/`. BDL runs every 15 minutes (`source-gus-bdl.yml`).
- **Release Categories:** The audit diagnostic classifies release directories into:
  - `current_manifest_and_metadata_verified`: Active release verified against current pointer and child metadata.
  - `manifest_present_unverified`: Retained historical folders containing `release.json`.
  - `no_manifest_candidate`: Incomplete candidate folders lacking `release.json` (aborted or interrupted runs).
- **Retention vs. Waste:** Retaining historical releases is deliberate under `retain_all_no_automatic_deletion`. Unknown promotion history must not be labeled verified waste.
- **Ingestion Gating Caution:** Raw envelope counts alone are unsafe as release change gates; metadata, dimensions, schema updates, corrections, code changes, and explicit rebuilds also trigger valid releases.

---

## 5. Minimal Correction Proposal for This Increment

1. **Explicit Documentation and Audit (Authorized Now):**
   - Keep the physical-versus-logical map and pointer mechanics explicit in repository documentation.
   - Retain the existing two publication namespaces (`releases/` and `bdl-platform/releases/`).
   - Use the read-only diagnostic (`src/drive_audit.py`, `scripts/audit_drive_structure.py`) to gather live evidence without modifying Drive.
2. **Future Structural Changes (Deferred Pending Lead Review):**
   - Any future migration (e.g. unifying BDL under `releases/bdl/` or moving `ingestion-control/` into `06_control/nbp/`) requires a full mapping of old/new IDs, updating all readers (`portal/src/services/googleDrive/releaseCatalog.ts`, `restore_release.py`) and writers (`bdl_platform.py`, `nbp_platform.py`), and a serialized GitHub Actions workflow with rollback support.
   - No speculative folder moves, permission changes, ingestion gating, or schedule pauses are executed in this checkpoint.
