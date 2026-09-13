# Google Drive Structure and Publication Guide

This guide explains the Google Drive storage layout for `zohelo-data` (`data.zohelo.com`), how current data is addressed, and why folders appear as they do.

> [!NOTE]
> **Verified 13 September 2026, 09:24 UTC:** Both current pointers passed manifest checksum, release identity/scope and referenced-file metadata checks. Release inventories completed without pagination or budget gaps. See the [dated evidence](audits/2026-09-13-drive-structure.json). This is a metadata audit, not a full data restore or semantic-validation result.

---

## 1. Owner FAQ: Understanding the Layout

### Why is there a separate `bdl-platform/` folder?
`src/bdl_platform.py:_release_root` deliberately routes BDL to `bdl-platform/releases/<release-id>/` and `bdl-platform/current-release.json`. NBP publishes under the platform root. The portal (`portal/src/services/googleDrive/releaseCatalog.ts`) reads both locations. Each source therefore has its own current pointer. The naming is inconsistent, but the separate BDL folder alone is not evidence of corruption.

### What do the `releases/` folders mean?
Each UUID folder is a publication attempt. A completed release packages its tables and catalogue artifacts together. The publisher uploads a new package, validates it, then updates the current pointer. Earlier packages remain available. Interrupted attempts can leave incomplete folders, so a folder's presence or modification date does not prove it is usable or current. Pointers and operational control files can change; published release contents are retained.

### Why does a release contain multiple medallion layers?
A release folder keeps its tables together, while `release.json` records each table's **logical layer**: Bronze for source-shaped records, Silver for typed data and revisions, and Gold for analytical tables. For example, `fact_fx_quotes` belongs to `04_gold`. These layer names describe the data's role; they do not require every current file to sit in the corresponding top-level Drive folder. The release ID ties the package together.

### Why do old releases exist?
The repository retention policy is `retain_all_no_automatic_deletion` (`src/capacity_report.py`). Prior releases are retained for analytical replay, historical verification against `code_sha`, and rollback support. Automated deletion or compaction is on hold (**H-CAP**) until safe pruning logic is reviewed and approved. Rollback requires validating target manifests before pointing; it is not instantaneous.

---

## 2. Physical Storage Map

The table below reconciles top-level paths with codebase producers and consumers:

| Path | Purpose | Primary Producer | Consumers | Status / Classification |
|---|---|---|---|---|
| [01_landing/](https://drive.google.com/drive/folders/18WYAR4yOf-fIRuXJe5IWs1K_L2f64YnM) | Exact raw HTTP responses and streamed bulk archives | `src/source_campaign.py`, `src/full_source_campaign.py`, `nbp_platform.py` | Ingestion loaders, raw replay | **Active** |
| [02_bronze/](https://drive.google.com/drive/folders/1iJYbUvwYbxFcpes7A6hTpgeU1TalPpTX) | Streaming Bronze entity Parquets (OpenData) and legacy NBP files | `src/ingestion/sources/opendata_bronze_loader.py`; legacy runners | Portal Lakehouse Explorer, dbt | **Active & Historical** |
| [03_silver/](https://drive.google.com/drive/folders/1JX698Sf4Nkqzmws-LU2BNQFwtqbeaK99) | Earlier NBP Silver outputs; three Table A/B/C subfolders with children last modified 31 August | Retired NBP transformation helpers | Historical data; current consumers follow releases | **Retained historical** |
| [04_gold/](https://drive.google.com/drive/folders/1NLIafFLNNj4b0_CgetQcTuX3bgIMRfVG) | Empty at inspection; current Gold tables are inside releases | Current publishers use release packages | Current consumers follow releases | **Empty physical folder** |
| [05_archive/](https://drive.google.com/drive/folders/1cLXi1sv17KXnkMpzYYXc0cR3upA879F9) | Retained historical lifecycle files and migration material | Maintenance scripts | Historical audit | **Retained Historical** |
| [06_control/](https://drive.google.com/drive/folders/1pH1bdJMn4_7Tb094DRfBMxEX5H2bewRs) | Source campaign state shards, quota ledgers, Landing pointers | `src/source_campaign.py`, `opendata_bronze_loader.py` | Campaign runners, portal | **Active** |
| [ingestion-control/](https://drive.google.com/drive/folders/1iulTAw9mzpi05fzLDfuGd-fVgJLi_cpq) | NBP state snapshots, attempts, and raw references | `src/nbp_platform.py` via `DriveStateStore` | NBP runner | **Active (NBP convention)** |
| [releases/](https://drive.google.com/drive/folders/1Usp7ZNNazESuPj3IOmTeBACFJ7MEaQDb) | NBP release directories; the current pointer is beside this folder at the platform root | `src/nbp_platform.py` via `release_protocol.py` | Portal, `restore_release.py` | **Active** |
| [bdl-platform/](https://drive.google.com/drive/folders/1bO2R04K72NBWMmOzORkZrUNnD1css2hg) | BDL publication namespace: release directories and pointer | `src/bdl_platform.py` via `release_protocol.py` | Portal release catalog | **Active (asymmetric root)** |
| `promotion-audits/` | Code-supported receipts path (not observed at root in baseline) | `scripts/promote_release.py` | Release audit | **Optional Code Path** |

---

## 3. How to Find Current Data

Always follow publication pointers rather than looking for the latest modified folder:

1. **Current NBP Platform Data:** Read root `current-release.json`. Its `manifest_file_id` points to `release.json` inside `releases/<release-id>/`, listing all 15 active tables and file IDs.
2. **Current GUS BDL Data:** Read `bdl-platform/current-release.json`. Its `manifest_file_id` points to `release.json` inside `bdl-platform/releases/<release-id>/`, listing active BDL tables.
3. **Current Landing Snapshots:** Read `06_control/source_campaigns/<source_id>/current-landing.json` for verified response envelopes under `01_landing/<source_id>/`.
4. **Current OpenData Bronze:** Read `06_control/source_campaigns/opendata_org_bronze/current-landing.json`, which points to entity Parquets in `02_bronze/opendata_org/`.

---

## 4. Verified Release Inventory

| Source | Release directories | Retained bytes | Current package bytes | Current tables |
|---|---:|---:|---:|---:|
| NBP | 11 | 113,074,252 | 12,174,348 | 15 |
| GUS BDL | 82 | 19,352,552,889 | 451,500,418 | 18 |

BDL retained release files use about **19.35 GB**; NBP uses **113.1 MB**. These totals cover release directories, not all Landing, Bronze or archive storage. All enumerated release files had known sizes. The earlier 79-directory BDL observation was a prior snapshot; ingestion continued during this work.

- [Current NBP package](https://drive.google.com/drive/folders/1EUzy-XJI7BjZz468ynZrlhCFJm3X23Mi): release `e8c025a4-a7c7-432a-84fa-8151c1479c98`, published 13 September at 02:16 UTC. Its 15 tables comprise four Bronze, five Silver and six Gold tables.
- [Current BDL package](https://drive.google.com/drive/folders/1u5NfnvX6fjD0vojW3pNt5somQz5tOeQ9): release `32eb0dde-d645-4f4d-93a1-3ddc747f97df`, published 13 September at 09:01 UTC. Its 18 tables comprise six tables in each layer.
- Every enumerated release directory contained `release.json`. The two current packages passed manifest and reference checks. The other 10 NBP and 81 BDL manifests were not full restore validations, and their prior promotion history is not established merely by their presence.
- Average gaps between observed folder creation times were 14.58 hours for NBP and 0.49 hours for BDL. BDL's workflow is scheduled every 15 minutes; serialized jobs and run duration determine actual cadence.
- No duplicate exact top-level folder names were found. `03_silver/` is not empty: it retains the three earlier NBP Table A/B/C paths. `04_gold/` was empty. `05_archive/` contains seven dated folders plus `opendata/`.
- A follow-up scan at 09:35 UTC compared all 82 BDL manifests from this inventory: **82 distinct declared dataset signatures**, with no identical complete bundles. The comparison uses dataset IDs and declared Parquet SHA-256/size values, excluding file IDs and catalogue/code differences. It does not prove that every change represents meaningful new business data. See [comparison evidence](audits/2026-09-13-bdl-signatures.json).
- Historical retention is intentional. Raw response counts alone are unsafe release change gates: dimensions, schema, corrections, code changes and explicit rebuilds also matter. Identical declared table hashes, if found, would require review before calling a retained release unnecessary.

The live audit inspected 2,485 unique file/folder metadata records in 188 counted requests and 69.1 seconds. Ingestion remained active, so this is a dated observation rather than an atomic snapshot. Dataset bytes were not downloaded or rehashed.

---

## 5. Minimal Correction Proposal for This Increment

1. **Explicit Documentation and Audit (Authorized Now):**
   - Keep the physical-versus-logical map and pointer mechanics explicit in repository documentation.
   - Retain the existing two publication namespaces (`releases/` and `bdl-platform/releases/`).
   - Use the read-only diagnostic (`src/drive_audit.py`, `scripts/audit_drive_structure.py`) to gather live evidence without modifying Drive.
2. **Future Structural Changes (Deferred Pending Lead Review):**
   - Any future migration (e.g. unifying BDL under `releases/bdl/` or moving `ingestion-control/` into `06_control/nbp/`) requires a full mapping of old/new IDs, updating all readers (`portal/src/services/googleDrive/releaseCatalog.ts`, `restore_release.py`) and writers (`bdl_platform.py`, `nbp_platform.py`), and a serialized GitHub Actions workflow with rollback support.
   - No speculative folder moves, permission changes, ingestion gating, or schedule pauses are executed in this checkpoint.

## 6. Repeat the Audit

Run from the repository with authorized Google Drive credentials. This command reads only metadata and small manifests; the outer timeout limits the whole process, while the diagnostic checks its request/file/time budgets between calls.

```bash
timeout --signal=TERM --kill-after=10s 240s python scripts/audit_drive_structure.py \
  --root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf \
  --max-files 15000 --max-requests 450 --max-seconds 180 \
  --output drive-audit.json
```

Read both the overall status and any incomplete reasons. A failure or partial inventory must not be interpreted as missing production data. Historical signature comparisons in the reusable diagnostic are sampled; complete data/restore validation remains a separate operation.
