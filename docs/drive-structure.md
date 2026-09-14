# Google Drive Structure and Canonical Publication Guide

This guide explains the Google Drive storage layout for `zohelo-data` (`data.zohelo.com`), how current data is addressed, how sources publish packages, and how the canonical medallion layout operates.

> [!NOTE]
> **Owner Approval 14 September 2026:** Physical consolidation approved. Live cutover is awaiting verification. All retained release packages, manifests, data bytes, and folder identities are strictly preserved.

---

## 1. Canonical Layout Architecture

The consolidated physical layout provides a consistent, transparent structure across all sources:

```
zohelo-data/ (Drive root: 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf)
├── 01_landing/
│   └── <source_id>/                     # Immutable raw API responses and bulk archives
├── 02_bronze/
│   └── current/<source_id>/             # Shortcuts to current Bronze Parquet files + navigation-index.json
├── 03_silver/
│   └── current/<source_id>/             # Shortcuts to current Silver Parquet files + navigation-index.json
├── 04_gold/
│   └── current/<source_id>/             # Shortcuts to current Gold Parquet files + navigation-index.json
├── 05_archive/                          # Retained historical lifecycle files and archived legacy wrappers
│   ├── bdl-platform/                    # Archived legacy BDL wrapper
│   └── wdi-platform/                    # Archived legacy WDI wrapper
├── 06_control/                          # Unified operational control root
│   ├── migration-journal.json           # Durable consolidation journal
│   ├── nbp/                             # NBP ingestion control: attempts/, states/, current-ingestion-state.json
│   └── source_campaigns/                # Ingestion campaign state shards (GUS BDL, World Bank, OpenData, etc.)
└── releases/                            # Unified publication root
    ├── nbp/                             # NBP source releases
    │   ├── current-release.json         # Current NBP release pointer
    │   └── <uuid>/                      # Immutable NBP release packages (release.json + tables)
    ├── bdl/                             # GUS BDL source releases
    │   ├── current-release.json         # Current BDL release pointer
    │   └── <uuid>/                      # Immutable BDL release packages (release.json + tables)
    └── wdi/                             # World Bank WDI source releases
        ├── current-release.json         # Current WDI release pointer
        └── <uuid>/                      # Immutable WDI release packages (release.json + tables)
```

---

## 2. Source Release Architecture

Each source (`nbp`, `bdl`, `wdi`) maintains an identical direct structure under `releases/<source>/`:
- **Pointer:** `releases/<source>/current-release.json` contains `format_version: 2`, `release_id: "<uuid>"`, and `manifest_file_id: "<id>"`.
- **Release Directory:** `releases/<source>/<uuid>/` contains:
  - `release.json`: Complete release manifest declaring datasets, schemas, logical medallion layer, row counts, and Parquet file checksums.
  - Parquet data files: Partitioned or table Parquet files.
  - Optional dbt artifacts: `manifest.json`, `catalog.json`, `run_results.json`.

There is no nested `releases/releases/` level.

### Multi-File Datasets (e.g. World Bank WDI)
For large or partitioned datasets such as World Bank WDI observations:
- In `releases/wdi/<uuid>/`, files are stored with part naming (e.g., `wdi_observations--part-0.parquet`, `wdi_observations--part-1.parquet`).
- In `release.json`, the dataset entry contains multiple items in its `files` array.
- In medallion navigation (`03_silver/current/wdi/`):
  - A directory `wdi_observations/` is maintained.
  - Non-authoritative Drive shortcuts point to each individual part file.
  - `navigation-index.json` indexes all part shortcuts, total file count, and byte sizes.

---

## 3. Medallion Layer Navigation

To allow data consumers and exploratory tools to navigate by medallion tier without copying Parquet bytes:
- Each medallion folder (`02_bronze/`, `03_silver/`, `04_gold/`) contains a `current/<source_id>/` directory.
- Single-file tables: Direct Drive shortcut named `<table_name>.parquet` pointing to the Parquet file in the current release.
- Multi-file datasets: Subdirectory `<table_name>/` containing shortcuts for each part file.
- `navigation-index.json`: Drive-native index document updated atomically with release promotion, recording verified target IDs and checksums.
- **Reconciliation and Pruning:** When a release advances, obsolete shortcuts from dropped tables or part changes are cleanly removed; user or non-shortcut files outside managed directories are never touched.

---

## 4. Operational Invariants and Concurrency

1. **Shared Concurrency Group:**
   All mutating production data operations (NBP daily ingestion/publish, BDL transform/release, WDI transform/release, Drive reconciliation, Drive migration) are bound to the shared GitHub Actions concurrency group:
   ```yaml
   concurrency:
     group: zohelo-production-data
     cancel-in-progress: false
     queue: max
   ```
   This prevents concurrent write conflicts on Google Drive while queuing pending scheduled runs.

2. **Migration Operations:**
   - **Plan (Dry Run):** `python scripts/migrate_drive_layout.py --operation plan`
   - **Apply:** `python scripts/migrate_drive_layout.py --operation apply --confirm --expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`
   - **Resume:** `python scripts/migrate_drive_layout.py --operation resume --confirm --expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`
   - **Rollback:** `python scripts/migrate_drive_layout.py --operation rollback --confirm --expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`
   - **Verify:** `python scripts/migrate_drive_layout.py --operation verify`
