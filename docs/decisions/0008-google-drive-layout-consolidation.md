# Google Drive physical layout consolidation and canonical medallion hierarchy

Status: Approved, implemented and independently verified on 14 September 2026. Delivery evidence belongs in [the canonical delivery record](../deliverables.md) and [verification receipt](../releases/2026-09-14-drive-layout.json).

## Context

Prior to this consolidation, the Google Drive production root presented a heterogeneous, confusing layout:
- Numbered medallion directories (`01_landing`, `02_bronze`, `03_silver`, `04_gold`, `05_archive`) existed alongside provider platform wrappers (`bdl-platform`, `wdi-platform`), `ingestion-control`, and `releases`.
- NBP release directories lived directly under `/releases/<uuid>`, with a mutable `current-release.json` pointer at the Drive root that appeared to be a platform-wide release rather than an NBP-specific pointer.
- BDL and WDI releases were nested under `bdl-platform/releases/<uuid>` and `wdi-platform/releases/<uuid>` with their own `current-release.json` pointers inside their wrapper folders.
- Ingestion control state for NBP lived in `ingestion-control/`, while provider campaign states were organized under `06_control/source_campaigns/`.

## Decision

Consolidate Google Drive into a single, consistent, physical architecture:

1. **Canonical Direct Releases Layout:**
   - `releases/nbp/`, `releases/bdl/`, `releases/wdi/`: each source contains its own `current-release.json` pointer and immutable UUID release directories directly.
   - Avoid an unnecessary nested `releases/releases/` level.
   - Preserves Parquet bytes, file IDs, checksums, and folder identities via individual Drive API metadata moves (`addParents`/`removeParents`). The full migration is journaled and is not an atomic transaction across Drive objects.

2. **Unified Control Root (`06_control/`):**
   - NBP ingestion control is moved and renamed from `ingestion-control` to `06_control/nbp/`.
   - Provider campaigns remain preserved at `06_control/source_campaigns/` with exact identities.

3. **Medallion Layer Navigation (`02_bronze`, `03_silver`, `04_gold`):**
   - Maintained under `current/<source_id>/` in each medallion layer using non-authoritative Drive shortcuts pointing directly to current release Parquet files, accompanied by `navigation-index.json`.
   - Supports single-file tables (`<table_name>.parquet` shortcut) and multi-file datasets (e.g. WDI multi-part datasets grouped in `<table_name>/<part>.parquet` shortcuts).
   - Pruning reconciles exact manifest membership across all 3 layers, trashing obsolete shortcuts and preserving non-shortcut user files.

4. **Reversible Wrapper Archival (`05_archive/`):**
   - Empty legacy wrapper folders `bdl-platform` and `wdi-platform` are reversibly moved to `05_archive/`.

5. **Migration Safety & Operational Invariants:**
   - Migration CLI defaults to bounded read-only plan (`--operation plan`).
   - Mutating operations (`apply`, `resume`, `rollback`) run through the reviewed main workflow with confirmation, the expected root `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`, compatibility commit, successful plan run ID, plan ID and plan SHA-256. The executing commit must match the plan commit; see the [operational runbook](../operations/drive-migration.md).
   - Pre-mutation drift checks pin pointer identities, file hashes, manifests, and NBP state before any changes.
   - All production data jobs (NBP, reconciliation, migration, and modeled BDL/WDI publishers) are bound to the shared concurrency group `zohelo-production-data` with `cancel-in-progress: false` and `queue: max`.

## Implementation-stage Tooling and Accounts Record

The following records the initial implementation stage. Subsequent CLI authentication, quota checks and AGY documentation delivery are recorded in [deliverables](../deliverables.md#delegation-policy-agreed--14-september-2026).

- Personal Google AI Pro account verified (`useG1Credits: false`).
- GitHub Copilot startup blocked by insufficient credits; AGY completed the initial Antigravity pass and implementation ownership then transferred to the internal fallback for review corrections and validation.
