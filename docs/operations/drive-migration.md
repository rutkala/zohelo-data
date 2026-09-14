# Google Drive Layout Migration Operational Runbook

This runbook defines the operational procedure for executing, resuming, verifying, or rolling back the physical Google Drive consolidation for `zohelo-data`.

> [!IMPORTANT]
> **Owner Approval (14 September 2026):** The owner approved the physical consolidation. Live cutover is awaiting verification. All production Drive mutations must be executed strictly through the reviewed GitHub Actions workflow on `main` after lead review.

---

## 1. Safety Principles & Preconditions

1. **Safety Pin:** All mutating operations require `--expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`.
2. **Read-Only Default:** `migrate_drive_layout.py` defaults to `--operation plan` (read-only dry-run). Mutating operations (`apply`, `resume`, `rollback`) fail closed unless `--confirm` is explicitly supplied.
3. **Concurrency Protection:** The migration workflow runs under the shared concurrency group:
   ```yaml
   concurrency:
     group: zohelo-production-data
     cancel-in-progress: false
     queue: max
   ```
4. **Pre-Cutover Drain & Compatibility Guard:**
   Before applying mutations, execute the automated cutover guard:
   ```bash
   python scripts/check_cutover_preconditions.py \
     --repo rutkala/zohelo-data \
     --portal-url https://rutkala.github.io/zohelo-data/portal-build.json
   ```
   This confirms:
   - The deployed portal site is compatible with release format 2 and direct releases.
   - No legacy publisher workflow runs (`source-gus-bdl`, `source-world-bank`, `daily-ingestion`) are active or queued in GitHub Actions.

---

## 2. Migration Execution Steps

### Step 1: Generate & Inspect Migration Plan
Generate the read-only migration plan and examine the planned steps and pins:
```bash
python scripts/migrate_drive_layout.py \
  --operation plan \
  --expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf
```
Receipt `plan.json` is generated locally containing:
- Pinned pointer identities, hashes, manifest identities, and NBP state snapshot references.
- Exact planned Drive API moves (`addParents`/`removeParents`).

### Step 2: Apply Migration
Execute migration with explicit confirmation:
```bash
python scripts/migrate_drive_layout.py \
  --operation apply \
  --confirm \
  --expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf \
  --plan-file plan.json
```
During execution:
- Pre-mutation drift checks verify all pins.
- Journal is written to `06_control/migration-journal.json`.
- Each step is executed atomically, verified via readback, and checkpointed in the journal.
- Post-migration validation verifies canonical layout mode, source release roots, and medallion navigation links.

### Step 3: Verify Canonical Layout
Inspect layout without mutations:
```bash
python scripts/migrate_drive_layout.py \
  --operation verify \
  --expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf
```

---

## 3. Failure & Recovery Procedures

### Resuming an Interrupted Migration
If a runner crashes or times out mid-migration, the durable journal in `06_control/migration-journal.json` persists the exact step progress.
To resume:
```bash
python scripts/migrate_drive_layout.py \
  --operation resume \
  --confirm \
  --expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf
```
The engine:
- Rehydrates destination folder mappings from the journal.
- Verifies drift (accounting for already-moved items).
- Idempotently verifies previously completed steps and executes remaining steps.

### Rolling Back to Legacy Layout
If rollback is required:
```bash
python scripts/migrate_drive_layout.py \
  --operation rollback \
  --confirm \
  --expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf
```
The engine:
- Reverses completed moves in reverse order using the durable journal.
- Restores original folder names and parents.
- Prunes newly created navigation shortcuts and empty migration folders.
- Verifies that legacy layout mode is restored.

---

## 4. Environment & Account Record

- **Personal Google AI Pro Account:** Verified, `useG1Credits: false`.
- **GitHub Copilot:** Blocked by insufficient credits.
- **Repository Permissions:** GHA workflow scoped to `contents: read`, `actions: read`.

---

## 5. Reviewed-plan provenance and recovery

Run plan through the main-branch **Migrate Google Drive layout** workflow. Review both
plan.json and plan-identity.json from its migration-plan artifact. Every apply,
resume, or rollback dispatch must use the same decimal plan_run_id and the exact
64-character plan_sha256 from that artifact.

Before credentials are used, the workflow verifies that the referenced run belongs to this
repository, used .github/workflows/migrate-drive-layout.yml, was a workflow_dispatch on
main, completed successfully, and has the same commit as both the artifact and the executing
workflow. The plan ID, root pins, and canonical plan digest must also match. A failed or stale
run cannot authorize a mutation.

The durable journal remains in 06_control and retains the original reviewed plan hash.
Resume and rollback reject a different hash. Download migration-receipts after every run;
it includes dispatch, plan-provenance, cutover-precondition, operation, error, verification,
and local journal receipts that were produced.

Navigation indexes use pending before any release pointer update. A successful reconciliation
checks every expected shortcut target and the complete multipart subtree, then records
current_verified. Retrying a publisher repairs pending navigation before changing the pointer.
Foreign navigation children stop reconciliation and are preserved.
