# Google Drive Layout Migration Operational Runbook

> **Historical workflow, not a current launch instruction:** `migrate-drive-layout.yml` was retired on 15 September 2026 after the accepted cutover. Preserve this completed-operation runbook and all plan/journal/provenance safeguards. Any new mutation requires a reviewed, available execution procedure first; do not bypass the safeguards or rerun the old plan from newer code.

This runbook defines the operational procedure for executing, resuming, verifying, or rolling back the physical Google Drive consolidation for `zohelo-data`.

> [!IMPORTANT]
> **Completed and verified (14 September 2026):** The owner-approved physical consolidation completed in [apply run 34871823795](https://github.com/rutkala/zohelo-data/actions/runs/34871823795). Independent preservation and navigation checks passed at 17:21 UTC. See the [delivery record](../deliverables.md) and [verification receipt](../releases/2026-09-14-drive-layout.json). Production Drive mutations use the reviewed GitHub Actions workflow on `main`.

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
   Before applying mutations, the workflow executes the automated cutover guard:
   ```bash
   python scripts/check_cutover_preconditions.py \
     --repo rutkala/zohelo-data \
     --portal-url https://data.zohelo.com/portal-build.json \
     --compatibility-commit <40-char-commit-sha>
   ```
   This confirms:
   - The deployed portal site (`https://data.zohelo.com/portal-build.json`) advertises canonical release roots (`canonical-release-roots-v1`) and runs the reviewed compatibility commit or a verified descendant. The receipt also records its supported release formats.
   - Active publisher workflows (`source-gus-bdl.yml`, `source-world-bank.yml`, `daily-ingestion.yml`, `deploy.yml`) are inspected: compatible current publishers run under the shared concurrency group and are serialized; old or unverified publishers block cutover until drained.
5. **Existing Services & Secret Protections:** Use the existing GitHub Actions and Drive configuration. Do not enable additional paid services or model credit overages, and never echo credential values to logs.

---

## 2. Production Actions Workflow Procedure

Production Drive mutations are performed exclusively through the main-branch GitHub Actions workflow: **Migrate Google Drive layout** (`.github/workflows/migrate-drive-layout.yml`). Never execute unreviewed local mutations or force-move `main`.

### Operation Input Requirements

| Input Parameter | `plan` | `verify` | `apply` | `resume` | `rollback` |
| --- | --- | --- | --- | --- | --- |
| `operation` | `plan` | `verify` | `apply` | `resume` | `rollback` |
| `expected_root_id` | `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf` | `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf` | `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf` | `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf` | `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf` |
| `confirm` | `false` | `false` | `true` | `true` | `true` |
| `compatibility_commit` | *(optional)* | *(optional)* | Required (40-char SHA) | Required (40-char SHA) | Required (40-char SHA) |
| `plan_run_id` | *(optional)* | *(optional)* | Required (positive integer) | Required (positive integer) | Required (positive integer) |
| `plan_id` | *(optional)* | *(optional)* | Required (`plan-...`) | Required (`plan-...`) | Required (`plan-...`) |
| `plan_sha256` | *(optional)* | *(optional)* | Required (64-char hex) | Required (64-char hex) | Required (64-char hex) |

### Step 1: Run Read-Only Plan Workflow
1. Navigate to **Actions** → **Migrate Google Drive layout**.
2. Click **Run workflow** on `main`:
   - `operation`: `plan`
   - `expected_root_id`: `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`
   - `confirm`: `false`
3. When the workflow completes successfully, inspect the summary and download the `migration-plan` artifact:
   - `plan.json`: Contains planned Drive operations, pins, and canonical digest.
   - `plan-identity.json`: Contains `format_version`, `repository`, `workflow_run_id`, `workflow_commit`, `plan_id`, and `plan_sha256`.
4. Note the workflow run ID (`plan_run_id`), `plan_id`, and canonical `plan_sha256`.

### Step 2: Apply Migration
1. Ensure the executing commit on `main` is unchanged from the plan run. If `main` has advanced, old plan artifacts cannot be executed; a new plan must be generated on the current commit.
2. Click **Run workflow** on `main`:
   - `operation`: `apply`
   - `expected_root_id`: `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`
   - `confirm`: `true`
   - `compatibility_commit`: Reviewed 40-character commit SHA (e.g. `f05e22da1c876d51ccce4380ac03efee6cbe2765`)
   - `plan_run_id`: Run ID of the successful plan workflow run
   - `plan_id`: Value from `plan-identity.json`
   - `plan_sha256`: Value from `plan-identity.json`
3. The workflow validates dispatch parameters, downloads and verifies plan provenance and cutover preconditions, executes moves idempotently, and publishes `migration-receipts`.

### Step 3: Verify Canonical Layout
1. Click **Run workflow** on `main`:
   - `operation`: `verify`
   - `expected_root_id`: `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`
   - `confirm`: `false`
2. Download and inspect `verification.json` from `migration-receipts` to verify that all source release roots and medallion shortcut structures are sound.

---

## 3. Failure & Recovery Procedures

Google Drive does not offer atomic multi-resource transactions. Changes are sequenced and tracked via a durable journal in `06_control/migration-journal.json`.

### Resuming an Interrupted Migration
If a runner times out or fails mid-migration:
1. Identify the original plan's `plan_run_id`, `plan_id`, and `plan_sha256`.
2. Ensure `main` has not advanced.
3. Dispatch **Migrate Google Drive layout** on `main`:
   - `operation`: `resume`
   - `expected_root_id`: `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`
   - `confirm`: `true`
   - `compatibility_commit`: Reviewed 40-character commit SHA
   - `plan_run_id`: Run ID of the original plan workflow run
   - `plan_id`: Original plan ID
   - `plan_sha256`: Original plan SHA-256
4. The workflow validates the journal's pinned plan digest against `plan_sha256`, rehydrates folder mappings, idempotently verifies completed steps, and executes remaining operations.

### Rolling Back to Legacy Layout
If migration must be reverted, first confirm that `main` still matches the original plan commit and that the journal remains applicable:
1. Dispatch **Migrate Google Drive layout** on `main`:
   - `operation`: `rollback`
   - `expected_root_id`: `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`
   - `confirm`: `true`
   - `compatibility_commit`: Reviewed 40-character commit SHA
   - `plan_run_id`: Run ID of the original plan workflow run
   - `plan_id`: Original plan ID
   - `plan_sha256`: Original plan SHA-256
2. The workflow verifies the remote journal hash, reverses completed moves in reverse order, restores original folder names and parents, and prunes newly created navigation shortcuts.

---

## 4. Local Diagnostics & Verification (Read-Only)

Diagnostic inspections may be run locally. Production writes are restricted to reviewed main Actions workflows.

```bash
# Read-only verification
python scripts/migrate_drive_layout.py \
  --operation verify \
  --expected-root-id 1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf

# Read-only cutover precondition check
python scripts/check_cutover_preconditions.py \
  --repo rutkala/zohelo-data \
  --portal-url https://data.zohelo.com/portal-build.json \
  --compatibility-commit f05e22da1c876d51ccce4380ac03efee6cbe2765
```

---

## 5. Reviewed-Plan Provenance and Receipts

Run plan through the main-branch **Migrate Google Drive layout** workflow. Review both `plan.json` and `plan-identity.json` from its `migration-plan` artifact. Every `apply`, `resume`, or `rollback` dispatch must supply the same `plan_id`, decimal `plan_run_id`, and exact 64-character `plan_sha256` from that artifact.

Before credentials are used, the workflow verifies that:
- The referenced run belongs to this repository (`rutkala/zohelo-data`).
- The run executed `.github/workflows/migrate-drive-layout.yml` triggered via `workflow_dispatch` on `main`.
- The run concluded successfully (`success`).
- The executing workflow commit exactly matches the plan run commit and artifact commit (`github.sha`).
- The root pins, plan ID, and canonical plan digest (`plan_sha256`) match.

A failed or stale run cannot authorize a mutation. Keep `main` at the reviewed plan commit until cutover verification and any required recovery are complete. After acceptance, normal development may advance `main`, but the old plan no longer authorizes mutations from that newer commit. Before a new migration, create and review a fresh plan. Recovery of an existing journal after `main` advances needs a newly reviewed recovery procedure; generating a fresh plan alone does not make the old journal resumable. Never bypass provenance or force-move `main`.

Download the `migration-receipts` artifact after each workflow execution when it is produced. Depending on the operation and how far it progressed, it contains `dispatch-preconditions.json`, `plan-provenance.json`, `cutover-preconditions.json`, `operation-result.json`, `journal.json`, and/or `verification.json`; failures may also produce `migration-error.json`. In particular, `verification.json` is produced by the separate read-only `verify` operation and is not an apply receipt.

Navigation indexes are written as pending prior to release pointer updates. Exact pointer readback must confirm bytes before final verification. For current delivery status, see [docs/deliverables.md](../deliverables.md). For agent collaboration policies, see [docs/collaboration.md](../collaboration.md).
