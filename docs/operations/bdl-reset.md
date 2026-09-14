# GUS BDL Reset Operational Runbook and Restoration Guide

This document governs the safe, reviewable administrative reset of all GUS BDL data across Google Drive storage layers, and the exact procedures for verification and restoration from trash.

## 1. Safety Principles & Invariants

1. **Recoverable Trash Only:**
   The reset CLI (`scripts/reset-bdl-campaign.py`) NEVER calls permanent Drive API `.delete()`. All targeted objects are moved to Google Drive trash using `files().update(fileId=..., body={"trashed": True})`. Items in Drive trash are retained for 30 days and can be restored.

2. **Strictly Bounded Scope:**
   Only discovered exact BDL roots are targeted:
   - `01_landing/gus_bdl`
   - `06_control/source_campaigns/gus_bdl`
   - `releases/bdl`
   - `05_archive/bdl-platform` (if present)
   - `bdl-platform` (legacy wrapper at root if present)
   - BDL navigation shortcuts and index under `02_bronze/current/bdl`, `03_silver/current/bdl`, `04_gold/current/bdl` (and `current/gus_bdl` if present)

   All non-BDL sources (`nbp`, `world_bank_wdi`, `eurostat`, `opendata_org`) and shared roots (`01_landing`, `06_control`, `releases`, medallion layers) are baselined and verified intact before and after apply.

3. **No Root Initialization or Hidden Write Opt-in:**
   Plan mode is strictly read-only (`--operation plan`) and requires zero production write permissions. Storage root resolution disables creation (`create=False`).

4. **Producer Exclusion Preconditions:**
   Before apply or resume can execute:
   - Workflow `.github/workflows/source-gus-bdl.yml` must be disabled (`state == "disabled_manually"`).
   - Zero runs of `source-gus-bdl.yml` may be active or pending (`queued`, `in_progress`, `pending`, `waiting`, `requested`).
   - Concurrency group `zohelo-production-data` is enforced.

5. **Durable Quota Retention:**
   External provider API rate limits (400 req/15m, 40,000 req/week) cannot be reset. The reset engine extracts `quota_attempts` and `provider_retry_at` from `06_control/source_campaigns/gus_bdl/current-ingestion-state.json` and preserves them in the plan, recovery receipt, and durable journal outside the wiped data.

---

## 2. Operational Procedure

### Step 1: Generate Read-Only Plan

Dispatch `.github/workflows/reset-bdl-campaign.yml` with:
- `operation`: `plan`
- `expected_root_id`: `1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`
- `confirm`: `false`

This produces the `bdl-reset-plan` artifact containing:
- `plan.json` (exact target inventory, counts, bytes, non-BDL baseline, quota evidence, and `plan_sha256`)
- `plan-identity.json` (plan run ID, commit SHA, and root ID)

### Step 2: Review Plan Artifact

Inspect `plan.json`:
- Confirm `total_target_count` and root breakdowns match expected BDL inventory.
- Confirm `non_bdl_baseline` contains all NBP, WDI, Eurostat, and OpenData structures.
- Record the `plan_run_id`, `plan_id`, and `plan_sha256`.

### Step 3: Ensure Producer Exclusion

Lead manually disables `.github/workflows/source-gus-bdl.yml` via GitHub Web UI or CLI:
```bash
gh workflow disable source-gus-bdl.yml
```
Confirm no active runs exist (drain or cancel any pending runs like 34883169715).

### Step 4: Apply Reset (Recoverable Trash)

Dispatch `.github/workflows/reset-bdl-campaign.yml` with:
- `operation`: `apply`
- `confirm`: `true`
- `plan_run_id`: `<run_id_from_step_1>`
- `plan_id`: `<plan_id_from_step_1>`
- `plan_sha256`: `<plan_sha256_from_step_1>`

The workflow downloads the reviewed plan artifact, verifies provenance and precondition checks, checks target drift, moves items to trash, and records `bdl-reset-receipts`.

### Step 5: Resume If Interrupted

If apply fails or times out midway:
- Do NOT re-run `apply`.
- Dispatch with `operation: resume`, passing the exact same `plan_run_id`, `plan_id`, and `plan_sha256`.
- The engine loads the durable journal, skips already-trashed items, and finishes remaining items.

---

## 3. Restoration from Google Drive Trash & 30-Day Retention Policy

> [!IMPORTANT]
> **Google Drive 30-Day Trash Expiration Policy:**
> Per official Google Drive v3 documentation, items in Google Drive trash (`trashed=true`) are permanently and automatically purged by Google after 30 days. Untrashing must occur within this 30-day window. Beyond 30 days, Drive permanently deletes trashed items; no retention or restore guarantee exists after expiration without an external backup.

If BDL data must be restored within the 30-day window:

1. **Durable Journal Contains Exact Root Identities:**
   Download the `bdl-reset-receipts` artifact or inspect `06_control/bdl_resets/<plan_id>/journal.json`.
   The journal's `root_checkpoints` array lists each of the 7 mutated BDL root folders, their IDs, and keys.

2. **Restore via Drive API (Root-Level Untrash):**
   Google Drive v3 inherits untrashed state (`trashed=false`) down the entire subtree. Setting `trashed: False` on the 7 BDL root folders automatically restores all 36,329 descendant files and subfolders without requiring tens of thousands of individual API calls.

   Run the following Python script in Codespaces or an authorized reviewed environment:
   ```python
   import json
   from storage_manager import StorageManager

   sm = StorageManager(backend="gdrive")
   journal = json.load(open("bdl-reset-journal.json"))

   # Untrash the 7 BDL root folders; descendants inherit trashed=False automatically
   for checkpoint in journal.get("root_checkpoints", []):
       root_id = checkpoint["root_id"]
       root_key = checkpoint["root_key"]
       print(f"Restoring root {root_key} ({root_id})...")
       sm.drive_service.files().update(
           fileId=root_id,
           body={"trashed": False},
           supportsAllDrives=True,
       ).execute()
   print("Root restoration complete. All descendants have inherited untrashed status.")
   ```

3. **Verify Restored Objects:**
   Confirm that all 7 root folders and their descendant files and shortcuts are visible and accessible (`trashed=false`) under their original parents.
