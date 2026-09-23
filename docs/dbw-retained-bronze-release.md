# Publish the retained DBW Bronze snapshot

> **Execution hold, 23 September 2026 (Warsaw):** this workflow is disabled after the unexpected Landing-folder creation. The run was cancelled; do not re-enable or rerun the unchanged implementation. Read the current hold, preserved snapshot and recovery requirements in [deliverables.md](deliverables.md).

This runbook covers the dated retained DBW inventory, not a new ingestion campaign,
complete provider coverage, native-to-Bronze lineage, Silver, Gold or semantic release.
Current delivery status belongs in [deliverables.md](deliverables.md); a workflow file
or successful fixture run is not evidence that production publication has happened.

## What becomes available

The portal's `02_bronze` layer exposes:

| Relation | Meaning |
| --- | --- |
| `br_dbw_indicators` | Retained indicator names and taxonomy. |
| `br_dbw_metadata` | Retained indicator metadata. The audited metadata has 1,531 rows, not one guaranteed row for each indicator. |
| `br_dbw_dictionaries` | Retained source dictionary entries. |
| `br_dbw_observations__indicator_<id>` | Observations for an explicitly selected published indicator. |

The unselected `br_dbw_observations` aggregate is not a request to download the entire
879,999,727-row retained inventory into the browser. Select an indicator in the data
explorer. Selections larger than 64 MiB expose explicit
`br_dbw_observations__indicator_<id>__part_<number>` relations. Fragment size remains
bounded by 8 MiB and the existing browser session budget remains in force. These
transport limits do not reduce the retained publication's 1,550-indicator target.

Start discovery with ordinary SQL:

```sql
SELECT indicator_id, indicator_name, thematic_area, domain
FROM "02_bronze"."br_dbw_indicators"
ORDER BY indicator_id
LIMIT 25;
```

Then select the corresponding observation relation in the explorer. Bronze values
retain the existing source-shaped schema and limitations; publishing them does not
assign new business semantics or convert them into approved Gold metrics.

## Run in GitHub Actions

Use **DBW retained Bronze release** (`.github/workflows/dbw-bronze-release.yml`).
Dispatch on `main`, supplying the exact reviewed 40-character main commit and the
existing production Drive root ID. The workflow refuses another revision or branch.
It uses the existing `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, and
`GOOGLE_OAUTH_REFRESH_TOKEN` repository secrets. Do not paste credentials into inputs.

1. Credential-free validation checks the implementation.
2. The serialized publisher job reconstructs and verifies the exact reviewed input
   package from authoritative Drive files on its own fresh runner. The historic audit
   report remains byte-identical; fresh restore evidence is a separate record.
3. The existing publisher acquires its operational Git-ref claim and Drive ownership,
   publishes immutable increments until all 1,550 retained indicators are covered,
   and preserves previous snapshots. No native source is downloaded again.
4. A separate read-only runner fetches every published fragment and validates hashes,
   actual schemas, row counts and non-null indicator binding. It requires zero pending
   indicators and the retained audited totals.
5. A fresh browser profile on `https://data.zohelo.com` discovers the exact verified
   manifest and executes SQL against an observation selection and all three fixed
   tables. This uses real authorized Drive requests, not fixtures.

The GitHub token has `contents: write` only in the publisher job because the existing
[operational claim](decisions/0010-retained-dbw-publication-ownership.md) is a Git ref.
Drive production-write opt-in is set only for the publication step. Neither input
restoration nor consumer verification constructs a Drive writer. BDL ingestion and
its ten-worker coordinator are not controlled by this workflow.

## Interpret the result

Publication progress is not live SQL acceptance. Inspect all three boundaries:

- `dbw-bronze-publication-evidence`: fresh input restoration and actual publication
  counts/snapshot identity. Zero pending indicators establishes only complete publication
  of this dated retained inventory.
- `consumer-evidence.json`: every published fragment independently downloaded and
  checked; retains `portal_sql_verified: false` because native checks are not browser proof.
- `portal-evidence.json`: `status: verified` and `portal_sql_verified: true` establish
  the actual deployed portal's acceptance query against the exact native-verified manifest.

Artifacts contain summaries only, never the input cache, indicator index, access tokens,
refresh tokens, screenshots or authenticated browser traces. The browser access token
exists only in the isolated runtime/session and is not printed or saved as an artifact.
The original native and retained Bronze objects are not removed.

## Failure and resume

A changed reviewed inventory fails before payload restoration. A failed restoration
invalidates its package; no publication is attempted from it. A publication failure
leaves verified earlier immutable increments intact and must not be called complete.
Consumer or browser failure does not automatically delete or roll back published data;
inspect the relevant failed step and evidence, then repair the actual failing boundary.

The workflow never takes over an existing operational claim merely because it is old.
Use ADR 0010's owner verification and exact-SHA recovery procedure if a job was terminated
while holding a claim. Do not blindly delete the ref or start a competing publisher.
Runner-local restoration directories are disposable; a new run restores from Drive.

No source-completeness or lineage label is upgraded by this runbook. The retained
manifest continues to carry `incomplete_retained_inventory` and
`unresolved_native_to_bronze`; the exact completed publication counts are reported
separately. Full native source reconciliation remains its own delivery work.


## Read-only verification of an existing publication

Use the existing **Publish existing DBW Bronze** workflow with `operation=verify_only`
when repairing a reader acceptance check or rechecking an already published snapshot.
Supply the exact reviewed current-main `expected_code_sha`, the existing Drive root,
the exact `expected_snapshot` UUID and `expected_publication_code_sha` recorded in
that snapshot. The verifier code revision and data publication revision are separate
pins; verification must not require republishing data to change only test code.
Leave `recover_drive_owner` empty. Verification-only rejects owner recovery and skips
the production writer. It still requires complete retained-inventory coverage, checks
every published file and executes SQL in the actual portal. It cannot turn a partial
snapshot into complete acceptance.

A fresh portal profile opens the Home tab. The live checker must select **New SQL
query** before waiting for the editor; merely discovering a correct manifest does
not create a SQL tab. The browser fixture now follows this same Home-to-query path.
If a publication completes but the old checker fails at that UI step, preserve the
published data and use read-only verification with the corrected checker. Do not
restart ingestion or publication for a checker-only repair.
