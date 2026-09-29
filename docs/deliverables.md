# Zohelo-data delivery plan

This is the single current delivery/status record. The complete preceding record is
preserved byte-for-byte in [the dated historical record](deliverables-before-r2-20260929.md).
That archive preserves all source-specific evidence, questions and holds; its dated
"current" and "running" statements are not current operational status.

## Autonomous continuation and execution plan — 29 September 2026

The owner requested independent engineering for the next 24 hours, advancing the
next approved task after each verified outcome rather than waiting for routine
reassignment. This is not permission to bypass execution safeguards, exhaust paid
quotas, introduce new providers/products, delete retained data or claim continuous
execution where only scheduled sessions exist.

**Actual continuation configured:** 24 hourly ChatGPT task invocations, first at
29 September 14:45:30 Europe/Warsaw, last at 30 September 13:45:30 Europe/Warsaw.
The task expires after those occurrences. They are scheduled engineering sessions,
not an always-running agent, and remain subject to permissions and usage limits.
Work this week and next week is a prioritized roadmap, not an additional unbounded
background-work promise. Source decisions and the current migration priority below
supersede stale Drive-only and Copilot/AGY directions in older agent instructions.

### Verified starting position and immediate implementation

- Current main was `29f942782f34c06395f17c78501e9b3da47381d8` at inspection.
  PRs #218 and #219 are merged; there were no open PRs before this increment.
- All-layer migration run `36547081711`, job `109335973750`, was still in its
  copy command when checked at approximately 11:46 UTC on 29 September.
  The live-log download returned BlobNotFound. Running status is NOT a verified
  copied-file count, and no copied-byte estimate is inferred from elapsed time.
- Do not restart or duplicate that job. Its existing production serialization and
  five-hour transfer envelope remain in force. Inspect the final receipt when
  available and resume only after the previous run is terminal.
- Open issues #130 (roadmap), #131 (BDL), #132 (DBW), #133 (WDI), #134 (Eurostat)
  and #135 (orchestration) were reviewed as the existing work containers. Their
  historical Drive-specific wording is not authorization to revert R2 migration
  or recreate already-verified WDI/NBP functionality. Do not close them based only
  on migration success or old count estimates.
- First proposed engineering increment: a bounded, read-only R2 progress observer
  using the existing private aggregate checkpoint, with explicit unknown/stale
  states and no payload reads, remote writes or automatic restart. Twenty-six
  offline tests passed in the assistant's disposable workspace.
- **Execution hold:** the attempt to create `src/r2_migration_status.py` on
  `feat/r2-migration-observer-20260929` was blocked by OpenAI safeguards.
  The runtime observer was NOT committed, deployed or run against R2. Do not
  replay that blocked write through a different tool, encoding, scheduled task
  or another agent. This documentation-only plan is independent of that held
  runtime publication. A schedule is not resolution of an execution safeguard.

### Today and the next 24 hours: 29–30 September

| Order | Work | Acceptance / next dependency |
| --- | --- | --- |
| P0 | Establish exact all-layer migration progress from permitted run/receipt reads; diagnose terminal failures and resumability gaps. | Report actual files/bytes/errors per available evidence, distinguish discovered from copied, preserve every successful R2 object and all Drive originals; no duplicate writer. |
| P1 | Finish all retained-object copy and before/after reconciliation, including Archive, Control, releases, empty folders and shortcuts. | Complete source inventory and verified destination mapping; explicit blockers for missing/unsupported objects; candidate only, not a portal cutover. |
| P2 | Resolve each current published table from accepted release manifests and register complete Bronze/Silver/Gold Iceberg membership. | No glob of historical releases into a current table; exact schema/count/membership tests and a fresh reader; retain DBW's already-verified table without recopying it. |
| P3 | Implement the authenticated R2-only data-access boundary for the existing comparison portal. | Preserve the accepted UI, prevent Drive fallback, keep credentials server-side, protect preview/production routes, and prove browser Range/auth behavior before switching live traffic. |
| P4 | Prepare producer output cutover one source at a time. | Fixture-tested native Landing storage adapter, independent downstream stages, recovery/replay and one serialized writer; do not let old schedules create an untracked divergent source after cutover. |

These are execution priorities, not guaranteed completion times. While an external
job runs or a specific action is held, use independent approved work: tests,
source contracts, read-only failure analysis, current instruction reconciliation
or the Actions audit. Do not manufacture unrelated features to appear busy.

### This week: 29 September–4 October

1. Complete migration reconciliation, accepted table registration and R2-only
   comparison-portal verification before declaring the storage cutover done.
2. Move existing producer outputs to R2 in reviewed, source-specific increments;
   preserve native transfer versus Landing-to-Bronze parsing versus dbt stages.
   Carry forward all partial-coverage and native-lineage limitations.
3. Review all current Actions against the new architecture: triggers, duplicate
   paths, dependency installation, timeouts, safe resumption, production locks,
   permissions, pinned actions and useful failure/progress receipts. Disable or
   remove obsolete pilot paths only after proving they are no longer needed and
   preserving their evidence; no blind mass deletion or schedule cancellation.
4. Align AGENTS.md, collaboration.md, architecture.md and developer instructions
   with the current R2 direction, sole AI-subscription budget and one-owner rule.
5. Measure object/catalog/request costs and unrestricted SQL previews, exact
   counts, multi-indicator queries and representative joins. Keep compaction a
   separate measured change; it must not invalidate migration identity proof.

### Next week: 5–11 October

1. Close remaining existing-source completeness and lineage gaps, using issues
   #131–#135 rather than onboarding new sources. BDL retains the approved ten
   WireGuard routes and correct checkpoint/ownership checks when intake resumes.
2. Complete missing source-shaped Bronze, conformed Silver, analytical Gold and
   documented semantic definitions with explicit grain, units and coverage.
3. Validate failure recovery, idempotency, fresh-consumer snapshot consistency and
   human-operable runbooks without dependence on an AI session or devcontainer.
4. Consolidate redundant workflows/adapters and stale instructions only when
   behavior and rollback evidence are preserved. Reassess spend from measured
   usage rather than assumed free-tier coverage.
5. Evaluate a minimal read-only data delivery service only where required for
   approved consumers; no speculative public SQL endpoint or new portal rebuild.
   BI/Evidence/Dash and the separate zohelo website remain deferred.

### Autonomy boundaries and handoff

One implementation owner and one active engineering branch at a time. Current-head
CI, behavioral tests, PR review, merge, deployment and data acceptance are separate
gates. Continue routine approved work without asking for repeated permission.
Escalate only a consequential business decision, unavailable credential or actual
execution/access/cost boundary. Never bypass a denial or move a held operation to
another mechanism. Do not delete Drive, reset checkpoints, expose private buckets,
force-push, enable paid APIs/overages or add subscriptions. End each session with
actual completed work, active external jobs, unmerged changes and concrete blockers.
At the end of the 24-hour window, start no new long-running work and leave a final
handoff; ongoing safely launched jobs are reported honestly, not silently cancelled.

## Current owner priority — 29 September 2026: migrate ALL project data to R2

The owner authorized migration of the entire existing `zohelo-data` Drive root,
including native Landing, Bronze, Silver, Gold, archives, control records, release
history and other retained project files. This is not permission to read/copy the
owner's personal Drive outside the project root, erase Drive, or discard partial
or historical project data. Preserve the existing portal as a temporary comparison
interface, but its replacement backend must read only Cloudflare, with no silent
fallback to Drive. See [ADR 0014](decisions/0014-all-data-r2-cutover.md).

**Reference preserved:** branch `reference/drive-platform-20260929` points to
`9ac479c0b004302d43a24c15ed9dbac47d7acfdb`. The original Drive objects and release
pointers remain untouched. Keeping a reference is not authorization to run two
uncoordinated production writers.

### Recovery from the interrupted chat response — 29 September 2026

The chat response failed, but its implementation was saved in
[PR #218](https://github.com/rutkala/zohelo-data/pull/218). The full
[repository validation](https://github.com/rutkala/zohelo-data/actions/runs/36544593685)
passed on exact head `603939a2bb5c0d13be9175038cc10fbf33a9451c`, including the
workflow safety policy. The transfer code, tests and workflow were reviewed on
resume; PR #218 merged as `4aa6271d316ffcfb4f70b436e247699925782d4e`.

The real all-layer copy launched at **09:07 UTC** in
[run 36547081711](https://github.com/rutkala/zohelo-data/actions/runs/36547081711).
The first observed job state was `in_progress`, with checkout, exact-revision
validation and Python setup passed and dependency installation underway. This is
launch evidence, not copied-file coverage or a completed migration. Check this
run's current steps and aggregate receipt before quoting progress. Its script has
a five-hour transfer envelope and reuses verified objects on a subsequent run.

**The deployed portal has not switched to R2.** Object-copy verification, complete
current-table registration, authenticated R2-only portal validation and producer
cutover remain required. Do not claim that the whole platform migrated merely
because this launch or its file-copy stage succeeds. Do not delete Drive data,
retire the reference, expose buckets publicly or create a second active writer.

### Verified baseline

- The R2 object/catalog generated-data contract passed in run `36479212904`.
- The retained DBW direct-copy readback passed in
  [run 36529428065](https://github.com/rutkala/zohelo-data/actions/runs/36529428065)
  at `9ac479c0b004302d43a24c15ed9dbac47d7acfdb`: 1,550 files, 4,737,200,817 bytes,
  879,999,727 rows, ordinary LIMIT 1000, filter and join. The rerun reused all files.
- The owner subsequently confirmed the files/catalog in Cloudflare and executed
  R2 SQL Studio queries. Their preview reported 3.64 seconds, 465 files, 1.52 MB
  and 875 requests. Its plan pushes LIMIT 100 into the scan. This is useful user
  evidence, not a controlled before/after optimization benchmark.
- The retained DBW snapshot does not establish current provider completeness or
  resolved native-to-Bronze value lineage. Those limitations remain unchanged.
- A live root listing on 29 September confirmed `01_landing`, `02_bronze`,
  `03_silver`, `04_gold`, `05_archive`, `06_control`, and `releases` on Drive.
  A complete fresh inventory is part of the migration; the old bounded storage
  estimate is not an exact current total.

### Delivery stages — do not collapse them into one "Done"

| Stage | State / acceptance |
| --- | --- |
| Full project inventory and byte-preserving R2 copy | Implementation merged via #218; run `36547081711` launched. All descendant pages must be exhausted. No source-format parser or row transformation runs during copy. Complete live coverage is not yet verified. |
| Retained reference and reconciliation | Original Drive and reference branch retained. Before/after inventory must agree before a complete copy candidate is published. Missing/inaccessible/unsupported objects block a completion claim. |
| All published Bronze/Silver/Gold Iceberg tables | Pending after object copy. Resolve each current release's exact file membership from verified manifests; do not glob all historical Parquet into one table. Existing DBW pilot is retained, not recopy/rewrite work. |
| Existing portal reads only R2 | Pending backend/auth integration and browser acceptance. Preserve existing explorer/catalog functionality; do not expose writer secrets or mix old Drive data with R2 data. No production cutover has occurred. |
| Ingestion/transformation output cutover | Pending source-by-source storage-adapter work and reconciliation. Existing scheduled jobs have not been rewritten to R2 by the copy implementation. |
| Final retirement of Drive | NOT authorized now. Keep the comparison/recovery copy until a later explicit decision. |

The migration workflow is `Migrate all zohelo-data files from Drive to R2`. It is
main-only, exact-revision checked, opt-in via dispatch confirmation or the reviewed
merge marker `[run-all-r2-migration]`, and serialized under the existing
`zohelo-production-data` group. It has a five-hour transfer budget within a bounded
job, four copy workers and a 400 GiB discovered-payload spend guard. A guard is an
explicit failure, never a truncated successful inventory. Reruns reuse verified
objects. No extra service subscription, public bucket access, compaction, deletion
rule or paid AI API is enabled.

Only aggregate progress/receipts go to Actions artifacts. Full file metadata,
source-to-R2 identity mappings and reconciliation evidence stay private in R2 under
`06_control/drive_to_r2/`. A successful copy produces `candidate.json`, not an
automatic production portal pointer. Control JSON remains byte-preserved; legacy
Drive IDs in its contents are resolved by the R2 index rather than followed to Drive.

### Current implementation checks

Twenty offline regression tests were recorded as passing in #218: all seven areas
included, no re-download on resume, multipart integrity, checksum/truncation/oversize
rejection, abort of incomplete multipart uploads, preservation of older destination
versions, duplicate names, path escaping, empty folders, shortcut handling, source
drift, project-root restriction and explicit time/storage-budget failures. The full
repository CI passed in run `36544593685`. The live workflow reruns the focused
transfer safeguards before receiving credentials. Live migration success remains
a distinct gate; it has not yet been claimed.

### Subsequent user architecture decisions retained

- Landing is source-native bytes. Python/source-specific parsers perform the
  separate Landing-to-Bronze stage; dbt normally starts from structured Bronze.
- GitHub Actions invokes dbt; dbt-duckdb uses DuckDB to execute transformations.
  R2 SQL Studio is an independent read/query service, not the batch writer.
- Cloudflare Explorer/SQL Studio may replace the long-term custom technical portal,
  but the owner now explicitly retains the existing portal temporarily for comparison.
- BI is later work in a separate repository; do not implement Evidence/Dash here.
- The primary personal AI is ChatGPT Plus with a $20/month target. Copilot is not
  used. Do not dispatch work to a paid Copilot/AGY subscription based on historical
  delegation instructions. Production AI/API spend remains a separate, unused budget.

All other source coverage limitations, definitions, source-specific approval
questions and historical evidence remain in the dated record above. This migration
must preserve those distinctions rather than reclassifying copied samples or partial
source collections as complete production ingestion.
