# Zohelo-data delivery plan

This is the single current delivery/status record. The complete preceding record is
preserved byte-for-byte in [the dated historical record](deliverables-before-r2-20260929.md).
That archive preserves all source-specific evidence, questions and holds; its dated
"current" and "running" statements are not current operational status.

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
