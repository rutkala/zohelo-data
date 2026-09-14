# Zohelo-data delivery plan

Approved work programme, 6 September 2026. The owner approved this scope; that approval did not settle the business or architecture choices recorded below. The initial user is the owner. The NBP scope is all already-ingested history for Tables A, B, C and gold prices.

## Current status

Updated 14 September 2026. **The owner-approved Google Drive consolidation is live and independently verified.** Implementation [PR #101](https://github.com/rutkala/zohelo-data/pull/101) merged, the compatible portal was deployed, and [apply run 34871823795](https://github.com/rutkala/zohelo-data/actions/runs/34871823795) completed successfully. Independent verification passed at 17:21 UTC.
The complete current WDI archive remains modeled and published; existing WDI acceptance and other source coverage records are preserved.

### Progressive Eurostat API Bronze → Silver → Gold → semantic delivery — 14 September 2026

This change adds the first production-gated Eurostat modeled release without narrowing or closing
the full-source commitment. It consumes every accepted response in the complete reviewed API
contract (three datasets × 27 current EU countries), decodes every JSON-stat cube cell including
explicit missing positions, preserves the full native dimension key and status, reconciles
revisions, and publishes eight source-shaped Bronze, Silver and Gold datasets. Twelve native
MetricFlow coverage/accountability metrics are checked against Gold before promotion. Generic
Eurostat values are deliberately not exposed as an additive business metric.

The release is independently restorable, hash-bound to all Landing fragments, and staged before
its immutable `releases/eurostat` pointer is promoted. Its coverage mart also binds the current
full-distribution campaign evidence, so the smaller modeled API contract cannot be mistaken for
complete Eurostat. All 473 repository data-platform tests pass, including exact sparse-cell,
revision, release-contract and native MetricFlow regressions. Production promotion and fresh
restore remain the acceptance gate for this change; no production release is claimed in advance.

Fresh orchestration evidence at 19:11 UTC: Eurostat
[run 34880889218](https://github.com/rutkala/zohelo-data/actions/runs/34880889218) completed with all
three inventories current, 3,540 of 21,247 current catalogue distributions validated (16.66%),
19,161 distribution tasks pending, zero failed pending tasks, 4,127 retained accepted versions and
13,337,432,191 raw bytes. Its API Landing has 1,108 accepted/published responses, zero publication
backlog and no remaining API tasks. The full catalogue therefore remains incomplete and the full
distribution payloads remain raw-only.

WDI [run 34881599243](https://github.com/rutkala/zohelo-data/actions/runs/34881599243) completed its
Landing continuation and was freshly verifying the already accepted complete-archive modeled
release under the shared production lock. BDL
[run 34879372531](https://github.com/rutkala/zohelo-data/actions/runs/34879372531) advanced Landing to
3,996 accepted/published responses with zero publication backlog and 2,409 API tasks pending; its
modeled job was cancelled when a later scheduled BDL run replaced the queued workflow. This is not
data loss, but the 15-minute workflow cadence can starve the downstream transform while another
source holds the shared production lock. Current BDL continuation
[run 34885171452](https://github.com/rutkala/zohelo-data/actions/runs/34885171452) was active and was
not duplicated. The separate authenticated Web-bulk route still has no accepted bulk archive and
BDL remains far from the 172,576-variable full scope.

NBP [run 34732275848](https://github.com/rutkala/zohelo-data/actions/runs/34732275848) remains the
latest successful daily acceptance: all four REST feeds report complete coverage through the
12 September check window, current observations through 11 September (Table B through 9 September),
all 15 datasets restore, and all five native metrics match Gold. Release
`e8c025a4-a7c7-432a-84fa-8151c1479c98` contains 426,687 FX fact rows and 3,454 gold fact rows.
Later NBP workflow invocations were path-triggered no-ops, not missed scheduled freshness work.

### Google Drive physical layout consolidation (Issue #100, PR #101) — 14 September 2026

The approved layout is now in place:

- Canonical releases: `releases/nbp/`, `releases/bdl/`, and `releases/wdi/`, each with its existing current pointer and immutable UUID release packages directly beneath it.
- Ingestion control: the former `ingestion-control/` folder is now `06_control/nbp/`, preserving its ID and contents. `06_control/source_campaigns/` retains its identity.
- Medallion browsing: 65 shortcuts and nine verified navigation indexes cover all 47 current tables across Bronze, Silver and Gold, including multipart WDI tables.
- The old `bdl-platform` and `wdi-platform` wrappers are retained under `05_archive/`. Their release packages are under the common release root.
- Production writers and migration operations share `zohelo-production-data` concurrency with `cancel-in-progress: false` and `queue: max`.

**Execution evidence:** The [read-only plan run 34870571745](https://github.com/rutkala/zohelo-data/actions/runs/34870571745) and successful apply ran on reviewed commit `f05e22da1c876d51ccce4380ac03efee6cbe2765`. The plan passed 368 independent checks before dispatch. Its 110 operations moved 20 existing objects and created 90 navigation objects (16 folders, 65 shortcuts and nine indexes). Main stayed at the reviewed commit through apply and independent verification. The deployed [portal compatibility marker](https://data.zohelo.com/portal-build.json) advertised canonical layout support, and the cutover guard found no old or unverified publisher blocking the operation.

**Independent acceptance:** All 3,673 baseline objects passed checks of identity, expected names/parents, non-trash state, ownership, exact sharing permissions and available size/checksum fields. All 90 new objects passed target/content checks; nine indexes reported `current_verified`. Exact bytes were unchanged for the three current pointers, their three manifests, the NBP ingestion-state pointer and its pinned snapshot. Hash-verified Gold Parquet samples were queried for NBP (`dim_source_table`, 3 rows), BDL (`dim_bdl_period`, 31 rows) and WDI (`dim_wdi_year`, 66 rows). Verification reported no failures.

The retained data comparison used Drive metadata and available checksums; it did not redownload every retained dataset. SQL checks covered the three named samples, and this cutover did not include an authenticated portal UI test. The detailed [sanitized receipt](releases/2026-09-14-drive-layout.json) records the scope and limits. Full plan, journal and apply receipts remain in the Actions artifacts and durable Drive journal; the independent baseline and verifier are retained in `/workspaces/zohelo-drive-cutover-100/` in the reusable Codespace.

Implementation validation was already completed for PR #101: the full [data](https://github.com/rutkala/zohelo-data/actions/runs/34822943346) and [portal](https://github.com/rutkala/zohelo-data/actions/runs/34822943447) gates passed, along with 220 apply and 220 rollback fault cases. [Portal deployment 34823799131](https://github.com/rutkala/zohelo-data/actions/runs/34823799131) succeeded on the reviewed commit. This final follow-up changes documentation and records only.

See the [folder guide](drive-structure.md) and [migration runbook](operations/drive-migration.md) for the layout and future recovery constraints.

### Delegation policy agreed — 14 September 2026

The owner approved Copilot Pro or Codespace AGY as the first execution tier, with internal
agents used only when needed. The lead retains planning, architecture, acceptance,
review, integration and communication. Work proceeds one engineering task at a time.
AGY uses only the personal Gemini subscription and one reusable Codespace with a separate
feature branch/worktree per task. See the [working agreement](collaboration.md#delegation-and-cost-policy).

The initial implementation used AGY and then an internal fallback after Copilot could not start.
The final documentation follow-up tested the agreed external-first route: Copilot CLI authenticated
and passed a small smoke request, but the actual documentation task failed with **monthly quota
exceeded**. Its authentication success does not mean capacity is available. Ownership then passed
to AGY, which completed the three-file documentation task using the verified personal Google AI Pro
account, **Gemini 3.8 Flash**, and `useG1Credits: false`. The lead reviewed the output and added the
verified operational status and evidence. No Cloud model billing, BYOK model credentials or credit
overages were enabled.

The saved GitHub CLI login was verified as `rutkala` with `repo`, `workflow` and `codespace`
scopes. It successfully dispatched the reviewed plan and apply workflows. In Codespaces, injected
`GH_TOKEN` / `GITHUB_TOKEN` can override saved CLI credentials; these operations used
`env -u GH_TOKEN -u GITHUB_TOKEN gh ...` to select the saved login without reading token values.
Execution took place in the reusable Codespace terminal. Direct SSH dispatch from the Work
runtime remains unconfigured. This records a verified manual delegation cycle; it does not claim
unattended orchestration or measured subscription savings.

### Delivered: WDI Bronze → Silver → Gold → semantic release — 13 September 2026

The implementation merged in [PR #96](https://github.com/rutkala/zohelo-data/pull/96) consumes the freshly verified current
official WDI CSV archive and fails closed unless campaign evidence is
`complete_current_catalogue` and the archive contains exactly its six contracted CSV members. It
publishes six Bronze relations, three Silver relations, three dimensions, an observation fact and
a Gold coverage mart in a separate immutable `wdi-platform` release. The portal discovers that
pointer alongside NBP and BDL and exposes its tables, catalogue and lineage through the existing
Lakehouse interface. Populated annual source cells
are modeled at geography (including source-published aggregates) × indicator × year grain. Large
relations are partitioned into bounded Parquet files without reducing scope. Nine native
MetricFlow metrics report archive/member and source-to-modeled coverage at snapshot grain; source
indicator values are not treated as generally additive. Unchanged archive bytes under the same
code SHA reuse the current modeled release rather than creating six-hour duplicate packages.

The release is production-gated: every part must be freshly restored, bound to the accepted raw
archive hash and size, matched to dbt/catalogue artifacts, and reconcile to a modeled-value
coverage ratio of exactly 1.0 before pointer promotion. `bash scripts/check-data.sh` passes all 417
tests, including exact six-member extraction, six source-cell-to-six-Gold-row reconciliation,
native execution of every WDI metric, bounded multi-file release/restore and unchanged NBP release
behavior. Portal validation passes 696 tests plus typecheck, lint and production build; the focused
release-catalog fixture proves all fourteen WDI datasets and multiple fact partitions are
discoverable. Portal deployment [34776118373](https://github.com/rutkala/zohelo-data/actions/runs/34776118373)
is green on the WDI-aware build.

Production run [34776118401](https://github.com/rutkala/zohelo-data/actions/runs/34776118401)
completed the serialized Landing continuation but failed closed before release promotion because
the initial member contract used normalized filenames rather than the official ZIP's exact
case-sensitive names (`WDICSV.csv`, `WDIcountry-series.csv`, `WDIfootnote.csv` and
`WDIseries-time.csv`). No partial modeled release was promoted. The corrective delivery now binds
the models and tests to the exact official member inventory; [PR #97](https://github.com/rutkala/zohelo-data/pull/97)
passed 417 tests and a no-findings Codex review before merge.

The transform-only continuation [34778030623](https://github.com/rutkala/zohelo-data/actions/runs/34778030623)
then published and freshly restored release `e36ad90a-efd8-42c8-b537-03b6ce46dabc` without repeating
source collection. Its fourteen datasets cover all six official archive members and all 9,015,914
populated annual values: 264 of 264 source geographies, 1,498 of 1,498 indicators and observations
from 1960 through 2025. The Gold fact contains 9,015,914 rows and the measured source-to-modeled
value ratio is exactly 1.0. The production dbt build passed 47 data tests across eleven table and
three view models; pre-promotion validation and the subsequent fresh-process restore both passed.
This accepts the complete current official WDI CSV archive through Gold and semantic coverage, not
every other World Bank product. WDI values remain heterogeneous and are not generally additive.

Fresh orchestration evidence after WDI acceptance: run 34776118401 left the separate WDI API
campaign at 3,239 accepted/published responses, zero publication backlog and 1,206 tasks. Eurostat
run [34777245877](https://github.com/rutkala/zohelo-data/actions/runs/34777245877) retained 3,613
accepted versions and verified 3,108 of 21,247 current distributions (14.63%), with 19,670 pending
tasks, zero failed pending tasks and 11,806,124,611 retained raw bytes; it remains raw-only. BDL run
[34775742467](https://github.com/rutkala/zohelo-data/actions/runs/34775742467) published release
`95beaad2-7fd1-4393-83e0-0fb1735ca6af` with 3,705,782 observations while still modeling only 41 of
172,576 variables (0.0238%), with 929 campaign tasks pending. Run
[34778117985](https://github.com/rutkala/zohelo-data/actions/runs/34778117985) is the current serialized
BDL continuation and was not duplicated. NBP run 34732275848
freshly verified all 15 datasets and five metrics, with 426,687 FX plus 3,454 gold fact rows and
current source checks through 11 September (Table B through 9 September).

### Completed: Drive structure and economical delegation (Issue #94) — 13 September 2026

The read-only audit and owner guide were integrated through PR #95. Existing ingestion schedules
continue; this completed audit is retained here as dated evidence rather than an active work item.

- **Owner finding:** `bdl-platform/` is a deliberate independent publication namespace. Both NBP and BDL current pointers passed exact-byte manifest checksum, release identity/scope, and dataset/artifact metadata and parent checks at 09:24 UTC. Keep the two publication locations; no physical migration is proposed by this increment.
- **Measured release inventory:** 11 NBP directories contain 113,074,252 known bytes; 82 BDL directories contain 19,352,552,889 known bytes. All listed release directories have a manifest, and all enumerated release files have known sizes. Only the current packages received current-pointer/reference validation; older promotion history and full restores are separate.
- **BDL repetition check:** all 82 manifests from that inventory were compared by 09:35 UTC, producing 82 distinct declared dataset hash/size signatures. No identical complete table bundles were found. This does not establish meaningful business change in every run or justify changing retention.
- **Physical layers:** `03_silver/` retains three NBP Table A/B/C subfolders whose children were last modified on 31 August. `04_gold/` was empty. Current modeled tables are in immutable release packages, with logical layers recorded in their manifests.
- **Evidence and guide:** [Drive structure](drive-structure.md), [dated audit](audits/2026-09-13-drive-structure.json), [BDL signature comparison](audits/2026-09-13-bdl-signatures.json).
- **Delegation receipt:** AGY 1.2.1 used `gemini-3.8-flash-medium` with the existing Antigravity account. Conversation `e8a59b2a-0173-4ebd-8a29-736873c9ad50` produced the implementation and one repair pass; a lower-cost reviewer and lead integration corrected remaining defects. `--mode accept-edits` plus scoped file and exact fixture-command rules completed the repair pass without denied actions. Automatic approval review rejected the requested global permission-bypass flag. Credential environment variables were removed from the agent worker; the controller ran reviewed live reads.
- **Validation:** `bash scripts/check-data.sh` passed 406 tests on the revised implementation. After the live API exposed a missing `supportsAllDrives` parameter, that parameter was corrected and 15 focused audit fixtures passed. The corrected live audit completed in 69.1 seconds, with 188 counted requests, 2,485 unique metadata records and no incomplete reasons. The BDL comparison completed in 278 counted requests. No Parquet/archive downloads or production writes were performed.
- **Usage reporting:** the successful AGY repair pass ran 08:59:13–09:01:43 UTC. Its conversation-wide returned counters were 520,084 input, 82,700 output, 32,876 thinking and 6,101,192 cache-read tokens. These are cumulative provider counters, not a cost or savings claim.
- **Review and integration receipt:** [PR #95](https://github.com/rutkala/zohelo-data/pull/95) contains the reviewed implementation, guide and live evidence; its checks and merge receipt establish integration. Initial draft-creation attempts returned connector errors, but creation succeeded using the qualified head reference after review. This increment recommends retaining the two publication locations. It does not claim production data changes, new source coverage or a portal deployment. [Issue #94](https://github.com/rutkala/zohelo-data/issues/94) records the completed checks and any remaining handoff.

Updated 12 September 2026. **The audit and selected NBP product acceptance are complete; full coverage of all four selected sources is not.** NBP has 15 published tables and five verified daily metrics; its current verified release is complete through 11 September with 430,141 Gold fact rows. WDI's complete current official archive is retained but remains raw-only. Eurostat has 2,694 of 21,247 current distributions verified and BDL models 41 of 172,576 source variables, so neither is close to full-source acceptance. The owner has delegated full feasible onboarding across the [182 researched products/families](source-research/README.md), with no source-by-source review gate. [ADR 0006](decisions/0006-complete-selected-source-coverage.md) replaces starter-only scope with complete selected-product coverage. Source-specific serialized Actions continue resumable collection; BDL now has live Bronze/Silver/Gold and coverage semantics, while WDI and Eurostat modeled releases remain open. The detailed fresh checkpoint, active continuation and remaining acceptance programme are below. The [registered BDL key](source-accounts.md) is active within its durable provider quota ledger.

This is the single project-status and owner-question record. Research, architecture, runbooks and dated release evidence support it; they are not additional task boards. The portal remains a data tool.

### Fresh-context completion agreement — 8 September 2026

The owner reaffirmed autonomous delivery within the initial boundaries and rejected stopping at
partial results as though the complete goal were delivered. The [working agreement](collaboration.md)
and [shared agent instructions](../AGENTS.md) apply this to every component, including the portal.
For ingestion the target remains every available dataset, dimension, geography, historical period
and accompanying metadata in each selected product. Execution limits produce resumable checkpoints;
they do not reduce that target. A source-access, provider, capacity or implementation blocker must
remain visible with its next action. Partial data can be useful while the complete goal stays open.

The fresh check of [run 34252801846](https://github.com/rutkala/zohelo-data/actions/runs/34252801846)
found all three jobs successful and the following measured progress. These figures are a dated
checkpoint, not an all-source completion claim or a claim of new-source modeled data.

| Selected product | Verified checkpoint | Remaining acceptance |
| --- | --- | --- |
| NBP REST A/B/C and gold | Existing accepted 15-table/five-metric release; latest previously recorded daily check is through 7 September | Continue daily freshness/recovery checks; other NBP statistical products stay separate inventory work. |
| World Bank WDI | Complete 282,845,220-byte official archive freshly restored at 16:51 UTC; 218 API response rows published | Full source-shaped/typed/modelled access, revision reconciliation and release acceptance; API reconciliation remains separate from archive coverage. |
| Eurostat | All three inventories current; 53 of 21,233 catalogue distributions accepted at 17:20 UTC; 21,180 pending, zero failed pending tasks; 304 API responses published | Exhaust the current catalogue, recover oversized/asynchronous distributions, validate published coverage and deliver typed/modelled access. |
| GUS BDL | 278 accepted/published responses at 17:00 UTC; 559 pending tasks, zero pending retries; variable catalogue reports 172,573 entries | Exhaust catalogue/history, reconcile page membership and completeness, refresh catalogue generations, then deliver typed/modelled access. Response count is not variable or observation coverage. |

Half-hour serialized Actions jobs were actually active during this check. The enabled daily
**Advance Zohelo-data delivery** task was also created on 8 September to review current Actions,
investigate stalls and advance feasible engineering work. It has not yet supplied a completed
review run. These are specific continuation mechanisms, not a promise of continuous AI execution.

Priority is to close correctness gaps before claiming completeness, then remove measured throughput
bottlenecks while retaining quotas and recovery guarantees. This correction rejects incomplete BDL response pages, binds fresh distribution-index verification
to the current campaign checkpoint, and coalesces index publication while keeping every raw/state
checkpoint durable. It does not close the remaining acceptance items. The remaining completion programme is:

1. **Catalogue and extraction:** version BDL catalogue discovery, reconcile expected/observed unique
   IDs and page totals, and track unavailable/failed scope explicitly. Complete Eurostat backfill
   using its durable queue, asynchronous preparation and adaptive partitions. Catalogue totals must
   be scoped to their endpoint/parent/generation; a child-subject total is not a global total.
2. **Operational convergence:** measure accepted useful work per runner minute and retained bytes,
   prioritize bulk over redundant starter reconciliation, detect progress stalls outside provider
   cooldowns and replace repeated full-state work where measurement warrants it. Keep Actions as the
   current human-operable orchestrator; revisit a dedicated orchestrator against these measurements.
3. **Completion proof:** reconcile current catalogue membership, accepted raw, published indexes,
   missing/failed work and revisions. Add a resumable full-current-raw audit before final full-source
   acceptance; existing fresh bulk restore samples only the latest raw object. A successful sample
   or a complete archive is not proof of all-source end-to-end delivery.
4. **Usable data delivery:** continue source-specific dbt Bronze/Silver, dimensional Gold and
   appropriate semantics, with replay/release/catalogue/SQL acceptance. Raw responses and archive
   indexes are intermediate access, not a substitute for this already-authorized outcome.

These items remain open until their actual acceptance evidence is recorded. Future improvements
are reprioritized from observed usefulness, correctness, reliability and cost in this same record.

### Production coverage and modeled-delivery checkpoint — 12 September 2026

Fresh Actions jobs were inspected rather than inferring progress from schedules or successful
starter batches. The current acceptance position is:

| Selected product | Fresh verified evidence | Remaining work |
| --- | --- | --- |
| NBP REST A/B/C and gold | [Run 34666809424](https://github.com/rutkala/zohelo-data/actions/runs/34666809424) published release `c4cfcadb-0cd0-4c93-b2b9-f408d082737e`: all four feeds are complete through 11 September, with 426,687 FX fact rows and 3,454 gold fact rows. Capacity remained within its configured bounds. | Continue daily freshness and retained-release checks; no coverage repair is currently indicated. |
| World Bank WDI | [Run 34711375091](https://github.com/rutkala/zohelo-data/actions/runs/34711375091) freshly verified the one current official bulk archive (`complete_current_catalogue`), 3,117 accepted/published API responses and zero publication backlog. | The bulk data remains `raw_distributions_only`; 1,203 API tasks remain, and the archive still needs source-specific Bronze/Silver/Gold/release/catalogue delivery. |
| Eurostat | [Run 34716919745](https://github.com/rutkala/zohelo-data/actions/runs/34716919745) verified 2,694 of 21,247 current catalogue distributions (12.68%), 3,063 retained accepted versions, 10,310,601,785 raw bytes, 20,138 pending tasks and zero failed pending tasks. | The resumable catalogue backfill is healthy but incomplete and still `raw_distributions_only`; progressive source-shaped modeling must not imply complete catalogue coverage. |
| GUS BDL | [Run 34715218197](https://github.com/rutkala/zohelo-data/actions/runs/34715218197) published and freshly verified modeled release `dbf19d62-837b-43fd-be0b-9973bbc54dbc`, with 2,416,897 current fact rows. It models 41 of 172,576 source variables (0.0238%), from 2,012 accepted/published Landing responses and zero publication backlog, with 920 campaign tasks pending. | Bronze/Silver/Gold and coverage semantics are live, but variable/history coverage is far from complete. Continue API batches and the reviewed authenticated Web bulk path until catalogue reconciliation proves completion. |

The active BDL schedule was not restarted: corrected-main
[run 34716993130](https://github.com/rutkala/zohelo-data/actions/runs/34716993130) is collecting its
next Landing batch, with a later schedule pending under the same serialized provider group. The
successful predecessor above proves that this is continuation, not a persistent platform stall.

[PR 90](https://github.com/rutkala/zohelo-data/pull/90) adds authenticated BDL Web bulk extraction
for subgroups whose browser export is more efficient than REST paging. Review found that its first
version could mistake an incomplete folder for completion, bypass the shared quota ledger, buffer
archives up to 2 GiB in Node, and upload those archives as short-lived Actions evidence. The
corrected implementation chains the bulk step after the ordinary BDL modeled release inside the
same serialized workflow, selects work from the checksum-pinned `dim_bdl_subject` in that immutable
release instead of restarting a live API traversal, records a final completion marker only after
archive/Parquet/manifest verification, streams ZIP hashing, and
uploads summaries only. Invalid, cyclic or incomplete subgroup ancestry is reported as a completion
blocker rather than silently omitted. A `complete` plan additionally requires both root subject
catalogues and every admitted child subject page to be exhausted, plus a zero-backlog Landing
checkpoint whose accepted-response count matches the campaign state embedded in the release.
Browser downloads are accepted
only from a new export control created after the current subgroup's successful generation request;
pre-existing matching exports must first leave their pending state (or the run fails closed), then
are distinguished by stable row-content fingerprints rather than reorderable DOM IDs. The provider
filename must carry both the subgroup identity and a timestamp within the current Europe/Warsaw
provider-clock generation window. The ambiguous whole-second POST boundary is captured from the
matching request callback at event emission and rejected rather than accepted as a possible prior
export. These controls are covered by focused regression tests;
[PR 90](https://github.com/rutkala/zohelo-data/pull/90) and its request-boundary correction in
[PR 91](https://github.com/rutkala/zohelo-data/pull/91) are merged after green portal/data-platform
checks and review. The first fresh main-branch execution,
[run 34715218197](https://github.com/rutkala/zohelo-data/actions/runs/34715218197), published and
verified a BDL release and then returned `catalogue_incomplete`: its checksum-pinned plan recorded
2,012 accepted/published Landing responses, zero publication backlog, 378 subject-catalogue tasks
pending, ten currently invalid incomplete-hierarchy subgroups, zero valid candidates and no bulk
Landing write. The browser and landing steps were therefore skipped rather than treating partial
catalogue materialization as completion. A corrected-main continuation is serialized behind that
run; no Web bulk coverage change is claimed until a candidate archive is verified and landed.

The next modeled-release implementation order is WDI first, because its complete current archive
is already retained and has a stable six-member CSV contract, followed by progressive Eurostat
releases built only from complete verified data/structure units while its much larger catalogue
continues. Both must reuse source-specific immutable pointers and staged validation; heterogeneous
values will not be presented as generally additive semantic metrics.

### Vectorized engine standardization and OpenData multi-table expansion — 11 September 2026

Following the owner's directive to make compiled vectorized execution the default standard for all current and future sources across all pipeline stages, [ADR 0007](decisions/0007-unified-vectorized-execution-standard.md) was adopted:
1. **Engine standardization:** DuckDB native C++ vectorized execution is mandatory across all stages. Single-threaded row-by-row CPython parsing (`json.loads` loops) is prohibited.
2. **OpenData streaming acceleration:** Replaced CPython JSON parsing in `opendata_bronze_loader.py` with native DuckDB SIMD parsing (`read_csv` with `delim='\x1e'`, unnested `json_each`). Benchmark on 173k-row member files dropped conversion time from ~120s to ~2s per file (100x speedup), and total end-to-end processing over network from 130s to 6.6s–7.4s per file.
3. **Multi-table Bronze expansion:** Expanded OpenData pipeline to process all three Senzing entity datasets (`organizations`, `locations`, `people`) with interleaved batch streaming. Verified Bronze Parquet schemas and dbt models (`br_opendata_locations`, `br_opendata_people`) with zero disk footprint.
4. **Live production verification:** Runs `34637381935` and `34637745962` verified in production Google Drive with **3,419,725 total Bronze rows**:
   - `br_opendata_organizations`: 17 Parquet files, 2,946,314 rows
   - `br_opendata_locations`: 2 Parquet files, 188,755 rows
   - `br_opendata_people`: 1 Parquet file, 284,656 rows
5. **Portal Lakehouse integration:** Updated `publish_opendata_bronze.py`, `landingCatalog.ts`, and `googleDriveSlice.ts`. Deployed live in run `34637381933` to [data.zohelo.com](https://data.zohelo.com), where all three Bronze tables are queryable in DuckDB WASM in the browser.

### Ingestion Action failures checked on 8 September

The owner's follow-up asked to check several hours of failed ingestion before other work.
The fresh run/job logs establish distinct causes:

| Failure evidence | Actual cause | Resolution / current evidence |
| --- | --- | --- |
| [34240148821](https://github.com/rutkala/zohelo-data/actions/runs/34240148821), [34241326444](https://github.com/rutkala/zohelo-data/actions/runs/34241326444), BDL | A valid parent-subject listing returned all 21 children despite `pageSize=20`; the parser rejected it. | Fixed by `1f841d6`; the later all-source successful run accepted BDL pages with zero failed requests. |
| 34241326444, WDI | Drive reported a ZIP media MIME type, and immutable raw verification rejected it. | Fixed by `75c1afd`; the complete 282,845,220-byte archive is accepted and freshly restored. |
| 34241326444, Eurostat | `RemoteDisconnected` on one API history request. | Retained progress was published; later runs resumed with zero pending retries. |
| [34245632263](https://github.com/rutkala/zohelo-data/actions/runs/34245632263), [34246624216](https://github.com/rutkala/zohelo-data/actions/runs/34246624216), WDI | Historical API reads timed out at 90 seconds (`BX.GSR.CCIS.CD` and `BX.GSR.INSF.ZS`). The bulk archive and other provider jobs succeeded. | Subsequent run 34252801846 succeeded for all three providers. This correction adds one bounded, quota-reserved in-run transport retry with retained failure receipts and separate recovered/unrecovered counts. |

The source workflow runs at minutes **7 and 37 UTC** with separate serialized WDI/BDL/Eurostat
jobs and a **60-minute job timeout**. Normal API collection uses three cycles, up to twelve
requests/240 seconds per cycle and a 900-second aggregate between-operation budget. WDI/Eurostat
run the first API cycle, then up to 24 full-distribution requests/1,200 seconds, then remaining
API cycles after successful collection/verification. Provider quotas and cooldowns apply to
every path. Work can span the next schedule; an older pending job may be superseded while the
active source writer finishes its checkpoint. A red workflow can contain successful provider
jobs and durable published increments. It is not an instruction to restart all data.

NBP is separate: its daily **02:00 UTC** [run 34179015910](https://github.com/rutkala/zohelo-data/actions/runs/34179015910)
succeeded on 8 September. Manual/configuration guidance remains in [campaign operations](source-campaign-operations.md).

**Status meanings:** **Done** = delivered and evidenced; **In progress** = active work or verification; **On hold** = explicitly excluded for now with a reason and reopening condition; **Waiting for input** = an owner choice; **Not started** = future implementation; **Cancelled** = intentionally retired.

| Deliverable | Status | What this means |
| --- | --- | --- |
| D1. Repository, architecture and all Actions audit | **Done** | All nine original workflows and the repository/data architecture were audited. Repairs are merged and tested; live acceptance passed. [Audit](audits/2026-09-07-readiness.md), [all workflow decisions](audits/2026-09-07-workflows.md). |
| D2. Reproducible environment and shared instructions | **Done** | Pinned Python/Node dependencies, fresh-container checks and standard human/agent instructions exist. Audit updates preserve the supported npm/Python path. |
| D3. Source contracts, correction evidence and gold design | **Done** | Official definitions, declarative ingestion policy and correction evidence are documented, implemented and tested. The new detection rules ran successfully in the verified live release. |
| D4. NBP end-to-end including semantic queries | **Done** | All 15 tables and five daily metrics are published. Fresh native queries matched gold; raw replay matched 1,329,363 rows exactly. The complete capacity inventory is below current warning thresholds. |
| D5. One data catalogue with source and metric lineage | **Done** | Five real metric definitions and source methodology/frequency/reuse metadata are in the matching release artifacts. The native viewer passes discovery and semantic-to-physical-lineage browser checks. |
| D6. Define and onboard additional sources | **In progress** | Research and the [implementation plan](source-expansion-plan.md) cover 182 families and 231 categories. ADR 0006 requires complete selected products. PRs 80–82 implement full WDI/Eurostat distributions and scalable BDL paging; the portal supports response tables and archive indexes. The complete WDI CSV archive is accepted, indexed and freshly verified. Eurostat and BDL backfills remain incomplete. Broader onboarding and new-source Bronze/Silver/Gold/semantic models remain open. [Measured coverage and validation](releases/2026-09-08-complete-source-correction.md). |
| D7. Portal UX and shared desktop/mobile theme | **Done** | Narrow palette alignment and identity/toolchain cleanup passed both deployment builds and all browser checks. Deployment evidence is linked below. |
| Working agreement for the Zohelo-data subproject | **Done** | [Collaboration instructions](collaboration.md) define focused chats, one GitHub status record, Drive data artifacts and human-operable handoffs. |
| Completed one-time migration workflow / old executable builders | **Cancelled** | Migration evidence and read-only comparison script remain; obsolete workflow and direct mutating CLI paths are retired. |

## Explicit holds and reopening conditions

These are visible limits, not claims of completed functionality. None requires inventing an owner methodology answer to finish the five daily source metrics.

| ID | Held work | Why / when to reopen |
| --- | --- | --- |
| H-CAP | Reference-safe state compaction and automatic archive/release deletion | Current policy retains all evidence. Capacity reporting warns at 70% of any build/state bound. Design/test compaction before that threshold or before onboarding a source that would cross it; never raise caps or delete old files blindly. |
| H-SERVE | Always-on/multi-user semantic API | On-demand native MetricFlow and CSV are the present supported experience. Reopen for a named BI integration, availability need or shared-service use case with hosting/authentication requirements. |
| H-DERIVED | Spread, period average, return, conversion and other derived metrics | Optional business calculations need an intended use and explicit aggregation/missing-date rules. Source-defined daily values proceed without them. |
| H-FX | Exhaustive historic currency identity/unit validation | Sampled official comparisons support API normalization; values remain unchanged. Validate the specific currency/history before governed conversion/accounting use. No PLN redenomination transform applies to the post-2002 API. |
| H-REUSE | Commercial redistribution of NBP data | Official material establishes public read access but did not establish the complete commercial redistribution/attribution grant. Resolve terms for the intended sharing product before releasing that feature. |
| H-HOST | Commercial hosted service on GitHub Pages | Pages has commercial-hosting restrictions. Select suitable hosting before turning the owner project portal into a commercial data/SaaS service. |
| H-GIT | Server-side branch protection/rulesets | The branch API reports `main` unprotected; the connector cannot administer this setting. Maintain reviewed PR/check discipline and configure enforcement before adding collaborators or depending on automatic protection. |
| H-REV | Whole-publication disappearance and transformation-only row-delta ledger | Comparable currency omissions are detected; absent publications are ambiguous. Immutable raw/releases and code/contract versions remain evidence. Reopen for a real anomaly or a model migration requiring explicit cross-release deltas. |
| H-SCALE | Broad portal feature pruning, module/package restructuring or orchestration/storage migration | The owner asked to keep the portal stable. Reopen against measured performance/maintenance needs or a selected source benchmark; no speculative framework replacement. |

## Owner decisions

**Q-LIVE / H-LIVE are resolved:** after the explicit production scope was presented, the owner replied **“Yes, I approve”** on 7 September 2026. This authorizes publishing and verifying the new NBP release in Google Drive, retaining previous releases and resuming daily ingestion. The approved live run passed and the daily schedule is restored; no further approval is pending for this delivery.

**Q-M1 is resolved for the baseline:** the 7 September instruction authorizes researching and implementing source-defined daily NBP observations. Five definitions are documented in [NBP methodology](nbp-business-definitions.md). It is no longer a blocker. Optional derived metrics are H-DERIVED.

**Q-S1 is resolved:** on 8 September 2026 the owner delegated source selection, implementation,
connection tests and production intake, targeting all feasible scope and recoverable history.
The owner requested stable domain/category coverage, granular FMCG/markets, media and sport,
and discovery of additional sources for gaps. [ADR 0004](decisions/0004-autonomous-source-onboarding.md)
records this authorization. There is no source-by-source review gate. New paid services,
paid-account access and unresolved consequential business definitions remain concrete future prerequisites. [ADR 0005](decisions/0005-agile-landing-and-source-access.md) adds free-account onboarding and a secure key setup path; free opportunities must be researched rather than deferred without action.

**Q-ACCESS-001 is resolved:** on 11 September 2026 the owner supplied and configured `GUS_BDL_API_KEY` in Actions secrets. The registered BDL profile is active with expanded 4x rate limits (400 requests/15m, 40,000/week) and X-ClientId transport headers.

**Decoupled per-source pipelines & ADF-style orchestration, 11 September:** Decomposed monolithic matrix into independent source-tailored pipelines:
- `GUS BDL` (`.github/workflows/source-gus-bdl.yml`): Runs every 15 minutes (`*/15 * * * *`) with up to 360 requests/run (6 cycles of 60 requests) to maximize collection under the registered 400 req/15m window. Features ADF-style chained stages (`ingest_landing` -> `platform_transform_and_release`), with support for on-demand `transform_only` execution without re-ingesting.
- `Eurostat` (`.github/workflows/source-eurostat.yml`): Hourly schedule (`12 * * * *`) for bulk distributions and API collection.
- `World Bank WDI` (`.github/workflows/source-world-bank.yml`): 6-hour schedule (`25 */6 * * *`) for bulk CSV and API indicators.
- `OpenData.org` (`.github/workflows/source-opendata.yml`): Dedicated streaming loader workflow (`src/ingestion/sources/opendata_bronze_loader.py`) that reads the 21.38 GB Senzing archive via seekable HTTP Range streams on Google Drive, flattens entity features into typed Parquet (`br_opendata_organizations`, `br_opendata_locations`, `br_opendata_people`), and writes to `02_bronze/opendata_org/` with zero disk extraction in Codespaces/CI. Checkpointing is tracked in `06_control/source_campaigns/opendata_org_bronze/checkpoint.json`.
  - **OpenData Bronze Portal Availability (Verified 11 September):** Published 867,322 organizations across 5 Parquet files (48.4 MB total) to Google Drive in `02_bronze/opendata_org/organizations/` with manifest `manifest-0bfcf711-973c-4562-bbcd-fc267a8b7cd2.json` and pointer `06_control/source_campaigns/opendata_org_bronze/current-landing.json`. Integrated into Portal Lakehouse Explorer under the `02_bronze` folder with DuckDB WASM view registration (`"02_bronze"."br_opendata_organizations"`). Deployed to production (`https://data.zohelo.com`).
- All four decoupled pipelines launched concurrently in production via serialized execution.

## Evidence

**Complete-source correction deployed; backfills in progress, 8 September:** the owner rejected the partial
production scope. Inspection found permanent WDI/BDL discovery ceilings, Eurostat's three
filtered starter datasets, repeated whole-state writes and Drive user-rate-limit failures.
[ADR 0006](decisions/0006-complete-selected-source-coverage.md) records full selected-product
acceptance. [PR 80](https://github.com/rutkala/zohelo-data/pull/80) merged as `1287f65`,
[PR 81](https://github.com/rutkala/zohelo-data/pull/81) as `1f841d6`, and
[PR 82](https://github.com/rutkala/zohelo-data/pull/82) as `75c1afd`.
Final [data CI](https://github.com/rutkala/zohelo-data/actions/runs/34245075797) passed 347 tests;
[portal CI](https://github.com/rutkala/zohelo-data/actions/runs/34240810496) passed both builds,
686 unit tests and 14 browser flows per base path. The portal deployed at `1287f65`;
the later fixes affect backend collection, storage verification and workflow ordering.
The [corrected production run](https://github.com/rutkala/zohelo-data/actions/runs/34245632263)
accepted and published the 282,845,220-byte WDI archive and passed fresh full-distribution
verification. Its six CSV members include 396,970 country–indicator rows with 1960–2025
year columns. BDL has 228 accepted/published responses with fresh verification passed and
a provider-issued rate-limit delay. Eurostat's data and codelist inventories now identify
16,964 distributions; the first complete dataset `AACT_ALI01` and its structure are accepted
and indexed alongside those two inventories. Its full collector remains active. These are dated
observations, not a claim of complete Eurostat/BDL or new modeled coverage.
[Detailed production evidence](releases/2026-09-08-complete-source-correction.md).

Agile source delivery, 8 September: [PR 77](https://github.com/rutkala/zohelo-data/pull/77) merged after [252 data tests](https://github.com/rutkala/zohelo-data/actions/runs/34221334104). An immediate earlier batch added 29 responses; the improved [production run](https://github.com/rutkala/zohelo-data/actions/runs/34221713083) then added 81 through three collect/publish cycles per source, leaving 178 accepted responses fully published and freshly verified. Half-hour triggers continue resumable collection. [PR 78](https://github.com/rutkala/zohelo-data/pull/78) passed two production builds, 685 unit tests and 14 browser flows per base path in [CI 34223422928](https://github.com/rutkala/zohelo-data/actions/runs/34223422928); [deployment 34223857331](https://github.com/rutkala/zohelo-data/actions/runs/34223857331) succeeded at `40844c3bd30aa84a3cec7b4e6fdb5a5995c05975`. Production data was verified through fresh Actions processes and portal SQL through browser fixtures; the cloud browser had no owner Drive session for an authenticated live data query. [Detailed snapshot evidence](releases/2026-09-08-agile-landing.md) separates response rows from facts, pending collection from pending publication, and registered support from an acquired key.

Audit acceptance release: `96b14b78-dc36-4422-952c-5fb3af726ac8`, producer [`8f29a0b`](https://github.com/rutkala/zohelo-data/commit/8f29a0b98876f6ae6b161aae3ec5be5cbeb247cb). [Successful publication, fresh metrics, raw replay and health run](https://github.com/rutkala/zohelo-data/actions/runs/34167068188). It contains 15 tables and 429,795 cleaned observations. Previous releases remain retained.

Latest scheduled release checked on 8 September: `8d08c7b4-f9e3-485c-acfc-b194fc180b08`, producer [`b0eb886`](https://github.com/rutkala/zohelo-data/commit/b0eb88698c74687147764c0496938a90dd0dfd7b). [Scheduled run 34179015910](https://github.com/rutkala/zohelo-data/actions/runs/34179015910) passed publication, fresh SQL/native MetricFlow and health checks, with 15 tables and 429,841 cleaned observations. All four feeds were checked through 7 September. No capacity warnings were emitted. Routine scheduled runs do not repeat exact raw replay; the audit acceptance release above retains that separate proof. Later daily runs may supersede this dated operational observation.

Desktop-menu follow-up, 8 September: [PR 70](https://github.com/rutkala/zohelo-data/pull/70) removed hover-only hiding from the shared table ellipsis, so published tables and Browser workspace relations expose the same visible menu on desktop and mobile. [Validation run 34196021287](https://github.com/rutkala/zohelo-data/actions/runs/34196021287) passed both production builds, 678 unit tests and 14 browser flows per deployment base, including non-hover visibility at desktop, phone and 768px widths. [Portal deployment 34196368805](https://github.com/rutkala/zohelo-data/actions/runs/34196368805) succeeded at commit `dc9f1283dd4153e40122e99c15fdff5568c52af8`. This portal correction did not run production ingestion or publish data. The resumed-chat audit found no unfinished essential deliverables, open PRs or active/queued Actions before this follow-up began; no separate AI monitoring task was started.

[Audit delivery evidence](releases/2026-09-07-platform-readiness.md) records the live results plus 138 data tests, 678 portal unit tests and 14 browser flows for each deployment configuration. The full project inventory is 156,260,745 bytes; the highest used build/state bound is 17.92%, below the 70% review threshold. These are measured observations, not future capacity guarantees.

[Current architecture](architecture.md) explains tool choices and commercial/service boundaries. [Operations](nbp-platform-operations.md) gives the Actions and command-line path without AI. [Working agreement](collaboration.md) explains how to start a focused new chat and resume from this record.

## Approved scope by deliverable

### D1. Platform, repository and GitHub Actions audit

Audit business goals, architecture, code quality, security, workflows, effective Drive boundaries, and cost feasibility. Keep a workflow and requirements map, repair OAuth/workflow/stage-handoff/publication failures, and base claims on evidence.

### D2. Reproducible development environment and AI instructions

Provide a fresh Codespaces path for bounded local data and portal checks with explicit Python and Node versions, locked dependencies, shared agent instructions, and visible side effects. Opening the environment must not run production pipelines or an unrestricted agent.

### D3. Data contracts, ingestion rules and gold dimensional design

For A/B/C FX rates and gold prices, document row grain, keys, units, date roles, corrections, deduplication, source batches, watermarks, overlaps, retries, schema handling, catch-up, and rebuild from retained raw. Use current validated values for normal analysis while retaining raw versions and detected changes; a historical-comparison UI is outside scope. The gold design must state fact/dimension grain and business aggregation rules rather than invent them.

### D4. Complete NBP end-to-end

Make all four NBP datasets, existing history, and agreed catch-up range available through bronze, silver, modeled gold, semantic definitions, and the portal. Record inputs for replay, identify unavailable legacy raw inputs, keep SQL and any semantic service on the matching release, restore a release in a fresh process, and preserve the prior complete release if publication fails.

### D5. Business data catalogue

The catalogue covers the connected release: sources and datasets (description, licence/reuse terms, coverage, frequency, status, and identity); release-specific lineage from source through gold and any approved semantic models; and approved metrics with definition, formula, unit, dimensions, time aggregation, approval state, and version. It distinguishes last attempt, last successful ingestion, latest observation, and latest validated publication. It must let the owner discover data and trace an approved metric to physical data and validated publication without GitHub or code. It must not contain project-management or owner-review content.

### D6. Source expansion research and onboarding

Maintain a business-led, prioritized onboarding path. A source must be public, free to obtain, and permitted for the intended commercial use; record attribution, redistribution, and dataset-specific conditions. For a chosen source, add contracts, fixtures, extraction, lineage, recovery, release controls, and testing without weakening the NBP platform boundary.

**Scope clarified 8 September 2026:** [ADR 0003](decisions/0003-source-expansion-scope.md)
records worldwide multicountry coverage, detailed Polish sources, accepted overlaps,
English documentation, recent collection alongside available historical backfill, and an
appropriate silver/gold/semantic role for each admitted source. Research also includes
paid/restricted candidates; initial production eligibility above is unchanged. No subscription
or implementation is authorized merely by a research row.

**Research delivered:** [the source landscape](source-research/README.md) has 182 stable
product/family IDs, including NBP, with 312 source/reference links to 285 official URLs.
134 entries have reviewed access documentation; 48 retain catalogue-only evidence. The
JSON/CSV inventory records access, cost, rights, history, updates, keys, proposed loading
and modeling, overlap, unresolved conditions and reference depth. Six sector reports,
a coverage matrix, a reviewed ingestion/modeling proposal and bounded future-test cards
support selection. Schema, IDs, evidence labels, exports and documentation links were
checked. Documentation research is not a successful connection or production acceptance.

**Implementation now authorized:** [ADR 0004](decisions/0004-autonomous-source-onboarding.md)
supersedes the owner-selection gate. The versioned taxonomy has 15 domains, 45 subdomains,
231 categories and 17 analytical dimensions, independent of official classification editions.
All 182 inventory IDs have candidate mappings; these do not establish dataset or ingested coverage.
The plan records 15 thematic gaps and seven additional candidates. Across all candidates, 177 of 231 categories have a research lead and 54 have none; neither number measures ingested coverage.

**First delivery boundary:** independent WDI/BDL/Eurostat Landing campaigns with exact raw
responses, durable recent/history queues, persisted quotas and source-scoped Drive state.
[Operations](source-campaign-operations.md) documents the bounded schedule, controls and capacity
limits. [Production run 34217059540](https://github.com/rutkala/zohelo-data/actions/runs/34217059540)
passed for all three providers and their fresh-process restore/replay steps at producer
`7e43ccf1de25fdc8207f21f29861e6e3eb5402a5`. Cumulative accepted responses were WDI 13,
BDL 22 and Eurostat 33; these include metadata and repeated representations, not unique facts.
Each source advanced recent and historical queues, with zero pending retries at this checkpoint.
The corrected implementation passed [222 full-suite tests](https://github.com/rutkala/zohelo-data/actions/runs/34216699560).
[The dated evidence record](releases/2026-09-08-source-campaigns.md) preserves initial failures,
repairs, run measurements and the bounded replay samples.

**Agile delivery correction:** the owner rejected inaccessible Landing and four-hour gaps between
small batches. A new immediate run of the existing workflow completed successfully, advancing
cumulative accepted responses to WDI 21, BDL 31 and Eurostat 45.
[ADR 0005](decisions/0005-agile-landing-and-source-access.md) implements consecutive collection
and publication, independently verified/queryable Landing snapshots, and the
[free-account and secure-key setup](source-accounts.md). Production collection and fresh
Landing verification passed; [portal deployment](https://github.com/rutkala/zohelo-data/actions/runs/34223857331)
also passed. Browser SQL behavior was verified with fixtures; an authenticated owner-session
browser query was not performed. The complete-source correction and current coverage are
recorded above.

**Following increments:** source-specific dbt Bronze/Silver and then Gold/semantics using only
the references needed for each slice. Broader source waves can progress alongside those models.
Scalable state, physical retained-byte accounting and reference editions are implemented when
needed for the next admitted scope, with measured limits preserved. Full-scope completion
remains open; only configured ingestion jobs continue automatically after a delivery turn.

### D7. Data-first portal and SQL experience

Deliver a focused data-first portal update: one native dbt catalogue for published data, lineage, and metric definitions; no project-management or owner-review UI; and SQL that automatically loads every released table referenced by a statement, including joins, unions, and CTEs. Keep published data-file downloads within 64 MiB per engine session and explain a limit failure before execution. Catalogue artifacts have a separate 16 MiB limit; these download limits are not guarantees about query memory use. A modern palette and layout may support this bounded experience. Portal validation, CI, and deployment are required before any of this new behavior is described as live.

## Cross-cutting acceptance requirements

Across D1–D5: verify business examples and representative fixtures; use the same inputs, cutoff, and revision policy for full/incremental equivalence; prove safe versioned publication and raw rebuild; expose release, provenance, status, errors, and source batches clearly; and measure operating limits without assuming a date, cost, SLA, or unlimited free capacity.
