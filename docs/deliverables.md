# Zohelo-data delivery plan

Approved work programme, 6 September 2026. The owner approved this scope; that approval did not settle the business or architecture choices recorded below. The initial user is the owner. The NBP scope is all already-ingested history for Tables A, B, C and gold prices.

## Current status

### BDL checkpoint-save recovery and ten-proxy restart — 24 September 2026

The owner requested BDL ingestion only: repair interruptions, clean stale runtime
state, and resume the last confirmed checkpoint with ten existing VPN/IP workers.
No native reset, downstream processing, new source or Drive reorganization is in scope.

Before changes, the stable Drive queue had SHA-256
`871d3a13dccb0481562674760f09ce720e82edaa243b94963167680c54058af8`:
2,420 subgroups, 2,075 selection-complete, 278 partial, 67 failed, 69 blocked
selections, 4,814 checkpoint-reported files and 1,287,389,217 bytes. All 2,420
referenced plans matched the unique control-object identities and stored hashes.
The nine interrupted plans were independently downloaded, checksum/structure
verified; all 551 native objects referenced by their landed receipts matched
Drive size/MD5/SHA-256 metadata. Native archive content was not re-downloaded or
parsed. The stale lock's PID 1343576 and all BDL browser workers were absent;
no BDL Actions writer was active. All ten existing proxies (ports 8081–8090)
were reachable, returned ten distinct egress IPs and reached BDL with HTTP 200.

The repair reconciles lost checkpoint-write responses through bounded read-only
checks of unique namespace, stable metadata and exact candidate bytes. It never
blindly repeats a create/update. An unresolved, damaged, conflicting or denied
operation still stops safely; the uncertain object rejects further writes.
Recovery emits a log event, not an additional Drive object. Completed receipt
validation now checks an isolated pending-state copy rather than rejecting an
already-landed node, preventing unnecessary recollection of valid completed work.
Native metadata transport failures remain control failures, not source retries.

`--require-proxy-count 10` verifies ten distinct configured localhost proxies
before constructing storage and prohibits silent direct/reused-proxy fallback.
The monitor accepts explicit BDL workspace/runtime overrides so it no longer
reports the obsolete September 21 summary; DBW monitoring behavior is unchanged.

A further transport review found that the worker environment contained proxy
settings but Playwright launches did not explicitly use them. Both selection and
catalogue browser workers now take a validated `BDL_WEB_PROXY` through Playwright's
proxy option. A pure Node regression validates all ten routes and rejects malformed
or remote proxy URLs. Real browser egress verification remains required before
resuming the ten-route operation; prior environment settings alone are not proof.

Validation so far: 27 focused recovery/receipt/proxy/monitor tests and 52 existing
BDL Web tests pass; the pinned Python environment passes pip check. Full merge CI
and actual resume/upload verification are still pending at this code handoff.
An initial Copilot patch was reviewed and corrected by the lead; no implementation
agent is continuing. All old workspaces, native payloads and checkpoints are retained.
The last writer's exit cause is not independently established; earlier logs prove
Drive plan/queue save timeouts. This repair does not claim full historical ingestion.



### Direct Landing and whole-table Bronze access — owner direction, 23 September 2026

**Requested outcome, not yet implemented:** every native object under the existing
project Drive Landing area must be discoverable in portal Landing without a modeled
release or a Bronze build. Every existing Bronze dataset must be accessible from its
Drive files as a complete logical relation. In particular, ordinary SQL against
`"02_bronze"."br_dbw_observations"` must not require an indicator or physical-part
selection. Keep native Landing bytes unchanged; Bronze remains source-shaped, Silver
is canonical reusable data for Gold and data science, and Gold contains modeled
facts/dimensions. NBP's layer responsibilities are the reference, not its eager
browser-download implementation. No new ingestion, whole-dataset publication copy,
folder reorganization or paid service is authorized by this adjustment.

**Execution evidence for this attempt:** current main was `9366ffdb5eee8798f434eee92249eedc996566bd`;
PR #157 was still open and the completed DBW production run remained distinct from
this new reader work. An isolated worktree and its pinned portal dependencies were
prepared. The agent preflight and two distinct authenticated-reader implementation
writes were rejected by the tool safety check before execution. The proposed
ServiceWorker range relay and disk-backed authenticated read adapter were not
created. Do not retry those rejected operations via another tool, encoded payload,
agent, or identity. One unused standalone hashing scratch file was removed; no
application code or workflow change remains from this attempt. Only this delivery
record is changed. No Drive payload, release pointer, ingestion process or live
portal was modified. No implementation owner or background continuation is running
for this task.

**Acceptance remains open:** recursive native-file discovery must exhaust pagination
and distinguish readable files, archives and unsupported formats; supported formats
must be explorable without durable transformation. Whole-dataset SQL must preserve
all rows, including duplicates, and exact snapshot membership, never silently return
only selected indicators. Test LIMIT, full counts, multi-indicator filters, joins,
auth expiry, missing/changed files and cold-session resource use. Remove the DBW
selector requirement only with a working complete-table reader; deleting the guard
alone would substitute the current eager-download/512-MiB failure for the selector
error. Preserve existing references and byte-integrity checks. State resource and
format limitations explicitly rather than claiming every query is instantaneous.
The old indicator-based browser test is not acceptance of this expanded request.


### DBW original-file registration repair deployed — 23 September 2026

[PR #155](https://github.com/rutkala/zohelo-data/pull/155) merged as
`2df5b04300706fbe1eacd3d7907e107535b26b78` after full data CI 35799088034,
portal CI 35799088047 (both base paths) and DBW validation 35799088053 passed
at exact head `8f13186b69f025195a24293f7d078d245b85e794`. Checked and merged
trees are identical. Portal deployment 35800224013 passed, and the actual
portal build reports the merged revision and retained-Bronze formats 1 and 2.
The new real-browser fixture queries all four DBW relations using original-file
references; that is fixture evidence, separate from live production data acceptance.

The repaired publication-only store does not initialize Landing and rejects source
response writes. It registers bounded existing Bronze files by their original Drive
IDs and SHA-256 rather than uploading copies. The saved reviewed inventory has 1,517
observation files within the existing 8 MiB bound and 33 above it. Existing query
parts remain reusable; only missing oversized-file parts are written under
`02_bronze/gus_dbw/query_parts/<inventory-sha256>/`. Prior snapshots and files stay
preserved. Cold restore and fresh consumer verification use four independent
read-only clients. This does not recollect DBW or build Bronze from Landing.

The cancelled run's log explicitly confirms termination of publisher PID 7537,
matching its exact Git claim. The original input inventory and the old partial
snapshot were rechecked unchanged. The abandoned Git claim was released only with
its exact expected-SHA lease; no Drive owner or data file was changed by that step.
The optional exact Drive-owner recovery is requested through the repaired Action,
under a new operational claim. Codespace enumeration was unavailable with the
current token scope; the actual claim owner was proved terminated in Actions.

The existing workflow is now named **Publish existing DBW Bronze**. Only the repaired
route was enabled after the compatible reader deployment. Run
[35800626942](https://github.com/rutkala/zohelo-data/actions/runs/35800626942) was
actually dispatched on main at 00:07:25 UTC, pinned to `2df5b0`, with the exact
abandoned Drive owner. It continues registration, required large-file preparation,
full independent data verification and live portal SQL checks. This supersedes
the execution hold on the old implementation, not any data acceptance requirement.

At dispatch no new DBW data publication or live SQL acceptance was established.
Read this specific run and its receipts before upgrading the status or starting
another writer. BDL, the dirty original checkout and original Drive payloads were
not changed. The immediate target remains the entire dated retained inventory;
current-provider completeness and native-to-Bronze lineage remain unresolved.
See the [integration receipt](audits/2026-09-23-dbw-direct-registration-integration.json).

### BDL actual Landing visibility check — 23 September 2026, 00:48 Warsaw

The owner reported no visible new BDL files for several hours. Read-only diagnostics
found the same ten-worker coordinator, PID `383069`, using pinned runtime `5097e7e`.
The local summary at 22:47:38 UTC on 22 September reported 909 new files in this run,
439,928,781 new bytes, 1,899/2,420 selection-complete subgroups and 63 blocked selections.
A separate stable Drive queue/owner read at 22:48:22 UTC reported 3,328 native files /
1,068,939,192 bytes, ten in-flight subgroups and heartbeat 22:48:14 UTC. Compared with
the 20:11:07 UTC checkpoint, these counters increased by 263 files / 156,758,335 bytes.
Counters remain transfer-checkpoint evidence, not a new full-byte or business-content audit.

Independent connected-Drive metadata located the actual native archive
`NARO_2889_CREL_20260923004330.zip` (89,434 bytes), created at 22:47:33 UTC on
22 September (00:47:33 Warsaw on 23 September). Its verified parent chain is
`01_landing/gus_bdl/web_bulk/P2889/0f4dd34309878d2ad27e91eb484e53ef0ec6385f4ad0cf18cc6114f4f4a60885/`.
The subgroup folder's modification date remains 17 September and `web_bulk` remains
12 September despite that new descendant. Thus these parent dates are not reliable
evidence of the last file upload. This is a concrete visibility explanation, not a
claim to have reproduced the owner's exact Drive view.

See [the evidence receipt](audits/2026-09-23-bdl-landing-visibility.json).
No BDL process, source setting, checkpoint, data object or folder was changed. No
replacement writer was launched. The additional terminal read of individual plan
payloads was tool-blocked; it was not retried, and no new archive-byte verification
is claimed. The earlier local reporting gap remains unexplained. Full native scope,
521 not-yet-complete subgroups, blocked-selection recovery and later layers remain open.
The separate DBW publication execution hold is unchanged.


### DBW publication stopped after unexpected Landing creation — 23 September 2026 (Warsaw)

**Current operation hold:** the owner reported that the Bronze release Action created
another DBW namespace in Landing and that they deleted it. The lead cancelled
[run 35783126258](https://github.com/rutkala/zohelo-data/actions/runs/35783126258);
GitHub confirmed cancellation at 22:22:40 UTC on 22 September (00:22:40 Warsaw on
23 September). Only `DBW retained Bronze release` is now `disabled_manually`.
No DBW replacement run was started. BDL and other source operations were not changed.

**Root cause in the executed code:** `DriveCampaignStore.__init__` eagerly creates
both `06_control/source_campaigns/<source_id>` and
`01_landing/<source_id>/responses`, even for this Bronze-only publisher. The retained
publisher then writes its Parquet copies/fragments, indicator indexes and manifests
under the control namespace's `landing_publications`, not under that responses folder.
Using the ingestion store introduced an unnecessary Landing side effect. Publication
is not metadata-only: existing Bronze files are read, large files repacked, and
publication copies written. No source recollection or Bronze-from-Landing build occurs.

**Post-stop evidence:** the deleted `01_landing/gus_dbw_retained_bronze` folder and its
empty `responses` child are in Trash. The original 3,103-object input inventory,
including all 1,550 observation partitions, still matches reviewed inventory SHA-256
`15f587a7d0631befac394e6cc1d0183fb0f564801f144b7d6ca340e8d092a12e` and
4,803,673,234 bytes. This is a metadata/descriptor reconciliation, not another full-byte audit.
The stable current publication is `37464abe-8c54-4610-a215-b5bbdb6f2529`: 160 published
indicators, 1,390 pending, and 22,010,748 observation rows. The publication folder contains
300 objects (260 Parquet and 40 JSON), totaling 440,616,632 bytes; these can include
not-yet-promoted files. The final independent fragment/browser acceptance did not run.
See [the containment receipt](audits/2026-09-23-dbw-publication-containment.json).

**Preserve and repair, do not restart unchanged:** the Git operational claim and Drive
owner record remain held following cancellation. They were not cleared; abandoned-owner
recovery must follow ADR 0010 after writer-stop and reference reconciliation. Preserve
all original Bronze/native data, completed publication fragments and the current partial
pointer. Do not recreate the deleted Landing folder, recollect DBW, delete referenced
publication files, or silently call the current 160-indicator snapshot complete.

**Next engineering boundary:** remove Landing initialization from publication-only
storage; evaluate direct registration of verified immutable existing Bronze files to
avoid unnecessary duplicate uploads; retain bounded derived parts only where the reader
needs them. Reconcile the chosen design with the common release lifecycle, then verify
all intended indicators and actual portal SQL before enabling a repaired release route.
No replacement implementation or full Bronze portal acceptance is claimed here.

### DBW Bronze Actions release path — 22 September 2026

The owner explicitly requested completion of DBW Bronze SQL availability in the portal.
[PR #151](https://github.com/rutkala/zohelo-data/pull/151) adds the missing main-only
Actions path: exact reviewed input restoration from Drive, serialized retained-Bronze
publication to all 1,550 indicators, a separate fresh read-only consumer checking every
published fragment, and real SQL against all four Bronze relations in data.zohelo.com.
See [the operation and acceptance runbook](dbw-retained-bronze-release.md). This replaces
the unfinished preflight-only implementation plan, not the source/lineage boundaries.

The selected scope is the dated audited retained inventory: 879,999,727 observation rows,
8,358,612 dictionary rows, 1,550 taxonomy rows and 1,531 metadata rows. Publication must
retain the unresolved native-to-Bronze lineage and source-completeness labels. It does
not authorize a new ingestion, Silver/Gold/semantic processing, or modification of BDL.

**Implementation merged:** PR #151 merged as `ea39f7ab9b3b8ae45aff1bd14260a3c0eb9bae96`
after [full data-platform CI 35781698355](https://github.com/rutkala/zohelo-data/actions/runs/35781698355)
and [DBW release validation 35781698395](https://github.com/rutkala/zohelo-data/actions/runs/35781698395)
passed at exact head `a9a0870d213cfc871a821cae98de0c976814033d`. The checked head and
merged commit have identical tree `63eca46036034d733c06a42a9be466ed51c06345`.
PR testing and production publication are separate workflows; the production writer
retains the mandatory main guard, operational claim and `queue: max` serialization.

**Production operation actually started:** [DBW Bronze release run 35783126258](https://github.com/rutkala/zohelo-data/actions/runs/35783126258)
was dispatched on `main` at 20:52:50 UTC (22:52:50 Warsaw), pinned to the merged code SHA
above and the existing production Drive root. It includes code validation, full reviewed
input restoration, publication until all 1,550 retained indicators are covered, and a
separate fresh native/browser consumer job. The initial observed phase was validation;
no publication completion or live SQL success was established at dispatch. Read this
specific run and its evidence before repeating an operation or upgrading the status.
This was an active external Actions operation; it is now cancelled and held as recorded above.
A successful PR test, input preflight, publisher process or native restore alone is not
portal acceptance. Only the final real-browser receipt establishes that boundary.
No local developer cache, checkout clean-up, new credential, paid overage or BDL restart
is required by this release procedure. Existing operational ownership claims must never
be removed automatically or bypassed merely to obtain a successful run.


### DBW retained-query integrity repair — 22 September 2026

Direct engineering resumed in a clean worktree based on `59dc6f9`. Two defects were reproduced with credential-free fixtures: an observation partition with a wrong, null or mixed `indicator_id` could be indexed under another indicator; and the oversized-fragment fallback bound COPY parameters in the wrong order, failing with `read_parquet(INTEGER)` instead of writing the requested slice.

The repair adds a one-column, vectorized indicator-identity check before publishing each new observation partition, binds COPY arguments by name, explicitly preserves order, and cleans disposable fallback spill directories on success and failure. It does not change native bytes, row meanings, schemas, audit hashes, publication ownership or browser size limits. Previously published fragments are not retrospectively declared row-identity-verified by this new-input check; fresh consumer/data acceptance remains required.

**Code repair accepted:** [PR #149](https://github.com/rutkala/zohelo-data/pull/149) merged as `5c1796b10bc9690a25c2200e5ab26bf150231e57` after [full data-platform CI 35729357684](https://github.com/rutkala/zohelo-data/actions/runs/35729357684) passed at exact head `667afa87b4d3d2c3db0201d4ec7dd17b47efa0f4`. Fifteen focused retained-publication/locking tests passed locally, including five new regressions for invalid identity, prior snapshot preservation, exact nonzero-offset slicing, actual bounded repacking and failure cleanup. The merged tracked tree was verified identical to the checked PR head; the clean task branch and worktree were removed.

**Real retained-input verification:** at 12:51:52 UTC, all 1,550 local observation partitions passed exact size/SHA-256, expected schema and non-null indicator-ID binding checks: 879,999,727 rows across 4,737,200,817 observation bytes, zero failures, 196.208 seconds. [Machine-readable evidence](audits/2026-09-22-dbw-input-identity.json). This is the complete dated observation cache, not a fresh Drive re-inventory, current provider completeness or native-to-Bronze lineage proof.

**Real bounded repacking:** retained indicator 573 (2,512,958 rows; 8,545,221 source bytes) was repacked into three fragments of 4,532,425, 4,537,176 and 351,238 bytes. Every fragment is below 8 MiB; typed ordered-row-stream equality and unchanged source SHA-256 passed. Disposable repacked files were removed, not uploaded. [Evidence](audits/2026-09-22-dbw-repacking.json). This is one real partition, separate from the forced-fallback regression fixture, not all-source repacking acceptance.

**Publication remains open:** a read-only connected-Drive traversal of the selected root through `06_control/source_campaigns` returned 17 direct child folders and no `gus_dbw_retained_bronze` namespace on 22 September. This is a connector-visible metadata observation, not a transactional release audit or authenticated portal SQL. No production publisher or BDL process was launched, stopped or changed. The earlier production-launch hold was not retried or rerouted. Actual immutable query publication, fresh consumer verification and live portal SQL still need completion. The full critical user outcome is not Done merely because this repair merged.

### Reconciled autonomous programme — 22 September 2026

**First local integration package merged:** PR #148 merged as `59dc6f98188cb431fc8e53a3d5d863d8fa6569bb` after data CI 35715635772 and devcontainer CI 35715635838 passed. The CI container confirmed the pinned Codex CLI, all 661 fixtures and the responding development portal. The owner's live container was not rebuilt. This accepts the setup package only. Its feature branch and clean worktree have now been removed after exact tree-equivalence and unpublished-change checks; the original dirty root, operational refs and remaining source/model work remain preserved.

**Current authority:** the owner requests autonomous delivery, routine review of all repository work and continued local-to-remote integration. Both outcomes remain mandatory: complete feasible source ingestion through independently validated layers, and real query availability in `data.zohelo.com`. A file inventory, merged adapter, running backfill or portal deployment is not full-source or live-SQL completion. This section supersedes dated operational summaries below without deleting their evidence.

**Review completed:** 70 current-main documentation files plus the local handoff, all six open issues, recent PRs/Actions, all local worktrees/branches and the 64 non-temporary changed files were inventoried/reconciled at baseline `522b72c`. Read [the audit](audits/2026-09-22-repository-reconciliation.md) for findings and [its receipt](audits/2026-09-22-repository-reconciliation.json) for hashes/checks. This is not a new full-data audit, external revalidation of all research references, or functional acceptance of every local adapter. Source-only backups and the separate DBW worktree's uncommitted note are preserved.

| Workstream | Current verified status | Next acceptance boundary |
| --- | --- | --- |
| BDL native history | Owner-launched PID 383069, ten workers/proxy routes. Stable 10:09 UTC Drive read: 2,464 native files / 639,962,821 bytes; 1,131/2,420 selection-complete subgroups; 62 blocked selections. | Continue the finite pass, investigate failed selections and control reliability, reconcile the entire selected native scope. Do not reduce the ten-worker setting silently or launch a second coordinator. |
| NBP | Selected A/B/C/gold product has accepted 15-table/five-metric delivery. Scheduled run 35678350460 succeeded today. | Preserve daily freshness, cold recovery and source-specific semantics; complete remaining stage-decoupling work without regressing the release. |
| WDI | Current official archive has accepted complete modeled-value coverage and 14 datasets; scheduled run 35696378889 succeeded. | Native-only stage separation and independent API reconciliation, not a second competing implementation of the already-delivered archive models. |
| Eurostat | Three-dataset modeled contract exists; run 35705285103 succeeded, and 35710248013 was active during review. | Complete catalogue-native collection and full-distribution modeling. Do not report an old raw percentage as current or the three-dataset contract as full Eurostat. |
| DBW retained portal query | #145 merged as `b7b0abb`; deployment 35704500109 succeeded. The audited retained input has 1,550 indicators / 879,999,727 observation rows. | Actual immutable query publication, fresh restore and authenticated portal SQL. Last verified publication check at 08:26 UTC found no query pointer; earlier production launch was tool-blocked. Normal GitHub CLI login is now verified but is not publication evidence. |
| Other retained/new sources | TERYT/PRG/GLEIF/MF outputs have dated retention evidence; local adapters and models remain unaccepted. | Safe source-specific publication and SQL access, with exact snapshot/coverage labels. IMGW/GIOS cannot be called production-complete from code alone. |
| Repository integration | Root `c7898d7` is 11 commits behind this review baseline; 11 tracked modifications, 53 non-temporary untracked files, and one excluded review archive. | Source-sized PRs on current main; preserve #136/#137 safeguards, backups and running runtime. End with a reconciled development checkout, not a permanent local fork. |

**GitHub reconciliation:** #146 and #145 are merged; there were no open PRs when this review started. Issues #130–#135 remain the programme breakdown, but their initial text is not current acceptance evidence. #133's WDI implementation request is substantially superseded by accepted releases; its residual stage-separation/recovery scope remains. #135 is not delivered by the current monitor-only supervisor: catalogue discovery alone must never authorize downstream data processing. The current main branch remains unprotected; PR discipline is still procedural rather than server-enforced.

**Next implementation order:** retain one active engineering owner. Running provider ingestion is independent of that engineering slot. First integrate the reviewed documentation/status reconciliation and small environment/handoff changes; next close the supported DBW publication and live-SQL gap when its execution boundary is available; then finish retained TERYT/PRG query contracts and source-local publication repairs. The twelve delete-before-create upload helpers, GLEIF parent-grain defect and IMGW missing/revision semantics are integration blockers, not reasons to discard existing bytes.

**Today — Tuesday 22 September:** keep and measure the ten-worker BDL pass; deliver the repository/document reconciliation; move the first safe local setup changes into a checked PR; advance DBW's actual query publication/verification rather than another inventory-only update. Complete or record a concrete blocker for each active item, retaining the full outcome. Do not restart native collection just because a newer release format exists.

**Rest of this week — 23–27 September:** stabilize Wave 0 and close the most immediate portal gaps. Finish DBW retained SQL acceptance and progress TERYT/PRG publication; repair source-sized local changes before merge; reconcile BDL failed selections and prepare its preservation-safe Bronze path. Begin/continue WDI and Eurostat native-only stage separation, followed by NBP/OpenData as appropriate. Automatic downstream work must wait for the selected source's verified native-completion contract; dated retained-output publication is its separately accepted exception, not permission to process arbitrary partial intake. Establish restart/control failure evidence and current freshness/coverage reporting.

**Next week — 28 September–4 October:** carry unfinished Wave 0 and publication work forward first; do not reset priorities merely because the calendar changes. Integrate feasible Wave 1 backbones (TERYT/PRG/GLEIF first, then appropriately scoped Wikidata/OSM) with tested keys, editions and query contracts. Start feasible Wave 2 macro/central-bank bulk sources (IMF WEO, BIS, ECB, NBP extensions, OECD) in dependency order within existing access/cost boundaries. Validate layers, dbt grain, source-defined semantics, fresh restore and portal SQL as each source qualifies. Complete executable stage handoffs and continue reducing local/remote drift.

These are delivery priorities and acceptance targets, not promised full-source completion dates. The 182 researched products/families and seven additional gap candidates are a long programme, not 189 active pipelines or a claim that all can be completed in two weeks. Waves 3–8 retain the fiscal/register/procurement, social/health/education, environment/energy/transport, corporate/knowledge, multilateral-sector and conditional-commercial backlog. Public access, research evidence, native receipt coverage, modeled coverage and query readiness remain separate.

**Local integration packages:** environment/Codex setup and explicit startup; test/runtime fixes; DBW performance changes ported onto #136; TERYT/PRG; GLEIF without regressing #137; MF VAT/IMGW/GIOS; separately reviewed line-ending changes. Preserve the Telegram branch outside the data-readiness critical path. New-source upload paths must preserve prior objects and verify immutable replacements before becoming production-capable. After each accepted PR, remove only its verified merged branch/worktree; never remove operational ownership refs or unresolved work.

**Autonomous operating cadence:** the existing hourly `Advance Zohelo-data delivery` task supplies scheduled review/engineering within available tool/model capacity, while BDL and scheduled provider jobs execute independently. It must read this current section, current PRs and fresh evidence; avoid duplicate engineering and source writers, and continue the highest-priority unblocked task without another routine owner approval. Notify material progress or actionable blockers, not every successful polling cycle. This is not continuous AI execution and does not guarantee a future task run. Keep the local devcontainer alive for local jobs; a disconnect/rebuild can interrupt them.

**Acceptance before Done:** reviewed code merged, applicable deployment verified, complete claimed scope reconciled, fresh independent data restore/query passed, portal discovery and actual SQL exercised, and remaining limitations stated. Neither green CI nor an inventory screen satisfies those outcomes alone. No new paid service, model overage, source-credential disclosure, destructive reset or scope reduction is authorized by this plan.

### Ten-worker BDL runtime verified after owner launch — 22 September 2026, 09:33 UTC

The owner manually launched the reviewed native-only resume coordinator after the remote
launch was blocked. PID `383069` is running from unchanged runtime `5097e7e`, with
`--concurrency 10` and workspace `.local/bdl-recovery-2026-09-22-ten`. The process inventory
contains one BDL coordinator and ten wireproxy processes. The log maps workers 0–9 to
separate loopback HTTP proxy ports 8081–8090 and distinct initial subgroups P4304–P4313.
A stable Drive read at 09:31 UTC independently confirmed all ten in-flight selections
and the active writer identity. This supersedes the earlier paused handover state.

At 09:33:26 UTC, another stable Drive read verified **2,421 checkpoint-reported native
files / 629,353,778 bytes**, compared with 2,418 / 628,920,977 before this launch:
**three new files / 432,801 bytes**. Selection-complete subgroups increased from 1,090
to **1,093 / 2,420**; 56 blocked selections remain. The writer heartbeat at 09:33:20 UTC
belongs to PID 383069. See the [sanitized verification receipt](audits/2026-09-22-bdl-ten-worker-verification.json).
The reads checked stable control metadata, size, MD5 and SHA-256; this is not an independent
full-byte audit of every new archive or proof of full BDL coverage. No source data was parsed,
no downstream stage was started, and no production setting was changed during verification.

Keep the ten-worker coordinator and VPN routes running without another competing writer.
Inspect this new workspace and fresh durable checkpoints in subsequent monitoring, not an
old PID or historical summary. Remaining full-source ingestion, failed-selection recovery,
Bronze safety and portal query publication remain open; the local container must stay running.

### Owner two-track delivery priority and local continuation — 22 September 2026

The owner requires both autonomous backend delivery across the approved source programme
and usable new data in `data.zohelo.com`. Routine engineering does not need owner supervision.
Portal SQL access is an acceptance requirement, not satisfied by physical inventory alone.
Preserve stage isolation and native-only Landing; expose each verified existing layer with
its actual scope and remaining blockers, without relabelling retained Bronze as native Landing.
Immediate engineering priority is finishing existing PR #145, followed by other retained
source query contracts while source ingestion/modeling remains an open programme.

At 07:54 UTC, a fresh, stable read of BDL's durable queue verified 2,385 reported native
files / 626,046,982 bytes, 1,087 selection-complete / 2,420 known subgroups and 56 blocked
selections. The writer record is released and no local worker is running; the monitor is
monitor-only. The last worker stopped after a Drive folder-create timeout, so further
reconciliation precedes any resume. This is retained native-transfer evidence, not full
coverage or Bronze availability. No BDL restart or downstream launch is claimed here.

The live portal still reports inventory-only commit `5e5f42a`. PR #145 remains unpublished.
Inherited uncommitted query work was preserved before continuation in
`.local/chat-delivery-2026-09-22/inherited-dbw-work.patch`. Review repairs now enforce exact
reviewed audit hashes before production mutation, verify taxonomy bytes before using them,
and apply the observation-selection guard to parsed SQL references instead of raw text.
ADR 0010 adds server-serialized ownership before Drive namespace mutation; real local-Git
race tests exercise this boundary. Ten focused Python tests and 38 focused portal tests pass.
Full current-revision checks are running. No merge, Drive publication or live SQL acceptance
is claimed until the corresponding subsequent receipt is recorded.

### Recovery deployed and retained DBW audit completed — 21 September 2026, 21:42 UTC

**BDL repair merged and resumed:** [PR #143](https://github.com/rutkala/zohelo-data/pull/143)
merged as `5097e7eab3e6e328b3be74d6ee0de4fe837b710e` after the complete credential-free
[data-platform CI](https://github.com/rutkala/zohelo-data/actions/runs/35656215024) passed.
The focused runner suite passed 21 tests and independent review found no blocker. The old local
full suite was later interrupted after its environment mismatch was diagnosed; the exact failed
fixture passed with the corrected runtime, and the complete CI run above is green. The repair
propagates worker/control failures, maintains serialized heartbeats, joins workers before
lock release, and denies completion after uncertain writes. The initiating historical
queue drift remains unexplained; the Drive writer record is still an application-level lock.

At 21:30:27 UTC, one native-only resume coordinator started as PID `79463`, using an
unchanged detached checkout of that merged commit under `.local/runtime-code/5097e7eab3e6e328b3be74d6ee0de4fe837b710e`,
the isolated Python 3.12 environment, a fresh `.local/bdl-recovery-2026-09-21` workspace,
and three existing proxy-backed workers. The command uses `--mode resume --allow-codespace
--concurrency 3`, without a campaign time cutoff or successor schedule. Existing native
files and the interrupted workspace remain preserved. A fresh pre-start read confirmed
that the queue had not changed and the old writer lock was released; no BDL Action was
active. Eurostat's independent workflow was active and was not changed.

A stable 21:32 UTC Drive read verified the new writer PID, active heartbeat and only the
three resumed in-flight subgroups (`P4150`, `P4174`, `P4216`). Queue SHA-256 was
`03d6182c15da232eddde3225fb4a2a36d0e99d5e70d56a09ee3b4341b0ad5467`; it still reported
1,085 / 2,420 selection-complete subgroups, 2,313 native files / 620,877,414 bytes, and
56 blocked selections. A further stable read at 21:35 UTC reports **2,317 files / 621,125,077 bytes**
(queue SHA-256 `53eb18fe98e7dd03648030be2aa7ad28e4212b0e2d0e6cbb379a774b5b24cca2`),
establishing four newly checkpointed native files / 247,663 bytes in this resumed run.
The 21:35:08 heartbeat still belongs to PID `79463`; full selection coverage is unchanged. The reviewed
monitor runs separately as PID `79464` under `.local/wave0-supervisor-recovery-2130`,
reports actual process/checkpoint state and never launches downstream work. A container
stop can interrupt both processes; resumability does not establish immunity to idle
suspension. Runtime receipt/logs are under `.local/recovery-2026-09-21/rebuilt-2047` and
the new campaign workspace. At 21:41 UTC an independent read-only check retrieved one
new P4174 ZIP (55,485 bytes): its durable plan, part receipt, length, MD5 and SHA-256 all
matched stable Drive metadata and the downloaded original bytes. Nothing was parsed or
unpacked. The [native verification receipt](audits/2026-09-21-bdl-native-verification.json)
provides the exact object identity and evidence scope.

**DBW retained audit completed at 21:29 UTC:** run
`bc79f7e9-6af6-429f-b327-8a34c3b74d71` verified all **3,103 objects / 4,803,673,234 bytes**,
reused 1,788 cached files and restored 1,315. Inventory SHA-256 is
`15f587a7d0631befac394e6cc1d0183fb0f564801f144b7d6ca340e8d092a12e`; the remote inventory
was unchanged after restoration. The dated 1,550 Landing receipts reconcile to 1,550
observation partitions. Verified Parquet footers report **879,999,727 observation rows**,
8,358,612 dictionary rows, 1,531 metadata rows and 1,550 taxonomy rows. The earlier
820,345,903 observation claim is superseded by this measurement (difference 59,653,824).
The [retained audit evidence](audits/2026-09-21-dbw-retained-report.json), SHA-256
`29c20b0daadb165784c5892bb39c42af43e934fd990dbee5418019e7bb5427ac`, records schemas and
limits. This is not a #136 release, current provider-catalogue coverage, native-to-Bronze
value lineage or a publication/completion marker. No DBW downstream writer was started.

**Recovery follow-up through 22:34 UTC:** the retained-DBW publisher passed an actual-audit local-store
run without Drive writes. Two increments advanced from 8 to 16 published indicators in 184.21
seconds; all 30 fixed-relation fragments and the first eight observation descriptors were reused
exactly on resume. The 8,358,612-row dictionary benchmark produced 28 fragments, each below 8 MiB,
with bidirectional `EXCEPT ALL` value/multiplicity proof. This establishes local publication and
resume behavior, not a Drive publication or deployed portal query. At 22:14 UTC, the active BDL
writer's fresh checkpoint reported 2,366 native files / 624,618,034 bytes, 1,087 of 2,420 complete subgroups,
three in flight and 56 blocked; its heartbeat was current. A separate read-only audit also verified
seven retained TERYT/PRG Parquet files / 130,767,618 bytes with stable checksums and schemas; their
publication remains follow-on work.

At 22:30:08 UTC the BDL coordinator stopped with `CampaignControlFailure` after a timed-out Drive
partition-receipt save (`TimeoutError`). Container OOM counters remain zero. Its in-memory report of
66 new files / 4,644,107 bytes is not durable coverage evidence. The queue, writer record and part
receipts require fresh reconciliation before another resume; no BDL writer is currently claimed.

**Portal inventory merged and deployed:** [PR #142](https://github.com/rutkala/zohelo-data/pull/142)
merged as `5e5f42a5c291eb8f33761f5d2f767c2fd37bad49` after both full portal CI variants
passed in [35593113336](https://github.com/rutkala/zohelo-data/actions/runs/35593113336)
and recovery review found no blocker. This includes lint, build/type checking, unit tests
and browser regressions. [Deployment 35657788866](https://github.com/rutkala/zohelo-data/actions/runs/35657788866)
succeeded. A fresh 21:35:36 UTC read of `https://data.zohelo.com/portal-build.json`
identifies the exact merged commit. The feature exposes bounded physical inventory separately from the existing
published query catalogue; it does not make retained DBW/BDL/native files queryable.
The earlier live resolver measured 71 Drive requests; the 120-page cap bounds each scan.
A disposable authenticated browser session against the live site then rendered all seven
source inventory entries at 21:38 UTC, including 2,322 checkpoint-reported BDL native
files and 1,553 DBW Bronze files. The complete catalogue-plus-inventory refresh made
151 read-only Drive requests and attempted no Drive writes. See the
[live browser receipt](audits/2026-09-21-portal-inventory-live.json); a screenshot remains
under the local recovery evidence directory. This exercises the deployed UI with the
existing owner OAuth access token; it does not claim a new interactive Google sign-in test.

**Active retained-DBW query work:** the audited dated Bronze snapshot now has an implementation
for value-preserving fragments, per-object content-addressed resume, bounded typed row-stream
verification, a strict four-dataset manifest, and an indicator/part selector in the portal. Local
fixtures and review are in progress; the largest retained indicator still requires actual-data
acceptance of the bounded stream proof before production continuation. No Drive
publication, merge, deployment or authenticated browser query is claimed yet; native-to-Bronze
lineage and current source completeness remain explicitly unresolved.

**Remaining authorized work:** continue this finite BDL pass, then review terminal failed
selections without removing retry caps or claiming full-source completion. BDL Bronze publication safety, unpublished source adapters, and
the remaining native-only migration stay unfinished. Original dirty source work and the
unpushed Telegram branch remain preserved. Both merged feature branches were deleted on
origin and refs pruned. The old local full suite was deliberately interrupted at 21:50 UTC after
its failure was traced to the stale root `.venv/bin/dbt` launcher (Python 3.11 with Python 3.12
packages). CI run 35656215024 passed, and the exact DBW fixture passed in 21.210 seconds with the
correct `/tmp` Python 3.12 runtime first in `PATH`. The clean merged test worktree and branch were
removed; the cleanup observer exited and its receipt remains at
`.local/recovery-2026-09-21/rebuilt-2047/local-validation-cleanup.json`.

### Second devcontainer recovery — 21 September 2026, 21:03 UTC

The owner requested reconstruction of today's work and autonomous continuation, with
BDL native ingestion first. No ingestion, browser, proxy, audit or monitor process survived
this rebuild. The current container's OOM counters are zero; the prior crash cause remains
unestablished. The original dirty checkout stays at `c7898d7`; 61 source/configuration/doc
files and the tracked diff were preserved under `.local/recovery-2026-09-21/rebuilt-2047/`,
with per-file SHA-256. Old worktree indices contain no additional staged changes; their
committed recovery branches remain preserved. The unpushed Telegram branch and draft
portal inventory PR #142 remain separate unfinished work.

**GitHub reconciled:** current main is `50e0465`, containing today's merged PRs #136–#141.
PR #142 (`f6785e4`) is an unmerged draft; it exposes file inventory, not new queryable data.
No active or queued BDL Action was present in the recovery check. Eurostat continues in
its own provider workflow; local recovery has not dispatched another source workflow.

**BDL durable evidence, 20:57 UTC:** a fresh read selected the existing production root
and verified queue size, MD5, SHA-256 and stable metadata. The queue SHA-256 is
`328d142e2387dfac6a55ce1f36697930101dcd4085bf5c9267b78bc7fe5e9e69`, matching the saved
11:10 checkpoint. It records **1,085 selection-complete / 2,420 candidates**, 257 partial
and 56 failed pass outcomes, ten interrupted in-flight subgroups, and 2,313 native files /
620,877,414 bytes in plan summaries. These file/byte totals are checkpoint reports, not a
fresh full-native-object audit. The last in-memory summary claimed 1,086 completed
selections; that extra selection is not established by the durable queue. Independent
part receipts remain available for reconciliation. The writer lock is released at
11:14:37 UTC. Worker logs show queue-drift errors escaping thread cleanup before release;
the initiating drift's cause is unresolved. BDL has not yet been resumed at this checkpoint.

**Recovery runtime:** the inherited `.venv` points to Python 3.11 while its packages are
for 3.12. A separate pinned Python 3.12 environment at `.local/runtime-python312` passes
`pip check`; the inherited environment remains preserved. Chromium launches after
restoring its browser files/system dependencies. Three existing WireGuard configurations
connect successfully through verified wireproxy 1.1.3. A real browser egress comparison
confirms that the installed browser honors the workers' proxy environment: direct traffic
uses a different route, while environment-only and explicit proxy options both use the
configured proxy. BDL returns HTTP 200 through that route. No browser proxy code change
is needed. The ordinary read-only Google diagnostic selects the retained
OAuth credentials and verifies root visibility; it performs no upload. Git push access
was verified with `--dry-run` through the existing VS Code credential helper.

**Active continuation:** the merged read-only DBW retained-output audit restarted at
20:59 UTC as PID 33551, run `bc79f7e9-6af6-429f-b327-8a34c3b74d71`, from an exact
`50e0465` code snapshot. It reverified/reused all 1,788 earlier cached objects and is
restoring the remaining objects from the 3,103-object inventory. Its state/log remain
under `.local/dbw-retained-audit/`; completion and any row reconciliation require the
matching final audit report and run status. No DBW publication or downstream build ran.
The single engineering task is BDL worker/control failure handling on isolated branch
`recovery/2026-09-21-resume-bdl`. Repair `26aeb15` passed 21 focused runner tests
and independent read-only review; the preceding broader BDL run passed 71 tests. The full
credential-free data suite is running. Merge and one native-only resumed writer remain
pending those checks. AGY and Copilot CLI are unavailable; a lighter internal implementation
owner and independent read-only reviewers are being used.

**Latest remote acceptance checked during this recovery:** NBP run
[35553328276](https://github.com/rutkala/zohelo-data/actions/runs/35553328276) succeeded
with release `f9b183cd-c81c-4b0c-8f57-d7468697a42f`, checked through 20 September.
WDI run [35639397373](https://github.com/rutkala/zohelo-data/actions/runs/35639397373)
succeeded without changing release `9c796ba2-1e7d-48dc-8c68-8bd60c76454a`.
Eurostat run [35650755363](https://github.com/rutkala/zohelo-data/actions/runs/35650755363)
succeeded with release `c96da055-8bb7-4866-abf5-06709fb1a3f3`; its current-distribution
snapshot reports 6,574 / 21,238 (30.95395%), while modeled coverage refers only to the
three-dataset contract. Full official catalogue coverage remains incomplete. No Actions
job was active or queued at the follow-up check. The live portal still identifies
`5864b8e` from #139; #142's two portal Check Runs passed, but that draft is not deployed.

**Unpublished source work:** TERYT, PRG, MF VAT, GLEIF, IMGW and GIOŚ adapters, Bronze
loaders, dbt models and tests are preserved but not accepted as production releases.
Several contain delete-before-upload paths and incomplete transfer/coverage checks.
The old local DBW optimization must be ported without replacing PR #136's snapshot,
byte-integrity, completion and immutable-publication guards; the old GLEIF adapter must
not replace the hardened local-only implementation from #137. Model grain/dependency
issues remain (including GLEIF parent joins and weather revisions). These source changes,
56 blocked BDL selections, portal inventory/query publication and the native-only rollout
remain unfinished; green code checks are not full-source coverage.

### Recovery monitor merged — 21 September 2026, 10:54 UTC

PR #138 merged as `139c1efa75f1074e23641132bcdd0aad9d908d9c` after full data-platform
CI run 35590372695 passed for its final head. The reviewed monitor is running locally;
it observes processes and reports downstream gates, and does not advance stages automatically.
The independent full local data suite remains active, so no local-suite completion is claimed.
The remote feature branch was deleted and tracking refs pruned. Its local worktree is retained
until the running local checks finish; cleanup must not disrupt those processes.

The retained DBW audit is active under PR #140, with a pinned 3,103-object inventory and
per-object byte verification. No complete audit or DBW publication is claimed yet. The owner's
immediate priority is all new data visible and usable in the portal while backend work continues.
The inventory UI is undergoing review; verified browse/query access remains the required next
outcome. The OpenData schema fix from #139 remains the currently deployed portal change.

### Retained DBW Bronze audit started — 21 September 2026

The read-only audit runs from the isolated recovery worktree as local PID `58944`, run
`1ef481a6-6a85-4851-9b60-59fde1c87fde`, with recovery state under
`/workspaces/zohelo-data/.local/dbw-retained-audit`. Its pinned inventory contains 3,103
objects: 1,550 legacy Landing receipts, 1,550 Bronze observation partitions and three fixed
Bronze relations. This remains an incomplete diagnostic; no row-count reconciliation,
stable-inventory acceptance, #136 release, downstream authorization or current provider
coverage is claimed. Completion requires matching `complete` `run-status.json` and
`audit-report.json`, followed by review under the
[operating procedure](source-campaign-operations.md#read-only-audit-of-retained-dbw-bronze).

**Proposed next increment:** after the audit and portal inventory work complete, extend the
existing source-campaign immutable-manifest and `current-landing.json` pointer infrastructure
with an explicit `gus_dbw_retained_bronze` snapshot. It would expose the dated observations,
taxonomy, metadata and dictionaries in `02_bronze`, marked incomplete and with
native-to-Bronze lineage unresolved. All 1,550 indicators must be discoverable, while queries
load only selected bounded partitions rather than the 4.7 GB observation collection. The
65 MB consolidated dictionary and every observation partition over the existing 8 MiB object
limit require verified fragments. This is an accepted design direction for a later increment,
not implemented publication, a completion marker, or a full-current-source claim.

The latest WDI run 35570717063 succeeded and published release
`0b9cc607-441c-47ac-b2dc-ccd5c42433b1`. Its reported current-archive coverage is 9,015,914
modeled values, 1,498 indicators and 264 geographies, with observation dates 1960–2025
and a modeled/source value ratio of 1.0. This covers the current WDI CSV archive;
the separate API reconciliation campaign remains incomplete. NBP's daily run
35553328276 also succeeded. These are inspected Actions results, separate from the ongoing
local recovery checks and from full-provider coverage claims.

### Recovery follow-up — 21 September 2026, 10:44 UTC

**Portal deployed:** PR #139 merged as `5864b8e28b4da5692ab2225fe78c84827c4c3b89`.
It fixes OpenData locations/people manifests being checked against the organizations schema.
Portal CI run 35589353396 passed and deployment 35589767596 succeeded; the live
`https://data.zohelo.com/portal-build.json` reports that exact commit. An authenticated owner-session
refresh has not been exercised. Local focused tests passed 11/11; the full local suite had 709 passes
and one unrelated signaling timeout, reproduced in isolation. No Drive payloads were changed.

**Recovery monitor:** ten focused tests pass and data-platform CI run 35588961828 passed for
`b722b81`. The full local data check remains running. The reviewed monitor is deployed locally
as PID 32792 using a copy under `.local/wave0-supervisor/`; PR #138 is still awaiting integration.
This is monitoring only, not automatic downstream advancement. BDL PID 13743 remains running;
at 10:42 UTC its checkpoint reported 1,065 selection-complete subgroups and 1,355 remaining.

**Eurostat:** run 35588640780 stopped after three distribution requests failed the approved-host
redirect guard; downstream publication was skipped. Its subsequent current/ Landing restore
checks passed. A bounded HEAD diagnostic of the official MIGR_ASYAPPCTZM TSV endpoint from
this devcontainer returned HTTP 307 to `https://sorry.ec.europa.eu/`. This is evidence of the current
local response, not proof of every redirect seen by the Actions runner. Do not allow a maintenance
page into native data merely by widening the host list. Retained cumulative receipts report 7,817
accepted distributions / 24,131,199,910 bytes; current catalogue coverage remains incomplete.

**Next work:** verify retained DBW Bronze bytes and schemas with resumable, read-only restoration;
add bounded portal Drive-file visibility distinct from validated query releases; investigate Eurostat
availability before retrying. The owner requested day/week/next-week planning: prioritize Wave 0
through this week, then integrate feasible later sources while continuing unfinished Wave 0 work.
These are delivery priorities, not promised full-source completion dates. Original dirty work remains
preserved and will be integrated selectively without replacing #136/#137 safeguards.

### Local crash recovery resumed — 21 September 2026, 10:10 UTC

The owner explicitly authorized local recovery and continuation of the AGY autonomous programme,
with Wave 0 first and independent native Landing/downstream stages. The recovered AGY conversation
`b04d2b40-34e2-4971-b9d3-fbe880f4a8af` contains the owner's 20 September agreements.
AGY's final recorded command was at 04:41 UTC; later heartbeat attempts received model quota errors.
The separate BDL supervisor continued logging until 08:25 UTC. No local ingestion, browser,
proxy or supervisor survived the container restart. Current cgroup OOM counters are zero;
this does not establish the previous container's crash cause.

**Preserved:** the original working tree remains at `c7898d7`, with its unpublished changes intact.
A local source-only backup with per-file SHA-256 covers 55 files, including the supplied handoff,
at `.local/recovery-2026-09-21/`. It excludes credentials and datasets. Remote refs were fetched:
`origin/main` is `ee2d944` (merged #136 and #137). Engineering uses the separate
`recovery/2026-09-21-supervisor` worktree; the old DBW/GLEIF files were not copied over those repairs.

**BDL recovery:** read-only Drive inspection verified the queue's stored checksum and stale writer
lock (heartbeat `2026-09-21T08:25:17.840035Z`, former PID 595473). The queue retained 2,420
candidates, 1,352 selection plans and ten interrupted in-flight subgroups. Public GitHub Actions
API checks found no in-progress or queued runs before resume. All ten existing WireGuard proxy
connections were restored and tested. One local BDL writer was launched as PID 13743, explicitly
in `resume` mode, concurrency 10, with a 24-hour execution bound, using the preserved operational
checkout. No reset, reload or replacement of successful native downloads was requested. At
10:10 UTC the resumed local summary reported 1,037 selection-complete subgroups, 1,383 remaining,
56 blocked selections and zero new files in this run so far; this is recovery progress, not
full-source acceptance. Logs append to `portal/test-results/bdl-web-bulk/bdl_extractor.log`.
The old supervisor has not been relaunched.

**DBW continuation:** fresh Drive metadata inspection found the earlier checkpoint and existing
Bronze folders, no `releases` namespace and no native completion marker in the Landing control
folder. The reported 820,345,903-row Bronze result needs reconciliation with #136. The previous dbt attempt
failed on missing local Parquet inputs. Preserve those bytes; reconcile provenance before any
new complete-release claim or downstream execution. The owner clarified that completed or partial
collection and downstream progress are retained work, not obsolete data: reconcile and reuse those
outputs, rather than restarting ingestion merely because the checkpoint format changed.

**Fresh retained-output inventory, 10:14 UTC:** Drive lists all 1,550 DBW Landing indicator
checkpoints and all 1,550 Bronze observation partitions (4,737,200,817 bytes), plus the consolidated
dictionary (65,499,189 bytes), taxonomy (84,403 bytes), and metadata (144,289 bytes). All listed
files have MD5 and recorded SHA-256 metadata and no duplicate names in these folders. This is
metadata inventory evidence, not a fresh byte hash or row recount. Reuse/verification of those
outputs is the next DBW step; no repeat collection is justified by a checkpoint-version change.
At 10:14 UTC, resumed BDL reports two newly landed native files (180,999 bytes), with 1,039
selection-complete subgroups. Drive independently reports the current writer PID 13743 and
fresh queue/heartbeat updates.

**Active continuation:** BDL is a real detached local process, which a further container stop can
still terminate. The first lighter internal implementation assignment returned no work and was stopped; the lead
implemented the recovery monitor in the isolated worktree. Seven focused recovery tests pass;
the full data check is running. Separate agents, explicitly requested by the owner, are auditing
portal/Drive visibility and the unpublished files. No AGY overage or API billing was enabled.
The lead owns integration, validation and the remaining Wave 0 programme. Supervisor changes
are not yet merged or deployed at this checkpoint. Its monitor-only mode records actual process
health and explicit downstream gates; automatic stage advancement remains unfinished work.

**Other saved outputs:** a fresh metadata comparison of the 20 objects referenced by TERYT,
PRG, GLEIF and Biała Lista local Landing/Bronze receipts matched presence, size and the
checksums supplied in each receipt (1,759,197,323 bytes total; zero mismatches). This verifies
retained object identity/metadata, not fresh full-content hashes, source completeness or model semantics.

### Local-devcontainer recovery and native publication review — 21 September 2026

**Preserve the local environment and recover from the reported crash.** The owner's 05:17 UTC process inventory and
Drive's 05:16 UTC heartbeat identify the BDL worker as local PID `595473`, with the
supervisor and ten proxy processes also present. These are dated observations, not a
continuous health check. Large collection stays in that devcontainer. Do not duplicate
the writer in Actions/Codespaces or pull, reset, clean, stash, switch branches, or restart
the active working tree while it contains the running jobs and unpublished work.
Later in this session the owner reported that AGY and the entire devcontainer crashed.
Current process health is therefore unverified. On reopening, first preserve unpublished
files and inspect fresh processes, logs and local/Drive checkpoints; do not blindly relaunch
the old supervisor or treat historical PIDs as current. The owner is moving toward Codex CLI
in that same local environment for continued work.

**Reconciled status at the review checkpoint:** [BDL native collection](https://drive.google.com/file/d/18TUYzBpZg1Hn0qcoZT_iiO1PAq2-eyCj/view) was incomplete
(849 landed plans / 2,420 candidates, with partial/failed plans and outstanding selections).
The queue last modified at 08:25 UTC advanced to 1,036 landed, 255 partial and 56 failed
plans, still against 2,420 candidates. This establishes later collection progress, not
continuous worker health or full BDL completion.
Eurostat full bulk coverage was 6,267 / 21,238 distributions (29.5084%); its published
modeled contract still covered three datasets ([latest accepted run](https://github.com/rutkala/zohelo-data/actions/runs/35561187440)).
The [current WDI archive product](https://github.com/rutkala/zohelo-data/actions/runs/35549893680)
reconciled all 9,015,914 values. [DBW's legacy checkpoint](https://drive.google.com/file/d/1XxOWlXaiML8WPkkCfmVd0N0dMrKpoLYr/view) reported 1,550 / 1,550 indicators and
820,345,903 rows, but that was not an independently recounted, snapshot-bound release under
the new #136 contract. Preserve those existing outputs and reconcile provenance before
deciding what needs recollection. Earlier dated "active" or "100%" statements below must
not be interpreted as newer acceptance evidence.

**Unpublished work was preserved and reviewed:** the owner's source-only archive contained
54 files, reconstructed separately over local base `c7898d7`. It includes TERYT, PRG,
GLEIF, MF VAT, IMGW and GIOŚ adapters/loaders/models. Review reproduced delete-before-upload
data-loss windows, accepted truncated transfers, incomplete inventories marked complete,
and parser/model defects. No observed defect alone proves existing Drive data is corrupt.
The local DBW memory/performance changes must be integrated selectively into current main;
copying the older loader over #136 would remove verified release safeguards.

**GLEIF native-cache repair (code only):** [PR #137](https://github.com/rutkala/zohelo-data/pull/137)
adds a local native downloader using the existing atomic bulk transport. It pins one provider
publication before fetching the three current CSV archives, caches each verified member for
recovery, rejects incomplete transfers and locks its workspace. Local completeness covers
only the selected current product, not historical snapshots or durable platform availability.
See [the operating boundary](source-campaign-operations.md#gleif-current-golden-copy-native-transfer).

**Drive publication is blocked:** GitHub review identified a cross-host race in the proposed
publisher: different workspaces could create duplicate hash-named objects. The publisher was
removed from this increment; upload flags fail before any workspace or network work. A local
lock or delayed Drive-list election cannot establish cross-host exclusion. Reopening publication
requires a proven serializer acquired before any namespace mutation. A reviewed option is a
root-bound, explicitly provisioned lock anchor with immutable claim/release records addressed
by pre-generated Drive IDs, no automatic expiry, and recovery only after proving the previous
writer stopped. This coordination must be implemented and verified before production rollout.

**Validation:** the earlier candidate passed `bash scripts/check-data.sh` (616 tests) using
Python 3.12 and pinned dependencies. The final local-only focused suite passed all 10 tests,
and independent review found no remaining issue in the narrowed delivery. The [PR checks](https://github.com/rutkala/zohelo-data/pull/137/checks)
on the final revision remain the integration gate.
A bounded, read-only check parsed the provider's 139,840-byte publication response and
pinned all three members of `2026-09-21 00:00:00`, including the real `YYYYMMDD-HHMM`
filename convention. No native archive was downloaded and no Drive data was written by
this repair session. No GLEIF Bronze
or portal availability is claimed, and no local-devcontainer rollout has occurred.

**Still open:** apply the verified native boundary to the other five supplied adapters;
repair the supplied Bronze acceptance/parsers and dbt grain/meaning defects; integrate DBW
performance changes; finish the committed BDL Bronze pipeline and retryable supervisor handoff;
then publish verified source releases and extend portal discovery. New Drive folders alone
do not satisfy the portal's release contracts. The reviewed portal source matched the deployed
17 September build, so rebuilding the same portal would not expose these sources.

### Fresh production acceptance and fail-closed GUS stage boundary — 20 September 2026

**Accepted current releases:** no GitHub Actions writer was active or pending at the 18:09 UTC
inspection. NBP [run 35483182223](https://github.com/rutkala/zohelo-data/actions/runs/35483182223)
published and freshly restored release `33fc1114-c8d3-48f6-b0e6-d7626b28f67a`: all four
Tables A/B/C/gold sources are coverage-complete through 19 September, the latest observations
are 18 September (Table B: 16 September), and Gold contains 427,028 FX rows plus 3,459 gold rows.
WDI [run 35511129843](https://github.com/rutkala/zohelo-data/actions/runs/35511129843)
published and freshly restored release `47933158-569a-44f5-a474-44594c5d501e`: the current
six-member official archive still reconciles all 9,015,914 modeled values at ratio 1.0. The
later scheduled [run 35529367207](https://github.com/rutkala/zohelo-data/actions/runs/35529367207)
also succeeded and freshly restored release `6900c12a-b63c-448b-8d30-d124e6b46535`; the archive
SHA-256 remains `2ab1d0d250ebe986ac8a9f7163f6e177fbe4cfb2750f822b18578d902aeb134f` and modeled
coverage remains 9,015,914 / 9,015,914. That run advanced the separate API reconciliation to
3,922 accepted responses with 1,202 tasks pending and no pending publication backlog. It remains
incomplete and is not needed to claim
the current bulk archive product complete and is not being confused with every World Bank product.

Eurostat [run 35528999652](https://github.com/rutkala/zohelo-data/actions/runs/35528999652)
published and freshly restored modeled release `526be40b-e070-483e-adb3-833186671e80`. Its complete
reviewed progressive contract remains three datasets / 81 country-series with admitted dataset
and series coverage ratios of 1.0 and latest observation date 31 August 2026. Full-distribution
Landing advanced to 6,089 / 21,238 validated current distributions (28.6703%), 7,477 accepted
versions, 22,833,356,544 raw bytes, 18,102 pending tasks and zero failed pending tasks. Earlier
[run 35491496570](https://github.com/rutkala/zohelo-data/actions/runs/35491496570) stopped after two
transient bulk transport failures; subsequent scheduled runs recovered them and have remained green.
Full Eurostat catalogue modeling is still not Done.

**Unreviewed GUS changes were not accepted as completion:** direct main commits `dc6e7bb` and
`9f54ec7` introduced DBW Web/Bronze and BDL proxy/runner changes without pull-request validation.
Review found that the
Bronze path could start on the currently available subset, delete a retained pilot object, replace
an existing stable-name object before a replacement was proven, and let a metadata-only DBW run
create the same completion-shaped checkpoint used by bulk collection. The BDL proxy call also broke
five established adaptive-runner regressions. The repair now preserves every prior object, makes
Landing folder resolution read-only, admits only v3 full-bulk indicator receipts, publishes a
checksum-bound catalogue completion record only when every discovered indicator reconciles, and
requires that record before DBW Bronze starts. Metadata-only runs remain explicitly incomplete;
provider revisions are retained under content-addressed native names and every indicator receipt is
bound to one durable native-refresh snapshot plus the exact catalogue hash and each native object's
Drive ID, byte size, MD5 and SHA-256. An incomplete refresh resumes the same remote snapshot; after
completion, the next run opens a new snapshot and re-fetches every indicator, so unchanged taxonomy
cannot make revised aggregate, metryka or ZIP payloads look current.
Bronze verifies the downloaded bytes again before parsing, rejects sampled or indicator-selected
publication into the complete release, and writes into a separate
`02_bronze/gus_dbw/releases/<native_snapshot_sha256>/` namespace, so the earlier partial files cannot be
silently reused by a complete-catalogue run; verified taxonomy and metadata outputs can be restored
on a fresh runner without depending on disposable local files. A full receipt additionally requires
the documented aggregate discovery envelope, at least one unique safe ZIP filename, and exact
reconciliation of that discovered inventory to the identity-bound landed ZIP descriptors. Bronze
persists one checksum-bearing dictionary partition per indicator (including an empty-schema
partition when the source has no dictionary rows) for fresh-runner consolidation and publishes its
release completion record only after both partition sets reconcile every indicator. The disabled-by-default
dbt source no longer reads the retained unversioned pilot folders: an operator must select the
verified snapshot explicitly with `ZOHELO_DBW_BRONZE_RELEASE_ID`, and all four source relations then
resolve only under `02_bronze/gus_dbw/releases/<native_snapshot_sha256>/`. Before dbt runs,
`python scripts/restore_dbw_bronze_release.py --release-id <native_snapshot_sha256>` restores the
marker, exact observation/dictionary partition sets, taxonomy, metadata and consolidated dictionary
from Drive into that local path, verifying size, MD5 and SHA-256 before an atomic directory rename.
Native snapshot selection is protected across Actions and explicitly authorized hosts by a durable,
source-wide expiring Drive lease with immutable acquisition/release records; losing election claims are
tombstoned immediately, the lease is elected in the already-existing control root before any Landing
folder or taxonomy mutation, and every exceptional exit releases through `finally`. Local ZIP resume
files are isolated by native snapshot identity. Landing renews the same elected owner before its
bounded whole-catalogue receipt sweep; Actions schedules indicators for at most 12,600 seconds and
stops verification at five hours inside a six-hour job, leaving shutdown time for the release tombstone.
The Bronze writer has the same cross-host protection before it creates
or writes release paths, with both stages retaining a one-hour end-of-run lease margin, exceeding
twice the measured 1,410-second largest-indicator transfer. Bronze publishes an immutable successor
claim for the same elected owner before whole-release reconciliation, giving finalization a fresh
six-hour window without opening a second-writer gap; both claims remain tracked until their individual
release tombstones succeed, including a retry from the session `finally`. Resumed Landing receipts are re-read and reconciled to their exact native
Drive objects before they count as complete. Bronze derives ZIP and metryka ownership from those
receipts (never provider filenames or untrusted CSV identity alone), while its completion marker,
restore boundary and dbt guard bind both partition names and the SHA-256 of every local observation
and dictionary partition, plus the taxonomy, metadata and consolidated dictionary files read by dbt.
The dbt external sources use only those sealed filenames; additional Parquet files are not readable
through the DBW source contract.
Reused Bronze partitions count toward completion only when Drive's server-computed SHA-256 still
matches the upload SHA recorded in object properties; changed bytes fail before the marker is sealed.
Landing now rejects a second completed receipt for the same indicator even when its native membership
matches, keeping the sealed snapshot consumable by Bronze's unique-receipt contract. Bronze parses
provider observation and dictionary CSVs with `ignore_errors=false`; malformed rows fail the indicator
before any partition or complete-release marker can be accepted instead of being silently discarded.
Provider ZIPs use indicator-scoped durable object names while retaining the provider filename in receipt
lineage, so byte-identical names shared by two indicators cannot collapse into one cross-owned object.
Changed-object names truncate the recognizable stem at a UTF-8 boundary against the final 240-byte
name, leaving room for Bronze's local prefixes and preventing non-ASCII provider names from crossing
storage or filesystem component limits. Every revision name is domain-separated with the full
SHA-256 of both its original logical name and its content. Bulk base objects are also mapped into a
separate `base--logical-sha256-...` namespace, while changed bytes use
`revision--logical-sha256-...--content-sha256-...`; a provider filename therefore cannot occupy a
generated revision identity, and distinct long names cannot collapse to one Drive object. Exact
unchanged base descriptors validate without constructing a revision identity; legacy objects remain
retained, while current receipts must satisfy the stronger identity.
Pre-boundary unscoped receipts remain preserved but do not count as completed during resume or Bronze
validation; the same native snapshot reprocesses those indicators, writes scoped objects and seals a
new content-addressed completion marker only after the current receipt contract reconciles.
Lease renewal in both Landing and Bronze now re-runs the durable election after the old claim is
tombstoned; a contender exposed by the handoff makes the successor self-tombstone instead of allowing
two writers. Disposable ZIP cache files are re-downloaded and atomically replaced, so a nonempty
prefix left by interruption is never reused. The Bronze restore path streams Drive objects to staging
in bounded chunks and verifies size, Drive SHA-256, local SHA-256 and MD5 before atomic publication,
keeping large indicator partitions independent of process RAM.
Failed restores remove their nonresumable UUID staging trees. Once a complete downloaded tree
is verified, it replaces any mismatched disposable local release cache through a rollback-capable
rename and removes the displaced cache, preventing supervisor retries from accumulating full release
copies. A mismatched release-scoped taxonomy cache is likewise recovered from checksum-verified
Drive bytes and replaced atomically.
The DBW Silver and Gold indicator models now preserve the Bronze taxonomy contract
(`thematic_area`, `domain_name`, `taxonomy_path`, `node_id`, and `parent_id`) without
inventing numeric hierarchy IDs; the acceptance fixture builds the complete DBW
Bronze-to-Silver-to-Gold lineage, not only its Bronze boundary. The local supervisor now
derives the exact completed release from its release-bound marker, restores and verifies that
authoritative Drive snapshot into a dedicated data root, and exports the matching release ID and
DuckDB path before dbt; a failed restore or model build remains retryable rather than being marked
triggered. This is model-contract and orchestration evidence, not a claim that the still-incomplete
production source has been published.
Launchers
refuse to kill or duplicate an already running local writer (including the later BDL-only
launcher) and now fail visibly when a background process loses the advisory-lock race; DBW Web shares the existing
DBW provider concurrency lane; and proxy-free BDL execution keeps its prior call contract while
configured workers still receive their assigned proxy.

The separately reported Codespace BDL/DBW jobs were not duplicated or interrupted during this
review. Their last recorded partial counts (BDL 1,093 / 2,420 subgroups; DBW Bronze 165+ / 1,550
indicators) remain progress, not accepted full Landing or modeled delivery. In particular, existing
partial DBW Bronze files remain retained but cannot authorize Silver/Gold. The next DBW Bronze run
must wait for the new full-catalogue completion record; its immutable native-snapshot release boundary
is now enforced in code.

### Autonomous 2-Week Multi-Source Roadmap, Stage Decoupling, and Bulk Ingestion Mandate — 20 September 2026

**Autonomous execution authorized for full multi-source data platform expansion (GitHub Issues #130–#135):**
- **Owner Architectural Directives Recorded:**
  1. **Strict Stage Decoupling**: Ingestion (Landing), Bronze, Silver, Gold, and Semantic layers must remain isolated, independent, resumable pipelines with separate error recovery and checkpoints. They are not merged into a monolithic execution.
  2. **Sequential Stage Progression**: For any given source, downstream processing (Bronze extraction) begins strictly after Landing native ingestion is 100% complete (catalogue exhaustion / all available partitions landed).
  3. **WebUI / Bulk Distribution Priority**: Using WebUI or official bulk distribution facilities is mandatory for historical bulk data. Low-throughput, rate-limited REST APIs must not be used for bulk historical extraction when an official bulk facility exists (e.g. BDL WebUI, DBW bulk catalog, Eurostat Bulk Download Facility, World Bank DataBank bulk ZIP). REST APIs are reserved exclusively for recent delta updates.
  4. **Full 2-Week Autonomy**: Authorized to execute the multi-source programme independently without intermediate reviews, tracking progress through the canonical delivery record.
- **GitHub Issues Created & Linked:**
  - [#130: [Roadmap] Autonomous Multi-Source Data Platform Execution Plan](https://github.com/rutkala/zohelo-data/issues/130)
  - [#131: [GUS BDL] Complete WebUI Bulk Landing & Launch Decoupled Bronze Pipeline](https://github.com/rutkala/zohelo-data/issues/131)
  - [#132: [GUS DBW] Complete Bronze Layer Observation Partitions & Build Conformed Silver Layer](https://github.com/rutkala/zohelo-data/issues/132)
  - [#133: [World Bank WDI] Complete Bulk Archive Ingestion & Vectorized Bronze Pipeline](https://github.com/rutkala/zohelo-data/issues/133)
  - [#134: [Eurostat] Bulk Download Facility Ingestion & Decoupled Bronze Pipeline](https://github.com/rutkala/zohelo-data/issues/134)
  - [#135: [Orchestration] Multi-Source Sequential Stage Orchestrator & Production Validation](https://github.com/rutkala/zohelo-data/issues/135)
- **Comprehensive 182-Source Candidate Review & 8-Wave Architecture Plan:**
  - Reviewed the entire `/workspaces/zohelo-data/docs` documentation corpus (73 documentation files across ADRs 0001–0009, `docs/source-research/`, `docs/source-candidates.md`, `docs/source-accounts.md`, and `docs/source-expansion-plan.md`).
  - Shifted platform roadmap focus beyond the initial 5 baseline sources (`NBP`, `DBW`, `BDL`, `WDI`, `Eurostat`) to incorporate the complete 182-candidate portfolio across 6 sectors: Global Public (`GL-001`–`GL-040`), Poland Statistics & Society (`PLS-001`–`PLS-035`), Poland Governance & Economy (`PLG-001`–`PLG-034`), Poland Environment & Geospatial (`PL-ENV-001`–`PL-ENV-035`), Commercial & Disclosures (`CD-001`–`CD-034`), and Specialist (`EX-001`–`EX-004`).
  - Formulated 8 strategic execution waves ordered by dimensional dependency and bulk facility availability: Wave 1 (Foundational Backbones: TERYT, PRG, GLEIF, Wikidata, OSM), Wave 2 (Macro & Central Banking Bulk: IMF WEO, BIS, ECB, NBP extended, OECD), Wave 3 (Poland Governance, Fiscal & Registers: MF JST budgets, State Debt, KRS, CEIDG, e-Zamówienia/TED, Sejm/ELI, PKW), Wave 4 (Poland Social, Health & Education: NSP/PSR Censuses, ZUS, NFZ, EPIMELD, RSPO/SIO, RAD-on/ELA, CKE), Wave 5 (Environment, Energy & Weather: IMGW Archive, GIOŚ, PSE v2, CEPiK, GDDKiA, City GTFS), Wave 6 (Corporate Filings & Global Knowledge: SEC EDGAR, UK Companies House, ESPI/EBI, OpenAlex, GDELT), Wave 7 (Multilateral Sectoral: FAOSTAT, ILOSTAT, WHO, UIS/UNICEF, WIPO/UPRP, UN WPP), Wave 8 (Commercial & Conditional).
- **Active Operational Status:**
  - **GUS BDL Landing**: 10-IP WireGuard proxy cluster and 10 concurrent browser workers running actively (371 selection-complete + 722 retained legacy = 1,093 / 2,420 subgroups; 521 bulk native archives, 132.1 MB landed on Google Drive; 0 failures).
  - **GUS DBW Bronze**: Full batch running in detached background session (`scripts/run_dbw_bronze.sh`). Phase A (`br_dbw_indicators.parquet`) and Phase B (`br_dbw_metadata.parquet`) 100% complete and uploaded. Phase C (Bulk Observations) active with 494+ / 1,550 indicators processed (including 119M observation indicator 568 processed in 1,410s with disk spillage) into partitioned Parquet on Google Drive.
  - **dbt Bronze Integration**: `bronze_dbw` source and 4 Bronze models (`br_dbw_observations`, `br_dbw_indicators`, `br_dbw_metadata`, `br_dbw_dictionaries`) verified with `dbt parse` passing cleanly.

### Cross-source modeled-publication outage — 19 September 2026

**Observed production state:** NBP run [35414737354](https://github.com/rutkala/zohelo-data/actions/runs/35414737354), WDI run [35443349867](https://github.com/rutkala/zohelo-data/actions/runs/35443349867), and repeated Eurostat runs including [35457678025](https://github.com/rutkala/zohelo-data/actions/runs/35457678025) all stopped before publication with `AmbiguousLayoutError`. WDI and Eurostat collection completed before their modeled jobs failed, so retained Landing progress is not lost; it is not evidence of a refreshed Gold/semantic release. NBP stopped at ingestion initialization and retained its last accepted release.

**Layout repair accepted and WDI recovered:** [PR #127](https://github.com/rutkala/zohelo-data/pull/127) merged as `e9a0ebbc976cfc0d47b6b2f83602234560d54733` after the complete 539-test data suite and review passed. `layout_resolution.py` previously classified the whole Drive root before selecting a source. Consequently, one unrelated legacy marker could stop every independent source writer after the verified canonical cutover. Writer resolution is now source-scoped: duplicate canonical containers and same-source canonical/legacy pointers still fail closed, while empty or unrelated legacy markers cannot create a platform-wide outage. NBP control resolution applies the same source-local rule and checks release ambiguity before authorizing ingestion-state writes.

WDI production run [35464169774](https://github.com/rutkala/zohelo-data/actions/runs/35464169774) succeeded and promoted immutable release `2bf4bbd1-98e6-4bb0-ab5e-106b2db33620`. Fresh acceptance independently matched all 9,015,914 source and modeled observations, 264 geographies, 1,498 indicators, and history from 1960 through 2025, with modeled value coverage 1.0. The verified current archive SHA-256 is `2ab1d0d250ebe986ac8a9f7163f6e177fbe4cfb2750f822b18578d902aeb134f`; snapshot and latest retrieval are both dated 19 September 2026.

Eurostat run [35464169768](https://github.com/rutkala/zohelo-data/actions/runs/35464169768) cleared the former layout failure: collection and the full 38-model/test dbt build succeeded. Promotion then failed closed while validating staged `eurostat_observation_revisions` because DuckDB's `HUGEINT` window-sum type is serialized to Parquet as `DOUBLE`, so the published bytes did not match source-relation metadata. [PR #128](https://github.com/rutkala/zohelo-data/pull/128) merged as `ddd2d20ceffafd9b67cac6330932184589b6572e` after 540 tests and review passed. It casts revision sequence/count values to the lossless portable `BIGINT` contract, verifies the complete SQL schema through a Parquet round trip, and identifies exact staged metadata mismatches. Production run [35468207929](https://github.com/rutkala/zohelo-data/actions/runs/35468207929) passed that repaired boundary and all 38 dbt models/tests, then stopped before pointer promotion because first-release medallion navigation required a pre-existing `eurostat` source folder. The current release was retained. First-release navigation now preflights all layers, creates only missing empty canonical source namespaces, rejects non-empty unowned folders before mutation, and immediately binds created folders to the durable navigation index. Fresh reviewed production acceptance remains required; progressive API modeling is still not complete Eurostat catalogue coverage.

NBP production run [35468207932](https://github.com/rutkala/zohelo-data/actions/runs/35468207932) succeeded on the same main commit and promoted release `70327041-2453-46a4-bfc5-91b8b8cdd438`. All four sources are checked through 18 September 2026. Fresh consumer verification opened all 15 Bronze/Silver/Gold datasets and verified semantic queries; raw replay independently rebuilt and matched 1,397,706 rows across all 15 tables from 431 immutable input batches (30,728,012 unique transferred bytes). Gold contains 427,028 FX quote rows and 3,459 gold-price rows. This accepts the NBP Tables A/B/C/gold modeled refresh; it does not change the separate BDL or Eurostat completion status.

**Concurrent ingestion evidence:** BDL Web run [35375837183](https://github.com/rutkala/zohelo-data/actions/runs/35375837183) retained 97 new native files (90,186,236 bytes) before a Google Drive read timeout. Catalogue discovery is exhausted at 2,420 known subgroups; 98 are selection-complete and 2,322 remain, with two blocked selections. This is resumable progress, not complete BDL coverage. Current WDI and Eurostat collectors were already active when this repair began and were not duplicated.

### GUS DBW (Dziedzinowe Bazy Wiedzy) onboarding, contract, adapter, and pipeline — 18 September 2026

**Autonomous onboarding and technical discovery of Statistics Poland Subject-Matter Knowledge Databases (DBW):**
- **Analysis of `https://dbw.stat.gov.pl/katalog/bulk` & `https://api-dbw.stat.gov.pl`:** Analyzed provider architecture, discovering that DBW exposes two complementary data delivery mechanisms: (1) an OpenAPI 3.0-governed public REST API on `https://api-dbw.stat.gov.pl` with 13 official endpoints covering thematic taxonomy, dictionaries, variable metadata, cross-sections, and multi-dimensional observation data; and (2) a Next.js web application on `https://dbw.stat.gov.pl/katalog/bulk` and `/katalog/hvd` providing direct table exports as native CSV, XLSX, and ZIP-CSV packages alongside dictionary bundles (`dictionary.csv`).
- **Hierarchy & Grain Mapping:** Mapped the complete domain knowledge hierarchy: Area (`Gospodarka`, `Społeczeństwo`, `Środowisko`, etc.) -> Group (`Budownictwo`, etc.) -> Subgroup -> Indicator / Variable -> Cross-section (`przekrój`) -> Dimension & Position -> Year × Period observation cells.
- **Source Contract (`docs/contracts/gus-dbw.md`):** Formalized full source contract specifying data scope, CC BY 4.0 licensing, endpoints, query parameters, reference dictionaries, quotas (anonymous 100 req/15min vs registered 500 req/15min with key `GUS_DBW_API_KEY`), and strict [ADR 0009](decisions/0009-native-only-landing.md) native landing layout under `01_landing/gus_dbw/`.
- **Ingestion Adapter (`src/ingestion/sources/gus_dbw.py`):** Implemented source adapter conforming to the repository campaign framework (`initial_tasks`, `recent_tasks`, `refresh_task`, `request_for`, `interpret`), generating bounded discovery and follow-up tasks for dictionaries, areas, variables, sections, and observations while storing native payloads without mutation.
- **Credential & Rate Limiting Integration (`src/ingestion/source_credentials.py`):** Configured transport-only `X-ClientId` header injection for `api-dbw.stat.gov.pl` when `GUS_DBW_API_KEY` is present, automatically raising request limits and adjusting quota windows.
- **Workflow & Pipeline (`.github/workflows/source-dbw.yml`):** Added GitHub Actions workflow enabling production collection runs on GitHub-hosted runners where connectivity to GUS succeeds (bypassing cloud edge firewalls that block Codespace Azure IPs).
- **Verification:** 7 new unit tests in `tests/test_gus_dbw.py` pass cleanly in 0.001s, `tests/test_source_credentials.py` passes (12 tests), `scripts/check-source-coverage.py` reports status `ok` across 182 sources, and configuration loads cleanly in `source_campaign.load_settings("gus_dbw")`.

### GitHub Actions autonomous 3-worker ingestion with automatic self-chaining — 18 September 2026

**Autonomous execution, multi-worker concurrency, and continuous self-chaining until catalogue completion:**
- **Codespace Limitations:** Because Codespaces automatically pauses upon client inactivity / browser disconnects, unattended long-running ingestion is transferred to GitHub Actions for 24/7 reliability.
- **Workflow Multi-Worker Concurrency (`.github/workflows/bdl-web-bootstrap.yml`):** Configured `--concurrency "${{ inputs.concurrency || '3' }}"` allowing 3 isolated workers to run in parallel on standard GitHub Actions runners (`ubuntu-latest` 4 vCPUs / 16 GB RAM).
- **Graceful Time-Slice Completion (`src/bdl_web_adaptive.py`):** Updated `main()` exit code logic so that reaching `--max-seconds 18600` (5 hours 10 minutes) returns exit code `0` (success), distinguishing an orderly budget-bounded pause from an unhandled fatal crash.
- **Automated Self-Chaining Step (`.github/workflows/bdl-web-bootstrap.yml`):**
  - Scoped `permissions: actions: write` to `job.ingest_web_history` in accordance with repository least-privilege security policy (`scripts/check-workflows.py`).
  - Added an end-of-run step that evaluates `portal/test-results/bdl-web-bulk/bootstrap-summary.json`. If `load_complete == false` and `auto_continue` is enabled, it automatically dispatches the next run (`gh workflow run bdl-web-bootstrap.yml --ref main -f auto_continue=true -f concurrency=3`).
  - Under `concurrency: group: zohelo-pipeline-gus_bdl, cancel-in-progress: false`, the next run waits in the GitHub Actions queue until the current run finishes and releases its Google Drive writer lock, then immediately starts and resumes from the latest checkpoint on Google Drive.
  - Terminates automatically when `load_complete == true` (all 2,420 subgroups landed).
- **Verification:** All 13 unit tests in `tests/test_bdl_web_adaptive_runner.py` passed in 5.1s (including new `test_main_exit_codes`), and `scripts/check-workflows.py` passed with 0 policy violations. Cumulative complete subgroups reached 54 on Google Drive.

### BDL Web Ingestion concurrency acceleration, ASP.NET worker session isolation, and error classification repairs — 18 September 2026

**Root-cause analysis of concurrent session collisions, worker isolation, and resilient ingestion resumption:**
- **Codespace Idle Timeout Diagnosis:** The Codespace automatically stopped after ~32 minutes of execution (started 05:51:52Z, stopped 06:23:34Z) due to GitHub Codespaces' default 30-minute idle inactivity timeout when no UI activity occurs in the browser tab. In GitHub account settings (`github.com -> Settings -> Codespaces -> Default idle timeout`), setting the timeout to the 240-minute (4-hour) maximum minimizes pause frequency during unattended runs.
- **Diagnosed ASP.NET Session Collision Root Cause:** Under concurrent execution (`--concurrency 3`), workers previously shared a single `bdl-session-state.json` file, sending identical `ASP.NET_SessionId` cookies to BDL simultaneously. Because ASP.NET WebForms serializes requests for a single session and maintains wizard state in server-side session memory, concurrent requests caused severe lock contention (60s page.goto timeouts) and wizard state cross-contamination (one subgroup picking up dimensions from another subgroup navigating in the same session).
- **Technical fixes implemented & verified:**
  - *Worker Session Isolation (`src/bdl_web_adaptive.py`):* Configured `BDL_SESSION_STATE_PATH` to point to each worker's own isolated workspace (`workspace / "bdl-session-state.json"`). Each concurrent worker now maintains its own independent session cookie and private ASP.NET wizard state, completely eliminating cross-subgroup contamination and session lock contention.
  - *Refined Stage Tracking & Failure Classification (`portal/scripts/bdl-web-selection-worker.mjs`):* Explicitly set `result.stage = 'navigation'` during initial subgroup page load and reserved `'login'` for the authentication endpoint. Navigation timeouts on `page.goto` are now classified as non-fatal `provider_timeout` rather than fatal `authentication_or_site`, preventing temporary network slowness from aborting the runner.
  - *Enhanced Dimension Wait Diagnostics (`portal/scripts/bdl-web-selection-worker.mjs`):* Detailed error logging prints wanted vs actual dimension values in DOM if a timeout occurs.
  - *Plan & Queue Reset on Google Drive:* Contaminated intermediate plans (`P1457, P1466, P1468, P1469, P1477, P1487`) were cleanly reset to root tasks, failures and pass_outcomes were cleared, in-flight state was cleared, and the writer lock was released.
- **Verification:** All 65 unit tests across `tests/test_bdl*` passed cleanly, and ESLint passed with 0 errors. Cumulative complete subgroups reached 46 with 420+ native partition archives on Google Drive.

### BDL Web Ingestion resumption, territorial cell budget bounding, and failure isolation — 17 September 2026

**Overnight run analysis, diagnosed failure modes on heavy subgroups, and pipeline hardening:**
- **Overnight execution results (`task-1522`):** Ran unattended for 1h 47m, successfully landed **58 new native ZIP files** (3.16 MB) and completed 8 full subgroups (`P1317`, `P1319`, `P1320`, `P1323`, `P1324`, `P1325`, `P1330`, `P1331`) with zero gaps, proving that cascading dimensions, RadListBox selection, layout selection, and TERYT transfer work reliably.
- **Territorial chunk budget bounding (`src/bdl_web_partitions.py`):** On large subgroups like `P1341` (2,790 dimension combinations), `accept_territories` with the previous `cell_budget=200000` created chunks of 71 territories (198,090 cells per table generation), exceeding BDL's safe web table threshold and triggering timeouts. Lowering the cell budget to 20,000 bounds chunk sizes (e.g. 7 territories on `P1341`), ensuring BDL renders result tables in seconds while maintaining optimal batch throughput.
- **Worker postback & download navigation stabilization (`portal/scripts/bdl-web-selection-worker.mjs`):** Extended `settle()` postback timeout from 60s to 120s and export deadline from 120s to 180s. Added `{ noWaitAfter: true }` to export dropdown and whole-subgroup download button clicks to prevent Playwright action timeouts when awaiting non-existent page navigations during file downloads.
- **Subgroup failure isolation & auto-replanning (`src/bdl_web_adaptive.py`):** Capped consecutive failures per subgroup (`max_subgroup_failures=5`) so that an intractable subgroup cannot consume all 20 global failure attempts and abort the run. A failing subgroup is cleanly marked `failed`/`partial` and the runner proceeds to the next unvisited candidate. Added automatic replanning for un-landed plans that experienced failures, ensuring they pick up the updated partition budget.
- **Verification & Deployment:** 65/65 unit tests in `tests/test_bdl*` passed (including new isolation tests), 705 portal tests and ESLint passed cleanly. Fixes merged and pushed to `main` (commit `7526efb`).
- **Active Ingestion Run:** Launched `python3 -u src/bdl_web_adaptive.py --workspace scratch/bdl_workspace --allow-codespace --mode resume` as background task (`task-1832`). Active writer lock acquired on Drive (`host: codespace`, `status: active`). Traversal across remaining 2,409 unvisited subgroups is actively underway.

### BDL Web selection worker repairs for cascading dimensions, layout switching, and TERYT synchronization — 17 September 2026

**Systematic diagnosis and resolution of BDL Web selection failure modes:**
Following root-cause analysis of the 10 current-pass failures (`P1314, P1317, P1319, P1320, P1323, P1324, P1325, P1330, P1331, P1336`) and 557 historical failures in `web-queue-v1.json`, all technical defects in `portal/scripts/bdl-web-selection-worker.mjs` were resolved and verified live:
- **Dynamic cascading dimension selection:** RadListBox dimension controls (`wym1`, `wym2`, ...) only load options after their parent dimension/year axis (`lata`) is selected and postback settles. The worker now orders dimension selection in dependency order (`lata` first, then `wym1`, `wym2`, ...), waits for options to populate in the DOM, and verifies matching dimension controls only after all selections are applied.
- **Telerik `list.postback()` JavaScript strict-mode crash fix:** `item.select()` inside `page.evaluate()` previously invoked Telerik's `_doPostBack()` which accessed `arguments.caller`, throwing a TypeError in ES strict mode. The worker now uses native Playwright DOM clicks (`click()` for the first item, `click({ modifiers: ['Control'] })` for subsequent items, or the control's `_SelectAll` button for full sets), originating cleanly from the browser event loop without strict-mode errors.
- **RadComboBox layout switching in `setLayout`:** Avoids triggering strict-mode postbacks when changing territorial layouts by clicking the RadComboBox arrow and target dropdown item via Playwright native locators, skipping redundant clicks when already selected.
- **TERYT Dalej control synchronization:** Fixed timing issues where the "Dalej" button remains disabled while territorial units are transferred. Selector now handles all button variations (`dalej`, `dalej1`, `dalej2`), waits for destination units to populate, and synchronizes on the button's enabled state before clicking.
- **Live verification:**
  - `P1314` (cascading dimensions): Discovered dimensions, selected cascading dimensions, and discovered layouts in under 15 seconds.
  - `P1344` (territorial transfer & Dalej): Successfully transferred regional territories, synchronized Dalej enable, and exported native zip `LUDN_1344_CREL_20260917223842.zip`.
  - `P1313` (3-level cascade, layout selection, 4,435-unit inventory, territorial partition slice): Executed all 4 partition steps and downloaded native zip `SAMO_1313_CREL_20260917224030.zip` with 0 HTTP 500 errors.
- **Regression suites:** All 10 unit tests in `tests/test_bdl_web_adaptive_runner.py` passed, all 705 portal tests in `npm --prefix portal test` passed, `scripts/check-workflows.py` passed, and ESLint passed with 0 errors. Committed as `59660fb` and pushed to `main`.

### BDL Web full-history runner, catalogue discovery fix, and workflow isolation — 17 September 2026

**PR #126 delivers the complete full-history BDL runner and operational fixes:** following the
handoff criteria in [PR #126 Comment 5713338019](https://github.com/rutkala/zohelo-data/pull/126#issuecomment-5713338019),
the BDL Web export pipeline is decoupled from fixed campaign timers, catalogue paging truncation is
resolved, writer mutual exclusion is guaranteed, and CI workflows are isolated from unattended triggers.

- **Dynamic catalogue discovery:** resolved ASP.NET RadGrid `PageRequestManager` async postback state
  races in `portal/scripts/bdl-web-catalogue.mjs`. Paging verifies postback completion and DOM row stabilization,
  tested live against public BDL tables 563 (11 pages), 570 (25 pages), and 640 (9 pages) with 100% table
  exhaustion and 0 errors, eliminating artificial catalogue truncation without hardcoding table limits.
- **Durable full-history runner:** `src/bdl_web_adaptive.py` executes without a campaign timer cutoff
  (`max_seconds=None` default) to traverse the complete BDL Web catalogue until genuine exhaustion.
  Schedules unvisited subgroups fairly before deep multi-partition slicing, supports `--mode resume`
  (validating existing stored receipts) and `--mode reload` (executing a fresh download pass while
  preserving historical native files), and records distinct outcome statuses (`pass_complete`, `load_complete`,
  `interrupted`).
- **Codespace authorization & exclusive writer lock:** standalone Codespace execution entrypoint
  (`--allow-codespace` / `ZOHELO_ALLOW_CODESPACE_EXECUTION=true`) verifies a clean git tree, checks credential
  presence without leaking secret values, and inspects active GitHub Actions runs. Enforces exclusive writer
  ownership via `bdl-writer-lock.json` in Drive control storage with a 60-second heartbeat and 10-minute
  stale lock takeover.
- **Fast-path whole-subgroup export & exact partition fallback:** preserves fast-path single-request
  exports with root-task scope alignment, falling back to exact hierarchical partitions (`src/bdl_web_partitions.py`)
  when large selections trigger provider errors (such as P1313).
- **Native Landing compliance:** downloads original provider archive bytes directly to Google Drive
  Landing (`gus_bdl/web_bulk/...`) with separate technical manifests and control records, strictly adhering
  to [ADR 0009](decisions/0009-native-only-landing.md).
- **Workflow & deployment isolation:** removed unattended `push` triggers from `.github/workflows/bdl-web-bootstrap.yml`
  (`workflow_dispatch` only); removed live portal diagnostic steps from `.github/workflows/data-validation.yml`;
  isolated BDL browser scripts from portal deployment paths in `.github/workflows/deploy-portal.yml` and
  `.github/workflows/portal-validation.yml`.
- **Verification:** 10 new unit tests in `tests/test_bdl_web_adaptive_runner.py` cover runtime > 18,600s, writer lock
  acquisition/conflict/override/release, codespace auth, whole export fast-path, P1313 fallback, fair slicing,
  and receipt corruption. Full test suite `bash scripts/check-data.sh` passed all 527 repository tests (including
  MetricFlow and storage checks), workflow syntax validated via `python scripts/check-workflows.py`, and portal
  `lint`, `typecheck`, `test` (705 passed), and `build` all succeeded cleanly.

### Native-only Landing for all sources — 16 September 2026

**Q-LANDING-001 is resolved:** the owner requires ingestion to download and store native
files/responses unchanged, with no data processing in the Landing step. This applies to
all existing and future sources, both full loads and increments. [ADR 0009](decisions/0009-native-only-landing.md)
and the first section of [AGENTS.md](../AGENTS.md) make the rule mandatory. Older descriptions
below of generated Landing Parquet, parsing and validation during ingestion are historical;
they are not exceptions to this decision. Only necessary transport/navigation, byte integrity
and lightweight resume metadata remain in ingestion. Content processing starts downstream.

**BDL implementation:** [PR #124](https://github.com/rutkala/zohelo-data/pull/124) removes archive
extraction, CSV parsing, schema inspection, row counting and Parquet conversion from
`bdl_bulk_ingest.py`. It uploads the native object under its original provider filename when
available, with small manifests/checkpoints in the existing `_control` area. New v2 receipts
say `landing_scope: native_bytes_only` and `content_validation: not_performed`; the queue reports
files and bytes. Existing completed raw downloads remain resumable; this change does not purge
old files, repeat successful downloads or mutate published analytical releases. Seven new
native-transfer regressions and the existing queue execution tests cover opaque/malformed
content, preserved bytes, filename forwarding, retry metadata and fail-closed storage errors.
CI/merge and subsequent production evidence are attached to PR #124; code completion does
not by itself establish new live downloads or full historical coverage.

**Known pre-change blocker:** run [35007743970](https://github.com/rutkala/zohelo-data/actions/runs/35007743970)
downloaded P1463 through the Web UI but stopped in DuckDB CSV conversion before persisting the
native ZIP. The raw-only path removes that dependency rather than ignoring malformed rows.
The previous diagnostic recorded 33 landed exports and incomplete catalogue discovery. That is
a dated checkpoint, not a refreshed total. Browser export failures and catalogue gaps remain
separate outstanding work; the native-only change does not claim they have been resolved.

**Cross-source rollout remains open, not Done:** apply the same boundary to WDI and Eurostat
API/bulk paths and their generated response-envelope publication, then NBP acquisition versus
validation/build orchestration, and OpenData acquisition versus its separate Bronze loader.
Inspect each real execution path; retain already-native files and current consumer pointers;
move remaining unpack/parse/convert/validate work into independently executable downstream
processing with its own checkpoint and failure state. No other source adapter is claimed to
have been refactored by PR #124. Verify byte-preserving transfer and that a downstream parsing
failure cannot block the next download for each source before accepting that rollout.

**Immediate continuation:** review/pass the existing data gate, merge the BDL correction,
start the existing single Web workflow using its explicit resume trigger, and verify native
objects/checkpoints, especially P1463. Do not reset the Web queue or introduce another workflow.
Full BDL catalogue/history completion, the earlier old-BDL purge/portal replacement and later
API incremental acceptance remain open. No further owner approval is needed for these routine
changes within the existing boundaries. The lead uses connected GitHub/CI because the recorded
Copilot quota blocker remains and this session has no Codespace terminal; local Git DNS also
failed. No external-agent dispatch or additional paid service is claimed.

### BDL Web-only historical ingestion resumed — 15 September 2026

**Full BDL history remains incomplete, but new Web-export data is now landing.** The owner
reaffirmed that the immediate task is ingestion only: historical numerical data through the
BDL website first, with API incremental ingestion later. Bronze/Silver/Gold processing is
not a prerequisite for this collection. Earlier dated descriptions of the mixed BDL
API/medallion workflow below are historical, not the current execution path.

[PR #122](https://github.com/rutkala/zohelo-data/pull/122) merged as
`d2b00671a54a7c7f4e98ed15d8909c2aa158ccd1` after the complete
[data validation](https://github.com/rutkala/zohelo-data/actions/runs/34964952709) and
[portal validation](https://github.com/rutkala/zohelo-data/actions/runs/34964952700) passed.
It replaces the approximately 51-minute receipt replay with a small durable Web queue,
adds paged Web metadata discovery, retains failed subgroups without stopping other exports,
and keeps conversion/Drive publication failures fail-closed. Fifteen queue regressions include
actual loop tests for provider failure continuation, restart, hash mismatch and ambiguous writes.
The stale tests referring to the deleted BDL workflow now assert the retained single-job,
Web-only workflow instead; the obsolete workflow was not restored.

The marked merge immediately started [BDL Web historical bootstrap run #9,
34966018325](https://github.com/rutkala/zohelo-data/actions/runs/34966018325) at 11:56 UTC.
The checkpoint at 12:05 UTC confirms these **three new** landed subgroups:

| Web subgroup | Rows recorded in its verified Landing completion receipt | Completed UTC |
| --- | ---: | --- |
| P1315 | 101,640 | 12:01:13 |
| P1316 | 776,412 | 12:03:28 |
| P1318 | 215,136 | 12:05:23 |

That is 1,093,188 newly landed source rows, separate from the previously landed P1312
(287,835 rows). These are source-export row counts, not proof of full BDL coverage or
published analytical facts. The run continued after P1314 returned BDL's server-error page.
Fresh Web discovery independently exhausted the root metadata table: 33 categories across
seven pages. Child group/subgroup discovery and complete historical export coverage remain open;
the old hash-pinned URL seed is explicitly incomplete and cannot satisfy completion.

**Independent data check:** P1315's completion marker, manifest, original ZIP and Parquet were
freshly downloaded from Drive. Manifest SHA-256/size, object SHA-256/MD5/size and ZIP CRC passed.
The freshly read source CSV contains exactly 101,640 rows, years 1995–2001 and 2,904 distinct
territorial codes. Its manifest and completion marker agree with that row count. No independent
Parquet row-by-row/SQL comparison was performed in this check: this session lacked a local
Parquet query engine. The producer performed its normal conversion and upload verification.
The verified P1315 package is in [its immutable Drive snapshot](https://drive.google.com/drive/folders/1iXp2XDu7o9aIBvIjz9TNBHfy3qWBzqLf).

**Continuation and human operation:** keep the single `ingest_web_history` job in
`.github/workflows/bdl-web-bootstrap.yml`. Its manual dispatch and six-hour schedule resume the
same source-owned queue; the runtime allowance is 330 minutes, with a 310-minute extraction
budget. Do not set `reset_web_bulk` during ordinary continuation. Exact exports, Parquet and
manifests land under `01_landing/gus_bdl/web_bulk/<subgroup>/<archive-sha256>/`; final completion
markers and `web-queue-v1.json` are under that Web-bulk area's `_control/`. Run summaries report
new landed rows/subgroups separately from catalogue exhaustion and outstanding failures. An
incomplete run that lands nothing exits nonzero. No BDL API observation key is supplied to this
workflow, and it does not run BDL medallion publication.

**Remaining work, not Done:** complete Web catalogue discovery and all exports; resolve failing
subgroups including P1313 and P1314 instead of treating failure or absence as completion; verify
full-history/dimension/territory coverage and continued restart behavior. The next failure repair
must investigate bounded Web selections for result-table errors rather than only retrying the
same all-territory request. The original full older-BDL purge/portal replacement and later API
incremental acceptance are not established by this ingestion restart. No additional owner
approval is pending for routine repair and verification within the existing boundaries.

**Execution route:** the prior canonical record reports Copilot's monthly quota exhausted.
This session had no connected Codespace terminal; local Git transport also failed DNS. The lead
completed this existing bounded repair using the connected GitHub tools and existing CI rather
than claiming an external-agent dispatch or enabling another paid model/account route.

Updated 14 September 2026. **The owner-approved Google Drive consolidation is live and independently verified.** Implementation [PR #101](https://github.com/rutkala/zohelo-data/pull/101) merged, the compatible portal was deployed, and [apply run 34871823795](https://github.com/rutkala/zohelo-data/actions/runs/34871823795) completed successfully. Independent verification passed at 17:21 UTC.
The complete current WDI archive remains modeled and published; existing WDI acceptance and other source coverage records are preserved.

### Progressive Eurostat API Bronze → Silver → Gold → semantic delivery — 14 September 2026

This change adds the first production-gated Eurostat modeled release without narrowing or closing
the full-source commitment. It consumes every accepted response in the complete reviewed API
contract (three datasets × 27 current EU countries), decodes every JSON-stat cube cell including
explicit missing positions, preserves the full native dimension key and status, reconciles
revisions (including value reversions), and publishes eight source-shaped Bronze, Silver and Gold
datasets. Twelve native MetricFlow coverage/accountability metrics are checked against Gold before
promotion. Generic Eurostat values are deliberately not exposed as an additive business metric.
The existing portal discovers the canonical Eurostat pointer, validates the exact release contract,
and exposes its tables, catalogue, lineage and source status alongside the other releases.

The release is independently restorable, hash-bound to all Landing fragments, and staged before
its immutable `releases/eurostat` pointer is promoted. Every Landing fragment is re-read and hashed
during staged validation. Its coverage mart also binds the current
full-distribution campaign evidence, so the smaller modeled API contract cannot be mistaken for
complete Eurostat: raw catalogue completion and modeled completion remain separate. All 474
repository data-platform tests pass, including exact sparse-cell,
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
Landing continuation and freshly published and restored complete-archive release
`e12f475f-9146-4fc0-9fef-bbf1b6e1b24e`: 9,015,914 of 9,015,914 populated values, 264 of 264
geographies and 1,498 of 1,498 indicators are modeled through Gold, for a ratio of exactly 1.0.
BDL
[run 34879372531](https://github.com/rutkala/zohelo-data/actions/runs/34879372531) advanced Landing to
3,996 accepted/published responses with zero publication backlog and 2,409 API tasks pending; its
modeled job hit its configured 25-minute timeout during the build. This is not data loss, but the
current transform timeout is shorter than the observed end-to-end build and fresh-restore path.
[Run 34885171452](https://github.com/rutkala/zohelo-data/actions/runs/34885171452) reproduced the same
limit: Landing succeeded, all 68 dbt build/tests passed, and the job was then cancelled at the
25-minute limit before release completion or fresh verification. No replacement writer was started.
The separate authenticated Web-bulk route still has no accepted bulk archive and BDL remains far
from the 172,576-variable full scope.

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
