# Repository and programme reconciliation — 22 September 2026

## Scope and evidence

Baseline: remote `main` at `522b72c24eb6c6007a50ea212b333b362be545fe`, freshly fetched without changing the original working tree. The owner requested a full documentation/programme review, local-versus-remote audit and autonomous integration, with source ingestion and real portal SQL availability both required.

Reviewed the 70 files in current-main `docs/` (60 Markdown, nine JSON and one CSV), the additional local handoff, all six open programme issues, recent PR/run metadata, local branches/worktrees, the 64 non-temporary changed files, and current BDL process/checkpoint evidence. All 26 changed Python files parse; 19 unpublished SQL models were read. The JSON and CSV source inventories reconcile to 182 unique IDs. [Machine-readable review receipt](2026-09-22-repository-reconciliation.json) pins documentation and changed-file hashes and records the static checks.

This is a repository/programme and changed-code review, not a forensic audit, a new test of every retained observation, or fresh verification of every external source URL. The 285 research URLs remain dated source evidence. No all-source completion, DBW publication, or new authenticated portal query is established by this review. One supplementary log/build read was blocked before execution and supplied no evidence. A BDL read that raced with the writer was discarded; a later stable read succeeded.

The canonical priorities, dates and current status remain in [the delivery record](../deliverables.md). This report is supporting audit evidence, not another task board.

## What is actually delivered

| Area | Established evidence | Boundary still open |
| --- | --- | --- |
| NBP A/B/C and gold | Existing accepted 15-table and five-daily-metric release path, cold restore/replay evidence and successful scheduled run 35678350460 on 22 September. | Native-only stage separation rollout, ongoing freshness/capacity and future products; the successful job is not a new exhaustive historical audit. |
| WDI current official CSV archive | Existing 14-dataset Bronze/Silver/Gold path, nine coverage metrics and accepted 9,015,914 populated values across the six-member archive; scheduled run 35696378889 succeeded. | Independent API reconciliation, native-only stage decoupling and non-WDI World Bank products. Do not implement a duplicate WDI pipeline from an old issue. |
| Eurostat | Existing three-dataset modeled contract, complete-distribution collection framework and successful run 35705285103; run 35710248013 was active during review. | Full catalogue intake and modeling remain incomplete. Older percentages are not current counts. |
| OpenData/Senzing | Three published Bronze contracts exist; latest inspected pipeline run 35379157940 succeeded on 18 September. The later schema resolver repair is merged. | Current complete-source/revision acceptance and further models are not established by those facts. |
| BDL Web | One reviewed native-only coordinator uses ten workers and ten configured proxy routes. Fresh durable progress is recorded below. | Whole-source completion, failed selections, legacy scope reconciliation and safe Web-to-Bronze publication. Historical API tables are not the new full Web collection. |
| DBW | Retained audit verified 3,103 objects, 1,550 observation partitions and 879,999,727 observation rows. PR #145 merged and portal deployment 35704500109 succeeded. | Actual retained-query publication and live browser SQL; current provider completeness and native-to-Bronze lineage remain unresolved. Code/UI deployment is not data publication. |
| TERYT, PRG, GLEIF, MF VAT, IMGW and GIOS | Local source work exists. Dated inventory/audits establish some retained TERYT/PRG/GLEIF/MF outputs. | Reviewed adapters, safe publication, source meanings, coverage and query release acceptance. IMGW/GIOS production completeness is not established. |

## Live BDL observation

At 10:09:34 UTC, a stable Drive queue read verified 2,464 checkpoint-reported native files / 639,962,821 bytes, 1,131 selection-complete subgroups out of 2,420, ten in-flight subgroups and 62 blocked selections. The active writer is PID 383069, with heartbeat 10:09:17 UTC. Compared with the ten-worker launch baseline, this is 46 more native files / 11,041,844 bytes and 41 more completed subgroups. These are checksum-verified control totals, not an independent full-native-byte audit.

The queue SHA-256 is `9c663abff2f8d4289836ef8127184a1b2c638e91135e2ce764e385c689e894e5`. Full selection coverage is not complete. Blocked selections rose from the launch baseline of 56 to 62; 60 failed subgroup pass outcomes and 62 blocked selections are different counters. Keep the requested ten-worker configuration, investigate failures without clearing their history, and preserve provider backoff and the single-coordinator boundary. No ingestion or VPN configuration was changed for this review.

## Local versus remote

The original root remains at `c7898d7`, with zero unique committed changes and 11 commits behind the reviewed main baseline. It contains 11 modified tracked files and 53 non-temporary untracked files. The separate `.tmp/zohelo-local-review.tar.gz` is excluded from integration; its contents were not inspected. A source-only backup, tracked patch and per-file hashes were saved under `.local/repo-review-2026-09-22/` before any integration work.

| Local group | Disposition |
| --- | --- |
| Codex installer, editor extension, setup and related instructions | Small environment PR, preserving the owner's approved pinned installation and explicit agent startup. Validate without rebuilding the live extraction container. |
| `.gitattributes` | Separate line-ending review; do not mix repository-wide normalization with feature delivery. |
| DBW memory/performance edits | Port selectively onto the current protected snapshot/release implementation from #136. Never replace that file with the older local loader. |
| TERYT/PRG adapters, loaders and models | Separate source contracts, safe immutable publication and geography/grain tests, then existing-data portal acceptance. |
| GLEIF adapter/loader/models | Keep #137's hardened local-only downloader; port useful local changes without restoring its rejected Drive publication path. |
| MF VAT, IMGW, GIOS adapters/models | Source-sized tested PRs after native transfer, publication and meaning defects are repaired. |
| Test import/timeout edits | Retain useful import isolation; establish correct runtime and measured fixture needs before accepting increased timeouts. |
| Three local audit JSONs | Byte-identical to current main; already integrated, not three new deliverables. |
| Root handoff/delivery text | Reconcile into the current delivery record as dated evidence; never overwrite newer main status with the root copy. |
| Unpublished Telegram branch | Preserve independently; not a substitute for portal query access or a reason to enable messaging. |

The DBW query branch's committed tree is identical to its squash merge `b7b0abb`. Its worktree still has a separate uncommitted delivery note, which was backed up before cleanup is considered. The retained DBW-audit branch also matches its merge exactly. The old supervisor branch has two differing portal files and must not be deleted merely because its main task merged. The unpushed Telegram branch contains six files and 712 added lines; it remains unaccepted work.

The intended endpoint is one current development main, source-sized review branches, and explicitly pinned operational checkouts only where a running job needs them. A preserved dirty root is a recovery measure, not the permanent development model. Do not `pull`, reset, clean, stash, switch or rebuild it while unclassified work and active operational dependencies remain.

## Concrete code risks

Twelve local modules contain the same delete-before-create upload pattern: DBW, GLEIF, IMGW, MF VAT, PRG and TERYT Bronze loaders, plus GIOS, GLEIF, PRG, TERYT, IMGW and MF VAT source adapters. A mismatched prior object is deleted before its replacement is uploaded and verified. The local DBW loader additionally deletes the `part_1_6.parquet` pilot. These are reproducible source-level data-loss windows, not evidence that existing Drive data is already corrupt. They must not be pushed as production-ready code.

Other specific review findings:

- `dim_corporate_entity` joins both direct and ultimate active parent relationships into a column named `direct_parent_lei`; this can duplicate entity grain and mislabel the parent role. Retain relationship types separately and prove cardinality with fixtures.
- IMGW daily staging does not reconcile multiple versions of a station/date observation, although the fact key assumes that grain. Weather flags turn null measurements into false through `ELSE 0`; unknown is not a measured absence. Threshold names and precipitation-code meanings need explicit source evidence before governed publication.
- The PRG Gold boundary model depends on a separately enabled TERYT model. Feature switches and reference-edition/grain dependencies need an executable contract rather than independent flags that can select an invalid graph.
- Several local folder resolvers select the first matching Drive folder rather than rejecting ambiguity. Native transfer validation, exact manifests and existing release safeguards must survive integration.

## Documentation and workflow drift

There are 12 current workflow files, not the eight or nine in the 7 September historical audits. Those old audits remain dated evidence. The access-check, upload-check and Drive-migration workflow files were removed on 15 September in `b430402`; the earlier BDL API workflow was removed in `9c5f04b`. Some current instructions still advertise those absent Actions buttons. Scripts and preservation/provenance requirements remain; a future mutation cannot bypass them merely because its former workflow was deleted.

The policy checker passes the current workflow source. This does not test every runtime outcome or replace main-branch enforcement: a fresh GitHub API read reports `main.protected=false`, with no required check contexts. Routine PR/check discipline remains necessary, and a compatible ruleset is a follow-up, not a setting changed by this audit. Source-specific Actions, portal deployment and runtime tests have different purposes; do not merge them merely to reduce file count.

The current source inventory has 182 product/family IDs plus seven separately mapped gap candidates. All 182 `connection_status` values belong to the dated research inventory, not the present operational state. The checker confirms 231 categories, of which 177 have some research lead and 54 have none. Neither research leads nor inventory size measure ingestion coverage.

Older owner-selection gates are superseded by ADR 0004. Starter coverage is superseded by ADR 0006. Processing-in-Landing language in earlier material is superseded by ADR 0009. Its cross-source implementation is not complete merely because the rule is written. Existing source-specific releases remain usable while intake and downstream stages are separated safely.

The 20 September eight-wave programme is the current delivery ordering, not a second list of newly approved products: Wave 0 closes the existing baseline; Wave 1 establishes reference backbones; Wave 2 adds macro/central-bank sources; Waves 3–8 retain the fiscal/register, social, environment, corporate/knowledge, multilateral and conditional-source backlog. The older research phasing remains a research organization, not competing execution authority.

The portal guide's 64 MiB session wording conflicts with newer architecture/operations text describing a 512 MiB shared session budget and a separate 64 MiB DBW indicator-selection boundary. This audit records that discrepancy; it does not raise a runtime limit or claim the blocked supplementary code inspection succeeded. Portal acceptance must measure the deployed behavior and make scope/limit errors clear.

Ten broken local documentation link targets were found. Historical references should be labeled or linked as historical rather than restoring obsolete production workflows. Current documentation gets a navigation index and dated precedence notes; historical metrics and release evidence are preserved.

## Acceptance and continuation

Both `python scripts/check-source-coverage.py` and `python scripts/check-workflows.py` passed from the isolated current-main checkout. All changed Python syntax parsed; JSON/CSV IDs matched. These lightweight checks are not the full application suite and do not validate unsafe local writers. Documentation-only changes require JSON/link/diff review, not another production data run.

No new engineering agent was dispatched: the owner reports the local Codex/AGY allowances exhausted, and this review was performed directly through the connected repository and devcontainer tools. The existing hourly delivery task is the scheduled engineering follow-through; ordinary source jobs execute independently. A scheduled task is not continuous AI execution or proof of the next run. The owner should not have to reassign routine work, but a concrete tool/authentication/billing boundary remains visible when it blocks delivery.
