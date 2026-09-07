# Repository, architecture and data-path audit — 7 September 2026

Audit baseline: remote `main` at `a6b4f0fa45dc7dc9eee33ca7a8aca1dab9aa4ffe`, tree `90b96841e62dbe64c44c30884d1cc782204f5a25`. The local checkout had different commit history but the identical tracked tree. Findings below describe that baseline and the chosen treatment; current delivery status and hold conditions are maintained in [the delivery record](../deliverables.md).

## Main findings and treatment

| Finding | Treatment in this audit |
| --- | --- |
| Uploaded Parquet was SQL-checked after the current pointer changed | Require staged content/provenance validation before platform promotion. Negative tests use invalid Parquet, metadata mismatches and broken evidence; they prove the prior pointer remains selected. |
| Retained releases had no supported recovery operation | Add expected-current/target-pinned promotion, full validation and append-only audit under production Actions concurrency. No automatic rollback or Drive transaction is claimed. |
| State, manifest inputs, catalogue datasets and dbt relations were only weakly tied together | Enforce exact source/input/catalogue/model bindings in staged validation. Preserve old release-reader compatibility. |
| Metrics were repeatedly blocked on a business-use question | Implement five source-defined daily MetricFlow metrics from official NBP documentation. Guard non-additive grain and validate actual native queries against gold. Optional derived calculations remain separate. |
| Source metadata omitted methodology/frequency/reuse fields | Research and publish these fields in the existing catalogue metadata. Commercial redistribution remains explicitly unresolved; public access is not a licence grant. |
| Missing source keys were silently retained without an investigation event | Add bounded same-publication currency missing/reappeared candidates, stable typed-record hashes, changed fields and versioned evidence. Keep current values. Entire missing publications remain ambiguous. |
| A historical cutoff could label a build that still included later data | Reject regressive cutoffs. Rebuild is not an as-of query. |
| Many source configuration fields had no effect | Split effective, versioned ingestion settings from provider metadata. Use dated endpoints for all catch-up/rechecks; remove misleading latest-only/folder fields. |
| Retry behavior did not match the stated policy | Bounded Retry-After handling and retry coverage for 408/429/5xx, preserving exhausted attempts. |
| Legacy Bronze/Silver scripts still offered mutating entry points | Unconditionally stop direct execution before imports/authentication; retain needed compatibility helpers and move current runtime metadata into its own module. |
| Portal had upstream identity and competing npm/Bun/Docker paths | Keep supported npm/static deployment, correct package/docs identity, remove inactive nested workflows and unsupported second toolchain. Preserve upstream licence. |
| Mandatory docs mixed historical proposals and live instructions | Current architecture and operating guide now describe supported behavior; old proposal/plans are clearly historical. |
| Desktop/mobile Run controls used different variants | Align shared theme variant and add light/dark computed-style browser regression. No redesign. |
| Long-term raw/state snapshots grow toward finite build bounds | Add read-only capacity and inventory reporting at 70% thresholds. Retain all evidence; reference-safe compaction/deletion is explicitly held with a reopening threshold. |

The [Actions audit](2026-09-07-workflows.md) inventories all nine original workflows. Eight remain active: the completed migration workflow is retired, useful diagnostics remain separate, production/deployment guards and action pins are added, and stale portal validation runs are cancelled. Fresh-container data tests intentionally overlap with normal data validation only for environment changes.

## Boundaries that remain deliberate

The failed-attempt state snapshot includes operational `last_attempt` metadata. Its pointer can change while the successful sequence and coverage remain unchanged. This is not a source-data promotion; tests verify no false progress. A separate status-pointer service would add complexity without changing the current release guarantee.

Production serialization is a workflow/operating rule, not a Drive locking primitive. A local production opt-in still exists for explicit recovery work; the operating guide directs normal production changes through the serialized workflow. Multiple independent writers would require a real lease/fencing design or different storage.

Release code SHA, raw IDs and typed record-contract versions distinguish transformation evidence from source evidence. The source change table does not pretend to capture transformation-only row deltas between builds. A full cross-version transformation delta ledger is held until a concrete investigation or model change requires it; immutable prior releases remain available for comparison.

The current data volume supports the chosen single-machine design. This does not establish unlimited retention, future-source performance or account allowances. Native high-water memory measurements are reported separately for parent and children; they are not a measured concurrent process-tree peak. Browser transfer budgets do not establish browser RAM or device performance.

## Repository maintainability

The existing top-level boundaries—config, source code, dbt models, tests and docs—are sensible. Generated databases, targets, logs, node_modules and build output are excluded. Small tracked database/CSV files are explicit browser fixtures. Direct and transitive Python requirements have distinct purposes and are retained.

The initial tracked tree had 474 files, approximately 4.69 MB; portal code accounted for 354 files and 4.05 MB. Its large DuckDB-WASM/Monaco assets and inherited workbench features dominate browser delivery. Broad feature removal and module splitting are postponed because the owner explicitly asked to leave the portal stable. A measured performance requirement should select that work rather than speculative rewrites.

The Python code remains a documented script-style project. Wholesale namespace/package migration, formatting churn and splitting every large module are not prerequisites for the existing CLI. New shared responsibilities were extracted where needed. Local development uses the pinned environment and normal editor tooling; adding a source adapter still requires engineering, even without AI.

## Repository settings and access limits

The public branch API reported `main.protected=false`, protection disabled and no required status contexts at audit time. The available GitHub connector has no administration mutation capability; source files cannot enforce server-side branch protection. Keep reviewed PR/check discipline now, and configure server-side protected-branch/ruleset requirements before adding collaborators or relying on automatic enforcement. No administration setting is claimed changed.

Hosted service conditions are recorded in [the current architecture](../architecture.md), including GitHub Pages' commercial-hosting restriction. The current owner tool is not a commercial SaaS launch approval. No additional source was ingested and no paid platform was added during this audit.

## Verification method

Use genuine dbt builds and native MetricFlow queries with credential-free fixtures; verify code in GitHub's production builds and browser matrix; publish only after those gates. Then check the actual Drive release in a fresh process, rebuild its raw inputs and record bounded storage/capacity measurements. Exact successful run IDs, counts and release identity belong in the final release evidence linked from the delivery record, not inferred from this audit text.
