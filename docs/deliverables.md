# Zohelo-data current status

## Owner priority — 29 September 2026: simplify before expanding

Keep the agreed architecture and existing sources. Reduce unnecessary workflows,
agent instructions, test execution and orchestration. Do not resume older roadmap
items merely because they appear in historical plans. The current owner task is
repository simplification, not a new migration run, ingestion or portal cutover.

## Baseline cleanup

The cleanup reduces active workflow definitions from 26 to 12, removes standalone
pilots/probes and duplicate validation workflows, and replaces the long historical
agent guide with one short common guide. Copilot, Claude and Gemini entrypoints
point to that guide rather than loading old work queues.

PR automation checks syntax and workflow safety only. A single Python test file
can be selected explicitly. NBP publication no longer appends restore/replay/health
work; portal deployment no longer appends lint/unit regressions. The main-only R2
copy Action has no commit-message trigger and runs the retained transfer command.
No data operation or portal deployment is part of accepting this cleanup.

## Still to simplify

The WDI/Eurostat pipelines and DBW publication paths retain their existing stage
orchestration. Simplify one at a time into direct, independently runnable intake,
Bronze and modeled operations. Preserve correct checkpoints, byte integrity,
writer ownership, native payloads and model semantics. Do not replace them with
a generic runner or a new orchestration framework. Existing schedules are unchanged.

The internal migration and NBP runners also retain their current implementation;
removing workflow overhead is not a claim that all runtime code is now minimal.
Target tests at the behavior being changed instead of running every historical
acceptance scenario for every task.

## Data/platform boundaries unchanged

R2 native Landing and Iceberg Bronze/Silver/Gold remain the target. This cleanup
neither proves copy completion nor changes Drive/R2 data, producer destinations,
release pointers or portal readers. Existing source-coverage limitations remain.
BDL retains the ten-route WireGuard design and resumable ownership checks.

The [preceding status at the inspected revision](https://github.com/rutkala/zohelo-data/blob/12d89525c57db1339a58ac1cbdc4ce6ed1786ea6/docs/deliverables.md)
and [earlier delivery evidence](deliverables-before-r2-20260929.md) remain historical
references. Preserve recorded execution holds; this cleanup does not resolve or
retry the held R2 observer publication. Any separately configured automation is
not automatically cancelled by editing this document; this record does not claim
otherwise. Current run status must be read from the execution system.

See the [current workflow inventory](audits/2026-09-07-workflows.md) for commands and
[architecture](architecture.md) for component boundaries. A PR/commit proves a code
change, not a successful data run or completed migration.
