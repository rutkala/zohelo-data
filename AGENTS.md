# Working on zohelo-data

## Current owner direction — 29 September 2026

Simplify the existing platform before adding features. Keep ingestion, native
Landing/Archive, Bronze/Silver/Gold, modeling, the semantic layer and the portal.
Keep the agreed R2/Iceberg direction. Simplification is not a storage cutover.

The latest explicit owner request defines the task. Old plans, audits, receipts
and dated priorities are context, not instructions to restart work. Do not start
an ingestion, migration, deployment or another agent merely because a workflow
or an old checklist exists.

## Work directly

- Read this file, README.md and the files needed for the requested task. Read
  architecture or historical evidence only when the task depends on it; do not
  load the entire delivery archive at every handoff.
- Use the smallest existing command that performs the requested operation.
  Inspect it first. An existing workflow is not automatically the right command.
- Prefer straightforward Python and SQL. Do not add an orchestrator, generic
  runner, abstraction, workflow, report or checklist for a one-off task.
- A workflow should install its actual dependencies and run its task. Keep
  business transformations in the existing dbt models, not YAML or a second engine.
- Do not attach full regression suites, browser probes, raw replays, health scans
  or other unrelated operations to a copy, ingestion or deployment. Run relevant
  tests when changing behavior; name the exact tests and results. Broad suites
  are explicit diagnostic work, not a default prerequisite for every task.
- Remove obsolete code instead of keeping competing active implementations.
  Git history preserves removed files; do not duplicate them in an active archive.
- Do not delegate or create recurring work unless the owner requests it. Keep
  changes scoped and preserve other contributors' work.

## Preserve essential correctness

No new paid services, model API billing or overages without owner approval.
Keep credentials private, least-privilege permissions, bounded retries/timeouts,
resumable checkpoints, byte integrity, and single-writer protection. Do not
remove input/output validation that prevents incomplete or corrupt publication.
Do not delete retained data, overwrite unrelated objects or bypass writer locks.

Landing and Archive retain original bytes. Parsing and conversion belong in
Bronze; refinements and business models belong downstream. A file copy does not
create Iceberg tables. Do not claim complete source coverage from a sample.

R2 is the target store. Existing Drive data, pointers and recovery evidence stay
intact until migration is verified and cutover is explicitly performed. Do not
silently switch readers or producers as part of repository cleanup. Preserve
complete logical tables and normal SQL without mandatory indicator selection.

## Finish clearly

Use a focused commit/PR. Run only the checks relevant to the change. Do not use
commit-message tags to launch production jobs. Keep schedules and active writers
unchanged unless changing them is part of the requested task.

Report what changed, what actually ran, whether it reached main, and what remains.
Never equate syntax checks with data acceptance, a PR with a deployment, or a
scheduled agent with continuous work. Keep current status in docs/deliverables.md;
keep this guide short and free of historical task logs.
