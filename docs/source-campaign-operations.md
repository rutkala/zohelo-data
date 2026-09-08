# Source campaign operations

The `Source ingestion campaigns` workflow collects exact public WDI, GUS BDL and Eurostat
responses into Drive Landing. It does not publish new portal tables or modify NBP releases.
See the canonical [delivery record](deliverables.md) for live evidence and
[implementation plan](source-expansion-plan.md) for the remaining programme.

## Run and pause

The schedule is `17 */4 * * *` UTC: six bounded runs daily, independent of NBP's daily schedule.
From Actions, run the workflow on `main`, choose all or one source, and optionally pause history.
The history input also pauses reconciliation. Recent and discovery tasks remain eligible.
Set a source's `enabled: false` in `config/source-campaigns.yaml` through a checked PR to pause it
persistently. Disabling the workflow stops all new campaigns and retains existing evidence.

A reviewed merge containing `[run-source-campaigns]` runs the changed campaign workflow once.
Ordinary code pushes do not opt into production writes. Drive writes require the explicit CLI
flag and the serialized `main` Actions runtime; do not launch another production writer outside
that route. The preparation job checks account headroom and creates shared source paths before
provider jobs execute independently. Existing configured OAuth secrets are used without consent
prompts or logging secret values.

For a selected provider the job calls:

```bash
python src/source_campaign.py --source world_bank_wdi --backend drive --allow-production-write --summary campaign-summary.json
```

A developer can collect a bounded public batch to an explicitly selected disposable local root:

```bash
python src/source_campaign.py --source world_bank_wdi --backend local --local-root /tmp/zohelo-campaign-check
```

This local command performs real source reads, not a fixture check. Use unit tests for routine
validation. All settings are versioned in `config/source-campaigns.yaml`; adapters admit only
contracted HTTPS hosts and reject redirects. No credentials are needed for the three source APIs.

## Durable state and recovery

Each source has independent control objects under `06_control/source_campaigns/<source>` and
raw response objects under the configured Landing zone's `<source>/responses`. Current-state
pointers identify immutable, verified state snapshots; hashes and sizes are checked on load and
write. Accepted receipts retain source task, request, retrieval time, code SHA, response metadata,
record count and exact-raw descriptor. Rejected bytes/receipts remain evidence and do not advance
successful checkpoints. Existing NBP state/release pointers are untouched.

Quota attempts are persisted before the source request. Restarting cannot reset a provider's
rolling limits. Recent generations wait for their older pages; history advances only after a
validated response. A provider-wide 429/503 Retry-After survives restart. A malformed/retired
series is retried later while unrelated tasks can advance; auth/service failures stop that
provider. An ambiguous Drive write stops the job for inspection rather than reporting a retry
as successful. Re-run the same workflow after the cause is resolved; never delete pointers or
successful history to force a retry. There is no automatic data deletion.

## Capacity and report meanings

Initial limits per source/run are 12 requests, 240 seconds and 8 MiB per response. The fair lane
cycle allocates recent/history/recent/history/discovery/reconcile slots; absent lanes yield their
slot. Provider quotas are independent and BDL's anonymous windows have explicit lower limits.
The raw counter is cumulative received body bytes, including repeated responses, not physical
Drive usage. Immutable states and receipts consume additional storage. Preparation checks enough
free account quota for a worst-case configured parallel run, including bounded state snapshots;
it cannot reserve capacity against unrelated account writes.

Initial admission caps: 2,048 pending tasks, 1,000 recurring roots, 5,000 completed tasks,
256 MiB cumulative received bytes per source, 4 MiB per state snapshot. Discovery yields before
pending/root limits. Reaching a state/raw bound pauses new collection; it does not mark completion.
Do not raise caps blindly: source-specific bulk/change discovery, a sharded completion ledger,
reference-safe compaction and physical retained-byte accounting are required scaling milestones.
At six runs/day the maximum is 72 source HTTP attempts/day before runtime/quota limits. This is
an initial measured batch size, not a promise to refresh all BDL variables or Eurostat slices daily.

Each job prints a JSON summary and writes the Actions step summary. Inspect per-lane successes,
failed requests, pending retries, next retry, last attempt/success and cumulative bytes. Received
records can include metadata, repeated representations and explicit missing observations;
they are not a count of unique analytical facts. `coverage_status: incomplete` and
`publication_layer: 01_landing` remain explicit. Run summaries retain each attempt failure even if later requests succeed. Fresh verification also
reports the newest three retained rejection diagnostics without raw object identifiers. The
verification step runs after partial collection failure, so retained metadata can still be
verified; the failed collection remains a failed job. Failed requests make the job fail even when other
requests succeeded and their durable progress is retained. A failed/no-op job is not coverage.

## Validation and next gates

Run `python scripts/check-source-coverage.py`, `python scripts/check-workflows.py` and
`bash scripts/check-data.sh`. These are credential-free. Tests cover fixtures, real local store
integration, mocked Drive recovery, quota persistence, source schemas and existing NBP/dbt behavior.
After collection, a fresh process restores the current state, samples at most the newest 12
accepted receipts and revalidates one exact response per represented lane. This verifies bounded
cold replay, not full-history replay. Live Actions evidence is separate. Further source families need concrete distribution contracts;
new models need dbt grain/unit/status tests, raw replay, release restore and catalogue/semantic
acceptance before they are described as published.

The WDI timeout is 90 seconds following a measured 45-second production timeout. Task IDs and
request parameters remain unchanged; time budgets are checked between attempts and do not
interrupt an in-flight bounded request or checkpoint save.

## Corrected parser and planning errors

After a parser correction, the explicit `retry_validation` workflow input or CLI
`--retry-validation-failures` can make eligible pending tasks immediately due. A reviewed
merge can opt in with `[retry-source-validation]` together with `[run-source-campaigns]`.
Only the latest retained rejection per task among the newest 20 receipts is considered: it
must be HTTP 200, `ValueError`, from an older code revision and match the exact pending task.
The control preserves failure counts, provider cooldowns, quota reservations, accepted work
and all receipts; it records a bounded durable audit before any new request. It cannot clear
HTTP 429/503 or network backoff, and repeated control under the same revision is a no-op.

The BDL contract migration retires only the two obsolete root-locality requests, preserving
their original tasks and reason in `plan_dispositions`. Municipality-scoped discovery replaces
them. Superseded requests are not counted as ingested or completed. The generic migration
guard rejects changes to quota, provider cooldowns, accepted evidence or other protected state.
