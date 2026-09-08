# Source campaign operations

The owner now requires complete selected-source coverage (ADR 0006). In addition to
the API response campaigns described below, the same serialized source job runs
`python src/full_source_campaign.py --source world_bank_wdi --allow-production-write`
or the Eurostat equivalent. Full archives are stored under
`01_landing/<source>/bulk`; their separate durable ledger is under
`06_control/source_campaigns/<source>_bulk`. The main provider ledger remains the
shared quota authority for both API and bulk requests.

The full path enumerates official distributions and retains all supplied dimensions
and periods. Drive may identify uploaded raw bytes as ZIP, gzip, XML or text even when
the upload requested a generic binary MIME type. Verification accepts non-native media
types while rejecting Google-native documents, folders and shortcuts; exact identity,
parent, ownership, size, MD5 and streamed SHA-256 checks still establish raw integrity. Its summary reports catalogue distributions, validated current
distributions, pending/failed tasks and raw bytes. Raw coverage is not a modeled
Silver/Gold/semantic release. A successful run can still have pending catalogue work.

Campaign state pointers now accept v1 and v2. On the next successful save, v1 data is
preserved in immutable v2 shards and the existing pointer is promoted only after
verification. No raw/receipt identities or existing Landing pointers are replaced.
Total accepted tasks, recurring roots and retained raw bytes no longer have starter
ceilings. Provider quotas, finite historical queue backpressure, object size bounds
and actual Drive/runner capacity checks remain operational controls.

State shards are bounded at 2 MiB. The writer recomputes their hash-prefix layout on
a changed save, compacting an earlier dense set of small shards while retaining every
old immutable object. This avoids hundreds of 40–50 KiB Drive operations when a full
catalogue only slightly exceeds the previous 512 KiB split threshold. State manifests
remain bounded at 1 MiB. Roll out through the serialized provider jobs: binaries with
the old 512 KiB read bound cannot read newly written larger shards. A rollback must
retain the 2 MiB reader bound; never delete current state to run an older binary.

WDI/Eurostat retain the 60 requests/15 minutes operator fair-use window, serialized
requests and provider Retry-After. The former 600/12 hours and 6,000/week starter
budgets were not documented provider quotas and no longer halt their bulk backfills.
Seven days of attempt history is retained. BDL's documented anonymous/registered
multi-window quota enforcement remains unchanged.

The `Source ingestion campaigns` workflow collects exact public WDI, GUS BDL and Eurostat
responses into Drive Landing and publishes verified response tables for portal preview and SQL.
Each source has its own Landing snapshot; the NBP release pointer remains separate.
See the canonical [delivery record](deliverables.md) for live evidence and
[implementation plan](source-expansion-plan.md) for the remaining programme.

## Run and pause

The schedule is `7,37 * * * *` UTC: a recovery/catch-up trigger every thirty minutes, independent
of NBP's daily schedule. Each source retains its requested API batch count (default three)
and a 900-second aggregate between-operation API budget. A batch remains bounded to twelve
source requests and 240 seconds between attempts. Normal WDI/Eurostat jobs run one API batch,
verify the provider ledger, then start the full-distribution session (up to 24 requests and
1,200 seconds between operations). After full collection and fresh restore succeed, remaining
API cycles use the original API budget minus elapsed initial API time. Fewer than 60 remaining
seconds skips that continuation. Any failed initial/bulk/restore stage prevents continuation
API writes. BDL, paused-history and publish-only operations keep the ordinary API sequence.

Source calls and durable publication already in flight finish safely; these between-operation
budgets are not hard wall-clock deadlines. The workflow has a 60-minute outer timeout. Quota,
capacity, no-due-work and source failures stop collection without spinning or resetting
provider history.

From Actions, run the workflow on `main`, choose all or one source, choose the batch count
(default three), and optionally pause history. `publish_only` exposes already-collected accepted
responses without making source API requests.
The history input also pauses reconciliation and the full-distribution backfill.
Recent API and discovery tasks remain eligible.
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
python src/source_campaign.py --source world_bank_wdi --backend drive --allow-production-write --cycles 3 --session-seconds 900 --summary campaign-summary.json
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

API campaigns have no cumulative raw-byte, pending-task, recurring-root or completed-task
ceiling. Discovery pauses when finite history/reconciliation work reaches 600 tasks and resumes
as that queue drains. Individual API responses remain bounded at 8 MiB; v2 manifests at 1 MiB
and shards at 2 MiB. Full distributions use streamed transfer with physical capacity checks,
independent of the API response limit. Provider quotas, execution budgets and measured physical
capacity remain enforced. The trigger frequency and consecutive batches use available budgets
sooner; they do not promise a daily refresh for every discovered series. When an execution or
capacity bound is reached, the run reports the specific reason and preserves progress for
continuation. Adding a key does not bypass storage or compute limits.

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
new business models need dbt grain/unit/status tests, raw replay, release restore and catalogue/semantic
acceptance. Transport-envelope Landing publication is an earlier, independently validated boundary.

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

## Queryable Landing publication

After each batch with accepted state, the publisher appends verified accepted responses to
immutable Parquet fragments and promotes a source-specific `current-landing.json` pointer under
`06_control/source_campaigns/<source>`. Its immutable manifest records exact accepted-receipt
checkpoint membership, files, row counts, hashes and remaining publication backlog. Old raw
objects, receipts and published fragments remain retained. New publication is bounded to 24
responses and 16 MiB of raw bytes per call, with an 8 MiB limit per Parquet file and a 1 MiB
manifest limit. A failed candidate leaves the previous validated snapshot available.

The portal exposes `01_landing.world_bank_wdi_responses`, `01_landing.gus_bdl_responses`
and `01_landing.eurostat_responses` when their published snapshots exist. One SQL row is one
accepted HTTP response. `payload_utf8` contains the source JSON or text; `record_count` is a
reported transport count that may include metadata, missing values and overlap. This is usable
Landing data, with source-specific modeled observations still developed through dbt.

A fresh process can verify and query published Landing without calling a source API:

```bash
python src/source_campaign.py --source gus_bdl --backend drive --allow-production-write --verify-landing
```

Run this through the serialized production workflow. Local development uses `--backend local`
and an explicit `--local-root`. The portal still applies its 64 MiB per-engine data download
budget; it does not silently load an unlimited archive.

## API keys and free accounts

Use [the source-account setup guide](source-accounts.md). Save the BDL key as the encrypted
repository secret `GUS_BDL_API_KEY`; the next BDL collection uses `X-ClientId` only at the
contracted BDL HTTPS origin. An absent/empty secret leaves anonymous collection enabled.
The registered profile uses 400 requests/15 minutes, 4,000/12 hours and 40,000/7 days, below
BDL's documented 500/5,000/50,000 registered limits. Existing quota attempts and cooldowns are
retained when the profile changes. Credentials are not added to saved requests or receipts.
Other registry entries distinguish prepared account setup from connected runtime adapters.
