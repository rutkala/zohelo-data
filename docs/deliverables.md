# Current status

Updated 2 October 2026 (Europe/Warsaw).

## Production baseline

Cloudflare R2 is the production store; Google sign-in is identity only.
The Drive-to-R2 copy was reported complete: 215,002 files, approximately
162.17 GB, zero errors. Later operation records report the Drive root removed.
The current architecture and lifecycle are documented in
[architecture.md](architecture.md) and [data-lifecycle.md](data-lifecycle.md).

Exactly three operational workflows remain: Ingestion, Transformations and
Deploy portal. Repository test suites and regression-test/CI workflows were
intentionally removed. Ingestion has no schedule and must not be started during
the retained-data reconciliation. It also stays paused afterward until the
owner reviews the actual dataflow and later instructs resumption.

## Active owner request

Finish reconciliation using only data already in R2, without new provider calls,
in this order: Eurostat → World Bank WDI → GUS DBW → OpenData.org → GUS BDL.
Continue without routine user supervision; notify on verified completion or a
blocker that actually requires the owner's action.

Completion requires source-specific evidence that retained inputs are represented
correctly in Bronze, recoverable archives with complete hash readback, and safe
live control references. A successful batch, a table's presence, or a partial
archive receipt does not certify a source as complete. Pending ingestion tasks
are outside this request and remain untouched.

## Evidence and remaining work

- NBP, GLEIF, PRG, TERYT and MF reconciliation was reported closed in the
  1 October operation records.
- Overnight Eurostat runs 36931994687 and 36936477069 committed another
  2,063 distributions and 414,422,836 rows. Distinct represented raw hashes
  increased from 999 to 3,062. The recovery run handled two transient catalog
  connection errors, then stopped at its five-hour budget at 06:36 Warsaw.
  It reported an incomplete bounded session, not a data-processing failure.
- Run 36936477069 attempt 2 (job 110713460245) finished at 12:06 Paris/Warsaw
  on 2 October with `bounded_session_finished` (exit 2). All 12 batches passed:
  another 154 distributions, 296,717,866 rows and 11,494,031,374 data bytes were
  committed; represented raw hashes increased from 3,062 to 3,216.
  Seven transient catalog connection errors recovered on their first retry.
  The last receipt implies 410 current retained distributions remain
  (412 missing before the last batch, 2 committed). This count describes this
  retained-data phase, not full Eurostat source coverage.
- Attempt 3 (job 110803292155) ran from 12:34 to 17:37 Paris on 2 October.
  All 18 batches passed: another 170 distributions, 535,608,639 rows and
  20,918,820,055 data bytes committed. Represented hashes increased from
  3,216 to 3,386, leaving 240 in this retained-data phase (248 before the
  final batch, 8 committed). There were no runtime exceptions or retry events;
  the session ended with `bounded_session_finished` and intentional exit 2.
- After checking that no running, queued or waiting Actions existed and that
  the pinned reconciliation code remains appropriate, attempt 4 was resumed
  at 18:09 Paris on 2 October. Job 110920804688 is running from durable Iceberg
  membership; completed distributions are skipped. It retains buffered batch
  logging; the newer live-log change on main applies to new dispatches.
  Archive, final coverage and portal-index steps have not yet run. Remaining
  standard responses and historical/control inputs still require explicit
  reconciliation; no Landing was deleted.
- WDI's bulk ZIP has a verified archive. Its 3,518 retained API response objects
  require source-specific comparison; `scripts/reconcile-wdi-retained-r2.py`
  performs that comparison read-only and does not declare completion.
- DBW requires exact retained native ZIP/CSV versus Bronze row comparison,
  including observations, dictionaries, metadata and taxonomy.
- OpenData's historical Landing publisher used the empty-byte SHA for its large
  ZIP; do not use that placeholder as identity proof. Recompute raw identity and
  prove all parsed member rows before archival/deletion.
- BDL Web requires receipt/selection/raw-archive linkage and exact parsed-row
  comparison. BDL API provenance must be checked separately.

`scripts/inspect-retained-reconciliation-r2.py` collects retained object and
table metadata without provider calls or remote writes. Its bounded probes are
diagnostics, not coverage proof. The recovery workflow stores its JSON as an
Actions artifact for source-specific follow-up.

The existing hourly “Reconcile remaining sources” task is responsible for
continuing safe bounded work and notifying the owner only when all required
sources have verified final evidence. Always inspect live Actions before starting
another writer. Keep steady-state workflows manual-only.
