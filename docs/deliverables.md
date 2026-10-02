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
the retained-data reconciliation.

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
- The final committed batch implies 564 current retained distributions remain
  (580 missing before that batch, 16 committed). This is a dated estimate,
  not a complete-source coverage percentage. Job 110633452659 was resumed
  on 2 October after 07:01 Warsaw, after confirming no competing writer.
  It resumes from Iceberg membership and skips completed distributions.
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
