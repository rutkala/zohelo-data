# Current status

Updated 4 October 2026 (Europe/Warsaw).

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
- Attempt 4 (job 110920804688) ran from 18:09 to 23:16 Warsaw on 2 October.
  Eleven completed batch receipts added another 62 distributions, 315,670,566
  rows and 12,315,373,140 data bytes. Represented hashes increased from 3,386
  to 3,448, leaving at most 178 in this retained-current phase. Five transient
  catalog connection errors recovered on their first retry. The twelfth wrapper
  invocation exhausted its final 501-second allowance, so the job ended with
  `subprocess.TimeoutExpired`; this was a bounded incomplete session, not a
  deterministic data error. Archive, final coverage and portal-index steps did
  not run, and no Landing object was deleted.
- Attempt 5 (job 111050472387) ran from 00:11 to 05:16 Warsaw on 3 October.
  Eight completed batch receipts added another 29 distributions, 365,805,538
  rows and 13,953,181,129 data bytes. Represented hashes increased from 3,448
  to 3,477, leaving at most 149 in this retained-current phase. Four transient
  catalog connection errors recovered on their first retry. The ninth wrapper
  invocation exhausted its final 463-second allowance, so the job ended with
  `subprocess.TimeoutExpired`; this was another bounded incomplete session,
  not a deterministic data error. Archive, final coverage and portal-index steps
  did not run, and no Landing object was deleted.
- Attempt 6 (job 111126238596) ran from 06:18 to 11:23 Warsaw on 3 October.
  Fourteen completed batch receipts added another 46 distributions, 492,390,228
  rows and 19,118,453,837 data bytes. Represented hashes increased from 3,477
  to 3,523, leaving at most 103 in this retained-current phase. The final wrapper
  invoked the child with a 116-second session allowance and exhausted its
  236-second process timeout, so the job ended with `subprocess.TimeoutExpired`;
  this was another bounded incomplete session, not a deterministic data error.
  Archive, final coverage and portal-index steps did not run, and no Landing
  object was deleted.
- Attempt 7 (job 111176652093) ran from 11:40 to 16:45 Warsaw on 3 October.
  Twelve completed batch receipts added another 31 distributions, 491,482,102
  rows and 19,017,269,501 data bytes. Represented hashes increased from 3,523
  to 3,554, leaving at most 72 in this retained-current phase. The final wrapper
  invoked the child with an 874-second session allowance and exhausted its
  994-second process timeout, so the job ended with `subprocess.TimeoutExpired`;
  this was another bounded incomplete session, not a deterministic data error.
  Archive, final coverage and portal-index steps did not run, and no Landing
  object was deleted.
- Attempt 8 (job 111229570741) ran from 17:10 to 22:16 Warsaw on 3 October.
  Twelve completed batch receipts added another 27 distributions, 453,139,381
  rows and 17,730,273,645 data bytes. Represented hashes increased from 3,554
  to 3,581, leaving at most 45 in this retained-current phase. The final wrapper
  invoked the child with a 130-second session allowance and exhausted its
  250-second process timeout, so the job ended with `subprocess.TimeoutExpired`;
  this was another bounded incomplete session, not a deterministic data error.
  Archive, final coverage and portal-index steps did not run, and no Landing
  object was deleted.
- Attempt 9 (job 111284970618) ran from 22:18 Warsaw on 3 October to 03:23
  Warsaw on 4 October. Seven completed batch receipts added another 13
  distributions, 246,635,305 rows and 9,826,589,466 data bytes. Represented
  hashes increased from 3,581 to 3,594, leaving at most 32 in this
  retained-current phase. Seven transient catalog read failures recovered on
  their first retry. The eighth child was invoked with a 900-second session
  allowance but exhausted the wrapper's remaining 1,769-second process timeout
  without an unambiguous receipt. Durable Iceberg membership is authoritative
  for any ambiguous final commit. This was another bounded incomplete session,
  not a deterministic data error. Archive, final coverage and portal-index
  steps did not run, and no Landing object was deleted. After confirming no
  competing writer and that the pinned membership/ambiguous-commit recovery
  logic remains appropriate, attempt 10 is requested from the exact existing
  job and resumes from durable Iceberg membership. Remaining standard responses
  and historical/control inputs still require explicit reconciliation after the
  retained-current phase.
- Attempt 10 (job 111343221687) ran from 04:21 to 09:27 Warsaw on
  4 October. Six completed batch receipts added another 9 distributions,
  324,487,076 rows and 12,550,990,138 data bytes. Represented hashes increased
  from 3,594 to 3,603, leaving at most 23 in this retained-current phase.
  Three transient catalog read failures recovered on their first retry. The
  seventh child was invoked with a 900-second session allowance but exhausted
  the wrapper's remaining 3,169-second process timeout without an unambiguous
  receipt. Durable Iceberg membership remains authoritative for any ambiguous
  final commit. This was another bounded incomplete session, not a deterministic
  data error. Archive, final coverage and portal-index steps did not run, and no
  Landing object was deleted. Remaining standard responses and historical/control
  inputs still require explicit reconciliation after the retained-current phase.
- Attempt 11 (job 111388581090) ran from 09:34 to 12:28 Warsaw on
  4 October. Durable membership started at 3,605 rather than 3,603, proving that
  attempt 10's ambiguous final child had committed two additional distributions.
  Four completed batch receipts then added another 5 distributions, 162,374,760
  rows and 6,434,521,588 data bytes. Represented hashes increased from 3,605 to
  3,610, leaving 16 in this retained-current phase. The fifth child failed
  deterministically while performing the exact duplicate-key aggregation:
  DuckDB reached the decoder's explicit 512 MiB memory ceiling. This is not a
  catalog transient and the unchanged job must not be rerun. The decoder is
  therefore adjusted within the existing Python/DuckDB production stack to use
  a bounded 4 GiB working limit, one aggregation thread and no insertion-order
  preservation; parsing, keys, values and validation semantics are unchanged.
  Archive, final coverage and portal-index steps did not run, and no Landing
  object was deleted. Remaining standard responses and historical/control
  inputs still require explicit reconciliation after the retained-current phase.
- Memory-fix continuation run 37198279662 (job 111424542101), pinned at
  a21c5d0, failed at 13:22 Warsaw on 4 October before reading or writing a
  Eurostat distribution: the three new DuckDB settings were misindented and
  Python raised IndentationError while importing the decoder. Commit 8c128bd
  corrects only that indentation (including the adjacent comments). The full
  decoder and existing embedded workflow Python both passed a local syntax
  parse; no regression suite or new workflow was introduced. Run 37198869621,
  pinned at 92c1c1f, started at 13:28 Warsaw with the correction; Transformations
  was immediately restored to manual-only at af35a69. This run still needs to
  demonstrate the bounded-memory settings against the previously failing large
  distribution. The last confirmed durable membership remains 3,610 represented
  hashes and 16 missing current retained distributions. Previously reported
  cumulative row totals must account for the two attempt-10 commits whose rows
  were not present in completed batch receipts before claiming a new exact total.
  Ingestion remains paused; archive/coverage and later-source reconciliation
  remain outstanding.
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
