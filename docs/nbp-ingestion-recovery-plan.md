# Planned NBP ingestion and recovery design

**Status: Planned; implementation is not complete.** This plan covers the four
configured NBP sources and fits the existing immutable silver-release protocol.
It sets technical recovery behavior without promising an owner-facing update
cadence, SLA, or metric definition.

## Durable state

Drive remains the durable authority. Add a root-level control area separate
from the four data zones, for example `ingestion-control/`:

| Object | Purpose |
| --- | --- |
| `attempts/{attempt_id}.json` | Append-only request outcome and provenance. |
| `raw-references/{source_id}/{response_sha256}.json` | Reference to immutable response bytes in landing/archive; no duplicate authoritative copy. |
| `states/{state_id}.json` | Immutable state snapshot for all four sources. |
| `current-ingestion-state.json` | Mutable pointer to the latest state snapshot. |
| `changes/{change_id}.json` | Append-only observed canonical-record transitions. |

One logical request record should contain:

```text
attempt_id, run_id, source_id, mode
request_url, params, interval_start, interval_end
started_at_utc, finished_at_utc, retry_count
final_http_status, outcome, response_sha256, raw_file_id
observation_min_date, observation_max_date, code_sha
```

The state snapshot should contain, per source, `last_attempt_at_utc`,
`last_checked_through_date`, `last_successful_ingestion_at_utc`,
`latest_observation_date`, `historical_cursor`, successful/no-observation
intervals, and `last_validated_release_id`.

These fields have distinct meanings:

- `last_checked_through_date` advances only over contiguous completed intervals
  with durable `data` or `no_observations` outcomes. A later successful request
  must not hide an earlier failed interval. Retain non-contiguous coverage
  explicitly; bootstrap missing state from configured history without assuming
  that a legacy maximum observation date proves historical completeness.
- `last_successful_ingestion_at_utc` means valid response bytes were retained;
  a valid 404 has no observation bytes but is still a completed check.
- `latest_observation_date` is the greatest effective/publication date in
  validated rows and can be older than the checked-through date.
- Coverage is the set of completed intervals and their observed date bounds;
  it must not claim one row for every calendar date.
- `last_validated_release_id` changes only after a complete silver publication.

The existing protocol reads and validates the prior release, uploads a
candidate, checks for pointer drift immediately before the mutable update, and
reads the pointer back after writing. This is pre-write drift detection plus
readback under the existing serialized workflow; Drive pointer replacement is
not compare-and-swap. Apply the same discipline to the ingestion-state
pointer, or leave the prior state usable when the update outcome is uncertain.

## Request planning and outcomes

The configured NBP API limit is 93 calendar days per dated request
([NBP API documentation](https://api.nbp.pl/en.html)). Full mode should use
inclusive intervals of at most 93 days, persist the next cursor only after the
interval and its attempt record are durable, and resume the failed interval
after interruption. Use UTC dates rather than the current local/naive clock.

For normal invocations, a latest-93-day recheck is a **proposed configurable
technical default**, not an agreed owner cadence or SLA. If the state is older
than that overlap, first request resumable catch-up intervals from the next
unchecked date, then request the latest overlap. The overlap catches recent
changes; it cannot discover corrections older than the overlap by itself.

A resumable rotating full-history sweep from each source's configured
historical start date is therefore required for older corrections. Its cadence,
chunking limits beyond the API maximum, and runner budget must be explicit
configuration measured against actual costs and runtime; they are not a
business promise or an assumption about how often NBP revises history.

Use these logical outcomes:

| Response or failure | State transition | Data action |
| --- | --- | --- |
| Valid 200 with rows | Advance completed interval and ingestion time | Retain raw bytes; validate/canonicalize rows. |
| Valid 404 for a requested interval | Advance checked interval only | Record `no_observations`; preserve all prior rows and releases. |
| 400 or another permanent request error | Do not advance checkpoint | Fix request/configuration; retain the failure record. |
| Timeout, transport error, 408, 429, or 5xx | Retry with bounded configured backoff; then fail if exhausted | No checkpoint advance after exhaustion. |
| 200 with invalid schema/JSON | Do not advance checkpoint | Retain bytes in quarantine for replay; mark `parse_error`. |
| Raw upload/state-pointer failure | Do not advance checkpoint | Keep the prior state and release; retry the same interval. |

The implementation must use an HTTP timeout. A successful raw upload may be
replayed safely by response hash; unchanged rechecks must not create duplicate
business rows or duplicate change events.

## Observed changes and evidence

Canonical typed rows are compared within their source observation keys:

- Tables A and B: `(table, effectiveDate, code)`;
- Table C: `(table, effectiveDate, code)`, while retaining `tradingDate`;
- Gold: the gold observation/publication date.

Maintain a current canonical-record hash index. For each key:

1. Same canonical hash means unchanged replay and emits no event.
2. A changed canonical measure or unit emits `source_value_changed`, with prior
   and new hashes, changed fields, request interval, raw file, batch, and
   detection time. This means “the source returned a different canonical
   record”; it does not establish that NBP officially announced a correction.
3. A changed provider label, table number, descriptive field, or other
   non-measure provenance field with unchanged canonical value emits
   `source_metadata_changed`. It must not be presented as a value correction.
4. A difference caused by parser, normalization, unit-conversion code, or
   another Git revision emits `transformation_changed` in build evidence and
   never a provider-correction event.

The API documents no revision number, correction flag, correction timestamp, or
change feed. A request interval, retrieval time, response hash, and our
detection time are **request evidence**. A provider correction notice or other
NBP-published revision reference is separate **source-published evidence** and
must be retained when found. In its absence, leave provider revision time and
reason unknown. Multiple changes between our observations cannot be
reconstructed, and history before the first retained observation is not
automatically recoverable.

## Stage and release handoff

Raw ingestion writes immutable responses under the existing canonical
`01_landing/{source_id}` layout; this matches the current bronze inventory's
dataset-folder contract. The current ingestor computes `source_system` and
`landing_subpath` for a displayed path but actually writes only the source ID,
so that mismatch must be resolved before relying on those config fields.

Bronze conversion may archive a landing file only after its Parquet output is
verified. A deterministic output identity must make an upload-before-move
retry idempotent. Silver consumes retained bronze/raw inputs, compares current
canonical rows, and publishes a candidate only after all four datasets and
tests pass. The candidate manifest should carry the ingestion-state snapshot
ID or hash and change-ledger summary. A failed candidate leaves the previous
release pointer and validated state usable; ingestion can already have retained
new raw bytes, so the next build must be able to replay them.

## Current defects to repair

`src/ingestion/base_ingestor.py` currently has no durable request ledger,
checkpoint, coverage inventory, correction index, timeout, or HTTP retry. Its
incremental mode calls `/last/1`, which cannot catch up an outage or provide
the proposed overlap. Full mode has no persisted cursor, uses a naive local
clock, silently discards 404s, and can fall back to 90-day chunks despite the
configured 93-day limit. Timestamp-only filenames do not deduplicate unchanged
responses or carry request/hash provenance. `BaseIngestor` also initializes and
creates the Drive infrastructure on every run, and a multi-source run can
retain partial successes without a durable run state.

`src/transformation/bronze_builder.py` can create a duplicate Parquet object if
upload succeeds but moving the source file to archive fails. The silver and
release code already validates an all-four candidate and protects the prior
release, but its manifest currently lacks ingestion checkpoint/change-ledger
state and legacy bronze links remain explicitly unverified.

## Acceptance tests

Add fixture and mocked-Drive tests for:

- inclusive 93-day planning, UTC boundaries, resumable full cursor, and
  catch-up followed by the configurable recent overlap;
- valid 200, valid 404, 400, timeout/429/5xx exhaustion, malformed response,
  and storage failure transitions;
- immutable content-addressed raw reuse and unchanged replay idempotency;
- canonical value change, metadata-only change, parser/transformation change,
  and old-period change found by a rotating full-history sweep;
- no deletion or checkpoint regression after 404, missing rows, or a failed
  candidate;
- a silver/build failure retaining the prior release, followed by successful
  replay and publication with state linkage;
- fresh-process restoration of the published all-four release and its
  ingestion evidence.

No implementation, schedule activation, SLA, provider-correction claim, or
business metric approval is implied by this plan.
