# NBP corrections: current values with a change record

6 September 2026. **Business direction accepted. Implementation evidence updated 7 September 2026.**

The owner wants detectable NBP corrections tracked and applied, and does not currently see a need for a comparison interface. Normal SQL, gold models and metrics should therefore use the latest validated source values. Keep earlier raw observations and published releases for investigation and recovery.

## Evidence and limits

The [NBP API documentation](https://api.nbp.pl/en.html) provides requests for dated tables and gold prices, including date ranges of at most 93 days. Its documented response fields do not include a revision number, correction flag, correction timestamp or change-feed endpoint. We did not establish that a particular historical NBP observation has been corrected. Do not infer that corrections never occur, or treat this API as a complete history of revisions.

Re-fetching the same source period and comparing its records with a previous observation can detect **a change returned by the source**. That is the implementation approach. It does not prove the reason for the change or the exact time NBP made it. If an official correction notice exists, retain its reference as additional evidence.

The implemented `nbp_change_events` relation compares versioned canonical typed records from retained, validated HTTP-200 response envelopes. It records stable typed-record hashes and changed fields while retaining response hashes as request evidence. The record contract and detection method are versioned separately from release code; a parser or transformation change belongs to the platform release evidence and must not be presented as an NBP event.

## Decision

- Normal analysis uses one current validated value for each dataset's observation key. For exchange rates, compare within the same NBP table, effective date and currency code. For gold prices, compare within the gold-price dataset and its observation date. Preserve table number and all source date roles as provenance; changing a table number must not conceal a changed observation.
- Compare canonical typed source records, including units and relevant source metadata. File names, JSON key ordering, whitespace, retrieval timestamps and formatting differences are not data corrections. Compare the same source representation; do not confuse website quotation multipliers or an internal transformation change with an NBP revision.
- Preserve each observed raw version and its request interval, retrieval time, content hash and batch identity. Record the prior/new record hashes, changed fields, detection time and affected dataset/keys. Separate value changes from descriptive metadata changes. Detection time is not the provider's revision time.
- When a later successful FX response still contains the same source publication date but omits a previously observed currency key, record `source_record_missing` as a candidate for investigation. Record a later return as `source_record_reappeared`. Neither event asserts that NBP officially withdrew or restored a quotation, and neither deletes the retained current value.
- Apply changes to silver and dependent gold/metric outputs only after validation. Publish a consistent new release with matching code, tests and definitions; retain the last complete release if a candidate fails.
- Surface a small catalogue status such as “source changes detected” with affected dates and counts. A side-by-side history UI and business metrics for comparing vintages are outside the current release scope. The change record exists to explain updates and recover from mistakes.

## Loading defaults and implementation boundaries

Use dated catch-up requests after an outage, then recheck the latest 93 calendar days on normal updates. This overlap fits one documented API date-range request per dataset. Older corrections need a historical reconciliation as well: perform one before accepting the first complete release and provide resumable historical sweeps. A monthly sweep is the initial technical cadence to validate against measured request volume and runner time before enabling it; it is not an NBP correction guarantee or an already enabled schedule.

Never advance checkpoints or replace a complete release after a partial response, schema/parse failure or failed publication. A previously present observation disappearing is a possible withdrawal to investigate only when comparable successful source evidence remains. The implemented comparison is deliberately narrow: it compares currency keys only across successful responses that both contain the same source publication date. An entirely absent publication is ambiguous and remains outside automated missing-record detection. An empty/404 response or transient failure cannot create a withdrawal candidate and must not silently delete validated history. Store successfully retrieved raw input for replay even when downstream validation fails.

Repeated unchanged input must not create duplicate business rows or duplicate change events. Multiple changes between our observations cannot be reconstructed, and revisions before our first retained observation are not automatically recoverable. Existing legacy files must be inventoried for usable provenance; filename sorting is not a trustworthy replacement for a source/change ledger.

## Acceptance checks

Fixtures now prove unchanged replay is idempotent, A→B→A value changes remain separate events, metadata-only changes are distinguished, duplicate source countries do not create spurious events, a missing currency can reappear without deleting current data, and an absent publication does not create a withdrawal candidate. Ingestion and release tests separately prove that completed 404 coverage does not materialize a data envelope and that a failed candidate leaves the prior release usable. These checks establish platform behavior; they do not prove that NBP announced a correction or withdrawal.
