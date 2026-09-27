# Complete coverage of selected source products

Status: Accepted owner direction, 8 September 2026. Delivery evidence belongs in
[the canonical delivery record](../deliverables.md).

Selecting a source means collecting its complete available scope: all published
datasets, dimensions, geographies, history and accompanying metadata within the
selected source product. Starter indicators, country lists and permanent task/raw
byte ceilings do not meet that acceptance condition. A bounded execution is an
operational checkpoint, not the end of the ingestion programme.

Use the complete official WDI CSV distribution for the baseline. Enumerate Eurostat's
official full-distribution inventories and retrieve the prepared files without
dimension filters. Follow asynchronous requests and catalogue revisions. BDL has no
documented complete bulk export or changed-variable feed: exhaust its catalogue and
observation pages while retaining provider quotas. Existing NBP exchange-rate A/B/C
and gold ingestion remains the complete selected REST product; other NBP statistical
publications and World Bank products beyond WDI are separate inventory entries and
must not be described as already ingested. The research inventory records these
provider/product distinctions rather than silently narrowing the owner's broader goal.

The campaign state uses immutable shards and a verified current pointer. Keep all
existing v1 receipt identities, ordering, quota reservations, raw versions and Landing
pointers during migration. Queue backpressure may wait for finite admitted historical
work; cumulative completed tasks and recurring series must never permanently close
catalogue discovery. Individual object, execution and real available storage bounds
remain necessary; a resource failure is a visible blocker with retained progress.

Stream large distributions to disposable local files, validate their complete content,
upload exact bytes to Drive, and verify the remote object before accepting it. Store
immutable receipts identifying the dataset, request, provider version, retrieval time,
archive members, hashes and sizes. Completed raw coverage is separate from completed
Bronze/Silver/Gold/semantic modeling. A small portal distribution index can identify
large archives; browser memory limits must not be presented as source ingestion limits.

Recent requests run before each full-distribution batch. Both paths share the provider
quota ledger and serialized writer group. Backfills retain checkpoints across runs;
new inventory versions receive their own work and do not wait for the entire historical
catalogue to finish. This provides progress on both fronts, not a promise of simultaneous
unlimited requests or daily complete BDL refreshes beyond its published quota.

Acceptance requires measured catalogue totals and validated current distributions,
explicit pending/failed work, fresh-process raw restore, and actual Actions execution.
“Workflow succeeded” and “all selected data ingested” remain different statements.
Free BDL registration improves available quota but does not remove the need for paging
or supply a change feed. No account is claimed without its configured credential.
