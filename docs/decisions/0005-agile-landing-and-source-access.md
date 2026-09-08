# Immediate collection, incremental data access and source credentials

Status: Accepted owner direction, 8 September 2026. Implementation and live evidence are
recorded in [the delivery record](../deliverables.md).

The owner rejected four-hour gaps between tiny collection batches, inaccessible new Landing
data, and treating free account opportunities as deferred without a usable credential path.
This supersedes the corresponding first-delivery limitations in ADR 0004 and the expansion
plan. It does not turn incomplete history or raw responses into modeled business facts.

## Decision

Start authorized collection immediately. Within each source job, run consecutive bounded
batches and publish accepted increments between batches. Retain provider quota reservations,
recent/history fairness, retry-after controls, source serialization and storage checks. Use
a thirty-minute recurring trigger as recovery and continued catch-up, with a bounded session
budget; execution need not wait for the next scheduled trigger after a reviewed deployment.

Expose real collected data before completing the entire medallion programme. Publish a
source-specific immutable Landing snapshot containing Parquet response-envelope rows. Each
row retains the exact decoded UTF-8 response payload, receipt/request identity, retrieval time,
raw hash/size and reported record count. Rejected responses remain evidence outside this
queryable snapshot. The SQL row grain is one accepted response, not one observation.

Each source promotes its own verified `current-landing.json` pointer. The portal discovers
and pins these snapshots alongside the existing NBP release, checks manifests and file
hashes, and exposes `01_landing.<source>_responses` for preview and SQL. NBP's immutable
release and dbt catalogue retain their existing validation. Landing response tables do not
claim Silver/Gold typing, semantic metrics or cross-source comparability.

Develop subsequent source-specific Bronze/Silver, Gold and semantic slices as usable
increments. Only the references and business definitions needed for the next slice are
dependencies; finishing every global classification or source family is not a delivery gate.

Use encrypted GitHub Actions repository secrets as the source credential store. Provide
exact secret names, official free-account registration routes and a hidden-input setup helper.
Secrets stay outside Git, browser storage, request descriptors and receipts; transport injects
them only for the contracted provider origin. BDL selects its registered quota profile when
`GUS_BDL_API_KEY` is configured, while retaining existing quota and cooldown history.

Research free accounts as part of normal onboarding, distinguishing higher limits from access
to otherwise unavailable datasets. Do not pretend that a saved key implements an adapter.
Free-account identity, email verification and account terms are concrete setup steps for the
owner when they cannot be completed with the supplied context. Paid subscriptions and
card-backed trials remain separate decisions. No new account is claimed to exist without
confirmation from its provider.

## Acceptance

- A live batch starts during delivery and retained accepted counts advance.
- The published Landing snapshot can be restored and queried independently of ingestion.
- The deployed portal lists and queries actual Landing data beside the existing NBP tables.
- New snapshots preserve previous accepted files and reject tampering or ambiguous promotion.
- A missing key preserves anonymous collection; a configured key uses only the intended
  provider and never appears in durable evidence or logs.
- Source-specific live access, empty responses, failures and incomplete scope stay distinct.
