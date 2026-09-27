# Domain-led autonomous source onboarding

Status: Accepted owner requirements and initial implementation decision, 8 September 2026.
The intake-only delivery boundary and account setup approach are superseded by
[ADR 0005](0005-agile-landing-and-source-access.md).

## Decision

The owner delegates source sequencing and routine implementation, connection testing and
production ingestion. The target is every feasible source, dimension, fact and recoverable
history, with worldwide context and deepest practical Poland coverage. Source-by-source
owner review is no longer a gate. This supersedes that gate in ADR 0003 and the dated
research review plan. Ask only for a consequential missing business decision or a concrete
account, credential, paid-service or restricted-use prerequisite.

Use stable domains, subdomains and categories independently of provider identities, analytical
dimensions and official classification editions. The versioned taxonomy and coverage ledger
map all 182 research families; candidate assignments establish discovery coverage only.
Actively register additional category-specific candidates where coverage is weak.

Implement an initial bounded Landing campaign for WDI, GUS BDL and Eurostat on the existing
Drive/Actions platform. Give each provider independent serialization, state and quota; schedule
recent collection and historical backfill in separate fair queues. Preserve exact bytes and
accepted/rejected receipts, reserve quota durably before requests, advance only after validation,
and stop on uncertain state writes. A serialized preparation job initializes shared folders.

## Consequences

This authorizes the first public-source production collection into the owner's private Drive
and its continuing scheduled batches. It does not authorize paid subscriptions, signups or
unreviewed public redistribution. No source-defined fact becomes an approved derived metric
merely because it was collected.

The first implementation ends at Landing. NBP keeps its established full medallion release.
New-source Bronze/Silver, domain-specific Gold, shared dimensions, semantic definitions,
source-release publication and cross-source release pinning are subsequent milestones in
[the full plan](../source-expansion-plan.md). They must satisfy model and recovery gates.

The initial queue and byte caps are admission envelopes, not a solution for unlimited history.
Before they constrain progress, implement measured sharding/compaction and source-specific
bulk or change discovery. Daily job execution does not prove every series refreshes daily.
Track each separately; never report a provider complete from starter datasets or category maps.
