# Review sequence and connection-test brief

**Recommendation for owner review, 8 September 2026. No source selection, account,
subscription or connection test is implied.** Project status and owner answers remain in
[the delivery record](../deliverables.md).

## Review the landscape before choosing ingestion

The inventory is intentionally broad. A review should first decide which analytical
questions deserve a complete, reliable vertical slice. Source availability alone is not a
reason to ingest everything immediately. Keep paid, restricted and difficult sources visible
so they can inform future products, while using the established free/public and permitted-use
criteria for initial production candidates.

Suggested sequence:

| Batch | Candidate families to review | Why review them together | Output of review |
| --- | --- | --- | --- |
| R0 — shared identifiers | TERYT; relevant NUTS/LAU correspondence; country/time/unit/classification metadata | Establish links that Polish and global statistical facts will reuse | Named reference distributions, historic version policy, mapping evidence |
| R1 — comparative statistics | GUS BDL and a small Eurostat slice; then World Bank WDI; assess DBW/OECD expansion | Combines detailed Poland coverage with EU/world context and exercises source overlap | A small named indicator list, geographic/time scope, comparison question and semantics |
| R2 — operational time series | One IMGW or GIOŚ source, or one PSE electricity dataset | Exercises live collection alongside historical loading and revisions | Required freshness, available history, quota scope and independent-lane acceptance example |
| R3 — public institutions and entities | Sejm/ELI; public procurement BZP/TED; KRS/REGON/GLEIF where access fits | Tests documents/events, organization identity and source-specific legal/access boundaries | Exact public fields, identity/join rules, document/notice version semantics |
| R4 — domain depth | ZUS/NFZ/education/health/agriculture/culture/local government and specialist global sources | Adds depth after shared dimensions and ingestion contracts are proven | One domain fact model and source comparison at a time |
| R5 — conditional products | Commercial market data, social APIs, account-scoped marketplaces, restricted disclosure products | Scope, retention, coverage and downstream use can differ sharply from public statistics | A concrete permitted product use, supported access and any approved cost |

These are proposed review batches, not launch dates or a numeric quality ranking. A source
may move earlier if the owner's intended analysis makes it more useful. Being `review-first`
does not establish that access, rights or complete history have passed onboarding.

## Suggested first review discussion

Consider **a Poland-and-Europe regional profile** as the first candidate vertical slice:
population, demographics and a carefully selected labour/economic measure at the finest
compatible published geography, alongside wider international comparisons. This is a
proposal rather than an invented requirement. It uses overlapping sources meaningfully,
requires versioned geography and forces statistical definitions to be explicit early.

Before any connection tests, settle only:

1. The decision or comparison the first slice should support.
2. The acceptable initial geographic/time scope and freshness; full history can fill later
   only where the source supplies it, with visible incomplete-history status.
3. The intended exposure: private analysis, derived public indicators, or redistributed
   source-level data. Rights checks depend on that distinction.

The owner can instead choose energy/environment, companies/procurement, health, or
another domain. The research supports that discussion without presuming their priority.

## Bounded connection-test card for each selected dataset

| Field | Must be concrete before the test |
| --- | --- |
| Source identity | Inventory ID, exact dataset and endpoint/distribution, version and documentation |
| Intended use | What is being answered; internal vs public/redistributed output |
| Allowed access | Required account/key, documented permission and terms for the requested operation |
| Request budget | Maximum calls, bytes, elapsed time and rate; taken from documentation and selected conservatively |
| Representative partitions | Recent published data, one historical period, and a sparse/empty/revised case where available |
| Assertions | Real schema; stable key; units; pagination; oldest/newest reachable dates; expected source totals |
| Recovery | Retry/backoff, expired cursor, duplicate page, partial response and restart behavior |
| Two-lane feasibility | Whether recent work can proceed during backfill without exceeding shared quota or blocking publication |
| Modeling | Proposed silver grain, source keys, reference joins, gold fact/dimension role and one semantic acceptance result |
| Measurement | Observed requests, transfer volume/time, row counts, errors and projected backfill bounds |
| Outcome | Proceed, narrow scope, change loading pattern, investigate rights, or defer—with evidence |

Use a development fixture/isolated environment for the later tests, then preserve a small
permitted representative fixture for repeatable validation. Do not test by launching an
unbounded production historical load. A documentation link or an HTTP 200 is not an
acceptance result.

## How to review the workbook

Start with Inventory filters for geography, domain, access evidence and proposed priority.
Use Access and Rights for authentication, cost, limits and unresolved conditions; Loading
and Modeling for history/revisions/keys/grain; Evidence for the official pages and their
verification depth. Review fields are a working worksheet. Send conclusions in chat so
the canonical delivery record and selected source contracts can be updated once agreed.

No overall weighted source score is used. Combining openness, data value, geographic
detail and implementation effort into one number would hide trade-offs and invent the
owner's preferences. Unknown limits or terms remain visible rather than receiving an
optimistic score.
