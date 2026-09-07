# Business review and decisions

The portal's [Review & decisions](https://data.zohelo.com/?view=review) view brings the two open owner choices together. The connected [business catalogue](https://data.zohelo.com/?view=catalog) remains the place to inspect source, status and lineage context.

**Status: owner input open.** The NBP definitions and every metric shown here are proposals. No response automatically approves a metric, changes published data, or onboards a source.

| Item | Current status |
| --- | --- |
| Source definitions and calculations described below | Researched proposal; not an executable metric. |
| A BR-001 or BR-002 response | Local draft of owner input; not approval. |
| Approved NBP metric | None. Explicit agreement and later executable validation are still required. |

## BR-001 — First NBP metrics

Choose one starting direction:

1. **Published daily values first** — Table A middle rates, Table B middle rates, Table C bid and ask values, and NBP gold prices, each on its actual publication date.
2. **Daily values plus Table C spread** — the same values plus the proposed calculation `ask - bid` for one Table C currency and publication.
3. **A different use case** — describe the decision or comparison that would be useful.

The researched definitions are in [NBP business definitions](nbp-business-definitions.md). They separate official source facts from proposed metrics. In particular, A/B middle rates and C bid/ask values are not interchangeable; rate levels are not summed; missing publication days are not filled automatically; and historical FX quote-unit normalization remains unresolved. Gold is the NBP-calculated PLN price of one gram at 1000 fineness. A time average or return is a later proposal requiring an explicit observation and missing-data rule.

## BR-002 — Next-source priorities

Select **Eurostat**, **World Bank WDI**, or describe another source, topic, country, or business question. The candidate research is deliberately not a recommendation to ingest: [source candidates](source-candidates.md) records official access, reuse evidence, attribution requirements, and dataset-specific exceptions. A selected dataset still needs its own licence and commercial-reuse review before onboarding.

## Sharing an answer

The form saves a draft only in the current browser and profile. It does not sync to another device or person. When ready, copy the generated response into this chat or download and attach it. That shares the owner’s preference for a follow-up scope; it is not an approval or an automatic platform change.
