# NBP gold design

The runner defines 15 platform datasets across bronze, silver, change evidence, facts and dimensions. See [the delivery record](deliverables.md) for the verified release and [source-observation definitions](nbp-business-definitions.md) for the five daily MetricFlow metrics. This design does not imply a period aggregation or cross-source master-data policy.

## Chosen design

Use a small Kimball-style gold layer with conformed dimensions and two facts:

- `fact_fx_quotes`, containing exchange-rate observations from Tables A, B and C.
- `fact_gold_prices`, containing NBP gold-price observations.

This fits an owner-only platform with four small datasets because the stars are easy to query, expose clear grains, and can be consumed directly by SQL or the daily MetricFlow models. Shared dates and currency identities can be governed once while source-specific measures remain visible. The requested business catalogue should expose each fact’s grain and source relationships, then connect approved metric definitions.

An Inmon-style normalized warehouse would centralize source history and relationships before presenting dimensional marts. That can help when many domains, teams, and integration rules need one canonical enterprise model, but it adds joins and governance work before this NBP use case needs them. A hybrid can retain a normalized integration layer and publish dimensional marts later; that is compatible with the platform’s bronze/silver layers and retained raw versions, but should be chosen only if future sources create a demonstrated need. The recommendation is therefore Kimball-style gold over the existing retained source history, as a technical implementation recommendation matching the owner’s requested facts, dimensions and bus matrix. The modeled layer is implemented on `main` and included in the verified v2 data release. Source-defined daily metrics use the native on-demand query interface; optional derived calculations and a hosted API are separate scope. The earlier v1 silver release remains retained as a historical baseline.

## Facts and grains

| Gold relation | One row means | Analytical key | Measures and units |
| --- | --- | --- | --- |
| `fact_fx_quotes` | One currency quotation in one NBP table publication | `(source_table_key, effective_date, currency_key)`; retain source `no` and batch identity as provenance | Table A/B `mid`; Table C `bid` and `ask`. Preserve the API numbers unchanged while historical FX unit normalization remains unresolved; expose values as published and do not apply website quotation multipliers. |
| `fact_gold_prices` | One NBP gold quotation for one publication date | `(effective_date, commodity_key)`; use the source-specific key `nbp_gold_1000_gram` | `price_pln_per_gram_1000`: NBP-calculated PLN price for 1 g of gold at 1000 millesimal fineness. Preserve the raw `cena` value. |

`fact_fx_quotes` must keep Table A/B average quotations distinct from Table C buy and sell quotations. A Table A or B row has `mid`; a Table C row has `bid` and `ask`, with no implied `mid`. `source_table_key` is required so a query cannot silently combine A, B and C values that have different publication behavior and meanings.

The v2 silver models use `(table type, effective date, code)` as the analytical current-value key after conflict validation. Unlike the earlier v1 projections, the verified-raw path retains the NBP table number `no`, Table C `tradingDate` when supplied, raw file identity/hash, and ingestion sequence. Gold carries these source fields through silver. Missing source fields stay missing; the build does not invent publication numbers, trading dates, or revision identifiers.

## Conformed dimensions and date roles

| Dimension | Shared use | Contract boundary |
| --- | --- | --- |
| `dim_date` | Publication/effective date for both facts; calendar attributes can support future grouping | The date spine must not imply an observation on a non-publication day. NBP may return no row for weekends, holidays, or another unavailable date. |
| `dim_currency` | Quoted currency for FX; PLN quote currency for both facts | Start with observed source `code`; keep source labels/provenance without inventing validity intervals. PLN is the documented unit of NBP quotations. Future cross-source mappings need evidence before merging identities. |
| `dim_source_table` | Distinguishes NBP Table A, B and C in `fact_fx_quotes` | Keep table type and source dataset identity; retain raw `no` and request/batch details for traceability. |
| `dim_commodity` | Intended role for `fact_gold_prices` | The API supplies no commodity code. A gold row may be described as 1000-fineness, 1 g, PLN only because that is the NBP source definition; use the technical source-specific key `nbp_gold_1000_gram`. Do not equate it to another provider’s commodity concept or quote unit without evidence. |

Use `effective_date` for the NBP publication date in both facts. Preserve `trading_date` as a separate date role for Table C when it is carried from bronze/raw; it is the quotation date, while `effective_date` is the publication date. Do not substitute one for the other or fill missing dates from a calendar.

## Bus matrix

| Fact | Date | Currency | Source table | Commodity | Measures |
| --- | :---: | :---: | :---: | :---: | --- |
| `fact_fx_quotes` | ✓ effective; C also trading role | ✓ | ✓ A/B/C |  | `mid` for A/B; `bid`, `ask` for C |
| `fact_gold_prices` | ✓ effective | ✓ PLN quote role |  | ✓ NBP-defined gold | `price_pln_per_gram_1000` |

The dimensions are conformed where their meanings match. Source-table and commodity distinctions prevent a currency rate from being treated as a gold price or a Table C bid/ask quotation from being treated as a Table A/B average.

## Aggregation boundaries and open decisions

FX rates and gold prices are measures of quotation level. They must never be summed across currencies or across days. A future time-series metric may average observations only after the owner approves the source table, currency/commodity selection, publication-day treatment, missing-observation rule, and handling of revised values. Table C bid and ask should remain separate unless an owner-approved spread or other formula is defined. Gold aggregation likewise remains open; the five daily source-observation metrics do not authorize monthly or cross-commodity aggregation.

The five daily source-observation definitions are researched and implemented in
MetricFlow YAML. `scripts/query_metrics.py` enforces their required grain; it is
the supported native query path. Currency keys, source-specific commodity keys
and calendar attributes remain engineering definitions. Historical currency
identity and cross-source mappings require evidence rather than owner guesses.

Current corrected source values feed normal analysis after validation. Retained
raw versions, typed-record change evidence and immutable releases support audit
and recovery. Optional derived metrics and an always-on semantic API are held
with reopening conditions in the delivery record.
