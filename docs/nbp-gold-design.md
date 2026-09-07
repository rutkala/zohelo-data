# Proposed NBP gold design

**Status: Proposed.** This is a dimensional design recommendation for the NBP release. It does not approve a business metric, a time aggregation rule, or a commodity master-data policy. The source facts and units below follow the [NBP data contracts](nbp-data-contracts.md) and [correction decision](decisions/0001-nbp-corrections.md).

## Recommendation

Use a small Kimball-style gold layer with conformed dimensions and two facts:

- `fact_fx_quotes`, containing exchange-rate observations from Tables A, B and C.
- `fact_gold_prices`, containing NBP gold-price observations.

This fits an owner-only platform with four small datasets because the stars are easy to query, expose clear grains, and can be consumed directly by SQL or a future MetricFlow model. Shared dates and currency identities can be governed once while source-specific measures remain visible. The requested business catalogue should expose each fact’s grain and source relationships, then connect approved metric definitions.

An Inmon-style normalized warehouse would centralize source history and relationships before presenting dimensional marts. That can help when many domains, teams, and integration rules need one canonical enterprise model, but it adds joins and governance work before this NBP use case needs them. A hybrid can retain a normalized integration layer and publish dimensional marts later; that is compatible with the platform’s bronze/silver layers and retained raw versions, but should be chosen only if future sources create a demonstrated need. The recommendation is therefore Kimball-style gold over the existing retained source history, as a technical implementation recommendation matching the owner’s requested facts, dimensions and bus matrix. The modeled layer is not implemented yet; business metric definitions still require the owner’s input.

## Facts and grains

| Gold relation | One row means | Analytical key | Measures and units |
| --- | --- | --- | --- |
| `fact_fx_quotes` | One currency quotation in one NBP table publication | `(source_table_key, effective_date, currency_key)`; retain source `no` and batch identity as provenance | Table A/B `mid`; Table C `bid` and `ask`. Preserve the API numbers unchanged until the historical unit question is resolved; do not apply website quotation multipliers. |
| `fact_gold_prices` | One NBP gold quotation for one publication date | `(effective_date, commodity_key)`; use the source-specific key `nbp_gold_1000_gram` | `price_pln_per_gram_1000`: NBP-calculated PLN price for 1 g of gold at 1000 millesimal fineness. Preserve the raw `cena` value. |

`fact_fx_quotes` must keep Table A/B average quotations distinct from Table C buy and sell quotations. A Table A or B row has `mid`; a Table C row has `bid` and `ask`, with no implied `mid`. `source_table_key` is required so a query cannot silently combine A, B and C values that have different publication behavior and meanings.

The current silver models use `(table type, effective date, code)` as the analytical current-value key after conflict validation. They do not retain every source field needed by the gold design: silver currently drops the NBP table number `no` and Table C `tradingDate`. Gold provenance and date-role enrichment must therefore read the actual bronze/raw fields and recorded batch metadata. Do not invent publication numbers, trading dates, or revision identifiers.

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

FX rates and gold prices are measures of quotation level. They must never be summed across currencies or across days. A future time-series metric may average observations only after the owner approves the source table, currency/commodity selection, publication-day treatment, missing-observation rule, and handling of revised values. Table C bid and ask should remain separate unless an owner-approved spread or other formula is defined. Gold aggregation likewise remains open; this document does not invent a daily, monthly, or cross-commodity metric.

The next owner decision is which business metric examples to support first;
the availability of dynamic queries is a separate later question. Native
MetricFlow remains the planned governed engine alongside SQL. Currency keys,
source-specific commodity keys and calendar attributes are engineering work;
ask the owner only when a consequential business interpretation is needed.
Historical unit checks and cross-source mappings require source evidence,
not an owner guess.

Current corrected source values should feed normal analysis after validation, while retained raw versions and detected-change records remain available for traceability. This design does not require a comparison interface.

