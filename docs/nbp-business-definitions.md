# NBP business definitions: proposal

**Research checked 7 September 2026.** This is a catalogue proposal, not an approval of metrics or a claim that a commercial licence exists.

## What the official source says

NBP's public Web API publishes current and historic foreign-exchange tables and NBP-calculated gold prices. Table **A** and **B** contain calculated *middle* exchange rates; Table **C** contains calculated *buy* (`bid`) and *sell* (`ask`) rates. A/B/C are therefore different quotations and must stay distinct. The API identifies `no` as a table number, `effectiveDate` as the **publication date**, and `tradingDate` as the **trading date for Table C only**. The API can return no data (including 404) for a requested publication date; it does not promise a value for every calendar day. [NBP Web API](https://api.nbp.pl/en.html)

Gold is a separate NBP quotation: the documented price of **1 gram of gold at 1000 millesimal fineness**, calculated at NBP. The current NBP page explicitly labels the price in PLN; the established model labels it `PLN per gram, 1000 fineness`. [NBP Web API](https://api.nbp.pl/en.html) · [NBP gold price](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/cena-zlota/)

The API does not supply a currency quote-unit or multiplier field for `mid`, `bid`, or `ask`. NBP web tables can show quantities such as 100 units for some currencies, while API values look normalized in checked examples. This is insufficient to state that every historical API FX value means “PLN per 1 currency unit.” Preserve the API number as published; do not retrospectively rescale it or infer a multiplier from `symbol`.

The API is public and uses HTTPS, but neither its documentation nor the pages checked grant commercial reuse or redistribution rights; the rates page carries an all-rights-reserved copyright notice. Commercial use, redistribution, and required attribution remain unresolved until applicable NBP terms or permission are reviewed. [NBP rates pages](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/)

## Proposed starting catalogue

| Proposed definition | Meaning and grain | Guardrails |
| --- | --- | --- |
| Daily Table A middle FX quote | One published Table A `mid` for `effective_date × currency` | Use only A; no cross-table blending; numeric unit is “as published by API.” |
| Daily Table B middle FX quote | One published Table B `mid` for `effective_date × currency` | Use only B; do not assume a daily cadence. |
| Daily Table C bid / ask FX quote | One published C `bid` and `ask` for `effective_date × currency`, carrying `trading_date` | Keep bid and ask as separate measures; no implied middle rate. |
| Daily NBP gold quote | One NBP gold price for `effective_date × nbp_gold_1000_gram` | PLN per gram at 1000 fineness; do not combine with another gold source or product. |

These are **proposed daily published values**, meaning observations on NBP publication days, rather than a fabricated daily calendar series. Rate levels and gold prices must never be summed across days, currencies, or source tables. Do not automatically calendar-forward-fill a missing publication.

### Optional later derived comparisons — not approved

- **Table C spread:** `ask - bid`, for the same Table C publication and currency. This is a proposed arithmetic difference, not an NBP-published metric.
- **Period average:** a simple arithmetic mean of included *published* observations only, reported with start/end dates and observation count. The scope must name table, currency/commodity, and missing-observation treatment.
- **Return/change:** compare consecutive published observations only after defining whether “previous” means the prior available publication and how source changes are handled. No missing date may silently become a zero, carry-forward value, or assumed observation.

## Implication for the existing gold model

The current facts already support exact source facts without inventing business semantics:

```text
fact_fx_quotes:  source_table_key + effective_date + currency_key
                 measures: A/B mid; C bid, ask; provenance: source_no;
                 C trading_date; has_zero_source_quote
fact_gold_prices: effective_date + commodity_key=nbp_gold_1000_gram
                  quote_currency_key=PLN; price_pln_per_gram_1000
```

`dim_currency` identifies the quoted FX currency (and PLN as quote currency); `dim_source_table` prevents A/B/C mixing; `dim_commodity` retains NBP's source-specific gold identity; `dim_date` supports grouping but must not imply that a calendar date was quoted. `source_no`, both date roles, and the zero-source flag preserve source facts: zero is a source value to retain, not a missing value to replace. These are model-grain implications, not approval to publish semantic models or a new data flow.

## One owner choice

**Use daily published values first, or also derived comparisons?**

## Reference appendix

- [NBP Web API documentation](https://api.nbp.pl/en.html): dataset scope, table meanings, response fields, publication-date queries, 404 behaviour, and gold definition.
- [NBP Table A](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-a/), [Table B](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-b/), [Table C](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-c/), and [gold](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/cena-zlota/): official publication pages; the FX pages show varying quote quantities and the site carries the copyright notice.
