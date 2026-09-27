# NBP source-observation definitions

**Source research checked 7 September 2026.** These definitions are the
governed baseline for the NBP data already in scope. They can be implemented
without choosing a downstream forecasting, conversion, accounting, or trading
use case because they expose the values NBP publishes at their source grain.
Derived comparisons and period statistics remain outside this baseline.

## Official source meaning

NBP's public Web API exposes current and historic foreign-exchange tables and
gold prices calculated at NBP. Tables **A** and **B** contain calculated
*middle* exchange rates. Table **C** contains calculated *buy* (`bid`) and
*sell* (`ask`) rates. The three table types are different publications and
their values must remain distinguishable. The API defines `no` as the table
number, `effectiveDate` as the **publication date**, and `tradingDate` as the
**trading date for Table C only**. It can return no data for a requested
publication date, including HTTP 404, and does not promise one observation for
every calendar day. [NBP Web API documentation](https://api.nbp.pl/en.html)

The official Table A and B pages describe their middle rates as rates of
foreign currencies **in Polish zlotys**; the Table C page describes its buy and
sell rates as rates of foreign currencies **for Polish zlotys**. The model can
therefore identify PLN as the quote currency. [NBP Table A](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-a/)
· [NBP Table B](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-b/)
· [NBP Table C](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-c/)

Gold is a separate quotation. NBP defines it as its calculated price of **1
gram of gold at 1000 millesimal fineness**, and its publication page labels the
price in PLN. The canonical model unit is therefore `PLN per gram at 1000
fineness`. [NBP Web API documentation](https://api.nbp.pl/en.html)
· [NBP gold price](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/cena-zlota/)

## FX API unit and website quotation quantities

The API response does not carry a quotation quantity or multiplier alongside
`mid`, `bid`, or `ask`. NBP's web tables do: for example, their displayed rows
can quote 100 HUF, 100 JPY, or 10,000 IDR while quoting one unit for many other
currencies. Those website quantities are presentation metadata and must not be
applied to an API value.

Same-date comparisons support that boundary. For Table A on 2 January 2002,
the API reports HUF `0.014511` and JPY `0.029991`, while the official archive
page presents 100 HUF as PLN `1.4511` and 100 JPY as PLN `2.9991`. Current
checked HUF and JPY observations have the same relationship: the API value is
the website amount divided by the displayed quantity. [NBP current Table A](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-a/)
· [API observation,
2002-01-02](https://api.nbp.pl/api/exchangerates/tables/a/2002-01-02/?format=json)
· [NBP archive table 1/A/NBP/2002](https://nbp.pl/archiwum-kursow/tabela-nr-1-a-nbp-2002-z-dnia-2002-01-02/)

This is strong evidence that the checked API observations are normalized to
PLN per one unit of quoted currency. It is still an inference rather than an
explicit API field contract, and the checks are not an exhaustive proof for
every historic currency identity or source representation. The canonical
fact must preserve the API number unchanged. Its supported baseline label is **NBP API quote in PLN, retained without
additional scaling**. The per-one-unit interpretation is an inference supported
by the sampled comparisons below, not an exhaustive historical field-level guarantee. A consumer that needs legal or
accounting-grade conversion for a particular historic currency must validate
that currency's identity and contemporaneous quotation evidence first.

No Polish-zloty redenomination adjustment applies to this API history. The API
begins on 2 January 2002, while the official Denomination Act made one new
zloty equal to 10,000 old zlotys from 1 January 1995. All available API
observations postdate that change. [NBP API history boundary](https://api.nbp.pl/en.html)
· [Act of 7 July 1994 on the denomination of the zloty](https://isap.sejm.gov.pl/isap.nsf/download.xsp/WDU19940840386/O/D19940386.pdf)

## Fact grain and date roles

| Dataset | One source observation | Measures | Required identifying dimensions |
| --- | --- | --- | --- |
| Table A | One currency's middle rate in one published A table | `mid` | `effective_date`, `currency_key`, source table A |
| Table B | One currency's middle rate in one published B table | `mid` | `effective_date`, `currency_key`, source table B |
| Table C | One currency's buy and sell rates in one published C table | `bid`, `ask` | `effective_date`, `currency_key`, source table C; carry `trading_date` |
| Gold | One NBP gold price on one publication date | `price_pln_per_gram_1000` | `effective_date`, `commodity_key=nbp_gold_1000_gram` |

`effective_date` is the primary time dimension for every definition because it
is the NBP publication date. `trading_date` is a separate Table C role. It can
be selected or filtered for source analysis, but must not replace publication
date, be copied to A/B, or be filled when absent. `dim_date` can provide
calendar attributes; its date spine does not assert that NBP published a value
on every date.

The current-value facts use the validated analytical keys
`(source_table_key, effective_date, currency_key)` for FX and
`(effective_date, commodity_key)` for gold. `source_no`, request, retrieval,
batch, and raw-file fields are provenance rather than metric dimensions.
Current corrected source values feed normal analysis under ADR 0001; retained
raw versions and detected-change records remain the audit path.

## Executable baseline metrics

These five metrics expose the source observation at the required grain. The supported `scripts/query_metrics.py` command supplies and enforces the
required daily dimensions automatically. Raw `mf` commands do not enforce this
project contract.

| Metric | Source expression | Required result grain | Unit |
| --- | --- | --- | --- |
| `nbp_table_a_mid` | `fact_fx_quotes.mid`, filtered to A | `effective_date × currency_key` | NBP API quote in PLN; no additional scaling |
| `nbp_table_b_mid` | `fact_fx_quotes.mid`, filtered to B | `effective_date × currency_key` | NBP API quote in PLN; no additional scaling |
| `nbp_table_c_bid` | `fact_fx_quotes.bid`, filtered to C | `effective_date × currency_key` | NBP API quote in PLN; no additional scaling |
| `nbp_table_c_ask` | `fact_fx_quotes.ask`, filtered to C | `effective_date × currency_key` | NBP API quote in PLN; no additional scaling |
| `nbp_gold_price_pln_per_gram_1000` | `fact_gold_prices.price_pln_per_gram_1000` | `effective_date × commodity_key` | PLN per gram at 1000 fineness |

The semantic measures use `max` as an identity operation over the validated
singleton fact grain. It returns the one stored source value and avoids the
false implication that quotation levels can be summed. It is not approval for
a maximum-rate analysis. A MetricFlow request that omits a required grain
dimension can technically collapse multiple observations with `max`; that
coarser result is outside these definitions and must not be presented as the
NBP daily quote.

FX and gold quotation levels are non-additive across dates. FX is also
non-additive across currencies and source tables. Table A and B `mid` values
must not be blended, and Table C `bid` and `ask` must remain separate. Missing
publication dates remain missing: no metric may insert zero, forward-fill a
quote, or treat a calendar row as a source observation. A published numeric
zero remains an observed source value and must not be converted to null; a
consumer must guard against dividing by it.

## Acceptance checks for the semantic layer

- Each metric must return the exact fact value when queried at its required
  grain.
- A and B metrics must filter their own table and exclude C rows; C metrics
  must filter C and keep bid and ask distinct.
- Gold must retain the source-specific commodity key and fixed PLN quote
  currency.
- A zero source quote must survive the semantic query unchanged.
- Definitions and regression checks must contain no sum, cross-table blend,
  implicit period average, calendar fill, or website multiplier.
- `effective_date` must be the primary metric time dimension; Table C
  `trading_date` must remain available as its separate source role.

## Derived metrics outside the baseline

The following calculations may be useful later, but they are not NBP-published
measures and are not part of the executable baseline:

- **Table C spread:** `ask - bid` for the same currency, source table, and
  publication. Its name, unit, and intended use should be agreed before it is
  published as a governed metric.
- **Period average:** an arithmetic mean of explicitly selected publication
  observations. Any definition must name the table, currency or commodity,
  start and end dates, observation count, and missing-publication treatment.
- **Change or return:** a comparison with a defined prior observation. A
  definition must state whether prior means the previous available
  publication and how detected source changes affect the series.

Users can explore these calculations in SQL. They do not acquire governed
meaning from being arithmetically possible.

## Access, reuse, and attribution

NBP calls the service a public Web API and documents unauthenticated HTTPS GET
requests. The same API page carries an all-rights-reserved notice. NBP's own
download guidance directs users to separate NBP regulations for rules on using
published data. The materials reviewed do not provide an explicit commercial
reuse or redistribution grant for the API dataset, and they do not establish
a specific attribution formula. [NBP Web API documentation](https://api.nbp.pl/en.html)
· [NBP download guidance](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/instrukcja-pobierania-kursow-walut/)
· [NBP regulations](https://nbp.pl/regulaminy/)

Catalogue the status precisely as: **public read access; commercial
redistribution unresolved; attribution requirements unresolved**. Public
access and the absence of an API key are not themselves a licence. Before a
commercial redistribution feature is released, review the terms that apply to
the intended use or obtain NBP permission. Preserve the source name, endpoint,
publication date, and retrieval provenance regardless.

## Primary references

- [NBP Web API documentation](https://api.nbp.pl/en.html): datasets, response
  fields, publication-date semantics, history limits, and error behavior.
- [NBP Table A](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-a/),
  [Table B](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-b/), and
  [Table C](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-c/): PLN
  quote direction and displayed quotation quantities.
- [NBP gold price](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/cena-zlota/):
  PLN price of 1 g of gold at 1000 fineness.
- [NBP exchange-rate download guidance](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/instrukcja-pobierania-kursow-walut/):
  archive-file semantics and pointer to NBP usage regulations.
