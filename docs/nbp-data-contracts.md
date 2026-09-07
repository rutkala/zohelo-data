# NBP data contracts

Checked 6 September 2026 against NBP's [English API documentation](https://api.nbp.pl/en.html), [Polish API documentation](https://api.nbp.pl/), current API JSON responses, and NBP's published rate pages. The API is a public HTTP GET interface for current and historic Table A, B and C exchange rates and NBP-calculated gold prices. Requests should use HTTPS; NBP says HTTP access ended on 1 August 2025.

This document separates source facts from implementation inferences and unresolved questions. It does not define business metrics or choose the gold dimensional model.

## Verified shared source behavior

- JSON is requested with `?format=json` (or `Accept: application/json`); XML is also supported. The source API defaults to JSON when no format is specified.
- Currency history is documented from **2002-01-02** and gold history from **2013-01-02**. A single date-range request may cover at most **93 days**. Date parameters use `YYYY-MM-DD`.
- A valid request for which the source has no data returns **404 Not Found**. An incorrectly formed request returns **400 Bad Request**; a request exceeding the response limit returns **400 Bad Request - Limit exceeded**. A 404 is therefore a no-observation result for that request, not permission to delete previously validated observations.
- The API supports current/latest, today, one publication date, latest `topCount`, and a date interval. The documented interval is a publication-date interval; it does not promise one row for every calendar day.
- The API documentation names these exchange response fields: `Table`, `No`, `TradingDate` (Table C), `EffectiveDate`, `Rates`, `Country`, `Symbol` (a numeric currency symbol for historic rates), `Currency`, `Code`, and the table-specific `Bid`, `Ask`, or `Mid`. Actual JSON uses lower-camel-case keys (`table`, `no`, `tradingDate`, `effectiveDate`, `rates`, `currency`, `code`, `bid`, `ask`, `mid`). Preserve the exact raw response and tolerate documented optional fields such as `country` and `symbol`.
- NBP documents `EffectiveDate` as the publication date. `TradingDate` applies only to Table C. NBP's current Table C response demonstrates the distinction: `tradingDate` is the preceding quotation date and `effectiveDate` is the publication date.
- The API documentation has no revision number, correction flag, correction timestamp, or change-feed endpoint. Re-fetching an interval and comparing canonical typed records can detect that the source now returns a different value, but cannot establish why or when NBP changed it, and cannot reconstruct changes between our observations or before our first retained snapshot.

## Contracts by source

The source publication key is the table number (`no`) within a table type. For the current analytical value, [ADR 0001](decisions/0001-nbp-corrections.md) uses `(table, effective_date, currency_code)` for exchange rates and retains `no`, all date roles, request interval, retrieval and batch metadata as provenance. A conflict in `trading_date` for a Table C analytical key must be surfaced for investigation rather than silently overwritten.

| Source | Request templates | One flattened row means | Keys and date roles | Measures and units | Availability / cadence |
| --- | --- | --- | --- | --- | --- |
| **Table A** (`nbp_exchange_rates_table_a`) | Full: [`/api/exchangerates/tables/A/{startDate}/{endDate}/?format=json`](https://api.nbp.pl/api/exchangerates/tables/A/{startDate}/{endDate}/?format=json). Latest: [`/api/exchangerates/tables/A/last/{topCount}/?format=json`](https://api.nbp.pl/api/exchangerates/tables/A/last/{topCount}/?format=json). | One currency's middle rate in one published Table A release. The response envelope is a publication and `rates` is its child list. | Raw child key `(table='A', no, code)`; current-value key `(A, effective_date, code)`. `effectiveDate` is publication date; no `tradingDate` is documented for A. | `mid` is NBP's calculated middle exchange rate. NBP's API field description does not state the quote quantity or explicitly state the PLN base. NBP's rate pages identify the quotations in PLN. See the unit caveat below. | FX history from 2002-01-02. NBP publishes observations on publication dates; non-publication dates can return 404. The API does not guarantee the repository's configured daily cadence. |
| **Table B** (`nbp_exchange_rates_table_b`) | Full: [`/api/exchangerates/tables/B/{startDate}/{endDate}/?format=json`](https://api.nbp.pl/api/exchangerates/tables/B/{startDate}/{endDate}/?format=json). Latest: [`/api/exchangerates/tables/B/last/{topCount}/?format=json`](https://api.nbp.pl/api/exchangerates/tables/B/last/{topCount}/?format=json). | One currency's middle rate in one published Table B release. | Raw child key `(table='B', no, code)`; current-value key `(B, effective_date, code)`. `effectiveDate` is publication date; no `tradingDate` is documented for B. | `mid` is NBP's calculated middle exchange rate. Quote quantity/multiplier is not a JSON field; see the unit caveat. | FX history from 2002-01-02. The repository metadata describes B as weekly/Wednesday, but that schedule is not a guarantee in the API contract; use returned `effectiveDate` and measure observed cadence. |
| **Table C** (`nbp_exchange_rates_table_c`) | Full: [`/api/exchangerates/tables/C/{startDate}/{endDate}/?format=json`](https://api.nbp.pl/api/exchangerates/tables/C/{startDate}/{endDate}/?format=json). Latest: [`/api/exchangerates/tables/C/last/{topCount}/?format=json`](https://api.nbp.pl/api/exchangerates/tables/C/last/{topCount}/?format=json). | One currency's buy/sell quotation in one published Table C release. | Raw child key `(table='C', no, code)`; current-value key `(C, effective_date, code)` per ADR 0001. Retain `tradingDate` and `no`; `tradingDate` is the quotation/trading date and `effectiveDate` is publication date. | `bid` is the calculated buy rate and `ask` the calculated sell rate. The API does not state quote quantity/multiplier or explicit PLN base in these field descriptions; see the unit caveat. No `mid` is documented for C. | FX history from 2002-01-02. A date with no Table C publication can return 404. |
| **Gold prices** (`nbp_gold_prices`) | Full: [`/api/cenyzlota/{startDate}/{endDate}/?format=json`](https://api.nbp.pl/api/cenyzlota/{startDate}/{endDate}/?format=json). Latest: [`/api/cenyzlota/last/{topCount}/?format=json`](https://api.nbp.pl/api/cenyzlota/last/{topCount}/?format=json). | One NBP gold-price quotation for one publication date. Actual JSON is an array of objects such as `{"data":"2026-09-04","cena":530.70}`. | Source/current key `observation_date` mapped from raw `data`; the API supplies no commodity code and no source revision ID. Use the dataset identity for gold only as a source key; a business commodity dimension remains to be decided. `data`/documented `Date` is publication date. | `cena` (documented as `Code`) is the NBP-calculated price of **1 gram of gold at 1000 fineness**. NBP's gold-price page labels the price in **PLN**. Keep a canonical unit such as PLN per gram at 1000 fineness, with the raw field retained. | Gold history from 2013-01-02. Gold is quoted on publication dates, not necessarily every calendar day; an unavailable date/range returns 404. |

## Unit and legacy multiplier caveat

The API documentation calls `mid`, `bid`, and `ask` “calculated” rates but does not include a `unit`, `quantity`, or `multiplier` field. NBP's published tables do expose quotation quantities: the official Table A page currently shows examples such as **100 JPY**, the Table B page shows **100 AFN**, and the Table C page shows **100 HUF**. Those quantities can vary by currency and can change in historical tables.

Current API JSON is normalized-looking for such currencies: the current Table A response reports `HUF` `mid` as `0.011876` and `JPY` `mid` as `0.023746`, whereas the NBP web table presents 100-unit quotations. This supports (but does not explicitly document) an inference that current API values are PLN per one currency unit. It is not evidence that every legacy file or representation used the same convention.

**Implementation boundary until resolved:** preserve the ingested API numbers unchanged. Do not apply a website/archive quotation multiplier to an API value that may already be normalized. Record unit evidence for the exact source representation before naming a normalized gold measure; a future explicit unit field must distinguish API values from web-table quantities. Do not recover a historical multiplier from `symbol`: NBP defines `Symbol` only as a numeric currency symbol for historic rates. Targeted same-date comparisons against NBP's official [Table A archive](https://nbp.pl/en/statistic-and-financial-reporting/rates/archive-table-a-csv-xls/), [Table B archive](https://nbp.pl/en/statistic-and-financial-reporting/rates/archive-table-b-csv-xls/) and [Table C archive](https://nbp.pl/en/statistic-and-financial-reporting/rates/archive-table-c-csv-xls/) should resolve this before historical unit claims are accepted. This research does not authorize a numeric rescaling of stored data.

## Corrections, retries and 404 handling

### Targeted unit evidence, 7 September 2026

Actual official API responses were compared with search-index excerpts of NBP's matching archive pages. Direct archive-page access was blocked, so this is a limited cross-check, not a complete historical archive validation.

| Publication date | Currency | API `mid` | NBP archive excerpt | Relationship |
| --- | --- | --- | --- | --- |
| 2002-01-02 | HUF | 0.014511 | 100 HUF = 1.4511 PLN | Archive value / 100 = API value |
| 2002-01-02 | JPY | 0.029991 | 100 JPY = 2.9991 PLN | Archive value / 100 = API value |
| 2026-09-04 | HUF | 0.011876 | 100 HUF = 1.1876 PLN | Archive value / 100 = API value |
| 2026-09-04 | JPY | 0.023746 | 100 JPY = 2.3746 PLN | Archive value / 100 = API value |

Sources: official API responses for [2002-01-02](https://api.nbp.pl/api/exchangerates/tables/a/2002-01-02/?format=json) and [2026-09-04](https://api.nbp.pl/api/exchangerates/tables/a/2026-09-04/?format=json); indexed excerpts from [Table 1/A/NBP/2002](https://nbp.pl/archiwum-kursow/tabela-nr-1-a-nbp-2002-z-dnia-2002-01-02/) and [Table 172/A/NBP/2026](https://nbp.pl/archiwum-kursow/tabela-nr-172-a-nbp-2026-z-dnia-2026-09-04/). The evidence supports the inference of PLN per one currency unit for these API observations and reinforces retaining API values without applying website multipliers. It does not certify every historical currency or input representation.

### Loading behavior

A source batch should retain request URL/range, retrieval time, response bytes and content hash. A successful re-fetch of the same interval is comparable only after canonical typing, including the quote unit/multiplier. If the returned value differs, record the prior/new record hashes, changed fields, detection time, source interval and affected keys, then validate a candidate release before replacing current silver/gold values. Detection time is the platform's observation time, not an NBP revision time.

Treat a 404 as “no source observation for this valid request” and record the interval/status. It may represent a weekend, holiday, not-yet-published `today` value, or a range with no observations. It must not silently clear retained history. Treat 400 responses as request/configuration or response-limit failures requiring correction or chunking. Retry policy for transient transport/rate-limit failures is an implementation concern; never advance a checkpoint or publish a replacement on a partial/failed response.

## Commercial reuse and attribution evidence

The API page calls the service public and identifies the datasets, but its footer says **“Copyright © 2024 Narodowy Bank Polski”** and **“All rights reserved.”** The API documentation does not grant a commercial-use, redistribution, database, or attribution license. NBP links separate [website Terms of Use](https://nbp.pl/en/about-nbp/contact/nbps-regulations/), but those terms were not established here as permission to redistribute API data. Public HTTP access and an absent API key do not establish commercial reuse permission.

Until NBP's applicable terms or a written permission are reviewed for the intended product, catalogue the source as: **access: public/no credentials shown in API requests; commercial redistribution: unresolved; attribution requirement: unresolved**. If reuse is approved, preserve the NBP source name, endpoint and retrieval/publication dates in catalogue metadata and follow the exact attribution and redistribution conditions supplied by NBP.

## Sources

- [NBP Web API documentation (English)](https://api.nbp.pl/en.html)
- [NBP Web API documentation (Polish; same field/error details)](https://api.nbp.pl/)
- [Current Table A JSON example](https://api.nbp.pl/api/exchangerates/tables/a/?format=json)
- [Current Table B JSON example](https://api.nbp.pl/api/exchangerates/tables/b/?format=json)
- [Current Table C JSON example](https://api.nbp.pl/api/exchangerates/tables/c/?format=json)
- [Current gold JSON example](https://api.nbp.pl/api/cenyzlota/?format=json)
- [NBP Table A page and quote quantities](https://nbp.pl/en/statistic-and-financial-reporting/rates/table-a/)
- [NBP Table B page and quote quantities](https://nbp.pl/en/statistic-and-financial-reporting/rates/table-b/)
- [NBP Table C page and quote quantities](https://nbp.pl/en/statistic-and-financial-reporting/rates/table-c/)
- [NBP gold price page (PLN; 1 g at 1000 fineness)](https://nbp.pl/en/statistic-and-financial-reporting/rates/gold-price/)
- [NBP website Terms of Use](https://nbp.pl/en/about-nbp/contact/nbps-regulations/)
# Historical response check, 7 September 2026

Read-only requests using the new configured transport returned HTTP 200 and
passed source validation for A, B and C over 2–4 January 2002, and gold over
2–4 January 2013. The A/C archive omits some currency names. A repeats EUR for
RFN and UGW with the same published value; B repeats common currency codes for
several countries. These are valid historical source rows, not correction
events. Bronze preserves country/symbol and raw links; silver collapses equal
same-request prices deterministically at the currency/date grain. Conflicting
same-request prices fail. This targeted check is not proof of every historical
interval or a new commercial-use licence.

Primary endpoints: [Table A](https://api.nbp.pl/api/exchangerates/tables/A/2002-01-02/2002-01-04/?format=json),
[Table B](https://api.nbp.pl/api/exchangerates/tables/B/2002-01-02/2002-01-04/?format=json),
[Table C](https://api.nbp.pl/api/exchangerates/tables/C/2002-01-02/2002-01-04/?format=json),
[gold](https://api.nbp.pl/api/cenyzlota/2013-01-02/2013-01-04/?format=json).
