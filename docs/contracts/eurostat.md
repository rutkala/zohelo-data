# Eurostat ingestion contract

Status: initial GL-001 starter contract, verified by bounded public-API probes on
8 September 2026. GL-002 Comext is described below but is not admitted by this
contract. This document does not claim that the whole Eurostat catalogue is ingested.

## Provider routes and discovery

The adapter uses the public Eurostat Statistics API:

`GET https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{data_code}`

Every request fixes `lang=en`, `format=JSON`, one admitted `geo`, the slice's
measure dimensions, and an inclusive `sinceTimePeriod` / `untilTimePeriod` window.
The response is native JSON-stat 2.0. Eurostat documents dimension filters as
repeatable query parameters, but the initial contract uses one value for every
filtered dimension so request cardinality and reuse geography remain explicit.

The adapter's metadata-only discovery request is:

`GET https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en`

The catalogue also publishes a multilingual XML table of contents, a full DCAT
catalogue, a DCAT `UPDATES` feed, update RSS feeds, and `metabase.txt.gz`, which
lists dataset, dimension and position-code triples. Those routes provide a path to
broader dataset discovery and change detection. Discovery never activates a dataset:
its dimensions, size, source ownership, history and reuse conditions must first be
reviewed and added to the allowlist. See Eurostat's [API introduction](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-introduction),
[Statistics API guide](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/api-statistics),
and [catalogue guide](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-getting-started/catalogue-api).

## Admitted initial slices

The geography scope is the 27 current EU member-country codes published by Eurostat:
`AT`, `BE`, `BG`, `HR`, `CY`, `CZ`, `DK`, `EE`, `FI`, `FR`, `DE`, `EL`, `HU`,
`IE`, `IT`, `LV`, `LT`, `LU`, `MT`, `NL`, `PL`, `PT`, `RO`, `SK`, `SI`, `ES`,
and `SE`. Each country is a separate request and retained response.

| Data code | Admitted observation | Fixed dimensions | History | Window |
| --- | --- | --- | --- | --- |
| `demo_pjan` | Total population on 1 January | `sex=T`, `age=TOTAL`; the response supplies `freq=A`, `unit=NR` | 1960 onward | 10 calendar years |
| `nama_10_gdp` | GDP at current market prices in millions of euro | `na_item=B1GQ`, `unit=CP_MEUR`; the response supplies `freq=A` | 1975 onward | 10 calendar years |
| `prc_hicp_minr` | Food and non-alcoholic beverages HICP, 2025=100 | `coicop18=CP01`, `unit=I25`; the response supplies `freq=M` | 1996 onward | 5 calendar years |

The start dates are supported by the datasets' current metadata. A campaign starts
one history root per data code and country, then advances only after a structurally
valid response. The scheduler can therefore resume each country independently and
give recent work priority over backfill. The annual windows contain at most ten
values per admitted series and the monthly windows at most sixty.

Recent roots reread the current and preceding two calendar years. They have a stable
`dataset:geo` recurrence key and a date-based task ID, allowing the scheduler to
refresh the same slice without confusing a new vintage with already-pending work.
This rolling window detects ordinary recent revisions. Older revisions require a
bounded `reconcile` task for the affected dataset/country/window after a catalogue
change; an update signal is not itself evidence that observation values changed.
Eurostat says it checks for data and structure updates twice daily, at 11:00 and
23:00 Europe/Brussels time, and retains only the latest dataset version. Zohelo must
therefore retain every exact accepted response with its retrieval time and hash.

The admitted population slice omits age- and sex-specific observations. GDP omits
other national-account items and units. The HICP slice omits other products, units
and rates of change. Regional NUTS observations, EU aggregates, EFTA, candidate and
acceding countries, and all other catalogue datasets remain outside this first
allowlist. These are expansion candidates, not assertions that the source lacks them.

## Native JSON-stat grain and validation

The retained response remains the source of truth. Python performs structural
validation and planning; business transformations belong in dbt.

For each response, `id` is the ordered dimension list and `size` supplies the aligned
cardinalities. Every `dimension.{id}.category.index` must enumerate exactly its
declared positions. A decoded observation key contains a position from every
dimension, including `freq`, `unit`, classification dimensions, `geo`, and `time`.
The stable raw observation identity is:

`data_code + complete ordered JSON-stat dimensional key + retained response identity`

`value` and `status` may be dense arrays or sparse position objects. The adapter
validates every position against the cube and decodes all cube cells. An absent or
null value remains an explicit missing cell; it is not zero and does not authorize
deletion of a previously published value. Status codes such as `e` (estimated) and
`p` (provisional), including combined codes supplied by Eurostat, remain attached to
their exact full key. Category labels and extension metadata are descriptive; codes
form the key.

The response must contain exactly the requested country and fixed dimension values,
and every returned time position must fall inside the requested window. An API error,
asynchronous `413` warning, malformed UTF-8/JSON, inconsistent category index,
out-of-cube value/status position, or escaped filter fails interpretation and does
not advance the history campaign.

All three measures are non-additive across overlapping classifications and geography
aggregates. Population is a dated stock, GDP is a period flow in a stated currency
unit, and HICP is an index. The HICP series must not be summed or treated as a price
level. A later base-unit or classification change creates a distinct series; it must
not be silently spliced into `I25` / `CP01` history.

## Reuse boundary and attribution

Eurostat authorises commercial and non-commercial reuse of its statistical data and
metadata when the source is acknowledged. A customised dataset should cite the
Eurostat data-code link and access date. Translations or modifications must be
disclosed and include Eurostat's non-responsibility disclaimer. Individual notices
and third-party ownership override the general permission. See the official
[copyright notice](https://ec.europa.eu/eurostat/web/main/help/copyright-notice).

The notice excludes commercial reuse of data for countries outside EU members, EFTA
members, and official EU acceding/candidate countries. The initial adapter uses only
EU member countries and rejects every other geography rather than downloading a
wider cube and filtering later. EFTA and official candidate/acceding countries are
permitted in principle but remain unadmitted until a maintained current membership
list and dataset-level ownership check are part of the contract.

Comext and PRODCOM `DS-` data codes use the separate
`https://ec.europa.eu/eurostat/api/comext/dissemination` base and cannot be downloaded
unfiltered in full. They are rejected by this adapter. A later GL-002 contract must
enforce the complete reporter/partner/product/flow/classification/period key and
confidentiality flags. It must also exclude from commercial output:

- trade declared by Switzerland or Liechtenstein from 1995 onward for HS, SITC, BEC,
  NSTR and national commodity classifications; and
- trade declared by Austria at Combined Nomenclature eight-digit detail.

Those are targeted exceptions. They do not create a blanket ban on Eurostat or all
commercial trade use, and partner-country appearances are not the same as the
restricted declaring-country observations described by the notice.

## Measured request size and operating bounds

The 8 September 2026 probes used one country and narrow measure filters:

| Slice | Probe period | Cells / values | Exact response bytes |
| --- | --- | ---: | ---: |
| `demo_pjan`, Poland | 2023–2025 | 3 / 3 | 2,740 |
| `nama_10_gdp`, Poland | 2023–2025 | 3 / 3 | 2,983 |
| `prc_hicp_minr`, Poland | 2025-01–2026-12 | 20 / 19 | 5,281 |

The last probe demonstrates a native sparse missing cell. These measurements are
examples, not size guarantees. Eurostat describes small filtered results as a few
kilobytes returned in seconds, while full datasets may be several megabytes and take
minutes. The adapter never issues an all-dataset request. The shared transport must
retain its response-byte ceiling, runtime/request budgets, retry policy and no-redirect
host validation. A Statistics API asynchronous warning is a failed bounded attempt;
the adapter does not poll an unlimited extraction.
