# Full-source download and discovery routes

Checked 8 September 2026. This note records the public, no-sign-up path to the
complete product scope for the four selected sources. It supplements the bounded
starter contracts; it does not treat those starter slices as complete.

## Implementation route matrix

| Source | Complete discovery authority | Data route | Unit of restart |
| --- | --- | --- | --- |
| World Bank WDI | [WDI Data Catalog record](https://datacatalog.worldbank.org/search/dataset/0037712/world-development-indicators) | [`WDI_CSV.zip`](https://databank.worldbank.org/data/download/WDI_CSV.zip), whose current official redirect target is `https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip` | whole ZIP, or byte offset only after a live `Range: bytes=0-0` probe returns `206` |
| Eurostat | [`inventory?type=data&lang=en`](https://ec.europa.eu/eurostat/api/dissemination/files/inventory?type=data&lang=en) | use each inventory row's exact TSV/CSV/SDMX URL; canonical full TSV pattern is `https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/{CODE}/1.0?format=tsv` | one dataflow; async key if returned; constrained partitions only after `413` |
| GUS BDL | paginated `/variables` plus subject, unit, locality and dictionary resources at `https://bdl.stat.gov.pl/api/v1` | `/data/by-variable/{variable-id}` with no `year` filter, page through all result units | one API page |
| NBP public statistics | the Statistics and Payment System navigation pages and every official downloadable attachment they link | API for exchange rates/gold; page-linked XLS/XLSX/CSV/XML/ZIP/PDF distributions for the rest | one API date window or one attachment |

HTTP byte-range support is not promised in the reviewed provider documentation for
any of these routes. A downloader may use ranges only after verifying `206 Partial
Content` and a stable validator for that exact object. The portable recovery unit is
the ZIP, Eurostat dataflow/partition, BDL page, or NBP attachment shown above.

## World Bank World Development Indicators

The official WDI guide directs bulk users to the Data Catalog and describes bulk
files as data plus metadata. The catalogue currently describes over 1,500 indicators,
over 200 countries and territories, annual temporal coverage 1960–2025, and the CSV
ZIP as **269.7 MB**, last updated **17 July 2026**. Catalogue metadata itself was last
updated **20 July 2026**.

Use the stable publisher URL:

```text
https://databank.worldbank.org/data/download/WDI_CSV.zip
```

It currently redirects to the official download host:

```text
https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip
```

For a reproducible bootstrap, the current dated object exposed by the catalogue is:

```text
https://datacatalogfiles.worldbank.org/ddh-published/0037712/DR0095335/WDI_CSV_2026_07_15.zip
```

The stable object changes when WDI is refreshed. Record the initial URL, final
redirect URL, response `ETag` and `Last-Modified` when present, byte count, retrieval
time, and SHA-256. Stream to a temporary file and atomically publish only after ZIP
integrity and every member have been checked. The catalogue does not publish a
normative member manifest, so enumerate and retain every ZIP member. In particular,
identify the observation file and country, series, country-series, series-time,
and footnote metadata by their headers; do not silently accept a data-only archive.

The catalogue marks this WDI dataset Public and [Creative Commons Attribution 4.0](https://datacatalog.worldbank.org/public-licenses?fragment=cc).
CC BY 4.0 permits sharing and adaptation, including commercial use, with attribution
and an indication of changes. No WDI-specific geography restriction was found.

Primary evidence:

- [WDI user guide: bulk downloads and archived versions](https://datatopics.worldbank.org/world-development-indicators/user-guide.html)
- [WDI Data Catalog: current resources, size, timestamps, coverage and licence](https://datacatalog.worldbank.org/search/dataset/0037712/world-development-indicators)
- [World Bank public licences](https://datacatalog.worldbank.org/public-licenses?fragment=cc)

## Eurostat

There is no single current archive for all ordinary Eurostat datasets. The supported
complete route is catalogue enumeration followed by one prepared, compressed full
file per inventory item. Eurostat explicitly warns that iterating the inventory can
start downloading the complete Eurostat data scope and require substantial disk.

### Complete discovery and metadata

```text
# Exact inventory intended for download scripts; TSV text
https://ec.europa.eu/eurostat/api/dissemination/files/inventory?type=data&lang=en

# All dataflow stubs, SDMX 2.1
https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/1.0?detail=allstubs

# All dataflows, SDMX 3.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/dataflow/ESTAT/*

# All current structural families, SDMX 3.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/codelist/ESTAT/*
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/conceptscheme/ESTAT/*
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/datastructure/ESTAT/*

# Complete catalogue and changes since the previous catalogue update
https://ec.europa.eu/eurostat/api/dissemination/catalogue/dcat/ESTAT/FULL
https://ec.europa.eu/eurostat/api/dissemination/catalogue/dcat/ESTAT/UPDATES

# Search/navigation table and series-to-dimension-position metabase
https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en
https://ec.europa.eu/eurostat/api/dissemination/catalogue/metabase.txt.gz
```

The inventory supplies each item's exact data download URLs and `Last data change`;
use those fields as the download authority. A dataflow also provides `OBS_COUNT`,
oldest/latest observation periods, `UPDATE_STRUCTURE`, `UPDATE_DATA`, the referenced
versioned DSD, and metadata links. Eurostat checks for dataset catalogue changes at
11:00 and 23:00 CET. Dataflow and data-constraint version `1.0` must be re-fetched;
DSDs, concept schemes and codelists use their referenced immutable version.

### Full data files and structures

Use the exact URL from the inventory. Equivalent full-dataset forms are:

```text
# TSV, compression on by default in SDMX 3.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/{CODE}/1.0?format=tsv

# SDMX-CSV 2.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/{CODE}/1.0?format=csvdata&formatVersion=2.0

# Older API-compatible gzip TSV
https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{CODE}?format=TSV&compressed=true

# Dataset-specific allowed positions and referenced DSD
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/dataconstraint/ESTAT/{CODE}/1.0
https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/dataflow/ESTAT/{CODE}/1.0
```

Eurostat says the inventory-linked full files are prepared in advance. An exact
cached request is returned synchronously before extraction-cost thresholds are
applied, so the full inventory TSV normally arrives as one compressed response even
when the theoretical cube exceeds five million cells. Validate the response body:
Eurostat says a prepared URL can rarely return an async XML envelope instead.

For an uncached extraction, the documented decision is:

- under 500,000 estimated cells: synchronous;
- 500,000 through 5,000,000: asynchronous;
- above 5,000,000: HTTP 413 and the query must be partitioned using the data constraint.

The async flow is:

```text
GET the original SDMX data URL -> XML containing queued id
GET https://ec.europa.eu/eurostat/api/dissemination/1.0/async/status/{id}
GET https://ec.europa.eu/eurostat/api/dissemination/1.0/async/data/{id}
```

Poll at a bounded interval while status is `SUBMITTED` or `PROCESSING`; fetch only
when `AVAILABLE`. `EXPIRED` means submit again; Eurostat says expiry happens after a
few days or when the dataset is updated. The provider recommends one extraction at a
time, no parallel extraction script, and bulk download where applicable.

Comext `DS-*` trade cubes use the separate
`https://ec.europa.eu/eurostat/api/comext/dissemination/...` base and can be far above
the extraction ceiling. Prefer Eurostat's prepacked Comext files where they cover the
required distribution:

```text
# Recursive official inventory with raw byte sizes and ISO Last-Modified values
https://ec.europa.eu/eurostat/api/dissemination/files?format=csv&dir=comext&hierarchy=true&sizeFormat=NONE&dateFormat=ISO

# Download one inventory path
https://ec.europa.eu/eurostat/api/dissemination/files?file=comext/{NAME}
```

Otherwise obtain the Comext content constraint and partition the query so every
uncached request stays at or below five million estimated cells. Preserve all
dimensions, time positions and observation flags across the partition union.

Eurostat generally authorises commercial and non-commercial reuse with source
acknowledgement. Therefore downloading all available geographies into a private,
non-commercial collection is compatible with the notice. A commercial publication
must enforce the stated exceptions: remove data for countries outside EU, EFTA and
official EU acceding/candidate countries; exclude restricted third-party material;
exclude Switzerland/Liechtenstein declaring-country trade from 1995 for HS, SITC,
BEC, NSTR and national classifications; and exclude Austrian declaring-country CN8
trade. Retain per-item copyright notices because they can add conditions.

Primary evidence:

- [Periodic download example and complete inventory](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-faq/examples)
- [SDMX 3.0 structures, complete datasets, formats and compression](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-getting-started/sdmx3.0)
- [Async thresholds, lifecycle and fair use](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/asynchronous-api)
- [Migration guide, inventories, metabase and Comext file catalogue](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-migrating/bulkdownload)
- [DCAT full catalogue and updates](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-detailed-guidelines/catalogue-api/dcat)
- [Eurostat reuse notice and geography/trade exceptions](https://ec.europa.eu/eurostat/web/main/help/copyright-notice)

## GUS Local Data Bank (BDL)

The reviewed [BDL v1 OpenAPI](https://bdl.stat.gov.pl/api/v1/swagger/doc/swagger.json)
has no bulk export and no changed-variable feed. Complete collection must enumerate
the API. The public API exposes the same statistical scope as the BDL application,
including annual and short-period statistics from 1995 onward.

```text
BASE=https://bdl.stat.gov.pl/api/v1

# Variables: zero-based pages, stable source-ID ordering
$BASE/variables?lang=pl&page={PAGE}&page-size={PAGE_SIZE}&sort=id
$BASE/variables/{VARIABLE_ID}?lang=en

# All observations for a variable: omit year and exhaust pages
$BASE/data/by-variable/{VARIABLE_ID}?lang=en&page={PAGE}&page-size={PAGE_SIZE}

# Metadata catalogues and details, in pl and en where labels are translated
$BASE/subjects?lang={LANG}&page={PAGE}&page-size={PAGE_SIZE}&sort=id
$BASE/subjects/{SUBJECT_ID}?lang={LANG}
$BASE/units?lang={LANG}&page={PAGE}&page-size={PAGE_SIZE}&sort=id
$BASE/units/{UNIT_ID}?lang={LANG}
$BASE/units/localities?parent-id={12_CHARACTER_MUNICIPALITY_ID}&lang={LANG}&page={PAGE}&page-size={PAGE_SIZE}
$BASE/units/localities/{LOCALITY_ID}?lang={LANG}
$BASE/attributes?lang={LANG}
$BASE/aggregates?lang={LANG}
$BASE/levels?lang={LANG}
$BASE/measures?lang={LANG}
$BASE/years?lang={LANG}
```

The live catalogue reported **172,573 variables** and **4,694 ordinary units** on
8 September 2026. Localities have no valid root listing: enumerate ordinary units,
then call `/units/localities` for every level-6 municipality using its 12-character
BDL ID. Preserve leading zeroes in unit IDs. Fetch Polish and English metadata, and
retain `lastUpdate`, descriptions, dimensions, units, aggregates, attributes,
precision and formatted values.

The official anonymous limits are 5 requests/second, 100/15 minutes, 1,000/12 hours,
and 10,000/seven days. Response headers `X-Rate-Limit-Limit`,
`X-Rate-Limit-Remaining`, and `X-Rate-Limit-Reset` may require a slower rate. A
complete run is necessarily multi-cycle: the known minimum already exceeds 534,000
requests before continuation pages. Checkpoint each validated page. Conditional
requests and resource `lastUpdate` help, but neither proves global completeness or
identifies every changed variable.

BDL data are published under CC BY 4.0. Attribute GUS/Statistics Poland and identify
BDL as the source. The official documentation states no geography restriction.

Primary evidence:

- [Official English BDL API guide](https://api.stat.gov.pl/Home/BdlApi?lang=en)
- [BDL v1 OpenAPI](https://bdl.stat.gov.pl/api/v1/swagger/doc/swagger.json)

## Narodowy Bank Polski public statistics

The [NBP Web API](https://api.nbp.pl/en.html) is not a catalogue of all NBP
statistics. Its documented scope is exchange-rate tables A, B and C plus NBP gold
prices. A complete collection for the selected **NBP public-statistics product** must
combine those feeds with the official statistical pages and their downloadable
distributions. This scope does not mean every document published by the NBP
organisation.

### Machine feeds and FX inventory

```text
# Exchange tables; split history into documented date windows
https://api.nbp.pl/api/exchangerates/tables/A/{START}/{END}/?format=json
https://api.nbp.pl/api/exchangerates/tables/B/{START}/{END}/?format=json
https://api.nbp.pl/api/exchangerates/tables/C/{START}/{END}/?format=json

# Gold
https://api.nbp.pl/api/cenyzlota/{START}/{END}/?format=json

# Official historical XML-file inventory and files
https://static.nbp.pl/dane/kursy/xml/dir.txt
https://static.nbp.pl/dane/kursy/xml/{FILENAME_FROM_DIR}.xml
```

The API supports intervals no longer than 93 days and can return `400 Limit
exceeded`; page the dates and treat valid no-data `404` responses as empty windows,
not deletions. The XML `dir.txt` is an official exact filename inventory for the FX
archive and complements the API, especially for representation-level completeness.

### Broader statistics inventory and distributions

Crawl only the following official navigation subtrees, follow their pagination/year
pages, and inventory every linked statistical XLS, XLSX, CSV, XML, ZIP, 7Z, TXT and
methodology PDF on `nbp.pl` or `static.nbp.pl`:

```text
https://nbp.pl/en/statistic-and-financial-reporting/
https://nbp.pl/en/statistic-and-financial-reporting/monetary-and-financial-statistics/
https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/
https://nbp.pl/en/statistic-and-financial-reporting/financial-markets/
https://nbp.pl/en/statistic-and-financial-reporting/instruments/
https://nbp.pl/en/statistic-and-financial-reporting/core-inflation/
https://nbp.pl/en/statistic-and-financial-reporting/inflation-expectations-and-forecasts/
https://nbp.pl/en/statistic-and-financial-reporting/rates/
https://nbp.pl/en/payment-system/statistical-data/
```

The monetary subtree includes, among other distributions, M3 and counterparts,
reserve money, MFI assets and liabilities, interest-rate statistics, financial
accounts, banking-sector financial data and Divisia monetary indexes. The
balance-of-payments subtree includes balance of payments, official reserves,
external debt, international investment position and international trade in
services. Financial markets, central-bank instruments, core inflation, inflation
expectations/forecasts, rates and payment-system statistics are separate inventories
and must not be represented by the four Web API feeds.

Known official durable workbook examples are:

```text
https://static.nbp.pl/dane/bilans-platniczy/bop_m.xlsx
https://static.nbp.pl/dane/bilans-platniczy/bop_q.xlsx
https://static.nbp.pl/dane/bilans-platniczy/bop_q_pln.xlsx
https://static.nbp.pl/dane/aktywa-rezerwowe/rez_m.xlsx
```

The first is monthly balance-of-payments history from January 2004, the next two are
quarterly distributions, and `rez_m.xlsx` is monthly official-reserve history from
January 1998. Discover these links from their current source pages rather than using
the examples as a closed allowlist.

Use the [NBP release calendar](https://nbp.pl/en/statistic-and-financial-reporting/calendar/)
as a second freshness signal, but do not treat it as a machine-readable attachment
catalogue. For each attachment retain the source page, displayed label/update date,
URL, response validators when supplied, byte size, retrieval timestamp and SHA-256.
Re-crawl the navigation pages and compare both URL and content hash. No reviewed NBP
document promises byte ranges, so the portable recovery action is to retry one file.

NBP pages and API documentation say the service is public, but the footer says “All
rights reserved,” and the API documentation grants no commercial redistribution or
database licence. The linked Terms of Use were not established here as a grant for
redistribution. Catalogue/download for private analysis can proceed; public or
commercial redistribution remains unresolved until the applicable terms or written
permission are reviewed. No geography restriction was found.

Primary evidence:

- [NBP Statistics landing page](https://nbp.pl/en/statistic-and-financial-reporting/)
- [NBP release calendar](https://nbp.pl/en/statistic-and-financial-reporting/calendar/)
- [NBP Web API documentation](https://api.nbp.pl/en.html)
- [NBP instructions for the historic exchange-rate file inventory](https://nbp.pl/en/statistic-and-financial-reporting/rates/the-instruction-how-to-retrieve-currency-exchange-rates-from-the-nbp-website/)
- [NBP monetary and financial statistics](https://nbp.pl/en/statistic-and-financial-reporting/monetary-and-financial-statistics/)
- [NBP balance-of-payments statistics](https://nbp.pl/en/statistic-and-financial-reporting/balance-of-payments-statistics/)
- [NBP payment-system statistical data](https://nbp.pl/en/payment-system/statistical-data/)
- [NBP regulations / Terms of Use entry point](https://nbp.pl/en/about-nbp/contact/nbps-regulations/)
