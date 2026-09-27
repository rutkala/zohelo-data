# World Bank World Development Indicators source contract

Status: bounded production Landing collection and fresh-process replay verified on
8 September 2026. Downstream dbt models and publication remain separate implementation
work. Official documentation and the WDI catalog record were reviewed on that date.
Checked-in responses remain representative contract fixtures; the separate
[live acceptance record](../releases/2026-09-08-source-campaigns.md) identifies actual
production runs and retained-response verification.

## Complete distribution extension

ADR 0006 adds the official complete CSV ZIP at
`https://databank.worldbank.org/data/download/WDI_CSV.zip`, including its official
download-host redirect. Retain the exact archive and every member. Streaming ZIP/CRC
and CSV checks require populated observation data plus country and series metadata;
the archive's supplied history and dimensions are not filtered during extraction.
Country-series, series-time, footnotes and other supplied members remain present.

Each daily check records source headers, hashes, retrieval time and inspected member
metadata. Content-addressed storage reuses unchanged bytes. The API recent/history
path remains separate evidence and shares the provider quota ledger; the bulk archive
does not prove API freshness beyond its own publisher version. The source product is
WDI; this is not a claim to have ingested every other World Bank dataset.
[Official archive, version and licence evidence](../research/full-source-routes-2026-09-08.md#world-bank-world-development-indicators).

## Dataset identity and permission

| Field | Contract |
| --- | --- |
| Internal source ID | `world_bank_wdi` |
| Provider | World Bank |
| Product | World Development Indicators (WDI) |
| Indicators API source | `2` |
| API version and host | API v2 at `https://api.worldbank.org`; public GET requests, no authentication |
| Catalog record | [World Development Indicators, dataset 0037712](https://datacatalog.worldbank.org/search/dataset/0037712/world-development-indicators) |
| Dataset classification | Public |
| Dataset license | Creative Commons Attribution 4.0 |
| Attribution | Attribute the World Bank and preserve the indicator's original source organization and notes |

The license conclusion applies to this WDI catalog record. It is not a blanket conclusion
about other World Bank products or every dataset reachable through World Bank APIs. The
catalog record says WDI is public and specifically assigns CC BY 4.0. The generic World Bank
site or API terms are not used as a substitute for that dataset-specific record. The current
[World Bank terms](https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets)
expressly direct Data Catalog datasets to their additional dataset-specific terms, reserve a
non-commercial default for other materials, prohibit implied endorsement and require
reasonable rather than excessive API request volume. Thus this contract admits WDI source 2
under its catalog license, retains attribution and change provenance, excludes World Bank
logos or endorsement claims, and makes no rights claim for another source ID. If a WDI
distribution or component is later marked with narrower third-party terms, that marked
component requires its own review before publication.

The WDI site describes roughly 1,400 time-series indicators, 217 economies, more than 40
country groups and more than 50 years for many series. The current catalog record describes
over 1,500 indicators, more than 200 countries and territories, annual coverage from 1960,
and current bulk files. These counts change. Completeness is therefore established from a
finished, retained `source=2` indicator-catalog crawl, not from either marketing count and
not from a fixed list in code. See the [WDI home](https://datatopics.worldbank.org/world-development-indicators/)
and the dataset catalog record above.

## API discovery and page contract

The adapter uses four task kinds. Every task requests exactly one page. The shared ingestion
framework supplies retry, byte/time/request bounds, exact-response retention, receipts and
the durable priority queue.

| Task kind | Lane | Endpoint | Purpose |
| --- | --- | --- | --- |
| `indicator_catalog` | discovery | `/v2/indicator?source=2` | Enumerate every indicator currently assigned to WDI API source 2 |
| `country_catalog` | discovery | `/v2/country` | Retain World Bank economy and aggregate metadata, codes and classifications |
| `indicator_history` | history | `/v2/country/all/indicator/{indicator}` with `source=2` and `date=1960:{end year}` | Fetch the full available annual history for one indicator |
| `indicator_recent` | recent | same data endpoint with a rolling five-calendar-year range | Re-fetch recent observations to detect changed source representations without waiting for history |

The official [indicator query documentation](https://datahelpdesk.worldbank.org/knowledgebase/articles/898599-indicator-api-queries)
documents indicator code, name, unit, source, source note, source organization and topics,
and requires the source ID to distinguish an indicator belonging to multiple sources. The
[basic call documentation](https://datahelpdesk.worldbank.org/knowledgebase/articles/898581-api-basic-call-structures)
documents `date`, `page`, `per_page`, multiple indicators and `footnote=y`. The adapter uses
one indicator per observation task so each history stream can resume independently and no
URL approaches the documented multi-indicator or URL-length ceilings.

API JSON pages must be a two-item array: a pagination object followed by an array of record
objects. `page`, `pages`, `per_page` and `total` may arrive as JSON numbers or digit strings.
An API error object, invalid JSON, an unstructured empty response, a mismatched page number,
or an empty page paired with a nonzero total fails validation and does not mark work complete.
The next page is derived only from a structurally valid response whose declared page count
is greater than the returned page.

Indicator catalog rows must contain a valid indicator ID, a name and `source.id == "2"`.
Optional metadata remains raw even when blank. The adapter never substitutes five seed
indicators for catalogue completion: the seeds only start useful history and recent work
immediately, while catalog pages add both task types for every discovered indicator.

Indicator discovery admits at most 25 series per API page, or at most 50 newly emitted
history/recent roots plus the next catalog page. This keeps each admission step below the
current queue backpressure boundary. The first-deployment ledger intentionally pauses at
its configured task and recurring-root capacity; it must expose `coverage_status=incomplete`
and the pending discovery cursor. Reaching all currently documented 1,400-plus series will
require the planned measured ledger sharding or compaction phase. A capacity pause is not a
finished catalogue, and this contract does not justify blindly increasing safety bounds.

The [country query documentation](https://datahelpdesk.worldbank.org/knowledgebase/articles/898590-country-api-queries)
defines the World Bank three- and two-character codes, name, region, administrative region,
income level, lending type, capital, longitude and latitude. The API also exposes official
region, income and lending aggregates as documented in [aggregate queries](https://datahelpdesk.worldbank.org/knowledgebase/articles/898614-aggregate-api-queries).
Country catalog entries with `region.id == "NA"` and the source label `Aggregates` are kept
as `country_group` entities; other entries are kept as `country_or_territory` entities. The
source codes and raw classification fields remain the authority. A group is never inferred
from its name or from whether its code resembles ISO.

## Durable task partition and freshness behavior

Stable task IDs include kind, indicator where applicable, bounded year range and page. A
history stream keeps the same range on every page and resumes from the first unfinished
page in the durable queue. Discovery pages are likewise durable. The framework deduplicates
the seed tasks when catalog discovery later emits the same IDs.

Every recent first-page root has a stable recurrence key `indicator:{indicator code}`. Its
task ID also includes an `asof` date, so different daily requests cannot collapse into the
same logical page. The framework refreshes that root once per admitted series and day, and
does not enqueue a newer generation while an earlier generation remains pending. Recent
work is a separate lane and can proceed while history remains incomplete.

Recent requests use an explicit five-calendar-year range rather than `mrv`. This gives each
entity the same period boundary, includes null observations, and creates an overlap in which
changed values, status flags and footnotes can be detected. It is not a change feed. A source
revision older than the rolling window will only be observed by a later historical
reconciliation pass. Scheduling that pass must use measured API and platform capacity; no
frequency or quota is invented here.

The API page metadata can include `lastupdated`, and the source metadata API documents a
source last-updated date. This value is retained as provider metadata. It is not treated as
a per-observation revision timestamp, an immutable snapshot identifier or an HTTP validator.
There is no documented transactionally consistent snapshot across pages, so exact request
parameters, exact bytes, retrieval time and content hash are the replay evidence. A later
page can reflect a newer upstream state; reconciliation must compare source keys and page
coverage before claiming a complete vintage.

The World Bank's [development practices](https://datahelpdesk.worldbank.org/knowledgebase/articles/902064-development-best-practices)
recommend client caching, warn that the API does not guarantee 100 percent uptime, and note
that some calls are slow. The reviewed official API pages do not state a numeric request
quota, response-byte allowance, uptime target, or conditional-request guarantee. Shared
framework bounds and backoff remain operational safeguards, not statements of provider
allowance.

## Raw observation and metadata fields

Exact JSON response bytes are the landing record. Later dbt parsing may read the following
source fields without requiring optional metadata to be populated.

| Record | Required source keys | Preserved descriptive and status fields |
| --- | --- | --- |
| Indicator | API source ID `2` + `id` | `name`, `unit`, `source.value`, `sourceNote`, `sourceOrganization`, `topics[]` |
| Entity | `id` | `iso2Code`, `name`, `region`, `adminregion`, `incomeLevel`, `lendingType`, `capitalCity`, `longitude`, `latitude`; derived entity type from the explicit aggregate marker |
| Observation | API source ID `2` + `indicator.id` + `countryiso3code` (fall back to namespaced `country.id` only when absent) + `date` | numeric-or-null `value`, `unit`, `obs_status`, integer `decimal`, `footnote`, source labels |

WDI source 2 is annual, so this adapter accepts decimal calendar-year `date` values. Null is
missing; it is not zero. `obs_status` is retained as a source code; official API documentation
gives `F` as the forecast example, but unknown codes remain opaque. Both catalog and
observation units are retained because either may be blank or differ. `decimal` is display
precision, not an inferred multiplier. The adapter does not request `scale=y`, so it does not
replace source values with automatically scaled display values.

Request receipts produced by `interpret` use these stable metadata keys:

| Key | Type | Meaning |
| --- | --- | --- |
| `api_page`, `api_pages`, `api_per_page`, `api_total` | integer | Normalized upstream pagination values |
| `api_last_updated` | string or null | Upstream page-level `lastupdated`, when supplied |
| `wdi_api_source_id` | string | Always `"2"` for this adapter |
| `task_kind` | string | Adapter task kind |
| `discovered_indicator_count` | integer | Indicator rows admitted from an indicator catalog page |
| `country_entity_count`, `aggregate_entity_count` | integer | Entity rows classified from a country catalog page |
| `indicator_id` | string | Observation stream indicator |
| `period_start_year`, `period_end_year` | integer | Inclusive requested annual range |
| `as_of_date` | ISO date string | Recent generation identity; absent from history |

## Statistical meaning and modeling limits

The compatible family grain is one WDI API source, indicator, source entity and calendar
year. Raw page provenance and retrieval identity remain attached through bronze and silver.
Repeated representations with the same source key are versions to compare; arrival order
alone does not establish why the World Bank changed a value.

Countries and source-published aggregates remain separate entity types. Aggregate values
are observations published by WDI, not sums calculated by this platform. The World Bank's
[methodology](https://datahelpdesk.worldbank.org/knowledgebase/articles/906531-methodologies)
warns that revisions occur and describes indicator-dependent aggregation rules, missing-data
approximations and possible discrepancies. Consequently no generic `SUM(value)` measure,
roll-up rule or specialist-source precedence is defined by this contract. Additivity and
semantic aggregation require indicator-specific metadata and an approved analytical use.

Fixtures under `tests/fixtures/sources/world_bank/` exercise the documented JSON envelopes,
source scoping, country/group distinction, numeric and null observations, units, forecast
status, footnotes and multi-page continuation. They are small structural examples and do not
prove current upstream counts, availability or production ingestion.

## Initial production evidence

[Run 34214627594](https://github.com/rutkala/zohelo-data/actions/runs/34214627594)
accepted three observation pages across recent and history lanes after earlier production
progress. The cumulative checkpoint held seven accepted responses and 1,422,521 received
body bytes. Its retained indicator-catalog page reported 1,498 indicators; discovery had
not finished. A fresh process restored state and replayed retained recent, history and
discovery responses successfully. These counts describe Landing representations, including
nulls and overlap; they are not unique analytical facts or complete coverage.

The original 45-second timeout failed on two measured requests. A source-specific bounded
90-second timeout preserved the same request identities and enabled successful subsequent
collection. The run time budget is checked between attempts, so an in-flight request and
checkpoint save can extend elapsed time beyond 240 seconds. Later runs may supersede these
dated counts; see the acceptance record for the final first-delivery observation.
