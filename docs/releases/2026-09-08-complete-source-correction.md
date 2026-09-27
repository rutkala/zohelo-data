# Complete-source ingestion correction — 8 September 2026

The owner rejected starter coverage as the production deliverable for selected sources.
[ADR 0006](../decisions/0006-complete-selected-source-coverage.md) defines complete available
scope and distinguishes raw archives, current catalogue coverage and modeled releases.

## Verified baseline

[Run 34238016513](https://github.com/rutkala/zohelo-data/actions/runs/34238016513)
used the previous producer `460bee85a8235b18948d1e39d1c1b0850d54e246`.

| Source campaign | Accepted responses | Published responses | Pending tasks | Observed result |
| --- | ---: | ---: | ---: | --- |
| World Bank WDI | 79 | 79 | 604 | One history request timed out |
| GUS BDL | 172 | 172 | 385 | One subject response failed the page-size validator |
| Eurostat API | 193 | 193 | 81 | Three batches completed with work remaining |

All three accepted-response snapshots passed fresh state and Landing verification.
These counts are response envelopes, not facts or complete datasets. Preflight verified
at least 3,675,258,880 bytes of available Drive capacity; the old producer did not log
actual available bytes. No stronger capacity claim follows from that preflight.

## Implementation and validation

[PR 80](https://github.com/rutkala/zohelo-data/pull/80) adds complete WDI archives,
all three Eurostat distribution inventories, streaming remote verification, asynchronous
and partition recovery, immutable state shards, Drive request reductions and compact
queryable distribution indexes. BDL catalogue discovery no longer has permanent starter
ceilings. Existing NBP release identities and models are preserved.

The official Eurostat inventory snapshot inspected during implementation contained 8,154
datasets, 656 codelists and 4,269 metadata packages: 21,233 distributions when dataset
structures are included. These are discovery counts, not production download counts.
The exact request routes and official evidence are in the
[route research](../research/full-source-routes-2026-09-08.md).

The local Python/dbt suite passed 340 tests; the final runner, fresh-store integration
and compact index tests passed separately after the last edits. Initial GitHub validation
passed 343 data tests, both portal builds and 686 portal unit tests per configuration.
One browser assertion caught a terminology change from “response” to “reply”; the
consistent wording was restored before the final acceptance run.

Final [data CI 34240810489](https://github.com/rutkala/zohelo-data/actions/runs/34240810489)
passed all 343 tests. Final [portal CI 34240810496](https://github.com/rutkala/zohelo-data/actions/runs/34240810496)
passed both builds, 686 unit tests and 14 browser flows for each base path. PR 80 was
merged as `1287f65b999af93aede74140f3acf920d59eb95c` with immediate production opt-in.

## Live corrections and deployment

The immediate [source run 34241326444](https://github.com/rutkala/zohelo-data/actions/runs/34241326444)
ran on merged producer `1287f65`. Its preparation measured **4,822,828,605,683 bytes** of
available Drive quota at 14:55:28 UTC, against a 3,675,258,880-byte preflight reserve.
This is a dated headroom observation, not a reservation or future capacity guarantee.
[Portal deployment 34241326729](https://github.com/rutkala/zohelo-data/actions/runs/34241326729)
succeeded at `1287f65`. Later fixes do not change the portal bundle.

The first production run exposed three concrete implementation issues:

- BDL's parent-scoped `subjects` route for K9 returned all 21 children while echoing
  `pageSize=20`. Pages 1 and 2 repeated the same complete child list. [PR 81](https://github.com/rutkala/zohelo-data/pull/81)
  accepts this specific complete-list response only when counts, unique IDs and every
  parent identity agree; it retains ordinary page validation and avoids redundant pages.
  It merged as `1f841d66354aea0edc205f81477e6433085f9e39` after
  [344 data tests](https://github.com/rutkala/zohelo-data/actions/runs/34241986463).
  The [next production run](https://github.com/rutkala/zohelo-data/actions/runs/34242541420)
  accepted both previously rejected subject tasks and reached 208 accepted/published
  responses; fresh raw and Landing verification passed.
- Drive successfully stored WDI's 282,845,220-byte archive but reported its MIME type as
  `application/x-zip`. The original octet-stream-only metadata check rejected it before
  acceptance and publication. [PR 82](https://github.com/rutkala/zohelo-data/pull/82)
  accepts valid non-native media MIME types while preserving exact file identity, parent,
  ownership, size, MD5 and streamed SHA256 checks. Native Google documents, folders and
  shortcuts remain rejected. The existing uploaded archive is safely reusable after verification.
- Eurostat's first inventory produced 256 pending-task shards and 166 recurring-root shards,
  mostly only 40–50 KiB each. Serial immutable writes made that first batch take 1,412.88
  seconds. PR 82 raises the per-shard bound to 2 MiB and recalculates the deterministic
  topology on the next save; it retains old immutable evidence. A fresh worker must read
  the old checkpoint once before writing the denser checkpoint. The workflow now runs
  one recent API batch before full-distribution work, then uses the remaining original API
  budget for any continuation. Full history no longer waits behind three API batches.

PR 82 merged as `75c1afd4d7ee7d36a51184690f7ea243071590b2` after
[347 data tests](https://github.com/rutkala/zohelo-data/actions/runs/34245075797).
It immediately started the corrected production run below. These live failures were
retained as failures; accepted partial progress was not discarded or called complete.

## Measured production coverage

The corrected [run 34245632263](https://github.com/rutkala/zohelo-data/actions/runs/34245632263)
uses producer `75c1afd`. The following observations were checked at 15:55 UTC on 8 September.

| Selected product | Verified production observation | Completion boundary |
| --- | --- | --- |
| NBP A/B/C exchange rates and gold REST feeds | The separately verified daily [run 34179015910](https://github.com/rutkala/zohelo-data/actions/runs/34179015910) published 15 tables and 429,841 cleaned observations, checked through 7 September. | Selected feeds have raw and modeled release acceptance. Other NBP publications are outside this implemented product scope. |
| World Bank WDI CSV product | The full 282,845,220-byte archive is accepted and indexed; full collection and fresh full-distribution restore steps both passed in job `102127040729`. | Current official WDI archive is complete in raw Landing. New WDI Bronze/Silver/Gold/semantic models are not delivered. |
| Eurostat | Two validated inventories identify 16,964 data, structure and codelist distributions. Complete dataset `AACT_ALI01` and its structure are accepted and indexed alongside the inventories: four accepted/published objects, 8,350,635 raw bytes. The collector is running in job `102127040657`. | Two current non-inventory distributions are accepted; 16,963 tasks remain, including the metadata inventory. Full coverage is incomplete. The separately researched 21,233 total is not an ingested count. |
| GUS BDL | Job `102127040297` passed fresh state/raw and Landing verification with 228 accepted and published responses and 485 pending tasks. | Full catalogue/history remains incomplete. This run added no responses while respecting a provider-issued Retry-After from an earlier HTTP 429 at 15:34:44 UTC. |

The WDI publication pointer was promoted at **15:43:39 UTC**. Its exact-size and SHA256
verified manifest reports `coverage_status: complete_current_catalogue`, one accepted and
published distribution, zero publication backlog, and table `world_bank_wdi_distributions`.
The archive SHA256 is `2ab1d0d250ebe986ac8a9f7163f6e177fbe4cfb2750f822b18578d902aeb134f`.
Its receipt records the complete official ZIP request, redirected download URL, supplied
last-modified value of 15 July 2026, member inspection and these six CSV files:

| Archive member | Supplied data rows | Scope |
| --- | ---: | --- |
| `WDICSV.csv` | 396,970 | Country–indicator rows, with year columns 1960–2025 |
| `WDICountry.csv` | 264 | Country/aggregate metadata |
| `WDISeries.csv` | 1,498 | Indicator definitions and metadata |
| `WDIcountry-series.csv` | 7,939 | Country–indicator notes |
| `WDIfootnote.csv` | 846,045 | Country–indicator–year footnotes |
| `WDIseries-time.csv` | 143 | Indicator–year notes |

These are CSV row counts, not counts of non-missing observations. The archive index has
one row because it identifies one complete distribution; it is not a one-observation sample.
The receipt and index retain the exact request and inspection details. Archives are not
automatically loaded into browser DuckDB memory.

The WDI job subsequently finished with an overall failure because the separate API
continuation timed out on `wdi:history:BX.GSR.CCIS.CD:1960-2026:p000001` at 15:47:45 UTC.
The full archive remained accepted and freshly verified. The API response campaign reached
160 accepted/published envelopes; both its fresh raw restore and Landing verification also
passed. Its one timed-out task remains queued for retry. Thus the red overall job is an API
history request failure, not a failed full-archive acceptance or lost publication.

Eurostat's initial accepted inventory contains 4,527,287 raw bytes. Its original job
`102112246707` passed the full-distribution step and a fresh full-distribution restore,
reporting 16,310 pending tasks and `inventories_current: false`. Two pending tasks are the
remaining inventory types. The earlier API batch retained 229 published responses despite
a connection failure on one historical request; a failed API step did not erase those bytes.
Reading a catalogue and queuing its entries is discovery, not completed dataset ingestion.

The corrected Eurostat checkpoint was promoted at **15:54:41 UTC** and its matching Landing
publication at **15:54:57 UTC**, with four accepted/published objects and no publication
backlog. Their manifest bytes and the accepted distribution receipts were checked against
their pointer/state hashes. Pending tasks and recurring roots now use **16 shards each**,
demonstrating the denser state topology in production. The complete `AACT_ALI01` gzip TSV
contains 111 series rows with dimensions `freq`, `am_item`, `unit`, `geo` and periods
1973–2025; its 12,252 raw bytes and 3,510,994-byte SDMX structure are accepted. The request
uses the catalogue's full dataset route with `format=TSV&compressed=true`, without geographic
or period filters. The first inventory passed a separate fresh-process restore in the earlier
run; the corrected job's final fresh restore is still pending at this observation.

The workflow continues from durable checkpoints on its half-hour triggers, with separate
provider concurrency and quota ledgers. A job may succeed while no request is currently
permitted; successful verification or an empty execution queue is not by itself proof of
complete source coverage. Full-distribution coverage is measured from validated inventory
versions and accepted bytes. No authenticated owner-session portal query was performed;
portal behavior passed browser fixtures and production data passed fresh Actions checks.

## Scope and remaining work

- WDI means the complete official WDI CSV product, including archive members and metadata;
  it does not mean every World Bank data product.
- Eurostat coverage is measured against the last validated official data, codelist and
  metadata inventories. A distribution is complete only after byte and content verification,
  or after every disjoint recovery partition is accepted.
- BDL requires exhaustive API paging within the effective provider quota. Free registered
  access is supported through `GUS_BDL_API_KEY`; an account/key is not claimed without
  the owner completing registration and configuring the encrypted secret.
- Existing NBP coverage is the selected A/B/C exchange-rate and gold REST product. Other
  NBP statistical publications remain separate inventory and implementation work.
- Full raw Landing coverage is distinct from Bronze/Silver/Gold/semantic delivery. The
  portal indexes raw archives without attempting to load those archives into browser memory.
