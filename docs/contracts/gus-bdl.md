# GUS Local Data Bank (BDL) source contract

Source ID: `gus_bdl`
Provider: Główny Urząd Statystyczny (GUS / Statistics Poland)
Product: Bank Danych Lokalnych (BDL / Local Data Bank)
Public API: `https://bdl.stat.gov.pl/api/v1`
Official documentation: [English BDL API guide](https://api.stat.gov.pl/Home/BdlApi?lang=en), [OpenAPI v1](https://bdl.stat.gov.pl/api/v1/swagger/doc/swagger.json)

## Scope and reuse

BDL is Statistics Poland's local-to-national database for the economy, society and
environment. The official guide says that its first data are from 1995, that it
contains annual and short-period statistics, and that the API exposes the same data
scope as the BDL application. The provider permits use of BDL API data under
[Creative Commons Attribution 4.0 International](https://creativecommons.org/licenses/by/4.0/).
Published products must attribute GUS / Statistics Poland and identify BDL as the
source. Exact response bytes and source metadata are retained so attribution and
provenance travel with the observations.

The target is progressive discovery of the complete public BDL v1 catalogue and
the available history of admitted variables. The initial runnable slice is not a
claim that a handful of variables represents BDL. Variable `72305`, source-labelled
`ludność ogółem` / `total population`, is the first acceptance series because it is
a low-complexity, generally useful regional statistic. Its two API variable-detail
responses are the authoritative identity and definition; the seed names only make
the initial task understandable before those responses arrive.

## Source resources and observed response shapes

The adapter requests JSON explicitly. It requests Polish and English separately for
resources with translated labels. The root runner retains the exact bytes; dbt later
turns them into rows.

| Resource | Endpoint | Fields preserved |
| --- | --- | --- |
| Variables | `/variables` and `/variables/{id}` | `id`, `subjectId`, `n1`…`n5`, `level`, `measureUnitId`, `measureUnitName`; details add `description` and `years` |
| Subjects | `/subjects`, child lists using `parent-id`, and `/subjects/{id}` | `id`, `parentId`, `name`, `hasVariables`, `children`, `levels`; details add `years`, `availability`, `dimensions`, `lastUpdate`, `description` |
| Territorial units | `/units` and `/units/{id}` | BDL `id`, `name`, `parentId`, `level`, `kind`, `hasDescription`; details add `description`, `years` and source availability fields |
| Statistical localities | `/units/localities?parent-id={municipality-id}` and `/units/localities/{id}` | locality ID and the same source unit metadata; kept separate from ordinary territorial units |
| Dictionaries | `/attributes`, `/aggregates`, `/levels`, `/measures`, `/years` | every returned dictionary object in Polish and English where language applies |
| Observations | `/data/by-variable/{variable-id}` | response `variableId`, `measureUnitId`, `aggregateId`, `lastUpdate`; each result unit `id`, `name`; each value's exact `year`, numeric value, `attrId`, `precision`, and any formatted value |

Verified bounded reads on 8 September 2026 found these live compact JSON shapes:

- `/variables` returned `totalRecords`, zero-based `page`, `pageSize`, paging
  `links`, and `results`. It reported 172,573 variables. Result objects contained a
  numeric variable ID, subject ID, up to five source dimension labels, validity
  level, and measure-unit ID and label.
- `/units` used the same paging envelope and reported 4,694 units. Unit IDs were
  strings with significant leading zeroes; results included a source label, level,
  optional parent and kind, and `hasDescription`.
- `/subjects/{id}` returned the subject hierarchy, years, per-year availability,
  source dimensions, `lastUpdate`, and a potentially long methodology description.
- A live by-unit example returned `values` with `year`, `val`, and `attrId`. The
  OpenAPI `YearValue` schema instead names the numeric member `value` and also
  documents `valueFormatted` and `precision`. The adapter accepts exactly one of
  `val` or `value` and retains the exact payload, preventing this provider/schema
  discrepancy from silently discarding values.
- A by-variable response is a paged `SingleVariableData` object. Its results are
  units with value arrays; the response also carries `variableId`, `measureUnitId`,
  `aggregateId`, and `lastUpdate`. Production responses retained on 8 September
  2026 omitted the catalogue-style `page` member even though the request specified
  it. The adapter therefore uses its validated request cursor for by-variable page
  identity. If `page` or `pageSize` is present, its type and value must still match
  the cursor. Catalogue responses must always provide both members.

`interpret()` performs structural checks only. It returns an observation count for
data pages and a result-object count for catalogue pages. It does not translate
labels, reinterpret measures, fill missing periods, aggregate values, or choose one
status over another.

## Grain, identifiers, and semantics

The retained observation grain is:

`BDL variable ID × BDL unit ID × exact returned year/period × attribute ID`

`attrId` is a source status/attribute dimension, including special presentation or
availability situations. Null remains null. It is not discarded merely because a
numeric value is present. `precision` and `valueFormatted` are also retained; the
numeric value is not rescaled in ingestion. Duplicate keys within one response page
fail structural validation.

Variable metadata supplies the measure unit and up to five source dimensions. Those
fields define the series and must remain joinable by variable ID. Ratios, shares,
indices, counts and monetary measures have different aggregation behavior; no
additive metric is inferred from the numeric type. Subject descriptions and variable
descriptions remain source methodology, with Polish and English versions stored
side by side rather than one replacing the other.

A BDL unit ID is kept as a source-owned string. The unit hierarchy combines Poland,
NUTS-compatible macroregions/regions/subregions, voivodships, counties,
municipalities, and a separate statistical-locality resource. `level` and `kind`
remain explicit. A BDL code is not asserted to be a TERYT identifier or a stable NUTS
code solely because its label, length, or hierarchy resembles one. Any future
TERYT/NUTS crosswalk must be versioned and supported by an explicit official mapping.
Administrative changes may change the meaning or availability of a unit through
time, so unit ID, hierarchy, years and source descriptions are all preserved.

## Task and partition design

Every task describes one GET and one API page. Task IDs are deterministic from the
lane, resource, language, variable, period window and page. Page links returned by
the service are evidence only; the adapter increments the validated page cursor and
rebuilds the URL on the allow-listed host. Catalogue requests explicitly sort by
source ID so page order is deterministic; a later catalogue reconciliation still
has to detect additions that occurred while a long campaign was running.

| Lane | Partition | Continuation |
| --- | --- | --- |
| `discovery` | one resource × source language × page; locality lists also include their municipality parent; one detail resource × source ID × language | paged variable, unit, locality and subject lists continue one page at a time; Polish variable pages admit every returned variable; municipality units (level 6) emit bilingual locality lists; subject children expand the full tree |
| `recent` | one admitted variable × current five-year window × unit page × collection date | page continuation preserves the date/window and `recurrence_key=variable:<id>`; `refresh_task()` creates a new dated generation only from page zero |
| `history` | one admitted variable × all available periods × unit page | no `year` filter is sent, so the source returns the available history; unit pages continue until `totalRecords` is exhausted |
| `reconcile` | reserved for explicitly scheduled rechecks | not emitted by this adapter; retained raw versions and later comparisons determine whether a value changed |

The recent lane does not wait for a variable's history. A newly admitted variable can
therefore collect recent values while its history pages remain queued. A failed or
long-running backfill cannot prevent the next dated recent generation. The current
and four previous calendar years keep routine requests cheap while allowing annual
publication lag and recent revisions. The official `years` dictionary included the
current year in the bounded check, and BDL documents repeated `year` filters on this
endpoint; variables without values in part of the window return only their available
observations. Older corrections require a bounded history
or reconcile campaign; recent collection is not presented as complete revision
detection.

The locality endpoint requires a 12-character parent unit ID and has no valid root
enumeration. Initial tasks therefore enumerate the full ordinary unit catalogue.
Each municipality returned at official level 6 emits Polish and English locality
list tasks with its exact BDL unit ID as `parent-id`. Level-6 unit details emit the
same tasks as a recovery path for unit pages accepted before this rule was added.
This makes the locality target progressive and resumable without treating the
rejected root request as an empty or complete catalogue.

Contract revision `gus-bdl-parent-scoped-localities-2026-09-08` retires exactly the
two former page-zero root locality tasks, one per language. `migrate_state()` removes
them only from pending work and records the full original tasks, reason, production
HTTP 400 evidence, and this revision in `plan_dispositions`. It never marks them
complete or removes their raw bodies and receipts. A matching legacy ID with any
other cursor or task shape fails migration instead of being silently discarded.

The Polish variable page is the expansion authority. For every discovered variable
it emits an English detail task, a recent page-zero root, and an all-history page-zero
root. Polish source labels are already present in the Polish catalogue; the English
detail adds an English description and validity years without paying for a second
detail request for all 172,573 variables. The initial population seed and the much
smaller subject/unit sets use bilingual details where useful. The English catalogue
independently preserves translated labels but does not emit duplicate data work.
Page-zero discovery starts with 20 results, deliberately
limiting the fan-out of each completed catalogue task. The shared scheduler may hold
later catalogue pages and deferred detail/data work when active work reaches its
capacity bound. Holding work is durable progress state, not removal from scope.

## Quotas, scale, and freshness claims

The official anonymous limits are 5 requests per second, 100 per 15 minutes, 1,000
per 12 hours, and 10,000 per seven days. Registered users receive higher limits, but
this adapter requires no account or API key. The shared runner should stay below all
rolling anonymous limits across sources; a conservative BDL allocation is at most 4
per second, 80 per 15 minutes, 800 per 12 hours and 8,000 per seven days, reduced
further when other sources share the same campaign budget. The adapter never sleeps
or bypasses a response limit. A `429` remains a transport failure for the runner to
retry only under its bounded policy. Official responses advertise
`X-Rate-Limit-Limit`, `X-Rate-Limit-Remaining`, and `X-Rate-Limit-Reset`; those headers
should tighten, never expand, the configured budget.

At the observed 172,573-variable size, bilingual variable listing alone needs 17,258
requests at the adapter's 20-item page size. English per-variable details add 172,573
minimum requests. One recent and one history first page per variable add another
345,146 before any data continuation pages. Units, localities, subjects and
dictionaries add further work. Thus a complete API expansion is necessarily a
multi-cycle campaign under anonymous quotas; even the known minimum exceeds 534,000
requests and full observation history is substantially larger. A later targeted
campaign may add Polish details when the Polish catalogue labels are insufficient for
a governed definition; their absence is reported rather than implied complete.

The v1 OpenAPI contract exposes no changed-variable feed and no bulk-export endpoint.
It exposes subject and data `lastUpdate` fields and supports conditional request
headers, but neither feature identifies all changed variables without catalogue or
data checks. Therefore:

- the system may claim the exact variables, pages and observation periods actually
  retained and validated;
- it may report catalogue totals and explicit queued/deferred work;
- it may report the last collection, source `lastUpdate`, and achieved recent
  coverage per admitted variable;
- it must not claim daily freshness for all 172,573 variables, complete BDL history,
  or a completed catalogue while deferred tasks remain.

A future official bulk distribution or change feed can replace the request-heavy
path after its identity, rights, versioning and completeness are verified. Optional
free registration can be considered if measured campaign duration requires it, but
the anonymous path remains functional and no account is assumed here.

## Failure and change behavior

Malformed JSON, a false-empty object, an explicit page/cursor disagreement, invalid identifiers,
non-numeric values, and duplicate observation grains fail the task. The task is not
marked complete and no follow-up page is inferred. Extra source fields remain in the
retained exact bytes for forward-compatible dbt handling.

BDL says resources are continuously supplemented, updated and corrected. A later
different response is an observed source change. It is not labelled an official
correction reason or date unless GUS metadata says so. Raw responses, task identity,
collection time, bilingual metadata and source `lastUpdate` provide the evidence for
downstream current-value and detected-change policy.
