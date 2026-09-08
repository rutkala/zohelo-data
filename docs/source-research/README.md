# Global and Polish data-source landscape

**Research date: 8 September 2026. Research delivered for review; new connections have
not been tested and onboarding has not started.**

This package covers **182 named source products or coherent families**, including the
existing NBP baseline. It combines multicountry context with detailed Polish public,
institutional, local and commercial sources. It contains source evidence, access and reuse
questions, proposed historical/incremental loading patterns, grains, identity links, overlap
handling and a silver/gold/semantic modeling proposal.

The owner's geographic, language and staged-delivery requirements are recorded in
[ADR 0003](../decisions/0003-source-expansion-scope.md). English is the documentation
language; original product names and source labels remain identifiable. Project status and
owner answers remain in [the delivery record](../deliverables.md).

## Read the package

| Part | Purpose | Products/families |
| --- | --- | ---: |
| [Inventory index](inventory-index.md) | Browse every product, geography, domain and evidence state | 182 |
| [Global public sources](global-public.md) | Eurostat/OECD/World Bank, finance, UN agencies, health, trade, environment and specialist statistics | 40 |
| [Poland: statistics and society](poland-social.md) | GUS products, ZUS/KRUS, health, education, labour, agriculture, culture and tourism | 35 |
| [Poland: governance and economy](poland-governance.md) | NBP, ministries, public finance, company registers, procurement, parliament, law, audits and justice | 34 |
| [Poland: environment and local data](poland-environment.md) | Geospatial data, weather, air/water, geology, energy, transport, buildings, broadband and city feeds | 35 |
| [Commercial, platform and disclosure sources](commercial-disclosures.md) | Market vendors, Polish exchanges/disclosures, corporate filings, social APIs, open knowledge and marketplace reports | 34 |
| [Additional intellectual-property and fire-service sources](additional.md) | UPRP statistics/search, EPO patent data and KG PSP publications | 4 |
| [Coverage and taxonomy](coverage-and-taxonomy.md) | Independent classification axes, domain matrix, overlap policy and institutional/local long tail | — |
| [Ingestion and modeling proposal](ingestion-and-modeling.md) | Recent/history/reconciliation design, platform gaps, conformed dimensions, fact families and semantic acceptance | — |
| [Review and test brief](review-plan.md) | Proposed review batches and bounded tests to define after review | — |

The complete structured research is available as [JSON](source-inventory.json) and
[CSV](source-inventory.csv). The companion Excel workbook separates discovery, access/rights,
loading/modeling, evidence and review notes into filterable sheets. It is a review aid;
its editable notes do not replace the canonical delivery record.

## What the evidence establishes

The inventory contains **312 source-to-reference links to 285 distinct official URLs**.
Each reference records its purpose, review date and whether the underlying page was opened
or only a search excerpt was available. Repeated links support different products and are
not counted as additional distinct sources.

| Evidence or reuse label | Count | Meaning |
| --- | ---: | --- |
| `documented` | 134 | An official access route was reviewed in opened material; endpoint operation, every technical detail and permission for the intended use are not thereby proven |
| `catalogue-only` | 48 | Discovery evidence exists, but the access contract still needs fuller documentation review; opened legal or catalogue pages alone may not settle it |
| `open-conditional` | 43 | Reviewed evidence supports some open reuse, subject to attribution, scope, third-party or other conditions |
| `dataset-specific` | 26 | The specific dataset/distribution or upstream provider determines the applicable rights |
| `restricted` | 28 | Explicit restrictions, contractual access or limited permitted-use programmes materially affect onboarding |
| `unresolved` | 85 | The reviewed material does not establish the needed permission; neither free reuse nor a prohibition is inferred |

Access and reuse are independent classifications. A documented API can have unresolved
rights, and a paid product can permit a particular use under contract. Unknown quotas are
left unknown. Numerical limits are recorded only where primary documentation supplies them;
they still need confirmation for the selected endpoint/account at connection-test time.

Every `connection_status` is `not-tested` **for this research phase**. NBP FX/gold is marked
`existing` in the priority field because it is already implemented and has separate live
acceptance evidence. This research did not rerun that acceptance or test any new connection.

`review-first` means a proposed early candidate within its domain, not an instruction to
launch 79 integrations at once. Other labels are `review-next`, `specialist` and `conditional`.
These recommendations express sequencing judgment; they are not owner selections, readiness
scores or approved commercial expenditure.

## Main design conclusions

**Use several grouping axes.** Provider type, producer/republisher role, geography, domain,
entity, grain, interface and update behavior answer different questions. BDL and DBW are GUS
products; APIs and bulk exports may be distributions of one dataset. Keep original-producer
lineage for data republished through Eurostat, international organizations or a catalogue.

**Start recent collection while recoverable history fills.** The proposal uses separate
recent, historical and revision work, durable partition checkpoints, shared provider quotas,
reserved fresh-data capacity and measurable historical progress. Short validated publication
steps replace an all-history completion dependency. Sources with only current snapshots
cannot supply historical states they never expose; a daily check cannot turn annual data
into daily observations. The source dossiers describe these differences.

**Conform meanings and keys before combining facts.** Preserve source-specific silver grains
and versions. Build domain gold facts linked through versioned geography, time, entity
identity, classification, units and methodology. Aggregate compatible facts separately before
aligning results, so joins do not multiply observations. Legal entities, sole-trader activities,
establishments, awards, notices, documents and sensor readings remain distinct model objects.

**Represent every admitted source appropriately in the semantic layer.** Some sources supply
reference dimensions or document entities; others supply measures. Record aggregation rules,
date roles, denominators, suppression flags and provenance. Only explicit, reviewed definitions
become executable derived metrics. Overlapping providers remain traceable rather than being
silently merged or dropped.

**Extend the current platform deliberately.** The repository currently validates an exact
15-table NBP v2 release, and its historical planner and workflow serialization do not provide
the requested general recent/history behavior. The [design proposal](ingestion-and-modeling.md)
identifies these extension points. Research does not change those contracts, runtime limits,
production data or the existing daily schedule.

## Proposed first review

Start with the shared geographic backbone (TERYT and applicable NUTS/LAU correspondence),
then review a small **GUS BDL + Eurostat** comparative slice and **World Bank WDI** for wider
context. Consider DBW and OECD expansion after the selected indicators' concepts, geographic
levels and vintages are understood. This is a suggested first analytical slice, not a binding
product choice. Evidence and qualifications are in the [Polish statistics dossier](poland-social.md)
and [global dossier](global-public.md).

Then choose one **IMGW, GIOŚ or PSE** family to exercise recent collection with historical
backfill, and a **Sejm/ELI, procurement or company-identity** slice to exercise events,
documents and entity links. Commercial market/platform products remain visible for later
use-case-specific review. Their account scope, history, retention and redistribution terms
need separate consideration. See the [review sequence](review-plan.md).

## Boundaries and remaining coverage work

This is a broad product-level landscape, not an exhaustive census of every dataset, company,
ministry attachment or Polish municipal service. A single listed product can contain thousands
of datasets with different grains and terms. The inventory deliberately retains overlapping
distributions and disclosure channels when their access or provenance differs, without
claiming each republishes independent facts.

The main remaining discovery work is the Polish municipal/county and subordinate-agency
long tail; detailed ministry/BIP attachments; less standardized culture/sport/tourism and
maritime publications; and dataset-level access/rights for catalogue-only entries. Social
platforms do not imply a complete reusable archive of all public content. Some restricted
microdata and private business records are outside open intake. Sector dossiers identify
their specific gaps, including unavailable or blocked documentation.

Next, review the landscape and choose a concrete analytical question. Expand the chosen
products into dataset contracts, verify their terms, and define a small bounded connection
test. Full source selection, successful connections, complete history and final executable
models are subsequent deliverables; none is inferred from this research.
