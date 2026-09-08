# Source expansion implementation plan

Status: implementation sequence for the accepted worldwide and detailed-Poland scope, 8 September 2026.

This plan turns the 182-product research inventory into a resumable onboarding programme. It does not say that an
inventory product covers every category assigned to it, that an untested source is connected, or that unavailable history
can be reconstructed. Candidate mappings live in [`config/source-domain-coverage.json`](../config/source-domain-coverage.json);
stable English subject IDs, analytical dimensions and classification systems live in
[`config/domain-taxonomy.yaml`](../config/domain-taxonomy.yaml).

The completion target is explicit disposition of every inventory ID, ingestion of every feasible admitted dataset and all
history the selected distribution actually makes recoverable, and measured continuing collection. A restricted or paid
source reaches `deferred-prerequisite` with a concrete access, rights, budget or credential prerequisite. Discovery never
authorizes a purchase or account signup.

## Delivery rules

1. Keep the current NBP release valid while source-neutral intake is added. A new source must not change
   NBP replay, validation, promotion or rollback behavior.
2. Treat provider, product/family, dataset, distribution and partition as different identities. The 182 records are
   products or families; onboarding expands them into dataset contracts.
3. Run recent, historical and reconciliation work as separate durable queues. Recent collection gets reserved quota and
   time; history uses measured spare capacity and resumes from its last validated partition.
4. Preserve received bytes, request identity, retrieval time, source vintage, hashes and response metadata. Transform in
   dbt after immutable intake; do not add source-specific business transformations to Python.
5. Treat independent source releases, transformations and cross-source builds as later phases. Landing checkpoint pointers
   identify collection progress; they are not published data releases and must not be presented as such.
6. Keep source labels and codes; add English descriptions. Store classification scheme, edition, validity and mapping
   provenance. A correspondence table does not make two codes or time series equivalent.
7. Admit freely accessible sources autonomously when the contract, reuse evidence, connection test and operational budget
   pass. There is no source-by-source owner review gate. Hold paid, key-gated or restricted sources until their recorded
   prerequisites are satisfied; never sign up for or buy a service autonomously.

## Current implementation boundary

The first delivered framework ends at **Landing intake and durable collection checkpoints**. It provides bounded WDI, BDL
and Eurostat adapters; separate recent, history, discovery and reconciliation work lanes; exact response retention;
immutable receipts; per-source state snapshots; conservative quotas and capacity pauses; and a serialized production
campaign entrypoint. A completed task means that a response was accepted and durably recorded. It does not mean that its
records were typed, deduplicated, reconciled into a business fact or published for analysis.

This first boundary does **not** implement independent source data releases, Bronze/Silver/Gold or semantic models, dbt
catalogue publication, cross-source marts, full operator commands, or a sharded scalable ledger. The present per-source
state is deliberately bounded and monolithic. Further all-source expansion must add sharding and compaction before state or
pending-task limits are approached; raising limits is not a scaling design.

Phase 2 passed bounded production acceptance in [run 34217059540](https://github.com/rutkala/zohelo-data/actions/runs/34217059540):
WDI, BDL and Eurostat each accepted recent and historical responses and passed fresh-process replay.
[The dated evidence record](releases/2026-09-08-source-campaigns.md) identifies the configured production root, code revisions,
checkpoint counts, resource measurements, failures and repairs. Collection runs every four hours; coverage remains incomplete
and the later modeling, publication and scaling phases are not delivered by this acceptance.

The discovery baseline contains 442 assignments from the 182 inventory products and 30 assignments from seven bounded
additional candidates. Inventory products touch 160 of 231 taxonomy categories; 71 have no inventory-product candidate.
After the additional candidates, 177 categories have at least one discovery lead and 54 still have none. The ledger also
records 15 thematic gaps. Gap records are not one-to-one with categories, and none of these counts measures connected,
ingested, historically complete or published data.

## Readiness states and evidence

| State | Required evidence | Permitted operation |
| --- | --- | --- |
| `inventoried` | Stable inventory ID and official references | Research and classification only |
| `contract-draft` | Dataset/distribution identity, grain, keys, time meaning, history route, rights and limits | Fixture design |
| `connection-ready` | Operations allowed for intended use; authentication and quota route understood | Bounded read test |
| `connection-verified` | Captured request/response receipt, schema sample and measured limits | Staged recent/history intake |
| `landing-operational` | Production raw intake, receipts and durable checkpoints pass recovery and budget checks | Scheduled bounded collection and backfill |
| `model-ready` | Future Bronze/Silver/Gold contract, classification versions, units and revision rules | Future candidate build |
| `publish-ready` | Future model tests, replay, provenance, resource budget and recovery pass | Future serialized source-release promotion |
| `operational` | Future publication freshness checks, runbook and alerts verified | Published analytical use |
| `available-complete` | Every planned recoverable partition validated; exclusions and unavailable periods recorded | Normal reconciliation |
| `deferred-prerequisite` | Named unmet licence, key, account, budget, privacy or distribution requirement | No collection |
| `retired` | Replacement/successor and retained-release policy recorded | No new collection |

These are per-dataset states. A product can contain operational, incomplete and deferred datasets at the same time.
`connection_status=not-tested` in the research inventory remains authoritative until a receipt is recorded by the
implementation.

## Dependency and readiness order

| Phase | Deliverable | Depends on | Exit measure |
| ---: | --- | --- | --- |
| 0 | Versioned taxonomy and coverage ledger | Research inventory | Exactly 182 unique inventory IDs mapped; every domain/category reference resolves; gaps and candidate-only caveats present |
| 1 | **Delivered:** source-neutral Landing intake and checkpoint framework | Existing storage boundaries and NBP isolation | Fixtures prove independent work lanes, bounded replay, shared quota controls, exact raw/receipt retention, recovery and fail-closed capacity pauses |
| 2 | **Delivered, bounded acceptance:** first WDI, BDL and Eurostat production campaigns | Phase 1; taxonomy; source contracts | Bounded production Landing runs retain verified responses/receipts and advance only durable checkpoints; coverage remains explicitly incomplete while queues remain |
| 3 | Sharded scalable ledger and full operator controls | Measured Phase 2 state/task growth | State shards, manifests and compaction survive interruption; operators can inspect and control work without loading or rewriting one unbounded state object |
| 4 | Geography and classification backbones | Phase 1; source contracts | TERYT and applicable NUTS/LAU editions plus official classification structures/crosswalk evidence are retained and modeled |
| 5 | Bronze and Silver models | Phases 2–4 | Raw replay produces typed source-grain records with keys, flags, versions and reconciliation tests |
| 6 | Independent source releases, Gold and semantics | Phase 5 | Validated immutable source releases, recoverable promotion, domain facts/dimensions, catalogue lineage and validated source-defined metrics |
| 7 | High-readiness public source waves and Poland depth | Phases 3–6; measured capacity | Each admitted source passes the applicable Landing and later modeling gates; no source starves recent work or blocks another source |
| 8 | Commercial, key-gated and restricted products | Concrete business need and recorded prerequisites | Contract/budget/rights decision exists before credentials or payment; permitted fields and downstream use are enforced |
| 9 | Whole-scope convergence | All prior phases | Every inventory ID has a current disposition; every admitted dataset reports recent and historical coverage independently |

Phase numbers express dependencies, not one long serial job. Once Phase 1 is safe, multiple source adapters can progress in
parallel within measured provider, runner, memory, disk and Drive budgets. Production Landing collection may continue while
later modeling phases are built; it must remain labeled as Landing coverage rather than analytical publication.

## Phase 0 — taxonomy and registry checks

Load the taxonomy and coverage ledger as validated configuration. CI must reject duplicate source/category/classification
IDs, unknown category references, missing English names, a taxonomy version mismatch, or an inventory ID added or removed
without a mapping update. Taxonomy identifiers are append-only within a major version. Labels may improve; identifiers are
deprecated instead of reassigned.

Dataset contracts extend product mappings with `dataset_id`, original publisher, upstream lineage, distribution, source
codes, grain, keys, units, geography, time roles, recoverable bounds, update behavior, deletion meaning, schema policy,
licence evidence, allowed operations, attribution, quota scope, recent partition rule, history partition rule, reconciliation
lookback and model names. Contract validation must finish before a live read.

## Phase 1 — delivered Landing intake and checkpoint framework

Implement one adapter interface with source-specific request planning and decoding. Generic HTTP configuration may express
headers, pagination and retries, but each adapter remains responsible for exact request identity, source errors, response
validation, date semantics and pagination termination.

The durable state model has separate queue items for `recent`, `history` and `reconcile`. Each item records dataset,
distribution, partition, attempt, status, not-before time, request/byte/runtime budget, immutable receipt and resulting raw
objects. Only a coordinator advances consolidated checkpoints. Queue priority reserves a configured allowance for the next
recent window and gives aging history work a minimum allowance. The scheduler enforces quota at the provider/account/key/IP/
endpoint scope stated in the contract.

The state snapshot pointer advances collection state only after immutable response and receipt writes are verified. It is
not a data-release pointer. The delivered framework records adapter-reported counts as transport metadata and does not claim
row-level observation validation. Failed extraction, raw upload, receipt creation or checkpoint persistence keeps previously
durable evidence and fails closed on ambiguous writes.

Framework acceptance uses fixtures to prove:

- a failed recent request does not erase its checkpoint and is retried before the freshness limit;
- a 1,000-partition history plan resumes after a forced interruption without repeating more than one configured chunk;
- recent work runs while history is incomplete and cannot be starved by history quota use;
- two datasets sharing one provider quota cannot exceed the shared cap;
- unchanged replay is idempotent and changed source values create evidence rather than duplicate current facts;
- a response beyond configured bytes, retained storage or state/task capacity pauses without silently dropping work;
- local and Drive-shaped stores preserve verified raw objects, receipts and the last readable checkpoint after failure.

## Phase 2 — first three production source campaigns

The first production campaigns exercise three statistical access patterns. They write only Landing responses, receipts and
collection checkpoints in this phase. Their future Bronze/Silver/Gold and semantic targets are recorded so intake retains
the needed provenance, but those targets are not implemented by a successful campaign.

| Campaign | Recent Landing milestone | Historical Landing milestone | Future Silver and Gold milestone | Future semantic/catalogue milestone |
| --- | --- | --- | --- | --- |
| `GL-005` World Bank WDI | Snapshot selected indicator metadata and the newest available periods; record API/bulk vintage and observation status | Partition the full selected indicator × economy history; checkpoint each bounded bulk member or API page | Silver grain: source indicator × economy/aggregate × period × source vintage. Gold: explicitly comparable annual indicator observations with unit, scale, status and methodology dimensions | Publish only source-defined observations; expose indicator definition, aggregation note, coverage and vintage. Do not choose specialist-source precedence silently |
| `PLS-001` GUS BDL | Snapshot subject/variable/unit metadata and re-read configured recent years for selected variables | Backfill selected variable × territorial level × year partitions; retain attributes and changed values | Silver grain follows the returned BDL variable/unit/year/attribute contract. Gold links source territorial codes to versioned geography without forcing historic units into current boundaries | Expose source measure/unit and territorial level; derived rates require a separately defined and validated numerator, denominator and boundary policy |
| `GL-001` Eurostat | Fetch selected dataflow structures and newest changed periods using the documented distribution; preserve status flags | Backfill by dataset code and bounded dimension/time slices or bulk file; retain structure and download vintage | Silver grain is dataset code plus every source dimension, period and vintage. Gold is dataset-specific and may use NUTS versions, NACE/CPA/COICOP/COFOG/ISCED or other code lists | Publish source observations and flags. Do not treat a label match with WDI/BDL as equivalence; any comparison records concept and vintage compatibility |

Official discovery evidence: [WDI](https://datatopics.worldbank.org/world-development-indicators/),
[BDL API documentation](https://api.stat.gov.pl/Home/BdlApi), and
[Eurostat API introduction](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-introduction).
Each dataset contract must refresh the applicable terms and dataset-level rights evidence before its bounded live test.

Campaign exit criteria are quantitative but bounded to Landing: configured production roots and serialization are enforced;
every attempted request has a durable success/failure outcome; successful responses and receipts pass size/hash checks; the
checkpoint resumes after interruption; recent and history queues both make measurable progress under the configured quota;
and requests, retained bytes, runtime, pending tasks and state size remain below the fail-closed limits. A bounded campaign
may exit successfully with `coverage_status=incomplete`. Full history means that every recoverable planned partition later
reaches a terminal state; it is not a prerequisite for the first recent Landing run.

## Phases 3–4 — scalable state, operator control and shared dimensions

Before many product families or fine-grained backfills are admitted, replace the monolithic per-source state with bounded
shards, for example provider/dataset/lane/time-bucket task segments plus immutable completion segments and a small manifest
pointer. Define shard rollover, compaction, duplicate-task detection, crash recovery, orphan inspection and measured maximum
read/write sizes. Preserve content-addressed raw responses independently of queue compaction.

Add the full operator surface described below in Phase 3. The initial campaign command and workflow are sufficient for the
three bounded sources, but they are not the final all-source control plane.

Onboard `PLS-007` TERYT and `PL-ENV-001` PRG as independent reference datasets. Preserve official identifiers, hierarchy,
validity and source vintage. Add NUTS and LAU versions/correspondences from their official distributions before regional
Eurostat/Poland marts. Never overwrite a historic geography member with its current name or boundary.

Load PKD/NACE, PKWiU/CPA/CPC, HS/CN, COICOP, COFOG, ISCED and ICD only from identified official editions. Store hierarchy
and edition as data. Crosswalk tables record source and target edition, direction, cardinality, validity and official mapping
evidence. PKD/NACE classify activity; PKWiU/CPA/CPC classify products and services; HS/CN classify traded goods; COICOP
classifies household consumption purpose; COFOG classifies government function; ISCED classifies education; ICD classifies
disease and causes of death. No classification is a replacement for the analytical domain taxonomy.

[Statistics Poland documents Polish classifications](https://stat.gov.pl/en/metainformation/classifications/), while
[Eurostat identifies NACE versions](https://ec.europa.eu/eurostat/web/nace),
[CPA versions](https://ec.europa.eu/eurostat/web/cpa) and correspondence tables. The
[EU Combined Nomenclature](https://taxation-customs.ec.europa.eu/customs/common-customs-tariff-cct/tariff-classification-goods/combined-nomenclature_en)
is an annually versioned EU goods classification based on HS. Version selection follows each observation's source metadata,
not the latest edition by default.

## Phases 5–6 — transformations, source releases and semantics

Phase 5 adds dbt Bronze and Silver models only after representative retained Landing responses define the real schemas.
Bronze preserves repeated source representations and raw-object lineage. Silver applies types and source-grain keys, retains
status/suppression/revision fields and records detected changes. Full and incremental builds must agree at the same source
cutoff. A successful Landing campaign alone cannot satisfy any Phase 5 test.

Phase 6 adds immutable source releases, verified promotion/rollback and consumer restore. Gold facts and dimensions retain
declared grain, units, aggregation rules, geography/classification versions and source-release identities. Semantic models
contain source-defined measures first; derived metrics require an explicit definition and validation, but this metric gate
does not block autonomous Landing collection or source onboarding. Cross-source marts pin compatible source releases and
disclose differing observation and publication times.

## Phase 7 — public source waves and Poland depth

Order public candidates by operational pattern and shared dependencies. An ID in a wave is a discovery candidate, not a
promise that its current access or reuse gate will pass.

| Wave | Initial IDs | Loading milestone | Modeling milestone |
| --- | --- | --- | --- |
| International statistical APIs/SDMX | `GL-003`, `GL-012`, `GL-015`, `GL-016`, `GL-018`, `GL-019`, `GL-023`–`GL-029`, `GL-037`, `GL-039` | Reuse bounded time-series/SDMX partitions, provider quota groups and structure-vintage capture | Source-specific observation facts plus conformed geography/time/classification links; no silent provider precedence |
| Polish official statistics and administration | `PLS-002`–`PLS-006`, `PLS-009`–`PLS-025`, `PLG-002`–`PLG-010`, `PLG-017`–`PLG-018`, `PLG-031`–`PLG-033` | Separate machine-readable tables from versioned documents; preserve editions, suppression and territorial basis | Domain facts at actual survey/administrative grain with population universe, denominator, classification and status dimensions |
| Registers and entity links | `PLS-008`, `PLS-020`–`PLS-023`, `PLG-011`–`PLG-016`, `CD-014`–`CD-016`, `CD-026`, `CD-029`, `EX-001`–`EX-004` | Snapshot plus delta/reconciliation where documented; never infer deletion from absence | Distinguish legal entity, sole trader, establishment, provider, filing, identifier, application, publication, grant and patent family |
| Events, notices and legal documents | `PLG-020`–`PLG-029`, `PLG-034`, `CD-006`–`CD-008`, `CD-012`–`CD-013`, `CD-030`, `EX-003` | Version notices/documents and their attachments; checkpoint event/date partitions | Separate document/version, procedure, lot, award, vote, act version, decision, incident and extracted assertion |
| Environment, energy and high-frequency feeds | `PL-ENV-008`–`PL-ENV-021`, `GL-034`–`GL-037` | Start recent telemetry/publication collection first; backfill station/resource partitions under shared quotas | Keep station/sensor/forecast/reanalysis/system interval, validation state and unit; publish live and validated series separately where the source does |
| Geospatial and built environment | `PL-ENV-002`–`PL-ENV-007`, `PL-ENV-013`–`PL-ENV-018`, `PL-ENV-033`, `PL-ENV-035`, `CD-027` | Snapshot service capabilities and dated packages; tile/feature partitions only after measured scale | Version geometries, CRS, validity and feature IDs; keep raster, feature, parcel, permit and plan entities distinct |
| Transport and municipal feeds | `PL-ENV-022`–`PL-ENV-032`, `PL-ENV-034` | Preserve schedules separately from real-time events; recent-first for ephemeral feeds | Conform operators/stops/routes cautiously; do not merge city identifiers or scheduled and observed movement |
| FMCG and agricultural depth | `GL-002`, `GL-022`, `PLS-006`, `PLS-032`–`PLS-034` plus `ADD-EU-AGRI-OBS` and `ADD-PL-INDUSTRIAL-PRODUCTS` | Partition by commodity/product, market, geography and period; retain report/file edition | Use versioned PKWiU/CPA/CPC and HS/CN codes. Keep production, farmgate/wholesale/retail price, trade, volume, inventory and consumer sales as separate facts |
| Culture, tourism, media and sport | `GL-026`, `PLS-026`–`PLS-028`, `PLS-035`, `CD-017`–`CD-025`, `ADD-PL-SPORT-GUS`, `ADD-EU-SPORT-EUROSTAT`, `ADD-GLOBAL-SPORT-FIFA`, `ADD-PL-MEDIA-KRRIT`, `ADD-PL-TELECOM-UKE` | Start with official downloadable tables; platform/federation access remains conditional | Separate work metadata, copyrighted content, post/video, audience measure, ad, facility, club, athlete/team, competition, fixture, result and ranking |

The remaining inventory IDs keep their coverage-ledger priority and move into the earliest compatible wave after their
evidence and contract gates pass. No source-by-source owner review is required. `review-next`, `specialist` and `conditional`
remain research-time sequencing hints only.

For Poland local depth, generate a publisher ledger from versioned TERYT units. Track, per unit, `publisher_identified`,
`catalogue_or_bip_inspected`, `dataset_discovered`, `access_understood`, `rights_understood`, `contracted` and `admitted`.
Inspect official catalogues, BIP pages, geospatial services and public operators. Register exact datasets; do not crawl all
government pages indiscriminately. Report counts and territorial coverage by current and historic unit version.

FMCG completion is category-specific. Meat, milk/dairy, eggs/poultry, grain/oilseed/feed, fruit/vegetables, sugar,
oils/fats, fish/seafood, processed food, non-alcoholic and alcoholic beverages, tobacco/nicotine, personal care/cosmetics,
household cleaning/paper, pet care and retail/distribution each get independent discovery, contract and history status.
Trade or industrial-production coverage does not establish retail sales, market share or consumer prices.

Sport completion is also category-specific: participation, facilities/clubs, athletes/teams, fixtures/results, rankings,
economics/employment, governance/funding and esports. Begin with the official GUS/Eurostat candidates. Federation web pages
such as [FIFA rankings](https://inside.fifa.com/fifa-rankings/world-ranking) are only discovery evidence until a documented
permitted distribution and history route are found.

The European Commission confirms distinct milk, meat, crops, sugar, fruit/vegetable, wine, fertiliser and other
[agricultural market observatories](https://agriculture.ec.europa.eu/data-and-analysis/markets/overviews/market-observatories_en).
Statistics Poland publishes downloadable physical-culture tables for
[Polish clubs, associations and participants](https://stat.gov.pl/obszary-tematyczne/kultura-turystyka-sport/sport/kultura-fizyczna-w-polsce-w-2024-r-%2C13%2C8.html).
These sources narrow two gaps but do not by themselves complete FMCG or sport coverage.

## Phase 8 — paid, key-gated and restricted products

Do not sign up for or buy a service during autonomous onboarding. Create a prerequisite record containing the source and
dataset, required account/key/contract, price or quote if already supplied by the vendor, permitted purpose, retention,
redistribution, field restrictions, expected history, rate limits, data residency/privacy implications, smallest useful
test, free/public alternative and business value. Credentials enter only the supported secret store after authorization;
secret values never enter Git, logs, receipts or chat.

Commercial market data (`CD-001`–`CD-011`, `CD-032`–`CD-034`), restricted platforms (`CD-018`–`CD-024`) and conditional
official services remain `deferred-prerequisite` until these fields are concrete. A public website, trial, search interface
or quoted product page does not establish bulk access or redistribution rights.

## Operator controls and runbook surface

The delivered command supports explicit source/backend/root selection, enforced production-write opt-in, configured request/
byte/runtime/state/task limits, a history pause, machine-readable run summaries and fail-closed capacity reporting. The
production workflow supplies serialization. These controls are sufficient for the bounded first campaigns; they are not a
complete operator surface.

Phase 3 adds bounded commands or workflow inputs for:

- `status` by source/dataset/queue, showing last attempt, last successful intake, latest observation, latest validated
  publication, recent SLA state, history complete/planned/unavailable counts, next partition and quota use;
- `plan --recent`, `plan --history`, and `plan --reconcile` with a no-write preview of partitions, requests and estimated
  bytes/runtime from measured throughput;
- `run` with explicit environment/root, source/dataset allow-list, queue, maximum requests/bytes/runtime/partitions and a
  production-write opt-in enforced by the invoked code;
- `pause`, `resume`, `cancel` and bounded `retry` without deleting successful checkpoints or immutable raw evidence;
- `validate-candidate`, `promote --expected-current`, `rollback --expected-current` and read-only `restore`, with a single
  serialized promotion boundary;
- provider-level circuit breakers, exponential retry with retry-after support, schema-drift quarantine, kill switch and
  recent-quota reservation;
- a safe disposition editor for `blocked-rights`, `blocked-auth`, `blocked-cost`, `source-unavailable`, `history-unavailable`
  and `retired`, with evidence URL/date and operator note.

Every production run must emit a machine-readable summary and concise operator report. The current Landing report covers
source, reason, work lanes, requests, bytes, retained state and incomplete/complete collection status. Phase 3 extends it
with config/taxonomy versions, dataset/distribution, planned partitions and checkpoint before/after; Phase 6 adds candidate/
release IDs and whether publication promotion occurred. Logs redact credentials and avoid printing response bodies that may
contain restricted or personal data.

## Completion scorecard

Report progress without collapsing distinct meanings:

| Measure | Numerator / denominator |
| --- | --- |
| Inventory disposition | Inventory IDs with a current readiness/disposition / 182 |
| Dataset discovery | Enumerated datasets / expected datasets for each admitted product family, with expectation source |
| Contract readiness | Dataset contracts passing schema and rights gates / discovered datasets |
| Recent coverage | Successful planned recent partitions / planned recent partitions, plus age of latest observation and publication |
| Historical coverage | Validated historical partitions / recoverable planned partitions; unavailable periods reported separately |
| Classification coverage | Distinct source codes with an edition-aware retained or mapped status / distinct source codes |
| Geographic coverage | Published units/features by source geography/version / discovered units/features for that contract |
| Model coverage | Admitted datasets with passing silver, declared gold/reference/document role and catalogue entry / admitted datasets |
| Recovery | Published datasets passing raw replay, cold restore and rollback tests / published datasets |
| Category discovery | Taxonomy categories with at least one researched candidate / all categories; never label this data coverage |

`available-complete` requires zero unexplained planned gaps and a recorded source cutoff. It does not mean all real-world data,
all historical versions or all taxonomy categories exist. New source editions, newly exposed archives and classification
changes reopen the affected discovery or history work through a versioned plan rather than silently changing the denominator.
