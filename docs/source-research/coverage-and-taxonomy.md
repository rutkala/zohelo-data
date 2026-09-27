# Source taxonomy, coverage and expansion method

The inventory is a landscape of named products and coherent dataset families. It is not
an enumeration of every series, file, legal entity, municipality or publisher on Earth.
Use product-level research to find candidates, then dataset-level discovery to establish
the exact intake universe. Breadth and verification depth are separate measures.

## Independent grouping axes

| Axis | Useful values | Why it stays separate |
| --- | --- | --- |
| Provider role | Original producer, statistical compiler, registry operator, republisher, gateway | The same measurement can travel through several organizations |
| Ownership/type | International public, national public, subnational public, commercial, private filer, community/academic | Ownership alone does not determine access or reuse terms |
| Geography | Worldwide multicountry, European/regional multicountry, Poland, subnational Poland | An international organization may cover only members or a selected country sample |
| Domain | Economy, people, health, business, public finance, governance, environment, infrastructure, etc. | A provider such as GUS publishes across many domains |
| Entity | Country, region, municipality, firm, institution, facility, station, instrument, document, population aggregate | Entity keys shape possible joins |
| Grain | Time series cell, current snapshot, event, document/version, spatial feature, telemetry | Finest available grain differs between products and often between datasets |
| Distribution | API, SDMX, bulk file, database export, geospatial service, HTML/PDF, live feed | Several distributions can contain the same dataset |
| Change behavior | Append, revision, delta, complete replacement, current-only snapshot, irregular document publication | Determines recoverable history and continuing load strategy |
| Rights/access | Public without login, registered, account-scoped, licensed, paid; separate reuse conditions | Public visibility and commercial redistribution are different questions |
| Evidence state | Documentation reviewed, catalogue-only; connection not tested | Research certainty must remain visible |

World Bank, OECD and Eurostat are multicountry statistical aggregators with different
geographic/subject coverage. WHO is a domain organization. YouTube/Meta/X are commercial
platforms whose developer access is not equivalent to their full public-facing corpus.
For Poland, NBP, ZUS, NIK, KRS and Sejm differ in institutional role and record type. BDL
and DBW are GUS products. The Bank Danych o Lasach acronym BDL refers to another
domain-specific product and must not collide with GUS Local Data Bank identifiers.
Source-specific evidence is recorded in the linked sector reports and inventory.

## Domain coverage to inspect during review

The following is a coverage checklist, not a claim that every possible dataset in each
domain is available or already documented. The source inventory and sector reports provide
the concrete candidates and access evidence.

| Domain | Global / multicountry discovery direction | Detailed Polish discovery direction | Typical gap to keep visible |
| --- | --- | --- | --- |
| Macro, prices and public finance | Eurostat, OECD, World Bank, IMF, BIS, ECB | GUS, NBP, finance and local-government reporting | Statistical revisions and accounting-basis comparability |
| Population and demography | UN/World Bank/Eurostat demographic products | GUS BDL, census and territorial metadata | Small-area suppression and changing boundaries |
| Work, income and social protection | ILOSTAT, OECD, Eurostat | GUS, ZUS, KRUS and labour/social ministries | Survey vs administrative populations and denominators |
| Health and care | WHO, UNICEF, OECD, Eurostat | NFZ, health ministry, NIZP PZH, facility registers | Patient records are not an open aggregate feed |
| Education, research and innovation | UNESCO UIS, OECD, WIPO, EPO, OpenAlex/Crossref | RSPO/RAD-on/ELA/CKE, UPRP and GUS | Cohorts, institutions and outputs are different entities |
| Agriculture and food | FAOSTAT, trade and development agencies | Agriculture ministry, ARiMR/KRUS/KOWR and GUS | Farm/parcel-level personal or commercially sensitive fields |
| Trade, industry and companies | UN Comtrade, WTO, UNCTAD, business identifiers | GUS/REGON, KRS/CEIDG, sector registers | Classification versions, mirror trade and enterprise vs establishment |
| Corporate accounts and markets | SEC/official filings, exchange/vendor products | KRS RDF, ESPI/EBI, GPW and licensed market data | Raw redistribution, amendments, consolidated accounts and paid history |
| Procurement and state activity | TED and international institutional sources | BZP, budget data, NIK, parliamentary and legal data | Notice/lot/award/payment distinction and document extraction |
| Law, democracy, justice and safety | Multicountry governance/crime sources | Sejm/ELI, PKW, justice/police/KG PSP publications | Legal text versions, revisions and uneven machine readability |
| Energy and utilities | IEA/IRENA/European energy data | PSE, URE, GAZ-SYSTEM and relevant reporting | Power vs energy, market interval and licensing scope |
| Climate, air, water and biodiversity | Copernicus/EEA/UN domain products | IMGW, GIOŚ, Wody Polskie, GDOŚ and PIG-PIB | High-frequency live feeds versus validated historical archives |
| Land, buildings, housing and planning | European spatial datasets and OSM | GUGiK, GUNB, local planning and GUS | Raster vs feature access, property rights and historic geometry |
| Transport and mobility | European/global transport sources and OSM | GDDKiA, CEPiK, UTK, ULC, ports and city transport | GTFS schedule vs realtime events; ephemeral history |
| Digital infrastructure and media | ITU and platform-specific services | UKE and permitted Polish market/platform reports | Account scope, query restrictions and commercial use |
| Culture, heritage, tourism and sport | UNESCO/statistical products and community catalogues | GUS, culture/sport bodies, NID, libraries and local institutions | Collection metadata vs copyrighted media and irregular reports |

## Polish institutional and municipal long tail

Use the [Gov.pl ministry and subordinate-body directory](https://www.gov.pl/web/gov/ministerstwa)
as an institutional discovery seed. For each relevant domain, inspect the responsible
ministry, subordinate agencies, independent regulators, public registers, publication/BIP
channels and national open-data catalogue entries. Names and responsibilities can change;
retain predecessor/successor metadata instead of treating a rename as a new observation source.

Track ministries by domain as well as organization: finance/economy, funds/regional policy,
energy, climate/environment, infrastructure, digital affairs, agriculture, education,
science/higher education, health, labour/social policy, culture, sport/tourism, justice,
interior, foreign affairs, defence and state-owned assets as applicable. This is a discovery
checklist, not a claim about today's exact ministerial structure or a verified API per ministry.
Foreign aid, public investment, defence aggregates and state-enterprise reporting merit
dataset-specific follow-up where they are material to a selected analysis.

For local coverage, enumerate units from versioned territorial reference data and record
each unit's official site, BIP, open-data catalogue, geospatial services and transport operator.
Large city portals are examples, not a representative sample of every Polish municipality.
District-level cadastral resources, local budgets, planning documents, municipal companies,
public-service availability and local environmental reports can add unique detail. The
coverage ledger should distinguish inspected publisher, dataset discovered, access understood,
terms understood and dataset admitted.

Do not build an indiscriminate crawler over every government domain. Prefer documented
catalogue/export/service discovery, trace the original producer, and register specific
resource contracts. PDFs and municipal websites can be legitimate sources; they require
versioned document handling and extraction quality checks rather than pretending to be APIs.

## Overlap policies to review explicitly

| Overlap | Preserve | Avoid |
| --- | --- | --- |
| Original agency → national aggregator → international aggregator | Producer and republisher; indicator definitions and vintages | Summing providers or assuming apparent label equality |
| NBP ↔ ECB / vendor exchange rates | Quotation date, currency pair, fixing/type, units, source | Treating different fixings as duplicates or silently using one conversion rule |
| BDL ↔ DBW ↔ Eurostat regional statistics | Dataset codes, territorial versions, population and methodology | Combining municipal and regional totals in one unrestricted sum |
| KRS ↔ REGON ↔ CEIDG ↔ LEI | Identifier namespaces, entity type, validity and evidence | Matching firms only by name/address; treating sole traders and legal entities identically |
| BZP ↔ TED ↔ local buyer publications | Procedure/notice/lot/award IDs and amendments | Counting one procurement at each publication stage |
| UPRP ↔ EPO ↔ WIPO | Publication/application/family identities and coverage | Equating publications, applications, grants and families |
| City transport ↔ national/operator feeds | Feed provenance, route/stop IDs, timestamps | Summing duplicate vehicle observations or confusing schedules with events |
| Health/social administrative data ↔ surveys | Universe, coverage, numerator/denominator, residence/workplace basis | Joining percentages without methodological reconciliation |

These policies are modeling recommendations. Actual equivalence and crosswalks require
source-specific evidence and representative-data tests after the review stage.
