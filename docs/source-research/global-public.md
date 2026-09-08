# Global and multicountry public-source landscape

Research date: 2026-09-08. This is documentation research only. No endpoint was called for data, no account was created, and no connection was tested. The forty inventory rows are source products or coherent dataset families, rather than organizations or individual time series. “History load” and “incremental load” are design proposals.

## Portfolio shape and recommended first review

The best first-review group is Eurostat, OECD Data Explorer, World Development Indicators, IMF WEO, BIS Data Portal, ECB Data Portal, FAOSTAT, ILOSTAT, both WHO surfaces, UIS, UNICEF’s Data Warehouse, UNDP Human Development Data, UNHCR Refugee Statistics, Copernicus CDS, EEA Datahub, the UN SDG database and World Population Prospects. Together they cover macroeconomics, labour, population, education, health, agriculture, finance, displacement and environment. They also illustrate why “public” is not one access or licence class.

Eurostat, OECD, BIS, IMF, UNICEF and several other providers use SDMX. That common transport helps discovery, but does not create a common data model. The safe natural key remains agency, dataflow, dataflow version and every dimension in the data structure. A country-year-indicator shortcut would collapse seasonal adjustment, units, counterpart areas, prices, population categories or source statuses. Codelists must be versioned with observations.

The World Bank portfolio should not be treated as one feed. WDI is a broad downstream compilation; WGI, Global Findex, Enterprise Surveys, IDS, GEM and ICP have distinct methods, entities, releases and reuse records. WDI’s official page documents bulk CSV/Excel with metadata, an indicators API, and revision of bulk files whenever WDI updates. The generic World Bank legal page now expressly incorporates dataset-specific terms and says API terms follow the relevant catalog record. Therefore each selected product needs its current Data Catalog licence captured before onboarding.

Many priority sources publish estimates or aggregates that should coexist with national observations. WEO is a forecast vintage; WPP is a demographic-estimation revision; ILOSTAT can distinguish modeled and nationally reported labour observations; WHO may publish modeled estimates and standardized rates; BIS harmonizes central-bank submissions. Zohelo-data should preserve these as separately sourced facts rather than choose a value based on a similar English label.

## Strong dossier: Eurostat and Comext

Eurostat offers free REST access through Statistics, SDMX 2.1, SDMX 3.0 and Catalogue APIs, with JSON-stat, SDMX-CSV/XML and TSV formats. Its documentation says datasets are checked for newer data or structural changes twice daily, but the database contains only the latest version and does not document past dataset versions. That makes internal raw-vintage retention mandatory if reproducibility matters. The source API page also routes DS-prefixed Comext and Prodcom datasets to separate endpoints, which supports keeping Comext as a distinct product dossier.

Eurostat’s own [reuse notice](https://ec.europa.eu/eurostat/web/main/help/copyright-notice) authorizes commercial statistical reuse with attribution, subject to specific exceptions. Those include third-party data, countries outside the EU/EFTA/official acceding and candidate groups, specified Swiss/Liechtenstein-declared trade since 1995, and Austrian CN8 trade. Modifications/translations require disclosure and the stated disclaimer. Editorial CC BY 4.0 wording is not a blanket licence for every statistical cell; the selected dataset, country and commodity scope must be checked.

Comext needs a bilateral-trade fact keyed by reporter, partner, flow, classification edition, commodity, period and measure. It must retain confidentiality/status flags and units. Summing both mirrored directions, multiple commodity hierarchy levels or EU aggregates with members will overstate trade. Differences from UN Comtrade, WTO and UNCTAD are expected because partner attribution, valuation, reporting dates, confidentiality and revisions differ.

## Strong dossier: OECD Data Explorer and PISA

OECD’s current Data Explorer API is SDMX-based and free. The documentation explicitly warns that a new dataflow version can contain non-backward-compatible changes such as renamed, added or removed dimensions. The API is rate-limited, but the opened page supplies no numeric threshold. The terms allow extracting, adapting, distributing and commercially using OECD-owned data with attribution, while requiring users to inspect metadata for third-party ownership or extra restrictions. This supports a dataset-level rights gate and a structural-diff gate before every refresh.

Geographic coverage must not be inferred from OECD membership. Some dataflows cover only members, while others include accession countries, partners, G20 participants or broader economies. A coverage matrix belongs at dataflow and indicator level. OECD aggregates are separate reference areas and should not be summed with constituent countries.

PISA deserves a separate specialist pipeline. The official page links cycle files from 2000 through 2022, including student, principal and parent responses, codebooks and technical material. Analysis requires survey and replicate weights plus plausible-value procedures. Student IDs identify anonymous records inside a survey design; they are not longitudinal person identifiers. Country-level PISA indicators overlap education portals only superficially because they measure assessment constructs and sampled populations.

## Strong dossier: IMF WEO and the new IMF Data surface

The 2026 WEO page documents twice-yearly publication, history from 1980, projections for mostly five years, downloadable Excel, vintage appendices and a historical forecast archive. WEO must therefore be modeled by release vintage. Overwriting October with April destroys the forecast-revision question the product is best suited to answer. Group definitions, actual/estimate/forecast status and subject units must remain dimensions or attributes.

IMF’s API page says data are available through SDMX 2.1 and 3.0, while the Swagger explorer requires a beta portal account. This is a deployment blocker until production authentication and stability are clarified. The October 2024 copyright policy is more permissive for statistical data than for general IMF content: it permits download, derivatives and distribution with attribution and transformation disclosure, yet asks potential commercial reusers to request permission. The same policy prohibits automated bulk download of general site content without permission. Zohelo-data should obtain a written position for commercial reuse and restrict automation to documented statistical interfaces.

IFS and DOTS remain valuable but their current dataflow coverage, catalog metadata and account behavior were not exposed well enough in the opened pages. They remain catalogue-only candidates until the new portal’s dataflow records are inventoried.

## Strong dossier: BIS and ECB

BIS is unusually strong technically: the Data Portal documents an SDMX REST API and full-topic bulk downloads in CSV and SDMX, each with visible release dates. Topics include locational and consolidated banking, debt securities, credit, derivatives, property prices, exchange rates, policy rates and payments. Residence-based and consolidated banking statistics describe different universes. Currency, sector and counterparty dimensions cannot be discarded.

BIS terms allow unrestricted use with attribution, but add a material condition for commercial products: including BIS statistics must not result in an additional charge to subscribers or users. This needs product/legal review because it may constrain a paid data product even if the data are accessible without authentication. BIS also notes that it stopped receiving data from Russian public authorities after 28 February 2022, a lineage break that should be surfaced.

The ECB’s general copyright page allows free reuse with accurate reproduction and citation, requires transformations such as seasonal adjustment to be disclosed, and requires sellers to tell customers the information is available free from the ECB. Its API help page was unavailable during research, so exact interface and quota claims remain unresolved. ECB, BIS and national-central-bank series should coexist with source and methodological basis intact.

## Strong dossier: health, education and child statistics

WHO currently presents two relevant products. The legacy GHO page documents an OData API with indicator and dimension discovery, filters, regions and time-begin/time-end fields. Time filters make efficient refresh windows, but do not prove change-data capture; old observations can be revised. The newer data.who.int terms say datasets are CC BY 4.0 unless otherwise stated, require metadata-driven citation, prohibit de-anonymization and misrepresentation, exclude third-party credited material, and allow WHO to modify or discontinue access. Before ingestion, IDs and definitions need a GHO-to-data.who.int crosswalk so parallel publication does not create duplicates.

UNICEF’s SDMX documentation is particularly useful. It defines dataflows, DSDs, codelists, attributes and full/partial downloads. It also warns that its CSV format may contain labels without codes and that labels are not guaranteed unique. JSON plus structural metadata is the safer exchange format. The page says downloaded datasets are CC BY 3.0 IGO, while other site content defaults to a non-commercial licence, so the licence attached to each dataset must travel with it.

UIS documents downloadable CSV, Excel and JSON plus metadata across education, demography, science/technology and culture, and shows domain release labels. Its broad UNESCO open-access page does not by itself settle each dataset’s reuse rights. ILOSTAT has the same outstanding need: authoritative domain value, but current bulk/API documentation and product-specific commercial terms require a focused follow-up. Both are review-first because they are likely upstream for SDG and World Bank indicators.

## Strong dossier: UNDP and UNHCR

UNDP’s Human Development Data Center covers globally comparable country and regional indices. Its terms explicitly permit sharing and commercial adaptation under CC BY 3.0 IGO, but also state that the entire HDI series and rankings are recalculated annually with current methods and input data. Historical HDI values from different report vintages are not directly comparable. The warehouse should keep annual release vintage as part of the key and retain source lineage for health, education and income components.

UNHCR’s Refugee Statistics API covers nearly seventy years and separates population, asylum applications, decisions, demographics and solutions. Its documentation supplies REST/JSON, CSV download behavior, year filters and UNHCR-to-ISO code options. A default page size of 100 is explicit, although no rate ceiling or maximum page size was found. Omitting country filters can produce aggregated rows, so loaders must make aggregation intent explicit. Stocks of refugees at year-end, asylum applications during a year and solutions are different facts.

## Strong dossier: climate and environment

Copernicus CDS offers global reanalysis, observations and projections at far finer grain than country statistics. API access needs registration, a personal token and manual acceptance of each dataset’s terms. The API documentation recommends a current client and labels the newer advanced client as incubating; the REST interface is described as advanced and unsupported. Dataset licences, correction notices and extraction manifests therefore belong in operational metadata.

Gridded climate fields should remain keyed by dataset, variable, level, grid, valid/reference time and ensemble member. Country aggregates are derived products requiring a versioned boundary, area/population weights and reproducible method. They must not be labeled as source-published national observations.

EEA Datahub is a strong European complement across 38 member/cooperating countries. Its legal notice makes EEA-owned content CC BY and reusable commercially with attribution, while preserving item-specific and third-party exceptions. EEA data spans sites, facilities, geospatial features and country indicators; separate fact models prevent invalid joins or sums. UNECE adds valuable Central/Eastern Europe, CIS, gender, forestry, transport and city data, but its current restructuring and discontinued-series notices make source status a required dimension.

## Trade, agriculture, innovation and crime

FAOSTAT is a high-priority specialist source because its domain codes, elements, flags and food-balance logic are richer than WDI. FAO’s website terms explicitly say statistical databases use a separate Open Data Licensing Policy and Statistical Databases Terms of Use. Those exact terms should be captured before onboarding. Area, item, element, partner and year codes form the core key; totals, indices, balances and bilateral flows require different aggregation rules.

UN Comtrade, WTO Stats and UNCTAD Data Hub remain valuable but less ready. Their public portals did not expose enough opened primary documentation to state current quotas, stable API contracts or commercial redistribution rights. UN Comtrade also mixes public and subscription-key access. These products should stay in review until terms, plan limits and classification metadata are captured. They should never be deduplicated solely because reporter, partner and value appear similar.

WIPO’s IP Statistics Data Center is stronger: its official page documents free latest and historical downloads, pre-1980 files and annual worldwide IP activity. WIPO generally uses CC BY 4.0 for new online content but preserves exceptions and third-party rights. Patent applications, grants, classes and designs have distinct counting units; applicant origin and filing office are separate geography concepts.

UNODC covers homicide, violent/sexual crime, drugs, prisons, justice, firearms and trafficking. Cross-country gaps and definition breaks are central, not data-cleaning noise. Police-recorded offences, victimization prevalence and modeled homicide estimates need distinct measure types. The product-specific licence and stable machine download remain unresolved.

## Coverage gaps and disposition

The portfolio is strongest at country-year indicators and international statistical classifications. It is weaker for fine subnational data outside Europe, real-time observations, business establishments, household microdata and globally consistent municipality identifiers. Enterprise Surveys and PISA provide microdata but require specialist survey modeling and registration/terms review. Facility/site data appear in EEA, while global equivalents remain fragmented.

Energy has a licensing gap. IEA has excellent balances and emissions but mixes public and subscriber products under restrictive, product-specific terms; it is conditional. IRENASTAT is promising for renewable capacity and generation but needs an opened licence and stable machine interface. ITU likewise needs clarification of free versus paid access and statistical reuse. UNIDO INDSTAT is conditional because product entitlements and commercial rights vary.

UNdata and IOM’s Migration Data Portal are best treated mainly as discovery catalogs. Both point to upstream producers and risk creating stale duplicates. The Global SDG database is operationally valuable as a harmonized reporting layer, but its observations overlap custodian agencies; country-reported, adjusted and global-estimate series must be distinguished. UN Tourism was considered but not promoted into the forty-row set because accessible official documentation did not establish a sufficiently clear reusable machine product.

Across all sources, a viable intake gate should require: exact product and dataset ID; opened access documentation; exact licence or terms record; complete dimensional key; codelist/version capture; source and methodology attributes; published revision behavior where available; and an explicit overlap decision. Unknown quotas, unclear commercial rights and absent archives are blockers, not reasons to infer permissive behavior.
