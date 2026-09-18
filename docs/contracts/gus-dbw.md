# GUS Dziedzinowe Bazy Wiedzy (DBW) source contract

Source ID: `gus_dbw`  
Provider: Główny Urząd Statystyczny (GUS / Statistics Poland)  
Product: Dziedzinowe Bazy Wiedzy (DBW / Subject-Matter Knowledge Databases)  
Web Portal: `https://dbw.stat.gov.pl`  
Bulk Catalog: `https://dbw.stat.gov.pl/katalog/bulk`  
High-Value Datasets (HVD): `https://dbw.stat.gov.pl/katalog/hvd`  
Public REST API: `https://api-dbw.stat.gov.pl`  
Official API Documentation: [API DBW Documentation](https://api-dbw.stat.gov.pl/apidocs/), [OpenAPI 3.0 (PL)](https://api-dbw.stat.gov.pl/apidocs/pl/dbw.json), [OpenAPI 3.0 (EN)](https://api-dbw.stat.gov.pl/apidocs/en/dbw.json)

## Scope and reuse

Dziedzinowe Bazy Wiedzy (DBW) is Statistics Poland's specialized thematic repository providing deep statistical domain series across macroeconomics, industry, construction, national accounts, demography, labor market, public finance, agriculture, transport, and environmental indicators. Unlike BDL, which is organized primarily by territorial administrative division (NUTS/TERYT levels), DBW organizes data into structured thematic domains with specialized multi-dimensional cross-sections (`przekroje`), dimensions (`wymiary`), positions (`pozycje`), and information presentation types (`typy informacji`).

Statistics Poland permits reuse of DBW data under [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/). Published products must attribute Statistics Poland (GUS) and identify DBW as the source. In accordance with [ADR 0009](docs/decisions/0009-native-only-landing.md), exact response bytes, original archive packages, and source metadata are landed unchanged in their native formats.

## Architecture and access channels

DBW exposes statistical data through two complementary channels:

### 1. Public REST API (`https://api-dbw.stat.gov.pl`)
An OpenAPI 3.0-governed HTTP service exposing hierarchical taxonomy, dimensional structures, reference dictionaries, and observations:

| Route | Description | Query / Path Parameters | Output Formats |
| --- | --- | --- | --- |
| `/api/area/area-area` | Full thematic area hierarchy (Area -> Group -> Subgroup) | `lang` (`pl`/`en`) | `application/json` |
| `/api/area/area-variable` | List variables within a specific thematic area | `id-obszaru`, `lang` | `application/json` |
| `/api/variable/variable-meta` | Full methodological metadata for a variable | `id-zmiennej`, `lang` | `application/json` |
| `/api/variable/variable-section-periods` | Variables with cross-sections, period types, and available time series | `ile-na-stronie`, `numer-strony`, `lang` | `application/json` |
| `/api/variable/variable-section-position` | Dimensions and positions for a cross-section | `id-przekroj`, `lang` | `application/json` |
| `/api/variable/variable-data-section` | Observations for variable × cross-section × year × period | `id-zmienna`, `id-przekroj`, `id-rok`, `id-okres`, `ile-na-stronie`, `numer-strony`, `lang` | `application/json`, `text/csv` |
| `/api/dictionaries/{dict}` | Dictionaries: `date-dictionary`, `periods-dictionary`, `way-of-presentation`, `no-value-dictionary`, `confidentionality-dictionary`, `flag-dictionary` | `filters`, `sorts`, `page`, `page-size`, `lang` | `application/json` |
| `/api/version` | Service version check | none | `application/json` |

### 2. Web Bulk Catalog (`https://dbw.stat.gov.pl/katalog/bulk` & `/katalog/hvd`)
The bulk catalog UI renders downloadable tables for all thematic variables, providing direct links to:
- Native `.csv` observation tables.
- Native `.xlsx` workbooks.
- Compressed `.zip` packages (`ZIP-CSV`) bundling raw data with dictionaries (`dictionary.csv`).
- Methodological metrics via `/api_app/GetMetrykaCSV?id_zmienne=...`.
- High-Value Datasets (HVD) catalog under Directive (EU) 2019/1024.

## Quotas and rate limiting

GUS enforces rate limits through its API gateway:
- **Anonymous access**:
  - 5 requests / second
  - 100 requests / 15 minutes
  - 1,000 requests / 12 hours
  - 10,000 requests / 7 days
- **Registered access** (via free API key configured in `GUS_DBW_API_KEY` sent via HTTP header `X-ClientId`):
  - 10 requests / second
  - 500 requests / 15 minutes
  - 5,000 requests / 12 hours
  - 50,000 requests / 7 days

Campaign orchestration enforces conservative quota windows locally before requests are dispatched, preventing HTTP 429 penalties.

## Network environment notes

GUS edge firewalls drop TCP SYN packets originating from Microsoft Azure / GitHub Codespaces cloud subnets on port 443 (`194.165.48.0/24`). Real ingestion runs are serialized through GitHub Actions on GitHub-hosted Ubuntu runners where GUS connectivity succeeds, while local development uses recorded fixtures and mocked transport tests.

## Native landing layout (ADR 0009)

In adherence to [ADR 0009](docs/decisions/0009-native-only-landing.md), all downloaded objects are stored in their exact provider bytes without parsing CSV, unzipping archives, validating business content, inferring schemas, or transforming to Parquet in the landing step:

```
01_landing/gus_dbw/
├── native/
│   ├── dictionaries/
│   │   └── {dictionary_name}_{lang}_{page}.json
│   ├── taxonomy/
│   │   ├── areas_{lang}.json
│   │   └── variables_{area_id}_{lang}.json
│   ├── sections/
│   │   ├── section_periods_p{page:06d}_{lang}.json
│   │   └── positions_s{section_id}_{lang}.json
│   ├── observations/
│   │   └── v{variable_id}_s{section_id}_y{year}_p{period}_pg{page:06d}.{ext}
│   └── bulk/
│       └── {indicator_id}/{sha256}/{filename}
└── _control/
    ├── manifests/
    │   └── manifest-{sha256}.json
    └── checkpoints/
        └── {entity_id}.json
```

Technical receipts and manifests are maintained independently in `_control/` and `06_control/source_campaigns/gus_dbw/` to guarantee resumability and byte integrity across batch executions.
