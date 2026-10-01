# zohelo-data

Data platform for zohelo.com: R2-native ingestion, native Landing/Archive,
Apache Iceberg Bronze/Silver/Gold, semantic models and the internal SQL/data
portal at https://data.zohelo.com/.

## Operate

There are three operational GitHub Actions:

- **Ingestion (R2 manual)** — explicitly selected source ingestion into private
  Cloudflare R2. It has no schedule.
- **Transformations (R2)** — medallion/Iceberg promotion and bounded maintenance.
- **Deploy portal** — builds and deploys the internal portal and its private R2
  read service.

Repository regression-test/CI workflows were intentionally removed. Data
operations still retain ordinary integrity checks, exact-byte verification,
bounded retries, resumable checkpoints and writer serialization.

## Storage

```text
zohelo-landing-prod
├── 01_landing/   exact/native current source payloads
└── 05_archive/   verified historical raw payloads

zohelo-lakehouse-prod
├── Apache Iceberg namespace: bronze
├── Apache Iceberg namespace: silver
├── Apache Iceberg namespace: gold
├── 06_control/   ingestion/checkpoint/portal operational metadata
└── R2 Data Catalog metadata
```

The internal platform has no `current/` or `releases/` publication gate.
Validated production data in R2 is internally discoverable. A future
customer-facing publishing product, if needed, belongs above Gold and is a
separate concern.

Google Drive is no longer a production data store. Google sign-in in the portal
is identity/authentication only.

See [Architecture](docs/architecture.md), [Data lifecycle](docs/data-lifecycle.md),
[Development](docs/development.md), [Status](docs/deliverables.md), and
[Agent guide](AGENTS.md).
