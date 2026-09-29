# zohelo-data

Data platform for zohelo.com: ingestion, native Landing/Archive, Bronze, Silver,
Gold, semantic models and a SQL portal at https://data.zohelo.com/.

## Operate

Only four Actions remain: **Ingestion**, **Transformations**, **Deploy portal**,
and **Copy Drive files to R2**. Choose the source or operation you need.
See [operating commands](docs/audits/2026-09-07-workflows.md).

Repository test suites and automatic CI/check workflows have been removed.
Data jobs run data commands, not repository regression suites or browser tests.
Existing error handling, byte integrity, checkpoints and writer locks remain.

R2 is the target store, with native Landing and Iceberg medallion tables.
Cleanup does not migrate data or switch existing Drive producers/readers.

[Development](docs/development.md) · [Architecture](docs/architecture.md) ·
[Status](docs/deliverables.md) · [Agent guide](AGENTS.md)
