# zohelo-data

Data platform for zohelo.com: ingestion, native Landing/Archive, Bronze, Silver,
Gold, semantic models and a SQL portal.

[Open the portal](https://data.zohelo.com/) · [Portal guide](docs/using-the-portal.md)

## Operate the platform

The [workflow inventory](docs/audits/2026-09-07-workflows.md) lists the current
Actions and their actual commands. Start there, not with a historical pilot.
Production Actions run existing Python/dbt code; they are not an agent framework.
A copy operation copies files. It does not run the platform regression suite,
convert files to Iceberg or deploy the portal.

R2 is the target durable store, with native Landing and Iceberg medallion tables.
Existing Drive data and release pointers must remain intact until migration and
cutover are verified. Repository cleanup does not establish that migration is done.

## Develop

Follow [AGENTS.md](AGENTS.md) and the current task. Prefer one direct command and
only the checks relevant to what changed. PR automation checks Python syntax and
workflow safety; a single Python test file can be selected manually. Full data or
portal regression suites are explicit diagnostics, not routine execution steps.

[Development](docs/development.md) · [Architecture](docs/architecture.md) ·
[Current status](docs/deliverables.md) · [Google authorization](docs/google-authorization.md)

Existing source adapters, dbt models, semantic definitions, storage and portal
code remain in place. The operating surface is being simplified first; the
source runners still contain legacy orchestration that needs focused cleanup.
