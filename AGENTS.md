# Working on zohelo-data

The owner's current request takes precedence over historical plans.

The production platform is Cloudflare R2 + R2 Data Catalog / Apache Iceberg.
Google Drive and legacy `releases/` / `current/` folders are historical
architecture, not current targets.

## Keep it simple

Use direct Python/SQL and the smallest command needed for the task. Inspect an
existing workflow before choosing it. Do not add frameworks, agent delegation,
automatic follow-up jobs, pilots, reports or workflows without an explicit need.

The owner explicitly requested removal of repository test suites and CI/check
workflows. Do not recreate regression-test workflows, fixtures or mandatory
preflight suites unless the owner asks.

## Preserve the real operation

Keep credentials private, bounded retries/timeouts, resumable checkpoints,
writer locks, original payload bytes and ordinary input/output integrity checks.

Never:

- silently fall back from R2 to Google Drive;
- recreate `releases/` or layer `current/` as an internal visibility gate;
- claim complete source coverage from partial data;
- enable an ingestion schedule without explicit owner approval;
- delete the only recoverable raw source copy;
- expose R2 writer credentials to the browser.

## Storage contract

- `zohelo-landing-prod/01_landing`: native/current raw inputs.
- `zohelo-landing-prod/05_archive`: verified raw history.
- R2 Data Catalog `bronze`, `silver`, `gold`: Apache Iceberg tables.
- `zohelo-lakehouse-prod/06_control`: operational state/checkpoints/receipts.

Use [docs/data-lifecycle.md](docs/data-lifecycle.md) for archival and
Landing→Bronze→Silver→Gold rules.

## Workflows

Keep exactly the three current operational Actions unless the owner asks for a
new one:

1. Ingestion (R2 manual)
2. Transformations (R2)
3. Deploy portal

No scheduled ingestion is currently authorized.

## Deliver

Report what changed, what actually ran, whether it reached main and what remains.
Preserve other contributors' work. Prefer idempotent data operations so a failed
run can resume from durable state instead of starting over.
