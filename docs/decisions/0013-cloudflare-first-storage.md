# ADR 0013: Cloudflare-first storage and lakehouse services

Status: Architecture decision, 28 September 2026. The owner authorized the
Cloudflare-first direction and created the production-named R2 Landing and
lakehouse buckets plus R2 Data Catalog. Migration remains staged; existing Google
Drive data is not deleted or repointed until the replacement path is verified.

## Context

ADR 0012 selected Apache Iceberg v2 on range-capable object storage but left the
provider open. The owner subsequently decided that object storage should become
the platform storage for all medallion layers rather than retaining Google Drive
as a permanent platform dependency. Landing still preserves source-native bytes;
Bronze, Silver and Gold remain table layers and should use Iceberg where the table
contract applies.

The wider Zohelo architecture should remain cost-controlled and portable. The
owner selected a Cloudflare-first direction because the same account can provide
R2 object storage, R2 Data Catalog and, where later justified, serverless query
and web/application services. DuckDB, dbt, Apache Iceberg and GitHub Actions stay
portable and are not replaced by proprietary equivalents merely because
Cloudflare hosts the storage.

## Decision

### 1. Use Cloudflare R2 as the target platform object store

The target physical layout is:

- `zohelo-landing-prod`: source-native Landing objects and their technical
  control/lineage records;
- `zohelo-lakehouse-prod`: Iceberg-backed Bronze, Silver and Gold tables.

Landing is not converted to Iceberg merely because it lives in object storage.
Native archives, JSON, CSV, ZIP and other provider files remain byte-preserved.
Downstream table layers are independently rebuildable from accepted Landing
inputs.

The current buckets use the Data-Catalog-compatible default jurisdiction. A
location hint is not a legal data-residency guarantee. Strict EU-only residency
is therefore not established by this decision and would require a separate
review if it becomes a requirement.

### 2. Use R2 Data Catalog as the initial Iceberg REST catalog

R2 Data Catalog is the selected managed catalog for the first production path.
It exposes the standard Iceberg REST interface and therefore does not change the
selected Apache Iceberg table format. Native DuckDB remains the default
writer/verifier and dbt remains the transformation layer.

R2 Data Catalog is currently a Cloudflare public-beta service. Treat provider
availability and pricing as monitored dependencies, not as reasons to encode
Cloudflare-specific table semantics into models.

### 3. Keep DuckDB/dbt/GitHub Actions as the portable compute plane

- DuckDB remains the batch writer, verifier, ad hoc analytical engine and local
  Data Science engine unless measured workloads justify another engine.
- dbt remains the transformation/model layer.
- GitHub Actions remains the default scheduled/batch execution plane while the
  current public-repository economics remain suitable.
- R2 SQL is an eligible serverless query/API/BI path to test later; it does not
  replace DuckDB by architecture decree.

### 4. Migrate in verified stages

The existing Google Drive platform remains readable and unchanged during
migration. No current release pointer is advanced merely because an R2 copy
exists.

Migration order:

1. prove synthetic R2 object and Iceberg contracts;
2. copy one bounded retained dataset with exact source identity/hash lineage;
3. prove complete SQL, snapshot agreement, replay and failure handling;
4. integrate the portal/query path;
5. migrate remaining current sources/layers with reconciliation;
6. only after accepted readback and rollback evidence, retire Drive as a platform
   dependency.

Google Drive may remain a personal storage service, but it is not part of the
target Zohelo data-platform architecture.

## Live provider proof

The bounded live
[Cloudflare R2/Iceberg run](https://github.com/rutkala/zohelo-data/actions/runs/36479212904)
passed on 28 September using generated data only.

It verified:

- a 4,753,049-byte private Landing Parquet object written through DuckDB/R2;
- authenticated `HEAD 200` and an exact 16,384-byte `GET 206` with
  `Content-Range`;
- an Iceberg v2 table committed through R2 Data Catalog with 160,000 rows across
  eight data files;
- ordinary `LIMIT 1000`, exact complete count, two-indicator filter and join;
- HTTP partial-content reads on preview/filter/join and zero additional known
  response bytes on the repeated cached preview;
- cleanup of the generated Landing object, Iceberg table and namespace.

No Google Drive data was read or migrated in that proof. The sanitized receipt is
[audits/2026-09-28-cloudflare-r2-iceberg-live.json](../audits/2026-09-28-cloudflare-r2-iceberg-live.json).

## Cost boundary

Cloudflare usage remains subject to the owner's explicit cost-control policy.
Budget alerts have been configured. Paid services beyond the currently
authorized R2/Data Catalog footprint require the same evidence-led review used
for the rest of the platform; availability in the Cloudflare dashboard is not an
instruction to enable every service.

## Consequences

- The next data-platform engineering step is a bounded real-data serving-copy
  pilot with exact retained-source lineage, not a bulk migration.
- New storage abstractions should avoid Google-Drive-specific IDs as primary
  table identities; source-provider identity and accepted hashes remain lineage.
- Portal/browser credentials must be separately scoped read-only credentials;
  the CI writer credential must never be shipped to browser code.
- Provider-specific APIs belong at the storage/catalog boundary. Models and
  analytical SQL should remain portable Iceberg/DuckDB/dbt assets.
