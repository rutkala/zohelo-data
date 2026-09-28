# ADR 0012: Separate authoritative retention from the Bronze query plane

Status: Architecture decision, 28 September 2026. This ADR established the
Bronze table/query contract. Its open provider/long-term Drive-authority decision
was subsequently resolved by [ADR 0013](0013-cloudflare-first-storage.md), which
selects the Cloudflare-first storage target. Implementation and production
evidence belong in [the delivery record](../deliverables.md).

## Context

The portal must expose each complete Bronze dataset as an ordinary logical SQL
table. For DBW, this includes a default
`SELECT * FROM "02_bronze"."gus_dbw_observations" LIMIT 1000`, complete counts,
multi-indicator filters and joins over the same pinned membership. A physical part
or indicator may be an optional filter, but cannot be a prerequisite.

The retained DBW snapshot is already much larger than the portal's eager-download
allowance. Synthetic Chromium tests proved that the installed DuckDB-WASM can use
authenticated HTTP ranges and push projection and filters into multiple Parquet
files. The live Google Drive test did not reproduce that storage contract:
authenticated media `HEAD` returned 200 but exposed neither `Content-Length` nor
`Accept-Ranges`; the installed client then issued `GET` without `Range`. The
guard correctly blocked both full-file fallbacks. This is a compatibility result
for the installed direct path, not evidence that Drive never supports partial
downloads.

A table format and a query engine solve different problems:

- Parquet is the columnar data-file format.
- Iceberg, Delta Lake and DuckLake add table snapshots, schema and file metadata.
- DuckDB, Spark and Trino execute queries.
- GitHub Actions schedules bounded batch jobs; it is not an interactive SQL
  endpoint.
- Storage must still offer an engine-compatible object/file contract. Wrapping
  the current Drive files in table metadata does not create missing size/range,
  conditional-read, CORS or object-version semantics.

## Decision

### 1. Keep Google Drive authoritative

Google Drive remains the authoritative store for native source bytes, immutable
release evidence, manifests and the existing release pointers. No retained object
is moved, deleted or downgraded. The current serialized Actions publication and
verification rules remain in force.

A future query-optimized Bronze copy is a derived serving projection. Every
published table snapshot must point back to the exact retained Drive inputs and
their accepted file IDs, revisions, sizes and hashes. Losing the serving copy must
not lose the source or prevent deterministic rebuild.

### 2. Select Apache Iceberg v2 as the target open table format

Iceberg is the preferred target for query-serving copies once compatible storage
is authorized. It provides explicit snapshots and schema/partition evolution while
remaining usable by DuckDB, Spark and Trino. DuckDB's official Iceberg extension
supports object-backed tables and REST catalogs, preserving the current DuckDB/dbt
investment without making Spark mandatory.

This is a target, not a claim that an Iceberg table exists today. Do not write
Iceberg metadata onto Drive and call the browser problem solved. The data and
metadata must live behind a verified storage contract, and multi-writer commits
require a compatible catalog.

References:

- [DuckDB lakehouse formats](https://duckdb.org/docs/current/lakehouse_formats)
- [DuckDB Iceberg overview](https://duckdb.org/docs/current/core_extensions/iceberg/overview)
- [DuckDB Iceberg catalogs](https://duckdb.org/docs/current/core_extensions/iceberg/catalogs)
- [Apache Iceberg specification](https://iceberg.apache.org/spec/)

### 3. Keep DuckDB as the default engine

Native DuckDB in GitHub Actions remains the writer, transformation and independent
verification engine unless measured workload evidence requires distributed
compute. DuckDB-WASM remains the preferred owner-portal reader when the serving
storage proves bounded browser range reads, scoped authentication and CORS.

PySpark is not adopted merely because Iceberg and medallion terminology are used.
Spark is an eligible interoperable reader/writer if a later benchmark proves that
one Actions runner cannot meet a bounded build window. This preserves
[ADR 0007](0007-unified-vectorized-execution-standard.md).

The open-source path does not pretend to be a managed serverless SQL service.
Trino can query Iceberg, but operating Trino and its catalog is a server
responsibility. If an always-on multi-user SQL endpoint becomes required,
Microsoft Fabric and Databricks are credible managed alternatives, but both add
an external service, storage and cost boundary that is not authorized here.
Fabric couples Delta tables in OneLake with a provisioned SQL analytics endpoint;
Databricks Unity Catalog manages Delta/Iceberg tables over cloud object storage.

References:

- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [Microsoft Fabric SQL analytics endpoint](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint)
- [Databricks managed tables](https://docs.databricks.com/aws/en/tables/managed)
- [Databricks external tables](https://docs.databricks.com/aws/en/tables/external)

### 4. Require an explicit query-storage decision before migration

The next production implementation requires an owner-authorized existing or new
storage endpoint that supports, at minimum:

- bounded byte-range reads with trustworthy object size;
- stable immutable object identity or versioning plus conditional reads;
- scoped browser authentication and CORS, without leaking credentials across
  origins;
- object listing and write semantics compatible with the chosen Iceberg catalog;
- measured storage, request, egress and retention cost inside the approved limit.

An S3-compatible object store is the default technical shape, not a provider
selection or permission to open an account. Google Cloud Storage and Azure Blob
are also supported by relevant engines. The existing Drive allocation does not
satisfy this gate merely because the Drive API can serve a Range request in some
clients.

This authorization gate applies to production data copies, live deployment and
promotion. It does not block local or synthetic feasibility work. Generated
fixtures, a local range-capable HTTP server and disposable catalog/storage may be
used to validate the Iceberg snapshot and query contract without an external
account, production data or a durable service.

## Alternatives considered

| Option | Result | Reason |
| --- | --- | --- |
| Keep raw Parquet manifests on Drive | Keep for authoritative retention, not complete interactive Bronze | Proven live browser path lacks the metadata contract needed by the installed lazy reader. |
| Iceberg on Drive | Reject | Table metadata does not repair transport, conditional-read or catalog semantics. |
| Delta Lake | Hold | Strong Fabric/Databricks/Spark fit, but weaker alignment with the current DuckDB-first and multi-engine target. |
| DuckLake | Hold | Attractive DuckDB-native lakehouse, but adds a SQL catalog database and has a narrower independent-engine ecosystem. |
| PySpark as default | Reject for now | No benchmark shows a distributed-compute requirement; it would add runtime and operational complexity without fixing storage. |
| Trino as “serverless” | Reject description | Trino is open source and capable, but it is an operated query service, not a zero-operations serverless endpoint. |
| Fabric or Databricks | Decision gate | They combine managed storage/catalog/query services, but require account, service, cost and governance approval. |
| New custom query server | Held | The owner rejected this route; the decision is not revived. |

## Required proof before promotion

A bounded pilot may use one immutable retained DBW snapshot but must not change the
production pointer. Promotion requires all of the following against the same exact
Iceberg snapshot:

1. source-to-serving lineage covers every accepted input file and hash;
2. ordinary `LIMIT 1000` succeeds without a selector and reports request/byte
   totals;
3. exact complete count, multi-indicator filter and representative join succeed;
4. native DuckDB and the portal agree on snapshot ID, schema and results;
5. authentication expiry, missing/changed objects, replay and interrupted
   publication fail closed;
6. cache, working memory, requests and transferred bytes stay within separate
   measured limits;
7. the existing Drive release remains recoverable and unchanged.

Until that proof and storage authorization exist, complete interactive DBW Bronze
SQL remains blocked. Batch transformation and fresh native verification in Actions
may continue; a green batch does not imply a deployed query plane.
