# Zohelo R2 data lifecycle

This is the production lifecycle for the internal Zohelo data platform.

The internal portal has no publication/release gate: data that exists in the
production R2 platform is discoverable internally. A future customer-facing
product may add a separate release/publishing boundary without changing this
internal lifecycle.

## Storage boundaries

### `zohelo-landing-prod`

- `01_landing/` — current immutable provider payloads and ingestion evidence.
- `05_archive/` — verified historical provider payloads retained for replay,
  audit, and recovery.

### `zohelo-lakehouse-prod`

- R2 Data Catalog namespace `bronze` — source-shaped Apache Iceberg tables.
- R2 Data Catalog namespace `silver` — typed, cleaned, deduplicated and
  conformed Apache Iceberg tables.
- R2 Data Catalog namespace `gold` — facts, dimensions and marts ready for
  analytical/semantic use.
- `06_control/` — ingestion state, receipts, lifecycle checkpoints and other
  small operational metadata.
- Cloudflare-managed Iceberg catalog metadata.

The obsolete `current/` and `releases/` publication layouts are not part of
the production architecture.

## Flow

```text
provider
   |
   v
01_landing (exact raw/native payload)
   |
   | source-specific parsing + validation
   v
bronze (Iceberg)
   |
   +---- after Bronze commit + validation ----> 05_archive
   |                                             verified raw history
   v
silver (Iceberg)
   |
   v
gold (Iceberg)
```

## 1. Ingestion -> Landing

Ingestion writes the exact provider response or native distribution to
`01_landing/<source>/...`.

Rules:

1. Raw objects are immutable and content-addressed where practical.
2. A source response is accepted only after its size/hash and source-specific
   validation succeed.
3. Durable campaign state and receipts are written under
   `06_control/source_campaigns/<source>/`.
4. Landing is a staging/current-raw boundary, not a table-format analytical
   layer.
5. Ingestion does not write Silver or Gold directly.
6. No automatic ingestion schedule is enabled until explicitly approved.

## 2. Landing -> Bronze Iceberg

A source-specific transformer reads verified Landing inputs and commits one or
more Bronze Iceberg tables.

Bronze preserves the provider's meaning and granularity while normalizing the
payload into table form. It may add technical provenance columns but should not
apply business-level reshaping.

A Landing generation is eligible for archival only when all of the following
are true:

- the intended Bronze Iceberg commit completed;
- the expected Landing inputs are represented by that Bronze checkpoint;
- schema/file/row acceptance checks passed;
- a representative catalog query succeeds.

A failed Bronze build never removes or archives away the only usable Landing
copy.

## 3. Landing -> Archive

Archive contains the original provider payload, not copies of Bronze/Silver/Gold.

Archive is created only after the Bronze gate above passes.

### Compression policy

Do not recompress formats that are already compressed or efficient binary
containers, including ZIP/GZIP/XZ/BZIP2/Zstandard archives and Parquet.

For compressible text/native payloads such as JSON, NDJSON, CSV, TSV and XML,
archive with Zstandard where the runtime supports it. The archive receipt keeps:

- original SHA-256;
- original byte count;
- archived byte count;
- compression method;
- source Landing key;
- Bronze checkpoint/snapshot that admitted the payload;
- archive key and timestamp.

An archive operation is accepted only if restoring/decompressing the archived
object reproduces the original SHA-256 exactly.

### Removal from Landing

Archiving and deleting are separate gates.

A Landing object may be removed only after:

1. Bronze acceptance passed;
2. archive readback passed;
3. no live ingestion-state pointer still requires the Landing object for
   recovery/replay, or that pointer has been safely advanced to a generation
   that no longer references it.

Until pointer rotation is implemented for a source, keep the verified Landing
object rather than risk breaking recovery. Storage optimization never outranks
recoverability.

## 4. Bronze -> Silver

Silver is source data made reliable for reuse:

- stable data types;
- normalized identifiers/dates;
- duplicate handling;
- explicit null/error treatment;
- conformed source terminology where appropriate;
- source-specific quality rules;
- provenance retained.

Silver is Apache Iceberg. Incremental updates should commit new snapshots or
perform controlled merge/upsert logic rather than create release folders.

## 5. Silver -> Gold

Gold contains analytical products:

- facts;
- dimensions;
- marts/coverage tables;
- semantic-ready relationships.

Gold is also Apache Iceberg. Internal availability follows successful Gold
commit/validation directly; there is no separate `release` or `current`
folder.

## 6. Iceberg maintenance

For tables with many small data files, use Iceberg/Data Catalog compaction after
the logical data is validated. Compaction must not change row membership.

Snapshot expiration is a maintenance operation, not ingestion. Initially keep a
conservative history and enable expiration only after the R2-only pipeline has
operated reliably for multiple cycles.

The platform should prefer:

- fewer reasonably large Parquet data files;
- Zstandard compression for rewritten Parquet where supported;
- zero-copy Iceberg references only when two layers are intentionally identical
  pass-throughs;
- physical materialization when Silver/Gold actually changes rows or columns.

## 7. Partial current coverage

The current platform may contain partial provider coverage. That is acceptable,
but the catalog must describe it as the data currently available rather than
claiming complete provider history.

Every available, validated dataset should still move as far through
Bronze -> Silver -> Gold as its current transformation contract supports.

## 8. Failure and deletion rules

Destructive actions are downstream-gated:

- never delete Landing because ingestion succeeded;
- never delete Landing because Bronze merely started;
- never delete a source Parquet because it was registered in Iceberg unless the
  catalog has first been moved to a validated replacement;
- never delete an archive that is the only recoverable raw copy;
- never enable ingestion schedules as a side effect of migration/testing.

## 9. Future external publishing

If `data.zohelo.com` later becomes a customer-facing product, add an explicit
external publication layer on top of Gold. That layer can define customer
datasets, versions, contracts, access controls and release dates.

It must remain separate from the internal R2/Iceberg lifecycle described here.
