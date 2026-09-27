# ADR 0009: Native-only Landing for every source

Status: Accepted by the owner on 16 September 2026.

## Decision

Ingestion into Landing has one data responsibility: download the source file or
response and store it unchanged, in its native format. This applies to BDL, NBP,
World Bank WDI, Eurostat, OpenData.org and every future source, for both full and
incremental ingestion. It is not a BDL-specific exception.

A ZIP remains a ZIP; CSV, JSON, XML, spreadsheets, Parquet and other provider
formats remain in that format. Preserve the payload bytes, encoding, delimiters,
compression, source fields and original filename where safely available. Do not
inject provenance columns into source data. API responses are saved in their
original representation, not reserialized or replaced with response envelopes.
Normal HTTP transfer decoding is distinct from unpacking a downloaded archive;
record the representation received by the client without changing the file format.

The ingestion path must not unpack/decompress source archives, parse observation
records, normalize values, infer schemas, count rows, deduplicate observations,
validate business content, convert formats, produce Parquet, or run dbt/semantic
models. It must not wait for any of these activities before saving a native object
or progressing to the next download. Installing a data engine is not permission
to use it in ingestion.

## Minimum operational work

Retain only what makes transfer safe and resumable: authentication; discovering
and selecting download URLs or native export controls; pagination and provider
quotas; bounded streaming/download and upload; retries; HTTP/download completion
checks; byte-length/checksum integrity; idempotent object naming; and a small
checkpoint. Source navigation may inspect routing metadata or next-page cursors;
it must not transform dataset contents or make source-value parsing a condition
for native persistence. Save a response before any downstream payload processing.

Checksums verify transport, not the meaning or quality of the data. Failed or
incomplete transfers stay outstanding. A login/error page is not a successful
native export. Uncertain writes still stop safely; do not suppress storage errors.

Keep source identifiers, sanitized request references, original filename, timestamp,
size, checksum and resume state as small technical sidecars/control records,
separate from the payload. Never store credentials or signed access tokens in them.
Progress reports count files/pages, bytes, pending downloads and transfer failures.
They must not present unmeasured record counts as zero or as verified observations.

## Downstream boundary

Unpacking, parsing, schema/content validation and format conversion belong in a
separately executable Landing-to-Bronze processing stage, with its own checkpoints,
status and failure handling. Silver/Gold/semantic work remains further downstream.
A processing failure must neither invalidate a completed transfer nor block later
ingestion. Fixing a parser must allow replay from retained native files without
redownloading the source. A readable portal preview or queryable Parquet must never
be a prerequisite for receiving data.

`landed` means native transfer completed. It does not mean parsed, validated,
modeled, published, or full-source acceptance. Catalogue/download completeness
and subsequent data-coverage validation are separate claims.

## Compatibility and rollout

This decision supersedes conflicting implementation details in ADR 0005 (generated
Landing Parquet response envelopes), ADR 0007 (processing engines in ingestion),
and older source/workflow documentation. Their agile-delivery and complete-coverage
goals remain; vectorized processing standards apply after Landing, not to transfer.

Apply the boundary to existing adapters as well as new ones. Keep existing native
objects, receipts and downstream releases intact while separating the stages; do
not re-download successful transfers merely to change their processing design.
Existing derived artifacts are legacy processing outputs, not native Landing
acceptance. Moving/removing them requires an explicit reference-safe migration.
The owner's earlier BDL reset requirement remains distinct from this repair.

The BDL correction removes local CSV/Parquet conversion from the native publisher
and changes its progress to files/bytes. Browser-export failures and catalogue gaps
still require separate repairs; native-only storage does not establish full history.
Track implementation rollout, remaining adapters, live proof and failures only in
[the delivery record](../deliverables.md), not as a second status list here.

## Acceptance

A byte-for-byte transfer test must pass for an opaque file and an archive containing
unparseable data without calling an unpacker/parser/data engine. Failed upload or
integrity verification must not create a successful checkpoint. Restart must skip
completed native transfers, and downstream parsing failures must not stop the
next download. Verify actual durable objects before claiming production rollout.
