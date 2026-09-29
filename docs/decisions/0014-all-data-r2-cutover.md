# ADR 0014: Entire project migration and an R2-only comparison portal

Status: owner-authorized direction, 29 September 2026; implementation and live
acceptance are tracked only in [the delivery record](../deliverables.md).

## Scope

Migrate every retained file beneath the existing `zohelo-data` Drive root
`1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf`, not the owner's personal Drive. Include
Landing, Bronze, Silver, Gold, Archive, Control, release history, metadata and
other descendants. Trash/purged provider revision history is not part of the
currently retained project tree; do not delete the original Drive reference.

The existing portal stays as a temporary UI for comparison with Cloudflare's
catalog/SQL Studio. After accepted cutover it must use R2 only; absent R2 objects
must fail visibly, not fall back to Drive. This does not authorize a public bucket,
a browser writer credential, new paid service, BI implementation or source expansion.

## Copy and semantic publication are separate

Use byte-preserving streaming copies. Do not parse, unpack or rewrite native
Landing bytes. Copy existing Parquet unchanged. R2 object copy and Iceberg table
registration have separate acceptance: a bucket full of files is not proof that
all tables are correctly registered, and a successful registration is not proof
of complete source coverage or portal acceptance.

Published tables are bound from current, hash-verified release manifests. Do not
combine retained historical/rejected/partial candidates merely because they have
the same schema or a `.parquet` extension. Preserve original manifests unchanged;
legacy file IDs are lineage keys resolved through a private R2 migration index.
Internal Drive shortcuts are represented as indexed aliases to migrated targets;
external or unresolved targets are explicit blockers, not silently discarded files.

## Storage and recovery

`01_landing/...` maps to `zohelo-landing-prod`; all other logical root paths map
to `zohelo-lakehouse-prod`. Preserve path metadata and deterministic escaping for
otherwise ambiguous object names. Never overwrite a conflicting copied object;
retain subsequent versions separately and bind them in the new snapshot index.
Reuse the already-verified DBW pilot bytes where exact identity/checksums match.

Read source bytes with bounded buffers, verify the pinned checksums, supply
Content-MD5 for R2 uploads/parts, and verify completed object metadata/ETags.
Multipart ETags are not source MD5 values. Incomplete transfer never publishes a
complete candidate. Inventory every page and reconcile source drift before a
complete candidate becomes eligible for downstream acceptance.

Use the existing production concurrency group to avoid copying across active
scheduled writes. Preserve the old code at `reference/drive-platform-20260929`
and keep Drive unchanged for recovery. A final source-writer cutover and fresh
reconciliation are required before R2 becomes the only active production store.

## Portal boundary

The portal needs a separately authenticated read-only storage/catalog interface.
CI writer credentials remain in Actions. The index must preserve release/hash,
file metadata, folder and alias semantics without sending any runtime reads to
Google Drive. Browser acceptance must prove all layers, ordinary complete-table
SQL, authorization failure, bounded Range reads and no Google Drive fallback.
A missing deployment credential is a concrete owner setup step, not permission
to make storage public or ship writer keys.

## References

- [R2 S3 compatibility](https://developers.cloudflare.com/r2/api/s3/api/)
- [R2 upload and multipart limits/ETags](https://developers.cloudflare.com/r2/objects/upload-objects/)
- [R2 consistency](https://developers.cloudflare.com/r2/reference/consistency/)
- [Drive search and field selection](https://developers.google.com/workspace/drive/api/guides/search-files)
