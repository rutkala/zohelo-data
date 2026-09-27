# ADR 0011: Landing files and SQL file metadata

Status: Owner direction, 27 September 2026. Implementation and live evidence are
tracked in [the delivery record](../deliverables.md).

## Decision

Landing shows the original files retained in Google Drive. Its SQL representation
is a file inventory: one table per actual source folder immediately under
`01_landing`, and one row per file within that folder, including nested folders.
Discover source folders from Drive rather than from a supported-source list or
published response/archive manifests.

SQL columns describe files: stable Drive identity, filename, path, parent folder,
format, size, available checksum and timestamps. Distinguish Drive creation and
modification times from the time the portal refreshed its metadata. None of these
timestamps establishes the provider's own data-refresh date. Leave unavailable
metadata empty rather than inventing a value.

Folder objects are navigation, not file rows. Shortcuts must not cause traversal
outside the selected source folder; explain their exclusion when applicable.
Duplicate names must remain distinguishable by Drive identity, including source
folders whose display names collide.

## Implementation boundary

Read only Drive metadata for the inventory. Do not download, unpack, parse or
duplicate source payloads to construct a metadata table. The inventory does not
require source-specific ingestion code, a modeled release, generated Parquet or
an additional publication pipeline.

Build browser-session SQL tables on demand for the requested source folders.
Exhaust folder pages and nested folders before exposing a successful table.
An interrupted, stale or bounded-out scan must not appear as a complete source
inventory. Refresh and authentication/session changes must invalidate stale
results. A completed scan describes retained files observed during that scan;
it does not establish full provider-data coverage.

The owner accepted the Landing layout and subsequently reported a slow BDL
metadata query. Optimize the on-demand scan through bounded, paginated Drive
parent searches, visible progress and cancellation, retaining complete-scan
and read-only guarantees. Measure a full retained BDL inventory before making
a BDL performance claim. A shallow navigation check or NBP metadata query does
not establish that result.

Completed uploads do not wait for SQL publication. Open folder listings and
completed SQL inventories remain cached until a user refresh. The owner
subsequently accepted this manual-refresh behavior and prioritized metadata
query performance. Retain the refresh and authentication/session invalidation
rules while optimizing source-scoped requests. A cross-login cache or persistent
inventory maintained during ingestion or through Drive change reconciliation
would be a separate architecture increment; neither is required by the current
scope. Do not weaken descendant-membership or permission checks merely to reuse
a previously built inventory.

## Compatibility

Remove legacy response-envelope and archive-index tables from the Landing
navigation. Preserve their existing direct SQL references and stored artifacts
while introducing distinct file-metadata table names. This is a portal correction,
not authorization to delete data, reset campaigns or change ingestion schedules.
The native-only boundary in ADR 0009 remains in force.

## Acceptance

- Landing remains one usable original-file browser on desktop and mobile.
- Every actual top-level source folder can supply a metadata SQL table,
  independently of source-specific publication support.
- Each table includes nested files with stable identities, accurate paths and
  clearly distinguished timestamps; empty source folders produce empty tables.
- SQL supports filtering and comparing metadata across source folders.
- Metadata queries perform no source-payload reads or Drive writes.
- Failed or superseded scans do not publish partial results, and refresh removes
  stale metadata from subsequent queries.
- Existing direct SQL compatibility and raw-file actions remain usable.
