# One data catalogue and a separate project record

Status: Accepted from the owner's portal feedback, 7 September 2026.

The owner rejected duplicate catalogues and the mixture of data discovery, project questions, status tracking and SQL tutorials inside a “Business catalogue”. The owner also clarified that SQL must work through normal editing and Run, including changing table references, joins, unions and CTEs. A focused update to the portal's palette and layout is authorized.

## Decisions

- The portal serves Zohelo-data: a SQL workspace and one **Data catalogue**. Use the installed, pinned open-source dbt Docs viewer. Do not maintain a second source/lineage/metric UI or fork dbt's viewer for project features.
- The catalogue reads the exact `manifest.json` and `catalog.json` artifacts referenced by the connected immutable data release, verifying their declared sizes and SHA-256 hashes within a separate 16 MiB documentation limit. A portal build ships a generic viewer template, never documentation generated from a different code checkout's data state.
- Add released ingestion status to a display-only copy of dbt's existing model metadata and overview. Preserve the source artifacts and actual dependency graph. Existing `business-catalog.json` remains a release metadata input for compatibility, not a second catalogue. Do not invent source nodes, semantic lineage or metric definitions. Different dbt commands may have different invocation IDs; the release binds the independently verified artifacts.
- Browser query preparation uses DuckDB's SQL parser to discover referenced published relations and loads their verified files before the ordinary execution. Download and release-consistency limits still apply. The browser database is temporary working data; Google Drive remains authoritative durable storage.
- `docs/deliverables.md` is the single project-status and open-question record. The owner can respond in chat and the implementing assistant records decisions there. Other documentation supplies supporting research and evidence. The portal contains no project-management forms. Older saved browser responses are retained through a read-only recovery route.
- Use a restrained, readable data-first visual style. Larger product features require their own clear purpose and acceptance criteria.

## Consequences

SQL, catalogue metadata and lineage remain tied to the same release. Publishing the portal does not publish a new data release or complete the semantic layer. Native MetricFlow execution remains separate work. Generated viewer code is disposable build output. Native dbt Docs remains recognizable and upgradeable with the pinned dbt dependency.

Browser checks must exercise the real dbt viewer, valid release fixtures, failure handling, ordinary SQL editing and the deployment base paths. Passing a fake embedded HTML page is insufficient evidence for catalogue functionality.
