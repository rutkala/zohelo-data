# NBP silver publication

This implements one intermediate delivery: all four NBP silver datasets in a verified immutable snapshot. It is not the complete gold/MetricFlow data-platform release. Verification results belong in the delivery PR and its Actions runs; implementing the protocol alone does not establish historical coverage.

## Build and read

`python src/transformation/silver_builder.py` resolves the configured existing Drive root and bronze zone, inventories all four dataset folders, and records exact input IDs, sizes and downloaded SHA-256 hashes. It uses a unique disposable working directory. Missing or ambiguous inputs fail the build. It neither ingests new NBP observations nor deletes legacy bronze or silver files.

The initial working-set limit is 256 MiB of declared bronze input and 2,048 files, checked before downloading. These are protective implementation limits, not measured capacity promises. Actual transfer size, output size, working-directory size and build/publish duration are reported; peak RAM, completeness and consumer latency still require separate measurements.

dbt builds and tests A/B/C and gold-price staging models. Gold prices expose publication date and PLN per gram at 1000 fineness. Identical numeric observations deduplicate; conflicting values for an analytical key fail until reliable source-revision provenance is available. Retained legacy filenames do not establish which correction is current. Existing FX numeric values remain unchanged; historical unit normalization and richer date/provenance columns belong to the subsequent contract/reconciliation work.

After dbt succeeds, the builder exports physical Parquet files, gathers schema/row counts/date bounds, and retains the exact `manifest.json`, `catalog.json` and build `run_results.json`. `dbt docs generate --no-compile` uses the compiled project. The build results are preserved across documentation generation.

## Publication protocol

1. Validate all four output descriptors and successful dbt build/test results before a candidate remote write.
2. Create `releases/<UUID>` beneath the selected root and upload candidate data and artifacts. Read every file back and compare its bytes/checksum.
3. Write immutable `release.json` with `release_scope=nbp_silver`, the code commit, inputs, outputs, tests and measurements; verify its contents.
4. Recheck the prior pointer, then create or replace the single root `current-release.json` only after the entire candidate is verified. Read the pointer back to confirm the result.
5. Keep previous releases and failed candidates. No dataset deletion or automatic garbage collection is part of this step.

All existing production ingestion, bronze, silver and folder-reconciliation workflows share the `zohelo-production-data` concurrency group and do not cancel a running writer. This serializes those workflows; it is **not** a multi-file Drive transaction, a compare-and-swap guarantee, or a replacement for the planned durable ingestion ledger. GitHub may replace queued runs; dated catch-up must address that later. The publisher detects pointer drift but relies on a single writer. Manual external changes remain outside this protocol.

The silver workflow retains its stage-completion/manual triggers. An already-authorized reviewed main merge can request the first existing-data publication with `[publish-nbp-silver]` in its commit message when its source/model/workflow paths change. Ordinary code pushes do not run that publication job. Existing ingestion scheduling is unchanged by this delivery.

## Consumers and recovery

The portal resolves the pointer and verifies the manifest hash. A catalog selection pins file IDs and verifies downloaded sizes/hashes. Only a genuinely absent pointer permits legacy folder discovery, clearly labeled unversioned. A malformed, ambiguous or inaccessible release fails visibly. Existing queries must not silently mix releases; switching a loaded session to another release requires a fresh query session. Browser download limits are not a guarantee of maximum browser RAM.

Fixture tests exercise actual dbt outputs through this protocol and restore Parquet into a fresh native DuckDB consumer after deleting the builder workspace. Failure tests verify that failed candidate uploads/readback do not change the previous pointer or files. An uncertain pointer-write response is accepted only if an exact readback proves the intended new pointer.

A failed candidate leaves existing release files available. An interrupted/uncertain pointer update needs inspection of the pointer and its verified manifest before retrying. A deliberately damaged pointer is not silently replaced or guessed from folder order. Repair/rollback should select a known retained, fully verified manifest. No automatic deletion of candidates is authorized by this document.

## Remaining release work

The legacy bronze inventory records that raw source-batch links are unverified. Row counts and date bounds are measurements, not a guarantee of no historical gaps. The complete release still requires durable raw/change records, dated catch-up and historical reconciliation, a single orchestration handoff, gold facts/dimensions, executable approved metrics and business source/lineage/metric catalogue views.

References: [Drive uploads](https://developers.google.com/workspace/drive/api/guides/manage-uploads), [dbt run results](https://docs.getdbt.com/reference/artifacts/run-results-json), [delivery plan](deliverables.md), [NBP correction policy](decisions/0001-nbp-corrections.md).
