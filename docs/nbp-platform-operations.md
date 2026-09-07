# Operate the NBP platform without AI

Use the [delivery record](deliverables.md) for current release identities and verified results. This guide describes supported operations; dated results live under `docs/releases/`.

**Publication approval:** the owner explicitly approved the live audit release, its verification and resuming daily ingestion on 7 September 2026. The temporary H-LIVE hold is cleared and the 02:00 UTC schedule is restored. See the delivery record for actual run results; approval alone is not verification.

## Normal operation in GitHub

Open [Actions](https://github.com/rutkala/zohelo-data/actions). The [workflow inventory](audits/2026-09-07-workflows.md) explains the purpose and permissions of every workflow.

| Need | Workflow / choice | Effect |
| --- | --- | --- |
| Daily ingestion/publication | **NBP data platform**, operation `publish`, mode `incremental` | Scheduled at 02:00 UTC; Fills gaps, rechecks recent dates and rotates through history, validates and publishes. |
| Resume a historical bootstrap | Same, mode `full` | Compatibility name for the same resumable planner. It does not erase checkpoints or force a redownload. |
| Rebuild from retained raw | Same, mode `rebuild` | No NBP request. Build and publish from saved verified responses. |
| Prove raw recovery as well | Enable `verify_raw_replay` | A separate process rebuilds exact released inputs and compares all tables. |
| Restore a previous retained release | Same, operation `promote_retained_release` | Requires target and expected-current release UUIDs; verifies data before switching. See recovery below. |
| Diagnose Google read access | **Check Google access** | Read-only credential/root check. |
| Verify a temporary upload | **Check Google upload** | Explicit temporary upload, content readback and deletion. |
| Create missing configured folders | **Reconcile Drive folders** | Manual metadata mutation; does not publish data. |

Use `main` for production. Production writers share one non-cancelling concurrency group. Do not start a parallel local production writer. A production push runs only when its reviewed merge message includes `[run-nbp-platform]`; normal commits do not silently ingest data.

A green deployment means the portal was deployed. A green data job means its listed validation stages passed. Read the stage that failed before retrying; do not interpret partial progress as a newly published release.

## Local setup and safe checks

Follow [development setup](development.md) and [Google authorization](google-authorization.md). The pinned Python environment and Linux/libseccomp are required for the supported native metric runner; Codespaces provides that path.

From the repository root:

```bash
.venv/bin/python scripts/check-workflows.py
bash scripts/check-data.sh
.venv/bin/python scripts/query_metrics.py --list
```

These checks use fixtures and do not publish production data. For a disposable development Drive root, set `ZOHELO_DRIVE_ROOT_ID` or `ZOHELO_DRIVE_ROOT_NAME` explicitly. Local production writes require an additional opt-in, but normal operation uses serialized Actions. Never place OAuth values in Git, SQL, screenshots or shared shell output.

## Restore current data and query metrics

The following command reads Drive and creates a new local directory. It refuses to overwrite an existing workspace. Choose a new directory when refreshing a release.

```bash
.venv/bin/python scripts/restore_release.py --output-dir .local/nbp-release
```

The output identifies the pinned release and producer SHA. The directory contains `release.duckdb`, `release.json` and the exact release artifacts, including `semantic_manifest.json` for semantic-enabled releases. Ordinary native DuckDB SQL can query the named medallion tables. No browser table-loading step is needed.

Query a source-defined daily metric:

```bash
.venv/bin/python scripts/query_metrics.py \
  --database .local/nbp-release/release.duckdb \
  --semantic-manifest .local/nbp-release/semantic_manifest.json \
  --metric nbp_table_c_bid \
  --start-date 2026-09-01 \
  --end-date 2026-09-04 \
  --output .local/nbp-table-c-bid.csv
```

Use a date range covered by the release. Required daily currency/source or commodity dimensions are supplied automatically. Missing publications yield no fabricated rows. The input database stays read-only and native MetricFlow runs without network access. This interface rejects incompatible definitions/grain; arbitrary raw `mf` queries can bypass the safeguards and are not the governed interface.

The five definitions are in [the source-methodology document](nbp-business-definitions.md) and `models/semantic/nbp_metrics.yml`. They preserve A/B middle rates, C buy/sell rates and NBP gold PLN/gram values. There is no implied time sum, period average, conversion, return, spread or forward filling. A static browser portal does not host the Python MetricFlow engine; local/native queries and CSV output are the supported semantic experience.

## Recovery and explicit promotion

A failed ingestion retains its verified progress. A failed candidate validation leaves the old current release selected. After correcting the cause, rerun the same incremental operation; do not delete raw/state files.

For a bad current release after publication, use **NBP data platform → Run workflow → promote_retained_release**. Enter the retained target UUID and the current UUID you actually observed. The operation:

1. Verifies the expected current release has not changed and its manifest identity/checksum is intact. Damaged current datasets or catalogue artifacts do not prevent recovery; a missing or damaged current manifest requires separate investigation.
2. Locates exactly one retained target, reads/verifies its files and SQL-visible content/provenance.
3. Writes an append-only promotion record and checks pointer drift again.
4. Changes only the current release reference, retaining both releases.

It does not rebuild old data using new model code. It does not make Drive transactional. A pointer mismatch stops; refresh the current identity and investigate instead of forcing the operation. Promotion is implemented/tested with isolated stores; do not assume a destructive live rollback drill occurred unless the delivery evidence says so.

The former migration workflow is retired because its one-time acceptance passed. Its read-only script remains available for a deliberate historical comparison:

```bash
.venv/bin/python scripts/check_nbp_migration.py \
  --baseline-release-id 1ab2f2f0-4325-42fc-bc92-cf3d9e9d9eea \
  --baseline-code-sha 474bbb61a2bb9d88266808e872f8a7613aca23d6
```

## Capacity, archive and limits

Effective ingestion settings are in `config/nbp-platform.yaml`; source metadata is in `config/sources.yaml`. The native build also has explicit observation/byte limits. Limits are safety bounds, not service promises.

```bash
.venv/bin/python scripts/check_platform_health.py --read-drive
.venv/bin/python scripts/check_platform_health.py --local-state .local/nbp-release/ingestion-state.json
```

Health checks are read-only. They distinguish build headroom, unique raw bytes, repeated observation evidence, state JSON size, bounded project inventory and account storage allowance. Incomplete inventory or unavailable quota is reported as unknown, not zero. At 70% of a build/state bound, the report requires a capacity review before expansion; it does not increase caps or delete evidence automatically.

Current retention is **retain all, no automatic deletion**. Immutable raw, attempt/state snapshots and release manifests support audit/replay. Repeated observations are retained because an A→B→A value history is meaningful. Long-term compaction and reference-aware garbage collection need a separately tested migration before limits are reached. Deleting files merely because they are old can break retained release recovery.

Source disappearance is an investigation event, not a deletion instruction. Current-value tables keep the last observed record. A 404 is never a withdrawal. Currency disappearance/reappearance within a returned publication is detected; entire missing publications remain an explicitly limited case. Failure-attempt snapshots may update operational attempt metadata, but do not advance coverage or the successful-observation sequence.

Raw history before the original migration was not retained in all cases and cannot be recreated. Rotating historical rechecks detect changes later; NBP does not provide the correction feed assumed by a CDC database. Use [the revision decision](decisions/0001-nbp-corrections.md) for exact semantics.
