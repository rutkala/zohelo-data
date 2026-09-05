# AGENTS.md

Shared instructions for coding work in this repository. Run commands from the repository root unless stated otherwise.

**Start with the task and repository context.**

- Read [README.md](README.md) and [docs/architecture.md](docs/architecture.md). The architecture document is marked **Proposed**: it describes a target, not a claim that every component exists or a request to implement every milestone.
- Follow the assigned task and explicitly accepted architecture decisions. Keep changes scoped, preserve unrelated work, and state assumptions in the pull request. Record a newly agreed architectural choice in an ADR when the task resolves one.
- Use this file as the common guide for GitHub agents and Codespaces work. For a tool that does not load it automatically, include “Read AGENTS.md and docs/architecture.md before editing” in its task prompt.

**Repository map.**

| Path | What belongs here |
| --- | --- |
| `config/sources.yaml` | Source definitions |
| `config/storage.yaml`, `src/storage_manager.py` | Storage configuration, Drive authentication and addressing; some runtime settings are currently hardcoded |
| `src/ingestion/` | Extraction and ingestion code |
| `src/transformation/` | Current bronze/silver orchestration and publication |
| `models/`, `dbt_project.yml`, `profiles.yml` | dbt SQL, model metadata and local DuckDB configuration |
| `portal/` | React/TypeScript/Vite portal with browser DuckDB WASM |
| `tests/` | Python unittest tests; current storage authentication tests mock Google clients |
| `.github/workflows/`, `.devcontainer/` | Batch/deployment workflows and development setup |

**Respect data and component boundaries.**

- Google Drive holds authoritative durable platform data; working DuckDB databases and downloaded files are disposable local compute state. Git holds code and definitions. Keep new transformation logic in dbt when implementing the proposed medallion design, with Python handling extraction, transfer and publication.
- Document dataset grain, keys, units and correction/deduplication behavior when changing models. Use dbt `source()` and `ref()` for real dependencies where supported by the implemented source boundary. Add relevant data tests with model behavior changes.
- MetricFlow and immutable release publication are planned work. Do not describe a dbt parse, portal deployment or ordinary commit as a complete data-platform release. When implementing release publication, validate candidates before switching consumers and preserve the previous complete release.
- Keep browser DuckDB work bounded. Dynamic MetricFlow execution requires a native runtime; a static portal build does not provide it. Keep failures and demo data distinguishable.
- Inspect effective runtime configuration before integration work. Changing `config/storage.yaml` alone does not redirect the current `StorageManager`, which hardcodes the root folder name `zohelo-data`.

**Setup and checks.**

Use an isolated Python environment. The devcontainer and silver workflow use Python 3.12; the portal deployment's dbt docs step currently uses 3.11. Portal deployment uses Node.js 20. Check the relevant workflow and package requirements when changing dependencies; Python dependencies are currently unpinned.

Install only the dependencies needed for the assigned work:

```bash
python -m pip install -r requirements.txt
npm --prefix portal ci
```

Choose checks for the affected component:

| Change | Check |
| --- | --- |
| Python storage/ingestion/orchestration | `python -m unittest discover -s tests -p 'test_*.py'`; add focused fixtures or mocks for changed behavior |
| dbt project/model definitions | `dbt parse --profiles-dir .` |
| Local NBP Table A and its gold mart | `dbt build --profiles-dir . --select +mart_exchange_rates_daily`, after preparing the local inputs described below |
| Portal logic or UI | `npm --prefix portal run lint`, `npm --prefix portal test`, and `npm --prefix portal run build` |
| Documentation/instructions only | Check referenced paths, commands, links and the diff; application test suites are not required |

For local dbt execution, set `ZOHELO_DATA_ROOT` to a fixture directory and `ZOHELO_DUCKDB_PATH` to a disposable local database. The Table A staging model expects matching Parquet under `02_bronze/nbp_exchange_rates_table_a/*.parquet` beneath that root, including its nested `rates` structure. Prepare equivalent inputs when selecting other models. Parsing does not validate data results. The repository does not yet provide a complete fixture-based medallion/MetricFlow test command; do not claim end-to-end coverage from the current auth tests.

The portal also exposes `typecheck`, `format:check`, and `test:e2e` scripts. Use additional checks when relevant, and inspect `portal/playwright.config.ts` and browser prerequisites before running end-to-end tests.

**Keep routine validation local.**

- Use mocks or fixtures for Drive interactions. Direct execution of `src/storage_manager.py` creates Drive folders; the ingestion and transformation entrypoints write remote data. The silver uploader currently deletes matching remote files before replacing them. These entrypoints are not smoke tests.
- Run remote ingestion, backfills, publication or deployment only when that operation is part of the authorized task. A development Drive root must be demonstrably enforced by the invoked code; a folder convention alone is insufficient.
- Keep credentials and downloaded datasets out of Git and PR output. Never put OAuth refresh tokens, client secrets or service-account private keys in the portal bundle. Keep generated databases, `target/`, `logs/`, `node_modules/` and build output out of changes.
- Report the commands actually run, their outcomes, skipped checks and missing prerequisites. Explain behavior changes and relevant recovery/consumer impacts in the PR. Update this guide when a task changes the documented workflow.

Instruction discovery references: [Codex](https://learn.chatgpt.com/docs/agent-configuration/agents-md), [GitHub Copilot](https://docs.github.com/en/copilot/how-tos/copilot-on-github/customize-copilot/add-custom-instructions/add-repository-instructions).
