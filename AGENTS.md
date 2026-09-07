# AGENTS.md

Shared instructions for coding work in this repository. Run commands from the repository root unless stated otherwise.

**Own the technical handoff.**

- The owner supplies goals and business decisions. Handle routine branches, pull requests, checks and deployment within the authorized task; do not leave those as unexplained homework for the owner.
- Creating a branch or passing tests is not the same as updating `main` or the live portal. State which of these has actually happened, and finish the authorized delivery or name the concrete blocker.
- Keep owner-facing updates short and in plain language. Ask one necessary business question at a time. If an owner action is unavoidable, give one clear next step.
- Do not imply that work continues after a reply unless a specific background task has actually been started.

**Start with the task and repository context.**

- Read [README.md](README.md) and [docs/architecture.md](docs/architecture.md). The architecture document is marked **Proposed**: it describes a target, not a claim that every component exists or a request to implement every milestone.
- Read [docs/deliverables.md](docs/deliverables.md) for the approved work programme and [the foundation audit](docs/audits/2026-09-06-foundation.md) for verified gaps. The new NBP release includes ingestion repair and the business catalogue; the earlier demonstration's ingestion deferral is historical.
- Apply [the NBP revision decision](docs/decisions/0001-nbp-corrections.md): current validated values for normal analysis, retained raw versions and detected-change records. No comparison UI is currently required. Do not call an observed source change an officially announced correction without evidence.
- Follow the assigned task and explicitly accepted architecture decisions. Keep changes scoped, preserve unrelated work, and state assumptions in the pull request. Record a newly agreed architectural choice in an ADR when the task resolves one.
- Use this file as the common guide for GitHub agents and Codespaces work. For a tool that does not load it automatically, include “Read AGENTS.md and docs/architecture.md before editing” in its task prompt.
- Keep Claude/Gemini/Copilot entrypoints as pointers to this guide, not competing instruction sets. Repository instructions guide agents; executable checks and reviewed evidence establish correctness.
- Separate observed facts, technical inferences and unresolved business decisions. Verify uncertain technical behavior against code, fixtures or current primary documentation. Ask the owner only for a consequential business choice; do not invent metric definitions, revision policy, availability promises or a new paid service.
- Use economical delegation for concrete independent tasks. Select a lighter available model explicitly for inventory or straightforward implementation; keep architecture, ambiguous failures and integration with the lead agent. Do not spawn agents merely to fill slots or let them silently inherit an expensive model. Report delegation honestly; actual billed savings may be unavailable.

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
- NBP MetricFlow definitions and complete platform publication remain unfinished; the intermediate immutable silver snapshot is live. Do not describe a dbt parse, portal deployment or ordinary commit as a complete data-platform release. When implementing release publication, validate candidates before switching consumers and preserve the previous complete release.
- Keep browser DuckDB work bounded. Dynamic MetricFlow execution requires a native runtime; a static portal build does not provide it. Keep failures and demo data distinguishable.
- Inspect effective runtime configuration before integration work. `StorageManager` reads `config/storage.yaml`; explicit constructor arguments and `ZOHELO_DRIVE_ROOT_NAME` / `ZOHELO_DRIVE_ROOT_ID` can select another root. Resolve zones through the storage manager. Outside Actions, writes to `zohelo-data` require an explicit production-write opt-in; read-only diagnostics remain available. See the development guide.

**Setup and checks.**

Use an isolated Python 3.12 environment and Node from `.node-version`. These runtime files are shared with CI; the devcontainer Node feature must match. Python's complete resolved dependency set is pinned in `requirements.txt`; direct dependencies are listed in `requirements.in`. See [docs/development.md](docs/development.md) for setup and update instructions. Opening Codespaces starts the local preview only; optional AI tools are installed and started explicitly.

Install only the dependencies needed for the assigned work:

```bash
python -m pip install -r requirements.txt
npm --prefix portal ci --ignore-scripts --no-audit --no-fund
```

Choose checks for the affected component:

| Change | Check |
| --- | --- |
| Python storage/ingestion/orchestration and dbt changes | `bash scripts/check-data.sh`; add focused fixtures or mocks for changed behavior |
| dbt project/model definitions | `dbt parse --profiles-dir .` |
| Local NBP Table A and its gold mart | `dbt build --profiles-dir . --select +mart_exchange_rates_daily`, after preparing the local inputs described below |
| Portal logic or UI | `npm --prefix portal run lint`, `npm --prefix portal test`, and `npm --prefix portal run build` |
| Documentation/instructions only | Check referenced paths, commands, links and the diff; application test suites are not required |

`bash scripts/check-data.sh` exercises all-four NBP dbt fixtures, expected rows, identical replay, conflicting legacy values, docs artifacts and missing-input failures. It also checks mocked authorization/storage boundaries and immutable silver publication/failure recovery, including a fresh Parquet consumer. A standalone synthetic MetricFlow fixture checks the local CLI/runtime independently of NBP definitions. Keep standalone dbt fixtures under `tests/fixtures/`, excluded from root dbt discovery through `.dbtignore`; the production-manifest regression must remain green. Ingestion catch-up, historical revision reconciliation, modeled gold and production NBP metrics remain separate unfinished work. Do not claim full platform coverage from this command.

For ad hoc dbt execution, set `ZOHELO_DATA_ROOT` to local fixtures and `ZOHELO_DUCKDB_PATH` to a disposable database. Models currently expect nested NBP Parquet under `02_bronze/nbp_exchange_rates_table_{a,b,c}/*.parquet`. Parsing alone does not validate data results. MetricFlow fixtures use the selected Python interpreter, disable dbt usage tracking, and run the unmodified CLI behind a Linux/libseccomp network-denial filter. Fail closed if isolation is unavailable; never substitute an updater monkeypatch or silently enable network calls.

The portal also exposes `typecheck`, `format:check`, and `test:e2e` scripts. Use additional checks when relevant, and inspect `portal/playwright.config.ts` and browser prerequisites before running end-to-end tests.

**Keep routine validation local.**

- Use mocks or fixtures for Drive interactions. Direct execution of `src/storage_manager.py` creates Drive folders; ingestion and transformation entrypoints write remote data. The silver builder now publishes a verified immutable `nbp_silver` snapshot and changes only a current-release pointer; it retains legacy files and prior releases. These entrypoints are not smoke tests. See [silver publication](docs/nbp-silver-publication.md).
- For an explicitly requested live authorization check, use `python scripts/check_google_access.py` in the relevant runtime. It performs only authentication and metadata reads, prints fixed status codes/presence booleans and never starts interactive consent. A pass does not prove uploads or another runtime's access. See [Google authorization](docs/google-authorization.md); never retrieve secret values into chat to debug them.
- For an explicitly requested live upload test, use `python scripts/check_google_upload.py --allow-write-test`. It uploads one small marked file under the existing platform root, checks the bytes and cleans up only its own file. Inspect all three verification flags and any cleanup failure; this does not validate the production publisher. The upload workflow is manual or explicitly opted in through a reviewed merge as described in the authorization guide. Routine CI uses its in-memory fixtures only.
- Run remote ingestion, backfills, publication or deployment only when that operation is part of the authorized task. A development Drive root must be demonstrably enforced by the invoked code; a folder convention alone is insufficient.
- Keep credentials and downloaded datasets out of Git and PR output. Never put OAuth refresh tokens, client secrets or service-account private keys in the portal bundle. Keep generated databases, `target/`, `logs/`, `node_modules/` and build output out of changes.
- Report the commands actually run, their outcomes, skipped checks and missing prerequisites. Explain behavior changes and relevant recovery/consumer impacts in the PR. Update this guide when a task changes the documented workflow.

Instruction discovery references: [Codex](https://learn.chatgpt.com/docs/agent-configuration/agents-md), [GitHub Copilot](https://docs.github.com/en/copilot/how-tos/copilot-on-github/customize-copilot/add-custom-instructions/add-repository-instructions).
