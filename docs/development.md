# Development and verification

This page is the shared runbook for human maintainers and coding agents. Run commands from the repository root unless a command says otherwise.

## Selected Drive root

Storage reads `config/storage.yaml`, including the existing `05_archive` zone. For a development Drive area, select a different root using `ZOHELO_DRIVE_ROOT_NAME` or an explicit `ZOHELO_DRIVE_ROOT_ID`; a supplied ID is verified and is never silently replaced with a name lookup. Python callers can supply `root_name`, `root_id` and `config_path` explicitly. Constructor settings take precedence over environment settings, then repository configuration.

Read-only `resolve_root()` / `resolve_zone()` calls do not initialize folders. Production mutators call `authorize_writes()`. Outside GitHub Actions, writing to the production root `zohelo-data` requires `ZOHELO_ALLOW_PRODUCTION_WRITES=true` for an already-authorized operation. An ordinary Codespace opening does not set that opt-in. This is a guard for repository entrypoints, not a security boundary against arbitrary direct API calls. See [NBP platform operations](nbp-platform-operations.md) for the current publisher and recovery limits.

## Supported environment

| Component | Configuration |
| --- | --- |
| Python | 3.12, from `.python-version`; isolated `.venv` in Codespaces |
| Node | 24.20.0, from `.node-version` and the matching devcontainer feature |
| Python packages | Exact direct versions in `requirements.in`; full resolved version set in `requirements.txt` |
| Portal packages | `portal/package-lock.json`, installed with `npm ci --ignore-scripts --no-audit --no-fund` |
| Editor | Python, Pylance, dbt and TOML extensions declared in `.devcontainer/devcontainer.json` |
| GitHub CLI | Official devcontainer feature; authentication remains a separate operation |

The Python dependency set starts from the packages recorded in the [successful deployment on 6 September](https://github.com/rutkala/zohelo-data/actions/runs/34023732492). CI validates it with Python 3.12 and the current models. The native MetricFlow CLI uses a separate distribution/version from its engine; see [runtime verification](metricflow-compatibility.md). A synthetic fixture is not a production metrics service. Node 24 is an LTS line; the previous Node 20 line is now EOL ([Node release status](https://nodejs.org/en/about/previous-releases)).

Python patch releases, the base image, devcontainer features and Actions tags can advance. This is a shared, version-pinned application setup, not a bit-for-bit immutable image. Further image/action pinning and dependency update automation remain audit follow-ups. The portal has one supported package-manager path: npm with `portal/package-lock.json`.

## Codespaces lifecycle

1. Creating/rebuilding the container runs `.devcontainer/setup.sh`: verify runtimes, create `.venv`, install the pinned Python and locked portal packages.
2. Starting the container starts only the local Vite preview on forwarded port 5173. Its PID and log are under ignored `.local/`. `--strictPort` avoids silently switching to a different port.
3. Opening the environment does not launch an AI agent or a production data job. Production OAuth variables are not forwarded by this configuration. Codespaces secrets are a distinct mechanism and are not removed by this change. After updating them, a running Codespace may need to be stopped and restarted before its environment sees the new values.

Optional AI CLIs are not required to build or test the project. Before installing one, the responsible agent verifies its current official installation method, version, authentication and cost. Installation and startup are explicit, and a missing optional tool must not break ordinary development. The previous automatic remote installer and unrestricted Antigravity startup have been removed.

The [development-container workflow](../.github/workflows/devcontainer-validation.yml) builds the actual configuration, executes data checks inside it and checks that the portal responds. This validates the container definition on a GitHub runner; it does not prove a particular owner's existing Codespace was rebuilt or authenticated. For the agent's bounded credential check, stop and restart a running Codespace after a secret update, then run `python scripts/check_google_access.py` in that environment. A rebuild is not required just to refresh secrets. The diagnostic is read-only and must not be replaced with an ingestion smoke test. See [devcontainers CI](https://github.com/devcontainers/ci) for the underlying action.

## Local data checks

From an environment with the pinned dependencies installed:

```bash
bash scripts/check-data.sh
```

This checks dependency consistency, mocked storage authentication, ingestion planning and state handling, and real dbt execution against [synthetic NBP fixtures](../tests/fixtures/nbp/README.md). It covers all four NBP sources, full and incremental planning, repeated and conflicting observations, the Bronze/Silver/Gold graph, release publication and recovery, fresh SQL reads, replay checks, missing-input failures, and matching dbt artifacts. The standalone MetricFlow fixture checks exact synthetic results in fresh native processes with network calls blocked by Linux/libseccomp. No credentials or Drive writes are required.

These fixture checks do not prove production history, live Drive contents, current NBP correctness, runtime capacity, or approved business metrics. Live release evidence and remaining work are recorded in [the delivery plan](deliverables.md) and [NBP platform operations](nbp-platform-operations.md).

Outside Codespaces, create a Python 3.12 virtual environment and install the lock before running the command:

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

For portal changes, use the [portal validation workflow](../.github/workflows/portal-validation.yml), which runs lint, build/types, unit tests and the fixture-based browser regression under both supported base paths. Local equivalents are documented in `AGENTS.md`.

## Dependency changes

Update intentional direct versions in `requirements.in`. Resolve them in a new, disposable Python 3.12 environment, check `pip check`, and regenerate the complete `requirements.txt` from that environment's `pip freeze` (excluding packaging tools). Review all transitive changes. Run the data, portal-build and container gates before merging. Do not freeze an unrelated environment or silently upgrade dependencies on every job.

When changing Node, update `.node-version` and the matching feature version together and rebuild the container. Keep `portal/package-lock.json` as the lock for the supported portal workflow.

## Production operations

The supported production entrypoint is the consolidated [`NBP data platform`](../.github/workflows/daily-ingestion.yml) workflow, which invokes `python src/nbp_platform.py --mode ...`. It runs ingestion, dbt build, validation, immutable publication, and fresh-read checks from one code revision. The former direct Bronze and Silver entrypoints are retired and must not be used. Folder reconciliation remains an explicitly manual operation.

Do not use a production entrypoint as a smoke test. Use fixtures for routine checks and the read-only authorization diagnostic when that check is explicitly requested. Production publication requires an explicitly selected Drive root and the write authorization described above. The publisher validates a complete candidate before replacing the current-release pointer and retains the preceding release on failure. See [NBP platform operations](nbp-platform-operations.md) for modes, limits, recovery, and current evidence; portal sign-in remains a separate authorization path.
