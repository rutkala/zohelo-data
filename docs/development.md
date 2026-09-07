# Development and verification

The assistant handles technical setup and delivery. This page is a runbook for maintainers and coding agents; the owner does not need to operate these commands.

## Selected Drive root

Storage reads `config/storage.yaml`, including the existing `05_archive` zone. For a development Drive area, select a different root using `ZOHELO_DRIVE_ROOT_NAME` or an explicit `ZOHELO_DRIVE_ROOT_ID`; a supplied ID is verified and is never silently replaced with a name lookup. Python callers can supply `root_name`, `root_id` and `config_path` explicitly. Constructor settings take precedence over environment settings, then repository configuration.

Read-only `resolve_root()` / `resolve_zone()` calls do not initialize folders. Production mutators call `authorize_writes()`. Outside GitHub Actions, writing to the production root `zohelo-data` requires `ZOHELO_ALLOW_PRODUCTION_WRITES=true` for an already-authorized operation. An ordinary Codespace opening does not set that opt-in. This is a guard for repository entrypoints, not a security boundary against arbitrary direct API calls. See [silver publication](nbp-silver-publication.md) for the serialized publisher and recovery limits.

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

Python patch releases, the base image, devcontainer features and Actions tags can advance. This is a shared, version-pinned application setup, not a bit-for-bit immutable image. Further image/action pinning and dependency update automation remain audit follow-ups. The inherited portal Dockerfile uses Bun and is not the supported Pages/Codespaces deployment path; do not claim it is covered by the npm checks.

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

This checks dependency consistency, mocked storage authentication and real dbt execution against [synthetic NBP fixtures](../tests/fixtures/nbp/README.md). It covers A/B/C and gold prices, identical-input replay, mixed historical schemas, the existing Table A mart projection, immutable publication and fresh SQL reads, missing-input failure, and matching dbt artifacts. The standalone MetricFlow fixture checks exact synthetic results in fresh native processes with network calls blocked by Linux/libseccomp. No credentials or Drive writes are required.

It does not yet test gold prices, ingestion catch-up, revised observations, immutable publication or executable business metrics. Those remain explicit work in [the delivery plan](deliverables.md). Synthetic tests are not proof of historical coverage or actual NBP correctness.

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

Folder reconciliation is now explicitly manual and named **Reconcile Drive folders**. Ordinary code merges no longer invoke it. The existing ingestion → bronze → silver chain remains operational code under audit: it does not yet carry immutable batch manifests or guarantee one code revision across all stages.

Do not use any production entrypoint as a smoke test. The current storage implementation still targets `zohelo-data`, and the current silver publisher deletes old files before uploading replacements. Fix root enforcement and publication before using a remote development root or claiming safe recovery. The [6 September Actions read-only OAuth check](google-authorization.md) succeeded, but it did not test writes; portal sign-in remains a separate authorization path.
