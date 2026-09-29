# Development and verification

Run commands from the repository root. Follow [AGENTS.md](../AGENTS.md) and the
current task. Use the Python and Node versions in `.python-version` and
`.node-version`; keep the existing dependency locks.

## Setup

```sh
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
npm --prefix portal ci --ignore-scripts --no-audit --no-fund
```

Install only the packages required by a storage-only script when it does not need
the complete platform environment. Constrain their versions with requirements.txt.
The devcontainer remains available; its setup installs dependencies and its start
hook runs the local Vite preview, not a production job or an AI agent.

## Check the change, not the entire repository by default

Routine PR automation checks Python syntax and the existing workflow-safety
checker. It does not install the complete platform environment or run regression
suites. A manually selected test file is available in the Code checks Action.
Locally, choose one relevant existing test file, for example:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_migration_plan_provenance.py'
```

For a portal build, run `npm --prefix portal run build`. Select relevant test or
lint commands from `portal/package.json` when changing that behavior. Broad data
checks remain `bash scripts/check-data.sh`, but are an explicitly chosen diagnostic,
not the starting command for every task. There are no separate portal or
devcontainer validation Actions after the workflow cleanup.

Dependency updates should deliberately update the direct requirements and lock
in a clean environment; check the affected components and dependency consistency.
Do not upgrade packages as a side effect of running an ordinary operation.

## Production and storage

Use the [current workflow inventory](audits/2026-09-07-workflows.md). NBP publication
still performs its own candidate/data validation; its workflow no longer appends
full restore/replay/health checks. Do not run a production writer as a smoke test.

Storage reads config/storage.yaml. Select a development Drive root through
ZOHELO_DRIVE_ROOT_NAME or an explicit ZOHELO_DRIVE_ROOT_ID. Read-only resolve calls
do not create folders. Production mutators require write authorization; outside
Actions an already-authorized operation requires ZOHELO_ALLOW_PRODUCTION_WRITES=true.
This opt-in is not authorization to run overlapping writers. Keep credentials out
of source control, logs and the browser. Google authorization remains separate
from signing into an AI coding tool.

R2 is the target storage platform, but cleanup does not switch producers, readers
or release pointers. Preserve native payloads, checkpoints and accepted data until
an explicit verified migration/cutover. See [architecture](architecture.md) and
[NBP operations](nbp-platform-operations.md) for task-specific details; the current
workflow inventory takes precedence over historical descriptions of Actions.
