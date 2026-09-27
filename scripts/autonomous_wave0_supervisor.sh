#!/usr/bin/env bash
# Local monitoring survives terminal detachment, but not a stopped container.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${ZOHELO_SUPERVISOR_PYTHON:-${REPO_ROOT}/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$(command -v python3)"
fi
exec "$PYTHON_BIN" "$REPO_ROOT/scripts/wave0_supervisor.py" "$@"
