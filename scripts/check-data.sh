#!/usr/bin/env bash
# Credential-free validation; never invoke the production ingestion/publisher entrypoints here.
set -euo pipefail

ZOHELO_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ZOHELO_REPO_ROOT}"
if [[ -x .venv/bin/python ]]; then
  ZOHELO_PYTHON=.venv/bin/python
else
  ZOHELO_PYTHON=python
fi

"${ZOHELO_PYTHON}" -m pip check
"${ZOHELO_PYTHON}" -m unittest discover -s tests -p 'test_*.py' -v
