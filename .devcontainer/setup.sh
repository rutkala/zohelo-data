#!/usr/bin/env bash
set -euo pipefail

ZOHELO_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ZOHELO_REPO_ROOT}"

python - <<'PY'
import sys
from pathlib import Path

expected = Path('.python-version').read_text().strip()
actual = '.'.join(map(str, sys.version_info[:2]))
if actual != expected:
    raise SystemExit(f'Expected Python {expected}, found {actual}. Rebuild the dev container.')
PY
node --input-type=module - <<'JS'
import { readFileSync } from 'node:fs';
const expected = readFileSync('.node-version', 'utf8').trim();
if (process.versions.node !== expected) {
  throw new Error(`Expected Node ${expected}, found ${process.versions.node}. Rebuild the dev container.`);
}
JS

python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
npm --prefix portal ci --ignore-scripts --no-audit --no-fund
echo 'Development setup complete. Local data checks: bash scripts/check-data.sh'
echo 'Optional AI tools are installed and started explicitly; see docs/development.md.'
