#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OUT_DIR="${REPO_ROOT}/portal/test-results/bdl-web-bulk"
mkdir -p "${OUT_DIR}"

LOG_FILE="${OUT_DIR}/bdl_extractor.log"
EXTRA_ARGS="${@}"

echo "Starting BDL Web Adaptive Extractor in background..."
echo "Args: ${EXTRA_ARGS:-<CONCURRENCY_10_24H>}"
echo "Log: ${LOG_FILE}"

cd "${REPO_ROOT}"
pkill -f "src/bdl_web_adaptive.py" 2>/dev/null || true

setsid -f bash -c "PYTHONPATH=src PYTHONUNBUFFERED=1 '${REPO_ROOT}/.venv/bin/python' src/bdl_web_adaptive.py --workspace '${OUT_DIR}' --allow-codespace --concurrency 10 --max-seconds 86400 ${EXTRA_ARGS} >> '${LOG_FILE}' 2>&1"

echo "BDL Web Extractor background session initiated."
