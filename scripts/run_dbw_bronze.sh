#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OUT_DIR="${REPO_ROOT}/portal/test-results/dbw-bronze"
mkdir -p "${OUT_DIR}"

LOG_FILE="${OUT_DIR}/bronze_loader.log"
EXTRA_ARGS="${@}"

echo "Starting DBW Bronze Loader in background..."
echo "Args: ${EXTRA_ARGS:-<FULL_BATCH>}"
echo "Log: ${LOG_FILE}"

cd "${REPO_ROOT}"
pkill -f "src/dbw_bronze_loader.py" 2>/dev/null || true

setsid -f bash -c "PYTHONPATH=src PYTHONUNBUFFERED=1 '${REPO_ROOT}/.venv/bin/python' src/dbw_bronze_loader.py --workspace '${OUT_DIR}' --allow-codespace ${EXTRA_ARGS} > '${LOG_FILE}' 2>&1"

echo "DBW Bronze Loader background session initiated."
