#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OUT_DIR="${REPO_ROOT}/portal/test-results/bdl-web-bulk"
mkdir -p "${OUT_DIR}"

LOG_FILE="${OUT_DIR}/bdl_extractor.log"

echo "Starting BDL Web Adaptive Extractor in background..."
echo "Args: ${*:-<CONCURRENCY_10_24H>}"
echo "Log: ${LOG_FILE}"

cd "${REPO_ROOT}"
if pgrep -f "[s]rc/bdl_web_adaptive.py" >/dev/null; then
  echo "A BDL Web extractor is already active; no second writer was started." >&2
  exit 1
fi

nohup flock -n "${OUT_DIR}/runner.lock" \
  env PYTHONPATH=src PYTHONUNBUFFERED=1 \
  "${REPO_ROOT}/.venv/bin/python" src/bdl_web_adaptive.py \
  --workspace "${OUT_DIR}" --allow-codespace \
  --concurrency 10 --max-seconds 86400 "$@" \
  >> "${LOG_FILE}" 2>&1 < /dev/null &
RUNNER_PID=$!

echo "BDL Web Extractor background session initiated with PID ${RUNNER_PID}."
