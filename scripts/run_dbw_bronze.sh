#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OUT_DIR="${REPO_ROOT}/portal/test-results/dbw-bronze"
mkdir -p "${OUT_DIR}"

LOG_FILE="${OUT_DIR}/bronze_loader.log"
echo "Starting DBW Bronze Loader in background..."
echo "Log: ${LOG_FILE}"

cd "${REPO_ROOT}"
if pgrep -f "[s]rc/dbw_bronze_loader.py" >/dev/null; then
  echo "A DBW Bronze loader is already active; no second writer was started." >&2
  exit 1
fi

nohup flock -n "${OUT_DIR}/runner.lock" \
  env PYTHONPATH=src PYTHONUNBUFFERED=1 \
  "${REPO_ROOT}/.venv/bin/python" src/dbw_bronze_loader.py \
  --workspace "${OUT_DIR}" --allow-codespace "$@" \
  >> "${LOG_FILE}" 2>&1 < /dev/null &
RUNNER_PID=$!

sleep 1
if ! kill -0 "${RUNNER_PID}" 2>/dev/null; then
  if wait "${RUNNER_PID}"; then
    START_STATUS=1
  else
    START_STATUS=$?
  fi
  echo "DBW Bronze Loader did not stay active; it may have lost the runner lock. See ${LOG_FILE}." >&2
  exit "${START_STATUS}"
fi

echo "DBW Bronze Loader background session initiated with PID ${RUNNER_PID}."
