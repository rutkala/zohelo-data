#!/usr/bin/env bash
set -euo pipefail

ZOHELO_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ZOHELO_STATE_DIR="${ZOHELO_REPO_ROOT}/.local"
ZOHELO_PID_FILE="${ZOHELO_STATE_DIR}/portal.pid"
ZOHELO_PORTAL_LOG="${ZOHELO_STATE_DIR}/portal.log"
mkdir -p "${ZOHELO_STATE_DIR}"

if [[ -f "${ZOHELO_PID_FILE}" ]] && kill -0 "$(cat "${ZOHELO_PID_FILE}")" 2>/dev/null; then
  echo 'Portal process is already running; forwarded port: 5173.'
  exit 0
fi

if [[ ! -d "${ZOHELO_REPO_ROOT}/portal/node_modules" ]]; then
  echo 'Portal dependencies are missing. Run bash .devcontainer/setup.sh first.' >&2
  exit 1
fi

nohup npm --prefix "${ZOHELO_REPO_ROOT}/portal" run dev -- --host 0.0.0.0 --port 5173 --strictPort >"${ZOHELO_PORTAL_LOG}" 2>&1 &
echo "$!" >"${ZOHELO_PID_FILE}"
echo "Portal starting on forwarded port 5173. Log: ${ZOHELO_PORTAL_LOG}"
