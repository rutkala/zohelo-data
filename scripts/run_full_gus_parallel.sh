#!/usr/bin/env bash
# Run full parallel data ingestion for GUS DBW and GUS BDL across all 10 WireGuard proxies
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

verify_background_started() {
  local pid="$1"
  local label="$2"
  local log_file="$3"
  local status
  sleep 1
  if ! kill -0 "${pid}" 2>/dev/null; then
    if wait "${pid}"; then
      status=1
    else
      status=$?
    fi
    echo "${label} did not stay active; it may have lost the runner lock. See ${log_file}." >&2
    return "${status}"
  fi
}

echo "=== 1. Checking and activating 10 WireGuard proxy tunnels ==="
python3 "${REPO_ROOT}/scripts/multi_vpn_manager.py"

DBW_DIR="${REPO_ROOT}/portal/test-results/dbw-web-bulk"
BDL_DIR="${REPO_ROOT}/portal/test-results/bdl-web-bulk"
mkdir -p "${DBW_DIR}" "${BDL_DIR}"

echo ""
echo "=== 2. Starting GUS DBW Web Extractor (10 concurrent workers across 10 IPs) ==="
if pgrep -f "[s]rc/dbw_web_extractor.py" >/dev/null; then
  echo "GUS DBW is already active; no second writer was started."
else
  nohup flock -n "${DBW_DIR}/runner.lock" env PYTHONPATH=src PYTHONUNBUFFERED=1 \
    .venv/bin/python src/dbw_web_extractor.py --allow-codespace \
    --workspace portal/test-results/dbw-web-bulk \
    --summary portal/test-results/dbw-web-bulk/dbw-summary.json \
    --concurrency 10 --max-seconds 86400 > "${DBW_DIR}/dbw_extractor.log" 2>&1 < /dev/null &
  DBW_PID=$!
  verify_background_started "${DBW_PID}" "GUS DBW" "${DBW_DIR}/dbw_extractor.log"
  echo "GUS DBW started with PID ${DBW_PID}. Logs: ${DBW_DIR}/dbw_extractor.log"
fi

echo ""
echo "=== 3. Starting GUS BDL Web Ingestor (10 concurrent workers across 10 IPs) ==="
if pgrep -f "[s]rc/bdl_web_adaptive.py" >/dev/null; then
  echo "GUS BDL is already active; no second writer was started."
else
  nohup flock -n "${BDL_DIR}/runner.lock" env PYTHONPATH=src PYTHONUNBUFFERED=1 \
    .venv/bin/python src/bdl_web_adaptive.py --workspace portal/test-results/bdl-web-bulk \
    --allow-codespace --concurrency 10 --max-seconds 86400 \
    > "${BDL_DIR}/bdl_extractor.log" 2>&1 < /dev/null &
  BDL_PID=$!
  verify_background_started "${BDL_PID}" "GUS BDL" "${BDL_DIR}/bdl_extractor.log"
  echo "GUS BDL started with PID ${BDL_PID}. Logs: ${BDL_DIR}/bdl_extractor.log"
fi

echo ""
echo "=== Summary ==="
echo "Both pipelines are active or were already active:"
echo "  * DBW: 10 concurrent workers -> 10 Polish IPs"
echo "  * BDL: 10 concurrent workers -> 10 Polish IPs"
echo "Monitor live logs with:"
echo "  tail -f portal/test-results/dbw-web-bulk/dbw_extractor.log"
echo "  tail -f portal/test-results/bdl-web-bulk/bdl_extractor.log"
