#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORTAL_DIR="${ROOT_DIR}/portal"
PORTAL_LOG="/tmp/zohelo-portal.log"
AGY_LOG="/tmp/zohelo-agy.log"

start_portal() {
  if pgrep -f "npm run dev -- --host 0.0.0.0 --port 5173" >/dev/null 2>&1; then
    return
  fi

  nohup bash -lc "cd '${PORTAL_DIR}' && npm run dev -- --host 0.0.0.0 --port 5173" >"${PORTAL_LOG}" 2>&1 &
}

install_agy_if_missing() {
  if command -v agy >/dev/null 2>&1; then
    return
  fi

  curl -fsSL https://antigravity.google/cli/install.sh | bash || true
}

get_agy_resume_flag() {
  local help_text
  help_text="$(agy --help 2>/dev/null || true)"

  if grep -q -- "--resume" <<<"${help_text}"; then
    echo "--resume"
  elif grep -q -- "--continue" <<<"${help_text}"; then
    echo "--continue"
  fi
}

start_agy() {
  local resume_flag

  if ! command -v agy >/dev/null 2>&1; then
    return
  fi

  if pgrep -f "agy --dangerously-skip-permissions" >/dev/null 2>&1; then
    return
  fi

  resume_flag="$(get_agy_resume_flag)"
  nohup bash -lc "agy --dangerously-skip-permissions ${resume_flag}" >"${AGY_LOG}" 2>&1 &
}

start_portal
install_agy_if_missing
start_agy
