#!/usr/bin/env bash
# Ensure userspace WireGuard proxy (wireproxy) is running on 127.0.0.1:8080 / 1080
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONF="${REPO_ROOT}/.wireguard/wireproxy.conf"
LOG="${REPO_ROOT}/.wireguard/wireproxy.log"

if [ ! -f "${CONF}" ]; then
  echo "Error: ${CONF} not found. Please place your WireGuard .conf in .wireguard/" >&2
  exit 1
fi

if curl -s -x http://127.0.0.1:8080 -m 2 https://api.ipify.org >/dev/null 2>&1; then
  echo "Wireproxy is already running and operational."
  exit 0
fi

echo "Starting wireproxy in userspace..."
pkill wireproxy 2>/dev/null || true
setsid wireproxy -c "${CONF}" > "${LOG}" 2>&1 < /dev/null &
sleep 2

if curl -s -x http://127.0.0.1:8080 -m 5 https://api.ipify.org >/dev/null 2>&1; then
  IP=$(curl -s -x http://127.0.0.1:8080 -m 5 https://api.ipify.org)
  echo "Wireproxy started successfully! Outbound IP: ${IP}"
else
  echo "Failed to verify wireproxy connection. Check ${LOG}" >&2
  exit 1
fi
