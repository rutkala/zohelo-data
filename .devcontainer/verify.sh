#!/usr/bin/env bash
# Run inside the built devcontainer, including its lifecycle setup.
set -euo pipefail

ZOHELO_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ZOHELO_REPO_ROOT}"
bash scripts/check-data.sh
node --version
gh --version
bash .devcontainer/post-start.sh
for attempt in {1..30}; do
  if curl --fail --silent http://127.0.0.1:5173/ > /dev/null; then
    echo 'Development portal responds on port 5173.'
    exit 0
  fi
  sleep 1
done
cat .local/portal.log
exit 1
