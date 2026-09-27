#!/usr/bin/env bash
set -euo pipefail

# Owner-requested CLI installation; never launch an agent or change authentication.
npm install --global --ignore-scripts --no-audit --no-fund @openai/codex@0.155.1

# Check the standalone install even when the VS Code extension also supplies codex.
"$(npm prefix --global)/bin/codex" --version
