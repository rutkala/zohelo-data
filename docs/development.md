# Development

Use the versions in `.python-version` and `.node-version`.

```sh
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
npm --prefix portal ci --ignore-scripts --no-audit --no-fund
```

Run the Python/SQL command needed for the task. For the portal, use
`npm --prefix portal run dev` or `npm --prefix portal run build`.

The owner removed repository tests, fixtures, test commands, dbt test declarations
and CI/check workflows. Do not recreate them automatically or follow historical
instructions to run them. No test suite runs during installation or deployment.

BDL's real browser ingestion still imports Chromium from `@playwright/test`.
That package is retained for this runtime use, not to run browser tests. Vitest is
removed from direct dependencies; old lockfile entries have not been regenerated.
Keep the lockfile rather than replacing reproducible installs with unlocked ones.
Legacy release metadata named `tests` remains for deployed-reader compatibility;
it is not evidence that a repository test suite ran.

Keep normal error handling, data integrity and resumability. Production writes
require existing authorization and writer locks. Cleanup is not permission to
change data, readers or destinations. Use the [workflow guide](audits/2026-09-07-workflows.md)
and [AGENTS.md](../AGENTS.md); old test instructions are superseded.
