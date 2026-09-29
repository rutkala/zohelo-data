# Working on zohelo-data

The owner's current request takes precedence over historical plans.
Keep ingestion, native Landing/Archive, Bronze/Silver/Gold, modeling, semantic
models and the portal. R2/Iceberg remains the target; cleanup is not cutover.

## Keep it simple

Use direct Python/SQL and the smallest command needed for the task. Inspect an
existing workflow before choosing it. Do not add frameworks, agent delegation,
automatic follow-up jobs, pilots, reports or workflows without an explicit need.
Read only task-relevant files; old plans are not an execution queue.

The owner explicitly requested removal of all repository tests. Do not recreate
test suites, fixtures, test runners or CI/check workflows unless the owner asks.
Do not replace deleted tests with renamed probes, acceptance suites or mandatory
preflights. There is no test prerequisite for development, ingestion or deployment.
Historical test results and removed workflow names are history, not instructions.

## Preserve the actual operation

Keep credentials private, bounded retries/timeouts, resumable checkpoints,
writer locks, original payload bytes and ordinary input/output error handling.
Removing tests does not mean silently publishing corrupt data or hiding errors.
Never delete retained production data as repository cleanup. Never claim complete
source coverage from a sample. Do not change storage readers/writers or release
pointers without explicit cutover. No new paid services or model API billing.

## Deliver

Keep four operational Actions: ingestion, transformations, portal deployment and
the temporary Drive-to-R2 copy. No CI/test Action. Do not silently chain optional
diagnostics or launch production operations to check repository edits.
Report what changed, what actually ran, whether it reached main and what remains.
Preserve other contributors' work. See README.md for current operating commands.
