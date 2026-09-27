# GitHub Actions audit, 7 September 2026

**Approval update:** the owner explicitly approved the first audit production publication, verification and resumed ingestion on 7 September 2026. The temporary H-LIVE pause is cleared; the 02:00 UTC schedule below is restored. Actual outcomes are recorded in [the delivery record](../deliverables.md).

**Scope.** This audit covers every repository-owned workflow present on remote
`main` at `a6b4f0fa45dc7dc9eee33ca7a8aca1dab9aa4ffe`. GitHub's public Actions API
confirmed that all nine source workflows were active at review time. Historical
runs for the deleted bronze and silver workflows remain visible in Actions but
are not part of the active inventory.

The review used workflow source as the operating authority. Repository or
environment settings outside Git were not assumed to supply missing branch,
permission, timeout, or supply-chain controls.

## Inventory and disposition

| Workflow at audit baseline | Purpose and external effect | Trigger and gate | Effective permissions | Concurrency | Runtime | Decision |
| --- | --- | --- | --- | --- | --- | --- |
| `daily-ingestion.yml` — **NBP data platform** | Ingests NBP source responses, builds and publishes a matched release and catalogue in Drive, restores and queries the release, can prove raw replay, then records read-only state, project-storage, and account-quota health. The repaired dispatch can also verify and promote an explicitly selected retained release. | 02:00 UTC daily; manual publish mode; selected `main` pushes with `[run-nbp-platform]`. Retained-release promotion is manual, requires target and expected-current release IDs, and is restricted to `main`. | `contents: read`; Google OAuth secrets are passed only to production steps. | `zohelo-production-data`, never cancel a running data operation. | Ubuntu, Python 3.12, 90-minute publish or 30-minute retained-release promotion. | **Keep and extend.** It remains the single production NBP release boundary. |
| `data-validation.yml` — **Validate data platform** | Runs credential-free Python, dbt, contract, replay, recovery, and workflow policy checks. It has no remote write path. | Relevant pull requests or manual dispatch. | `contents: read`. | Per ref, cancel stale runs. | Ubuntu, Python 3.12, 10 minutes. | **Keep.** It is the data and workflow merge gate. |
| `deploy-portal.yml` — **Deploy portal** | Stages the generic dbt viewer, lints and unit-tests the exact commit, builds the portal, uploads the Pages artifact, and deploys it. | Relevant `main` pushes or manual dispatch from `main`; both jobs enforce the branch in source. | Build: `contents: read`, `pages: read`. Deploy: `pages: write`, `id-token: write`. | `portal-pages`, allow the running deployment to finish. | Ubuntu, Python 3.12, Node 24.20.0; build 15 minutes, deploy 10 minutes. | **Keep; rename display name.** The shorter name matches repository terminology while the file path preserves run history. |
| `deploy.yml` — **Reconcile Drive folders** | Creates missing configured Drive root and zone folders. It does not ingest or transform data. | Manual dispatch from `main` only. | `contents: read`; selected Google credentials are supplied to the reconciliation command. | `zohelo-production-data`, never interrupt a production data operation. | Ubuntu, Python 3.12, 60 minutes. | **Keep.** It is a separate, explicit storage maintenance operation. Preserve the file path for run history. |
| `devcontainer-validation.yml` — **Validate development container** | Builds the declared container, runs the complete data suite inside it, and verifies portal startup. | Pull requests that change runtime/container/profile inputs, or manual dispatch. | `contents: read`. | Per ref, cancel stale runs. | Ubuntu host, declared Python 3.12/Node 24.20.0 container, 20 minutes. | **Keep.** Its overlap with data validation is intentional: the full suite verifies the fresh declared environment, and it runs only for runtime-affecting changes. |
| `google-access-check.yml` — **Check Google Drive access** | Performs authentication and folder metadata reads without data writes. | Manual dispatch or selected `main` pushes; job enforces `main`. | `contents: read`; selected Google credentials are supplied to the read-only script. | `google-access-check`, do not interrupt. | Ubuntu, Python 3.12, 5 minutes. | **Keep separate.** Read-only authorization diagnosis stays distinct from a write probe. |
| `google-upload-check.yml` — **Check Google Drive upload** | Uploads one marked temporary object, verifies its bytes, and removes only that object. | Manual dispatch or selected `main` pushes with `[verify-drive-upload]`; job enforces `main` and explicit intent. | `contents: read`; selected Google credentials are supplied to the bounded probe. | `google-upload-check`, do not interrupt. | Ubuntu, Python 3.12, 10 minutes. | **Keep separate.** The write effect remains visible and opt-in. |
| `nbp-migration-check.yml` — **Verify existing NBP history** | Compared the current release with one fixed pre-migration Silver baseline. | Manual dispatch or selected `main` pushes with `[verify-nbp-migration]`. | `contents: read`; Google OAuth credentials. | `zohelo-production-data`, do not interrupt. | Ubuntu, Python 3.12, 20 minutes. | **Retire.** Migration acceptance passed in run `34098420713`. The script and dated evidence remain; an obsolete fixed-baseline job no longer competes with production operations. |
| `portal-validation.yml` — **Validate portal** | Lints, builds, unit-tests, and runs Drive/catalogue/SQL browser regressions for root and repository base paths; retains failure evidence for seven days. | Relevant portal pull requests or manual dispatch. | `contents: read`. | Per ref, cancel stale two-path matrices. | Ubuntu, Python 3.12, Node 24.20.0, two 15-minute matrix jobs. | **Keep.** Browser and alternate-base-path coverage stays in the pull-request gate; deployment repeats lint and unit tests on the deployed commit. |

The resulting active set has eight workflows. Data validation and portal
validation remain separate because their dependencies, failure domains, and
costs differ. Google access and upload remain separate because one is read-only
and the other intentionally writes. Portal validation and deployment remain
separate because pull requests need two-path browser coverage while deployment
needs one Pages-specific build of the exact commit.

## Repairs

Two manual production paths previously trusted the branch selected in the
Actions dispatch UI. `deploy-portal.yml` could deploy an arbitrary branch to the
public site, and `deploy.yml` could execute branch-controlled Python with Drive
credentials. Both now require `refs/heads/main` in workflow source. All other
credentialed manual workflows already had that boundary.

Every external action reference is now pinned to the full commit resolved by
the corresponding official GitHub tag API on 7 September 2026:

| Action tag | Pinned commit |
| --- | --- |
| `actions/checkout@v4` | `11d5960a326750d5838078e36cf38b85af677262` |
| `actions/setup-python@v5` | `a26af69be951a213d495a4c3e4e4022e16d87065` |
| `actions/setup-node@v4` | `49933ea5288caeca8642d1e84afbd3f7d6820020` |
| `actions/configure-pages@v5` | `983d7736d9b0ae728b81ab479565c72886d7745b` |
| `actions/upload-pages-artifact@v3` | `56afc609e74202658d3ffba0e8f6dda462b719fa` |
| `actions/deploy-pages@v4` | `d6db90164ac5ed86f2b6aed7e0febac5b3c0c03e` |
| `actions/upload-artifact@v4` | `ea165f8d65b6e75b540449e92b4886f43607fa02` |
| `devcontainers/ci@v0.3` | `513af61f4de4f75d37e4438f184ba4358f0fc1ca` |

`scripts/check-workflows.py` makes the source policy executable. It rejects
mutable action references, implicit permissions, top-level write permissions,
unbounded jobs, persisted checkout credentials, `pull_request_target`, and
manual secret/write jobs without a main-ref guard. It uses PyYAML's
`BaseLoader`, avoiding YAML 1.1 conversion of the `on` key, and is run by the
existing data validation workflow. Version updates remain a normal reviewed
source change: resolve a tag through the action owner's API, replace its SHA and
version comment, and let the policy and component checks run.

The NBP push filter now includes the Python dependency lock and input, Python
version, dbt project/profile/ignore files, and the workflow itself. A marked
runtime or dbt configuration change can therefore exercise production
deliberately instead of waiting for the next schedule. The portal deployment
now runs lint and unit/engine regressions against the exact commit before
artifact upload; the full two-base-path browser matrix remains the merge gate.

After a published release passes fresh-process restore and any requested raw
replay, the same production job runs the bounded read-only platform health
check. Its JSON is redirected away from the log and retained for 14 days as
`nbp-platform-health-<run_id>-<run_attempt>`. Artifact upload uses `always()` so
the fixed, identity-free failure payload remains available when the diagnostic
itself fails. Unknown account quota values remain unknown rather than being
reported as zero.

## Run evidence and remaining limits

The most recent 100 repository runs available from GitHub's public API included
24 data-validation runs (22 passed), 22 portal-validation runs (12 passed), and
12 portal deployments (all passed). The successive portal failures were
development iterations followed by passing runs; cancelable per-ref concurrency
now prevents superseded matrices from continuing. The current NBP workflow had
10 runs in that window, including three successes and seven failures spanning
the pre-repair history. Its latest relevant successes were runs `34075583235`,
`34096483209`, and `34142977073`.

No routine scheduled run of the final consolidated NBP implementation had
completed by audit time, so the existing platform audit's measurement follow-up
still applies. Reconcile Drive folders had one old failed push run before it was
made manual-only; the source repair does not claim a new live reconciliation.
The access check passed three observed runs, the upload check passed its one
observed run, and the retired migration check passed its one observed run. This
audit changed no repository or environment settings and performed no remote
write, deployment, ingestion, or retained-release promotion.
