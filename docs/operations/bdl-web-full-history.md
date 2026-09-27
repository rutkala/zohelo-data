# BDL Web full-history execution contract

> **Current operational qualification, 26 September 2026:** the owner selected GitHub Actions with ten existing WireGuard routes and no dependency on a devcontainer or unpushed work. The Drive writer record is an application-level guard, not an atomic cross-host lock. An active record cannot be taken over merely because its heartbeat is old. Review the live preflight receipt and explicitly reconcile ownership before manually dispatching ingestion; current checkpoint details are in [the delivery record](../deliverables.md).

Owner requirement clarified on 17 September 2026. This is the implementation and
acceptance contract for PR #126, not a claim that a Codespace campaign is deployed
or that full BDL coverage has been collected. Current project delivery status
belongs in `docs/deliverables.md`.

## Outcome and execution host

One explicitly started full-history pass must traverse the complete BDL Web UI
subgroup inventory. Elapsed time must not be its normal completion condition.
The current historical continuation uses a manual, main-only GitHub Actions
workflow and the existing version-controlled Python/browser ingestion components.
Its application budget is at most 16,200 seconds and also shrinks when setup
consumes the absolute 350-minute job deadline. At least 20 minutes are reserved
for worker drain, durable checkpoint/release and continuation decision. The historical source
scope remains the full inventory across bounded runs.

No cron, marked-commit start, scheduled full reload or ingestion-on-push is
authorized. A deliberate manual start begins a finite campaign. A successor
Action is dispatched only after a clean bounded stop, confirmed durable queue
and released writer, and selectable work with no global control failure. A
complete pass with terminal failures stops for review. A subsequent failure-review pass is explicitly
started after reviewing the prior pass and deploying its repairs.

## Catalogue, pass, and data coverage are separate

1. Traverse and reconcile all Web category/group/subgroup pages. Save the native
   UI routing inventory and its identity/hash; do not hard-code 2,221 as the total.
   Outstanding pages or unverified seed entries prevent catalogue-complete status.
   Known entries may continue downloading during discovery, but a source-wide
   full-pass report is forbidden until discovery has been reconciled.
2. Give the pass an identity and a persisted subgroup work list. Every subgroup
   receives a current-pass outcome: a native export saved, a retained export
   checked and explicitly reused in resume mode, or a specific recorded failure.
   A prior failure is not an attempt in the new pass. Pending or interrupted work
   cannot be relabelled as failure merely to satisfy a counter.
3. For a requested fresh reload, iterate/download the full current inventory into
   a new campaign version without deleting successful previous native files.
   Resume mode reuses only the same campaign's verified checkpoints and records
   reused coverage separately; do not silently replace a fresh reload with resume.
4. `pass_complete` requires reconciled catalogue discovery and zero unvisited or
   in-flight subgroups. `load_complete` additionally requires no unresolved
   subgroup/selection failures and native-download selection coverage for the full
   planned inventory. A fully traversed pass with failures is NOT a successful
   full load. Native selection coverage is not numerical-content validation.

## Loop and failure behaviour

The full-history command has no 310-minute campaign budget. Request/navigation/
export/upload timeouts remain bounded so one stalled operation cannot monopolize
the pass. Give unvisited subgroups fair turns before repeated deep retries of one
subgroup. Continue after subgroup-specific export errors and retain all error
signatures. For large failing selections, use disjoint territorial batches, then
smaller year/dimension batches where needed, retaining the complete selection
union. A partial partition never creates a whole-subgroup completion marker.

Authentication failure affecting the entire campaign, provider throttling/site
outage, storage ambiguity, lost execution ownership and exhausted authorized
compute/storage allowance are operational interruptions, not successful end
conditions. Back off or stop safely as appropriate, persist progress, and state
precisely what still has not been visited. Do not repeatedly split requests when
the real problem is global authentication, rate limiting or storage.

Preserve run #17's original native files, queue and failure-review package.
Keep the successful whole-export path for subgroups where it works. Adaptive
selection recovery must not force every small working export into thousands of
unnecessary downloads.

## Controlled parallelism

Parallelism is an optimization, not the completeness mechanism. Establish the
single-worker baseline, then test two independent browser sessions using the
existing authorized account. Compare verified files/hour, response latency,
server errors, session invalidation and export-to-subgroup/selection identity.
Raise concurrency only when measured results improve and provider behaviour
permits it; reduce it or back off on throttling and rising errors. Two workers is
a proposed test setting, not a verified BDL allowance.

Browser contexts must not share mutable selection state. Isolation of cookies
does not prove the provider's account-side export queue is isolated. Keep
account-scoped generated-package creation/collection serialized unless reliable
request-to-file correlation under concurrency is proven. Do not use extra
accounts, IP rotation or other means to evade provider restrictions.

One coordinator owns the durable campaign checkpoint and commits download
receipts; browser workers do not concurrently rewrite `web-queue-v1.json`.
A selected subgroup/partition has exactly one active owner. Verify the existing
Actions writer is inactive and cannot overlap before enabling the Codespace
writer; an Actions concurrency group does not lock a standalone process.

## Native Landing and operating controls

Save each provider-native file unchanged to Drive immediately after transfer,
with separate subgroup/selection, filename, size, checksum and completion
metadata. No archive extraction, CSV parsing, row counts, schema validation,
Parquet, API observation requests or medallion transformations run in ingestion.
Reconcile receipt/object identities on restart, and preserve uncertain writes for
investigation rather than redownloading or claiming completion blindly.

Before production start, verify the actual existing Codespace, reviewed code
revision, Python/Node/browser prerequisites, Web and Drive credential availability
without displaying values, production-root selection, exclusive writer ownership,
remaining included compute/storage allowance and the existing no-overage policy.
Do not weaken or spoof the current Actions-only production guard to make a
Codespace command run; implement an explicit, tested standalone authorization
path. Use a supervised process with durable logs/status/checkpoints and verified
terminal activity/disconnect behaviour. `nohup` or `tmux` alone is not proof that
Codespaces idle suspension cannot interrupt a task. Preserve interruption/resume
state and restore the normal Codespace idle behaviour after execution; stop an
otherwise unused Codespace after the task under the owner's existing cost rules.

## Legacy standalone commands and runner controls

These local commands remain for compatibility with an explicitly authorized standalone environment. The current migration uses the manual main-only Actions workflow described below; no local checkout or devcontainer is required. From a standalone repository root:

```bash
# Explicit resume mode (reuses verified existing plans and stored partition receipts)
python src/bdl_web_adaptive.py \
  --workspace /path/to/disposable-workspace \
  --mode resume \
  --allow-codespace

# Fresh reload mode (downloads fresh inventory pass; preserves prior native files)
python src/bdl_web_adaptive.py \
  --workspace /path/to/disposable-workspace \
  --mode reload \
  --allow-codespace
```

Operational invariants enforced by the runner:
1. **Writer Exclusivity:** An active writer acquires `bdl-writer-lock.json` in the Drive control zone. A live heartbeat is maintained throughout execution. Any active lock rejects startup regardless of its age. The owner's no-devcontainer assumption supplies the absent-host premise; a separate serialized recovery must match exact inspected lock and queue bytes, reconcile uncertain receipts, and explicitly release ownership before an Actions launch.
2. **Coverage Termination:** Default `max_seconds=None` prevents arbitrary time cutoffs. The pass terminates only on catalogue exhaustion and zero unvisited subgroups (`pass_complete` / `load_complete`), or explicitly bounded interruption (`interrupted`).
3. **Fair Scheduling:** Unvisited subgroups are attempted fairly before repeated deep slicing of a single multi-partition subgroup.
4. **Receipt Validation:** Drive partition objects are verified on restart; missing or tampered receipts are rejected and re-downloaded.
5. **Control Failure:** A queue conflict or heartbeat publication failure interrupts the whole runner. It stops scheduling, lets bounded in-flight workers finish their current recovery boundary, joins them before releasing the writer lock, and refuses further queue writes once durable state is uncertain. The local summary reports `control_error`; its progress counts are explicitly non-durable and cannot claim pass or load completion.

## Delivered implementation and verified acceptance

Delivered in PR #126:
- Dynamic catalogue traversal in `portal/scripts/bdl-web-catalogue.mjs` fixed and verified on public BDL tables 563 (11 pages), 570 (25 pages), and 640 (9 pages) with 100% exhaustion.
- Full-history runner in `src/bdl_web_adaptive.py` with standalone Codespace authorization, exclusive writer locking, fair slicing, and `--mode resume`/`--mode reload`.
- Fast-path whole subgroup export with automatic partition fallback for large selections.
- Verified by unit test suite `tests/test_bdl_web_adaptive_runner.py` (all 10 tests passed) and full repository regression `bash scripts/check-data.sh` (all 527 tests passed).


## GitHub Actions migration handoff

The main-only `.github/workflows/bdl-web-preflight.yml` can be dispatched
manually or runs on a reviewed main push that changes its route implementation.
It checks `ZOHELO_WORKER1` through `ZOHELO_WORKER10`, each a **full** encrypted
WireGuard config. Missing secrets are reported by name only. It installs the
`github.com/windtf/wireproxy/cmd/wireproxy@v1.1.3` Go module with the public Go
sum database enabled, starts ten localhost listeners in a private runner temp
directory, and checks distinct non-direct egress and BDL reachability using
Playwright's same `bdlBrowserOptions` launch path as ingestion. It has no Drive
credentials, writes no Drive data, starts no ingestion, prints no addresses or
config/key diagnostics, and cleans up only its owned processes. A green code
check is distinct from this actual live preflight receipt.

After reviewing that receipt and establishing explicit writer handover, manually
dispatch `.github/workflows/bdl-web-bootstrap.yml` on main. It uses the same
ten-route gate and existing durable queue (`--mode resume --concurrency 10
--require-proxy-count 10`, explicit `BDL_PROXY_CLUSTER_FILE`). The workflow
contains no reset input. Its local evidence artifact includes only enumerated
summary/task files, never config files. It cannot resume while the recorded lock
is active; a 26 September read-only audit still found the prior Codespace lock
active with an old heartbeat. The owner's no-devcontainer assumption does not
change the fail-closed lock rule or require reconnecting that host. Reconcile
interrupted checkpoint receipts and perform a serialized release bound to the
exact inspected lock and queue bytes before launch. The distinct
operational milestones are ready code, passed live ten-route preflight, and
actual checkpointed ingestion with native byte receipts. None proves full BDL
history is complete.

The local `scripts/multi_vpn_manager.py` is one-shot and portable; it owns only
processes it started. For non-Actions local use, `BDL_PROXY_CLUSTER_FILE` may
still select the repository's `.wireguard/cluster.json`, and the standalone runner
can operate without required routes when explicitly authorized. This does not
relax the ten-route Actions contract.

## Current primary references

Checked 17 September 2026:

- [GitHub Actions limits](https://docs.github.com/en/enterprise-cloud%40latest/actions/reference/limits): six hours per GitHub-hosted job; workflow and job limits differ.
- [Codespaces timeout behaviour](https://docs.github.com/en/codespaces/setting-your-user-preferences/setting-your-timeout-period-for-github-codespaces): idle timeout is distinct from a fixed job-runtime cap; terminal activity resets it, and active compute consumes the account's allowance.
- [Codespace lifecycle](https://docs.github.com/en/codespaces/about-codespaces/understanding-the-codespace-lifecycle): stopping a Codespace stops its processes; saved files persist subject to the Codespace lifecycle.
