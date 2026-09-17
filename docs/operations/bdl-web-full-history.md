# BDL Web full-history execution contract

Owner requirement clarified on 17 September 2026. This is the implementation and
acceptance contract for PR #126, not a claim that a Codespace campaign is deployed
or that full BDL coverage has been collected. Current project delivery status
belongs in `docs/deliverables.md`.

## Outcome and execution host

One explicitly started full-history pass must traverse the complete BDL Web UI
subgroup inventory. Elapsed time must not be its normal completion condition.
The initial historical load and exceptional on-demand reloads use the existing,
authorized reusable Codespace as the preferred execution host. Use the same
version-controlled Python/browser ingestion components, not a second extraction
implementation. GitHub Actions remains useful for CI; a GitHub-hosted job's
six-hour execution limit must not determine historical source scope.

No cron, periodic dispatch, marked-commit start, automatic successor Action or
scheduled full reload is authorized. A deliberate start begins a finite campaign.
Internal work queues, bounded request retries and recovery within that campaign
are not recurring schedules. A subsequent failure-review pass is explicitly
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

## Acceptance before calling this implementation delivered

- A fixture catalogue larger than one old runtime window is fully traversed;
  arbitrary elapsed time cannot cause full-pass success or omit its final item.
- Mixed success, failure, newly discovered entries and already-retained files
  reconcile exactly, with separate current-pass and cumulative counts.
- Restart preserves successful uploads and pending/failed work; invalid/missing
  receipt objects cannot be silently counted as complete.
- Subgroup-specific failures do not prevent later subgroups from being visited;
  global interruption never reports `pass_complete` or `load_complete`.
- Partition unions preserve every selected territory/year/dimension and account
  for all unresolved leaves. Parallel ownership and single-writer behaviour pass
  regression tests before concurrency is enabled.
- The live host/session/credentials and safe disconnect/restart path are verified;
  the source-wide report proves catalogue exhaustion and zero unvisited entries.
- A successful full-load claim additionally proves zero unresolved required
  selections and retained native objects for the complete planned scope.

PR #126 must be revised to this contract before deployment. Its existing timed
runner and Actions-only guard are not a delivered Codespace solution. The current
session can read/write the GitHub repository but does not expose a connected
Codespace terminal; local GitHub DNS and CLI authentication were unavailable.
Terminal access, account capacity and live parallel-session behaviour therefore
remain unverified, not assumed working.

## Current primary references

Checked 17 September 2026:

- [GitHub Actions limits](https://docs.github.com/en/enterprise-cloud%40latest/actions/reference/limits): six hours per GitHub-hosted job; workflow and job limits differ.
- [Codespaces timeout behaviour](https://docs.github.com/en/codespaces/setting-your-user-preferences/setting-your-timeout-period-for-github-codespaces): idle timeout is distinct from a fixed job-runtime cap; terminal activity resets it, and active compute consumes the account's allowance.
- [Codespace lifecycle](https://docs.github.com/en/codespaces/about-codespaces/understanding-the-codespace-lifecycle): stopping a Codespace stops its processes; saved files persist subject to the Codespace lifecycle.
