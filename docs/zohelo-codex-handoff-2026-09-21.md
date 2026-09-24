# Zohelo-data: local Codex handoff, 21 September 2026

This is a dated handoff from the owner's ChatGPT Work session, not a replacement for the repository's canonical delivery record. Recheck live GitHub state and local files before acting.

## Immediate situation

The owner reports that AGY and the whole local devcontainer crashed. AGY had previously reached its quota. The owner's goal is to continue autonomous engineering and recover long-running ingestion locally, using Codex CLI signed in with the ChatGPT account rather than API-key billing.

Repository: https://github.com/rutkala/zohelo-data
Local working directory: `/workspaces/zohelo-data`.
The cloud assistant cannot currently inspect or operate this devcontainer directly. Account sign-in alone does not transfer the entire chat transcript or attach its shell to this conversation.

## Starting Codex CLI with the ChatGPT account

Run inside the recovered devcontainer's VS Code terminal:

```bash
npm install -g @openai/codex
codex login --device-auth
cd /workspaces/zohelo-data
codex
```

Open the printed device-login link in a browser and sign in with the ChatGPT account. If requested, enable device-code login in ChatGPT security settings. This sign-in uses subscription access rather than an API key. Codex can then read and operate the local workspace subject to its permissions. The CLI session is separate from the cloud chat; this handoff and GitHub carry the project state across.

Save this file in the repository directory and give Codex the suggested first instruction at the end. Later, run `codex resume --last` from that directory to resume the local CLI conversation.

Official references: [CLI installation](https://learn.chatgpt.com/docs/codex/cli), [account and device authentication](https://learn.chatgpt.com/docs/auth), and [resuming a local session](https://learn.chatgpt.com/docs/cli/reference#codex-resume).

## First actions in the local environment

1. Read `AGENTS.md`, `README.md`, `docs/architecture.md`, `docs/collaboration.md`, and `docs/deliverables.md`. Inspect current local HEAD and the actual uncommitted/untracked work before editing. Fetching remote refs is safe; do not immediately pull, reset, clean, stash, or switch the existing working tree.
2. Preserve the unpublished source work and recovery state before changing or rebuilding anything. Keep credentials out of source archives, Git, logs and chat. The earlier `/tmp/zohelo-local-review.tar.gz` may no longer exist after the crash; verify it rather than assuming it survived.
3. Inspect fresh process identities, process groups, memory, disk, logs, checkpoints and locks. Historical PIDs below are evidence only and must never be used as current kill targets. Do not assume a stale heartbeat means a process is still alive, or that an absent terminal means every subprocess stopped.
4. Determine why the container crashed if local evidence permits: inspect resource/OOM evidence available to the user, recent application errors and incomplete writes. No diagnosis of the crash has yet been established.
5. Reconcile local recovery state against Drive control records before restarting one writer per source. Preserve verified successful downloads. Do not blindly launch the old supervisor: its handoff/retry defects remain open.
6. Use a separate checkout/worktree for engineering changes if needed. Keep massive downloads and long ingestion in this local devcontainer; do not start duplicate Actions/Codespaces collection.

Initial read-only terminal inventory:

```bash
date -u
ps -eo user,pid,ppid,pgid,stat,etimes,pcpu,pmem,rss,comm --forest
pgrep -l -f 'bdl|dbw|bronze|supervisor|imgw|gios|agy|codex|dbt'
free -h
df -h /workspaces/zohelo-data
git -C /workspaces/zohelo-data status --short --branch
git -C /workspaces/zohelo-data log -1 --format='%h %cI %s'
```

Avoid printing full process arguments or environment variables into shared output; they can contain credentials.

## Local source snapshot already reviewed

At 05:17 UTC, local HEAD was `c7898d7` (20 September). Six tracked files were modified and 48 files were untracked. A source-only 54-file archive was reviewed separately in the cloud. It included native adapters, Bronze loaders and models for TERYT, PRG, GLEIF, MF VAT, IMGW and GIOS, plus DBW changes and test/config edits. Preserve these files; do not overwrite them with a clean checkout.

Current main at the start of cloud repair was `6b14a2b43655a49bed6c18246a4c8c14ee495267`, from PR #136. That repair added stricter GUS/DBW snapshot, completion, publication and Bronze boundaries. Copying the older local `src/dbw_bronze_loader.py` wholesale over main would remove those safeguards. Integrate performance changes selectively.

At 05:17 UTC, BDL's worker was PID 595473, with a shell/supervisor, ten `wireproxy` processes and AGY also present. The machine had 7.2 GiB RAM, 5.1 GiB available, 1.4 GiB swap in use and about 155 GiB disk available. These are historical measurements, not proof of the later crash's cause or current health.

## Cloud engineering work: PR #137

https://github.com/rutkala/zohelo-data/pull/137

Title: `fix(gleif): verify resumable local native downloads and block unsafe publication`.
Final reviewed revision: `ccfa1fc0e77c9a16364ea8742882388a792458b1`.
CI: https://github.com/rutkala/zohelo-data/actions/runs/35585338359

PR #137 is merged. Verified remote `main` is `ee2d944ea307a7f3c9e856da61e3eaabe701cb67`, the squash commit on parent `6b14a2b43655a49bed6c18246a4c8c14ee495267`. Final-revision CI passed all **613 tests** (`Ran 613 tests in 666.472s`, `OK`). Recheck for subsequent changes before integrating this into the older local working tree. Do not overwrite unpublished local work or duplicate this completed repair.

The final version is a **local native downloader/cache only**:
- Discover and pin one GLEIF provider publication, its `lei2`, `rr`, and `repex` dated CSV ZIP URLs and advertised sizes.
- Transfer native bytes unchanged; atomically validate before replacing a local target.
- Persist verified cache records after every member and rehash before reuse.
- Enforce one local workspace owner with a Linux file lock.
- Reject upload flags before creating a workspace or making source requests, unless `--skip-upload` explicitly requests local behavior.

An earlier PR revision included a Drive publisher. GitHub review found a cross-host race in same-name object creation. That publisher was removed completely. Do not copy it from earlier commits or re-enable it with an environment flag. Production publication requires a proven cross-host serializer before source namespace creation or writes. A delayed listing/election and lease timeout are not an established distributed mutex. One reviewed future design uses an explicitly provisioned, root-bound anchor and immutable claim/release records addressed by pre-generated Drive IDs; it requires implementation, verification and safe crash recovery before use.

Validation: the final local-only code passed all 10 focused tests, independent review, and the final 613-test CI suite. A read-only provider check parsed 139,840 bytes of live publication metadata for `2026-09-21 00:00:00`, confirming the actual `YYYYMMDD-HHMM` filename convention. This cloud repair downloaded no native archive, wrote no Drive data, enabled no ingestion schedule, and did not modify the owner's devcontainer.

## Data status: dated evidence, not completion claims

- BDL queue: https://drive.google.com/file/d/18TUYzBpZg1Hn0qcoZT_iiO1PAq2-eyCj/view . The 08:25 UTC checkpoint had 1,036 landed, 255 partial and 56 failed plan outcomes against 2,420 candidates. Earlier review had 849 landed. BDL remains incomplete; these figures do not establish current worker health.
- Eurostat: 6,267 of 21,238 full distributions, 29.5084%, at the reviewed checkpoint. The accepted modeled contract covers three datasets. https://github.com/rutkala/zohelo-data/actions/runs/35561187440
- WDI: the accepted current official archive product reconciled 9,015,914 modeled values. Separate API reconciliation and other World Bank products remain distinct. https://github.com/rutkala/zohelo-data/actions/runs/35549893680
- DBW legacy checkpoint: https://drive.google.com/file/d/1XxOWlXaiML8WPkkCfmVd0N0dMrKpoLYr/view . It reported 1,550/1,550 indicators and 820,345,903 rows; this is not an independently recounted, snapshot-bound release under #136. Preserve existing outputs and reconcile provenance before deciding what needs recollection.
- NBP has an accepted release path. Consult the delivery record for exact release IDs and newer evidence.

Drive is authoritative for durable data; Git is authoritative for code and definitions. The portal supports specific published source/release contracts. New folders in Drive do not automatically become visible. At review, the portal source matched its deployed 17 September build; rebuilding the same source would not expose the additional sources.

## Remaining verified engineering gaps

The supplied new adapters included delete-before-upload data-loss windows, truncated-transfer acceptance and incomplete inventories marked complete. These code defects do not themselves prove existing Drive bytes are corrupt. Other findings include Bronze/parser acceptance problems, dbt grain and meaning defects, DBW performance work that must preserve #136 safeguards, unfinished BDL Bronze execution, and a supervisor that can mark downstream work before successful launch without reliable retries.

Keep these existing issues and the delivery record reconciled:
- #130: autonomous programme
- #131: BDL native collection and Bronze transition
- #132: DBW Bronze/Silver/Gold
- #133: WDI work; reconcile its older wording against already accepted releases
- #134: Eurostat full bulk and Bronze
- #135: stage orchestration and production validation

Do not declare a source Done from a partial archive, a sample, a running backfill, a metadata-only checkpoint, a parser skeleton, or a green test suite.

## Operating constraints

- Landing is native transfer only: authentication, discovery, paging, quotas, bounded retries, exact original bytes, integrity and small control metadata. No unpacking, payload parsing, row counting, Parquet conversion or dbt in ingestion. Downstream work has separate acceptance and recovery.
- Preserve previous raw objects and complete releases until replacements are verified. Keep selected current-product completion distinct from full historical/provider coverage.
- One active engineering task and one implementation owner. Continue routine authorized work autonomously, with focused review, tests, PRs and truthful delivery evidence. Ask only for consequential new choices or missing access.
- Use the pinned Python 3.12 environment and component checks from AGENTS.md. `bash scripts/check-data.sh` is the data-change gate. Routine validation uses fixtures, not production write entrypoints.
- Codex account login authenticates Codex. Verify local GitHub and Google Drive tooling/credentials separately without displaying secret values; do not assume this chat's connector sessions transferred automatically.
- Long ingestion should be supervised independently of the agent conversation and recover from durable checkpoints. A container crash can stop detached jobs too; terminal detachment alone is not crash recovery.

## Suggested first instruction to local Codex

Read this handoff and the repository instructions. Start with read-only crash recovery: inspect current processes, logs, Git changes and checkpoints; preserve unpublished AGY work; determine which jobs can safely resume without duplicates. Check the current state of PR #137 and current main without overwriting this working tree. Report the recovered state and then continue the highest-priority safe work under the agreed local-ingestion and native-only boundaries.
