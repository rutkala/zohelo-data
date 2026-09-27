# ADR 0010: Serialize retained DBW publication before Drive mutation

Date: 22 September 2026. Scope: the dated retained-DBW Bronze query publisher.
Implementation decision within the owner's existing data-preservation and cost boundaries.

## Context

The retained publisher must not race another host when creating content-addressed
objects or promoting a query snapshot. Drive read/replace/read owner checks detect
some interference but are not an atomic acquisition primitive. This increment must
not represent those checks as cross-host exclusion or weaken existing source gates.

## Decision

Use the existing repository's dedicated operational Git ref
`refs/heads/ops-locks/gus-dbw-retained-bronze` as a non-expiring exclusive claim.
A claim is a unique commit recording owner ID, code SHA, process ID and a hashed
Drive-root identity. Its tree is the reviewed code tree; no development branch is
switched and no source file is changed. Acquisition pushes with an explicitly
empty expected ref value. Git's server must accept creation only when that ref is
absent. Release deletes only the exact acquired SHA using an explicit lease.

Acquire the claim before constructing the Drive store, and retain it across every
finite publication increment. Verify ownership before each increment. The existing
Drive owner record remains an additional diagnostic/recovery guard, not the serializer.
A network-ambiguous acquisition is accepted only after reading back the exact own
claim. An unconfirmed claim remains fail-closed, with no time-based takeover.

Reference: [Git push explicit leases](https://git-scm.com/docs/git-push).

## Recovery and consequences

Before recovering an abandoned operational ref, establish that its exact recorded
owner stopped and that no publisher is active on any authorized execution host.
Do not delete a claim just because it is old. Read and retain its SHA and commit
record, then delete only that exact SHA with an explicit lease. If a Drive owner
record also remains held, acquire a fresh Git claim through the supported CLI and
use its exact `--recover-stale-owner` and `--recovery-identity` options before resume.
Never include this operational ref in routine feature-branch cleanup.

The production CLI also authenticates the pinned, reviewed inventory and audit-report
hashes before acquiring either owner or creating Drive namespaces. Local fixtures
remain possible without pretending that arbitrary self-consistent data is approved
for production. Every source byte and previously published snapshot is preserved.

This uses the existing authenticated Git remote and adds no paid service. Failure
of that remote pauses publication safely. The lock applies to this publisher only;
it does not fix or replace other sources' current coordination protocols. Release
and source workflows are scoped to main or explicit dispatch, not this operational ref.

Real local-bare-Git tests cover simultaneous empty-ref contenders, ordinary release,
reacquisition and refusal to delete a changed claim. Live publication/deployment and
SQL acceptance remain separate evidence in `docs/deliverables.md`.
