#!/usr/bin/env python3
"""Fail-closed cutover guard for the reviewed Drive layout migration.

The guard performs read-only portal and Actions API checks.  It is intentionally
strict: an unavailable API, missing compatibility evidence, or an unknown active
publisher blocks a live migration.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_REPO = "rutkala/zohelo-data"
DEFAULT_PORTAL_URL = "https://data.zohelo.com/portal-build.json"
CANONICAL_LAYOUT_MARKER = "canonical-release-roots-v1"
ACTIVE_STATUSES = {"queued", "in_progress", "pending", "waiting", "requested"}
PUBLISHER_WORKFLOWS = {
    "source-gus-bdl.yml",
    "source-world-bank.yml",
    "daily-ingestion.yml",
    "daily-reconciliation.yml",
    "platform_transform_and_release.yml",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument(
        "--compatibility-commit", required=True,
        help="Reviewed portal compatibility commit; deployed code must equal or descend from it.",
    )
    parser.add_argument("--portal-url", default=DEFAULT_PORTAL_URL)
    parser.add_argument(
        "--receipt-path", type=Path, default=Path("cutover-preconditions.json")
    )
    parser.add_argument("--skip-portal-check", action="store_true")
    parser.add_argument("--skip-actions-check", action="store_true")
    return parser.parse_args()


def _json_request(url: str, headers: dict[str, str], timeout: int = 15) -> tuple[dict, dict]:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8")), dict(response.headers.items())


def _github_headers(token: str | None) -> dict[str, str]:
    headers = {
        "User-Agent": "zohelo-cutover-guard",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def commit_is_same_or_descendant(repo: str, required: str, candidate: str,
                                 token: str | None) -> tuple[bool, str]:
    """Return whether candidate is the reviewed commit or a verified descendant."""
    if not required or not candidate:
        return False, "missing_commit"
    if candidate == required:
        return True, "exact"
    url = (
        f"https://api.github.com/repos/{repo}/compare/"
        f"{urllib.parse.quote(required, safe='')}...{urllib.parse.quote(candidate, safe='')}"
    )
    try:
        data, _ = _json_request(url, _github_headers(token))
    except Exception as exc:
        return False, f"compare_failed:{exc}"
    status = data.get("status")
    if status in {"identical", "ahead"}:
        return True, status
    return False, f"not_descendant:{status}"


def check_deployed_portal(portal_url: str, repo: str, required_commit: str,
                          token: str | None) -> dict:
    try:
        data, _ = _json_request(
            portal_url, {"User-Agent": "zohelo-cutover-guard"}, timeout=10
        )
    except Exception as exc:
        return {
            "status": "portal_check_failed", "url": portal_url, "error": str(exc),
            "compatible": False,
        }

    deployed = str(data.get("git_commit") or "")
    layouts = data.get("supported_drive_layouts")
    if not isinstance(layouts, list):
        layouts = []
    descendant, relation = commit_is_same_or_descendant(
        repo, required_commit, deployed, token
    )
    compatible = CANONICAL_LAYOUT_MARKER in layouts and descendant
    return {
        "status": "verified" if compatible else "incompatible",
        "url": portal_url,
        "deployed_commit": deployed,
        "required_commit": required_commit,
        "commit_relation": relation,
        "supported_release_formats": data.get("supported_release_formats", []),
        "supported_drive_layouts": layouts,
        "compatible": compatible,
    }


def _list_all_runs(repo: str, token: str | None) -> tuple[list[dict], str | None]:
    """Read all run pages, refusing repeated page tokens or malformed responses."""
    headers = _github_headers(token)
    runs: list[dict] = []
    page = 1
    seen_pages: set[int] = set()
    try:
        while True:
            if page in seen_pages or page > 100:
                return [], "pagination_loop_or_limit"
            seen_pages.add(page)
            url = (
                f"https://api.github.com/repos/{repo}/actions/runs"
                f"?per_page=100&page={page}"
            )
            data, _ = _json_request(url, headers)
            batch = data.get("workflow_runs")
            if not isinstance(batch, list):
                return [], "malformed_runs_response"
            runs.extend(batch)
            if len(batch) < 100:
                return runs, None
            page += 1
    except urllib.error.HTTPError as exc:
        return [], f"http_{exc.code}:{exc}"
    except Exception as exc:
        return [], f"request_failed:{exc}"


def check_active_workflows(repo: str, token: str | None,
                           compatibility_commit: str) -> dict:
    runs, error = _list_all_runs(repo, token)
    if error:
        return {
            "status": "api_unavailable", "error": error,
            "active_critical_runs_count": -1, "clean": False,
        }

    blockers: list[dict] = []
    for run in runs:
        path = Path(str(run.get("path") or "")).name
        status = str(run.get("status") or "")
        if path not in PUBLISHER_WORKFLOWS or status not in ACTIVE_STATUSES:
            continue
        head_sha = str(run.get("head_sha") or "")
        compatible, relation = commit_is_same_or_descendant(
            repo, compatibility_commit, head_sha, token
        )
        # Compatible jobs use the shared migration/publication lock and are
        # serialized behind this job; only old/unknown publishers must drain.
        if compatible:
            continue
        blockers.append({
            "id": run.get("id"), "workflow": path, "status": status,
            "head_sha": head_sha, "compatibility_relation": relation,
            "html_url": run.get("html_url"),
        })

    return {
        "status": "verified",
        "active_critical_runs_count": len(blockers),
        "active_runs": blockers,
        "clean": not blockers,
    }


def main() -> int:
    args = parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    report: dict = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "repo": args.repo,
        "required_compatibility_commit": args.compatibility_commit,
        "preconditions_met": True,
        "checks": {},
    }

    if args.skip_portal_check:
        report["checks"]["deployed_portal"] = {"status": "skipped", "compatible": True}
    else:
        portal = check_deployed_portal(
            args.portal_url, args.repo, args.compatibility_commit, token
        )
        report["checks"]["deployed_portal"] = portal
        if not portal["compatible"]:
            report["preconditions_met"] = False
            report["blocking_reason"] = "portal deployment lacks reviewed canonical-layout support"

    if args.skip_actions_check:
        report["checks"]["active_workflows"] = {"status": "skipped", "clean": True}
    else:
        actions = check_active_workflows(args.repo, token, args.compatibility_commit)
        report["checks"]["active_workflows"] = actions
        if not actions["clean"]:
            report["preconditions_met"] = False
            report["blocking_reason"] = (
                f"found {actions['active_critical_runs_count']} old or unverified publisher runs"
            )

    rendered = json.dumps(report, indent=2, sort_keys=True)
    args.receipt_path.parent.mkdir(parents=True, exist_ok=True)
    args.receipt_path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    if not report["preconditions_met"]:
        print("Cutover preconditions NOT met", file=sys.stderr)
        return 1
    print("Cutover preconditions verified", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
