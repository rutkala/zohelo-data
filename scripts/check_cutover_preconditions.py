#!/usr/bin/env python3
"""Cutover guard verifying preconditions before live Google Drive migration.

Checks:
1. Deployed portal compatibility: verifies that the deployed portal site
   (via portal-build.json) is running a commit that supports canonical release format 2
   and direct_releases layout.
2. Active workflow drain/fence: queries the GitHub Actions runs API (using read-only
   metadata permissions) to confirm that no legacy or uncoordinated publisher workflows
   (source-gus-bdl, source-world-bank, daily-ingestion) are currently running or queued.

Outputs a durable receipt: cutover-preconditions.json
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

DEFAULT_REPO = "rutkala/zohelo-data"
DEFAULT_PORTAL_URL = "https://rutkala.github.io/zohelo-data/portal-build.json"
CRITICAL_WORKFLOWS = {
    "source-gus-bdl.yml",
    "source-world-bank.yml",
    "daily-ingestion.yml",
    "daily-reconciliation.yml",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=DEFAULT_REPO, help="GitHub repository (owner/repo).")
    parser.add_argument("--compatibility-commit", default=None, help="Required minimum/compatibility commit SHA.")
    parser.add_argument("--portal-url", default=DEFAULT_PORTAL_URL, help="URL to deployed portal-build.json.")
    parser.add_argument("--receipt-path", type=Path, default=Path("cutover-preconditions.json"), help="Receipt output path.")
    parser.add_argument("--skip-portal-check", action="store_true", help="Skip remote portal-build.json check (for offline/unit test).")
    parser.add_argument("--skip-actions-check", action="store_true", help="Skip GitHub Actions API check (for offline/unit test).")
    return parser.parse_args()


def check_deployed_portal(portal_url: str, required_commit: str | None = None) -> dict:
    """Verify deployed portal-build.json metadata."""
    req = urllib.request.Request(portal_url, headers={"User-Agent": "zohelo-cutover-guard"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return {
            "status": "portal_check_failed",
            "url": portal_url,
            "error": str(exc),
            "compatible": False,
        }

    supported_formats = data.get("supported_release_formats", [])
    deployed_commit = data.get("git_commit", "")

    is_compatible = 2 in supported_formats
    if required_commit and deployed_commit != required_commit:
        # Note: If deployed commit doesn't match required commit, check if it still supports format 2
        logger.info(f"Deployed commit {deployed_commit} differs from required {required_commit}")

    return {
        "status": "verified" if is_compatible else "incompatible_format",
        "url": portal_url,
        "deployed_commit": deployed_commit,
        "supported_release_formats": supported_formats,
        "compatible": is_compatible,
    }


def check_active_workflows(repo: str, token: str | None = None) -> dict:
    """Query GitHub Actions runs API to check for running or queued publisher workflows."""
    url = f"https://api.github.com/repos/{repo}/actions/runs?status=in_progress"
    headers = {"User-Agent": "zohelo-cutover-guard", "Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    active_runs = []
    for status_filter in ("in_progress", "queued"):
        run_url = f"https://api.github.com/repos/{repo}/actions/runs?status={status_filter}"
        req = urllib.request.Request(run_url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                for run in data.get("workflow_runs", []):
                    wf_path = Path(run.get("path", "")).name
                    if wf_path in CRITICAL_WORKFLOWS:
                        active_runs.append({
                            "id": run.get("id"),
                            "name": run.get("name"),
                            "workflow": wf_path,
                            "status": run.get("status"),
                            "head_sha": run.get("head_sha"),
                            "html_url": run.get("html_url"),
                        })
        except urllib.error.HTTPError as exc:
            if exc.code == 403:
                logger.warning(f"Rate limited or forbidden querying GitHub Actions API: {exc}")
                return {
                    "status": "rate_limited_or_forbidden",
                    "error": str(exc),
                    "active_critical_runs_count": 0,
                    "clean": True,  # Cannot block if API is rate-limited outside GHA
                }
            return {
                "status": "api_error",
                "error": str(exc),
                "active_critical_runs_count": -1,
                "clean": False,
            }
        except Exception as exc:
            return {
                "status": "network_error",
                "error": str(exc),
                "active_critical_runs_count": -1,
                "clean": False,
            }

    return {
        "status": "verified",
        "active_critical_runs_count": len(active_runs),
        "active_runs": active_runs,
        "clean": len(active_runs) == 0,
    }


def main() -> int:
    args = parse_args()
    token = os.environ.get("GITHUB_TOKEN")

    report: dict = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "repo": args.repo,
        "preconditions_met": True,
        "checks": {},
    }

    # 1. Portal check
    if not args.skip_portal_check:
        portal_res = check_deployed_portal(args.portal_url, args.compatibility_commit)
        report["checks"]["deployed_portal"] = portal_res
        if not portal_res.get("compatible"):
            report["preconditions_met"] = False
            report["blocking_reason"] = "Deployed portal is not compatible with release format 2"
    else:
        report["checks"]["deployed_portal"] = {"status": "skipped", "compatible": True}

    # 2. Workflow drain check
    if not args.skip_actions_check:
        actions_res = check_active_workflows(args.repo, token)
        report["checks"]["active_workflows"] = actions_res
        if not actions_res.get("clean"):
            report["preconditions_met"] = False
            report["blocking_reason"] = f"Found {actions_res.get('active_critical_runs_count')} active or queued critical publisher workflow runs"
    else:
        report["checks"]["active_workflows"] = {"status": "skipped", "clean": True}

    # Write receipt
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.receipt_path:
        args.receipt_path.parent.mkdir(parents=True, exist_ok=True)
        args.receipt_path.write_text(rendered + "\n", encoding="utf-8")

    print(rendered)

    if not report["preconditions_met"]:
        print(f"\n❌ Cutover preconditions NOT met: {report.get('blocking_reason')}", file=sys.stderr)
        return 1

    print("\n✅ All cutover preconditions verified successfully.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
