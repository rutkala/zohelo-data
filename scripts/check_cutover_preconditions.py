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
import re
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
    "deploy.yml",
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


def _header(headers: dict, name: str) -> str:
    for key, value in headers.items():
        if str(key).lower() == name.lower():
            return str(value)
    return ""


def _next_link(link_header: str) -> str | None:
    found: list[str] = []
    for part in link_header.split(","):
        sections = [section.strip() for section in part.split(";")]
        if not sections or not sections[0].startswith("<") or not sections[0].endswith(">"):
            continue
        rels = {
            token.strip().strip('"')
            for section in sections[1:]
            if "=" in section
            for key, token in [section.split("=", 1)]
            if key.strip().lower() == "rel"
        }
        if "next" in rels:
            found.append(sections[0][1:-1])
    if len(found) > 1:
        raise ValueError("multiple rel=next links")
    return found[0] if found else None


def _list_all_runs(repo: str, token: str | None) -> tuple[list[dict], str | None]:
    """Read every active run; reject partial, repeated, or malformed pagination."""
    headers = _github_headers(token)
    runs: list[dict] = []
    seen_ids: set[int] = set()
    try:
        for status in sorted(ACTIVE_STATUSES):
            page = 1
            expected_total: int | None = None
            status_count = 0
            while page <= 100:
                url = (
                    f"https://api.github.com/repos/{repo}/actions/runs"
                    f"?status={urllib.parse.quote(status)}&per_page=100&page={page}"
                )
                data, response_headers = _json_request(url, headers)
                if not isinstance(data, dict):
                    return [], "malformed_runs_response"
                total_count = data.get("total_count")
                batch = data.get("workflow_runs")
                if (
                    not isinstance(total_count, int)
                    or isinstance(total_count, bool)
                    or total_count < 0
                    or not isinstance(batch, list)
                    or len(batch) > 100
                ):
                    return [], "malformed_runs_response"
                if expected_total is None:
                    expected_total = total_count
                elif total_count != expected_total:
                    return [], "changing_total_count"
                for run in batch:
                    if not isinstance(run, dict) or not isinstance(run.get("id"), int):
                        return [], "malformed_active_run"
                    if run["id"] in seen_ids:
                        return [], "repeated_active_run"
                    seen_ids.add(run["id"])
                    runs.append(run)
                    status_count += 1
                next_url = _next_link(_header(response_headers, "link"))
                if status_count == expected_total:
                    if next_url is not None:
                        return [], "unexpected_next_page"
                    break
                if status_count > expected_total or not batch:
                    return [], "partial_active_runs_response"
                expected_url = (
                    f"https://api.github.com/repos/{repo}/actions/runs"
                    f"?status={urllib.parse.quote(status)}&per_page=100&page={page + 1}"
                )
                if next_url != expected_url:
                    return [], "partial_active_runs_response"
                page += 1
            else:
                return [], "pagination_loop_or_limit"
        return runs, None
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
        raw_path = run.get("path")
        status = run.get("status")
        head_sha = run.get("head_sha")
        if (
            not isinstance(raw_path, str)
            or not raw_path.startswith(".github/workflows/")
            or Path(raw_path).name != raw_path.removeprefix(".github/workflows/")
            or status not in ACTIVE_STATUSES
            or not isinstance(head_sha, str)
            or re.fullmatch(r"[0-9a-fA-F]{40}", head_sha) is None
        ):
            return {
                "status": "malformed_active_run",
                "active_critical_runs_count": -1,
                "clean": False,
            }
        path = Path(raw_path).name
        if path not in PUBLISHER_WORKFLOWS:
            continue
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

    portal = check_deployed_portal(
        args.portal_url, args.repo, args.compatibility_commit, token
    )
    report["checks"]["deployed_portal"] = portal
    if not portal["compatible"]:
        report["preconditions_met"] = False
        report["blocking_reason"] = "portal deployment lacks reviewed canonical-layout support"

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
