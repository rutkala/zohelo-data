#!/usr/bin/env python3
"""Strict fail-closed guard verifying that BDL producer workflow is disabled with zero active runs."""
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
BDL_WORKFLOW = "source-gus-bdl.yml"
ACTIVE_STATUSES = {"queued", "in_progress", "pending", "waiting", "requested"}


def _github_headers(token: str | None) -> dict[str, str]:
    headers = {
        "User-Agent": "zohelo-bdl-precondition-guard",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _json_request(url: str, headers: dict[str, str], timeout: int = 15) -> tuple[dict, dict]:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8")), dict(response.headers.items())


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


def check_bdl_producer(repo: str, token: str | None) -> dict:
    headers = _github_headers(token)

    # 1. Check workflow state (must exist and be disabled)
    wf_url = f"https://api.github.com/repos/{repo}/actions/workflows/{BDL_WORKFLOW}"
    try:
        wf_data, _ = _json_request(wf_url, headers)
        if not isinstance(wf_data, dict):
            return {"status": "malformed_response", "workflow": BDL_WORKFLOW, "clean": False, "error": "malformed workflow json"}
        state = wf_data.get("state")
        if state not in ("disabled_manually", "disabled_inactivity"):
            return {
                "status": "workflow_not_disabled",
                "workflow": BDL_WORKFLOW,
                "workflow_state": state,
                "disabled": False,
                "clean": False,
                "error": f"Workflow is not disabled (state: '{state}')",
            }
    except Exception as exc:
        # Fails closed on 404, network error, timeout, permission error
        return {
            "status": "workflow_check_failed",
            "workflow": BDL_WORKFLOW,
            "clean": False,
            "error": f"Failed to check workflow {BDL_WORKFLOW}: {exc}",
        }

    # 2. Check for active or pending runs using strict pagination
    active_runs = []
    try:
        for status in sorted(ACTIVE_STATUSES):
            page = 1
            expected_total = None
            status_count = 0
            while page <= 50:
                url = (
                    f"https://api.github.com/repos/{repo}/actions/workflows/{BDL_WORKFLOW}/runs"
                    f"?status={urllib.parse.quote(status)}&per_page=100&page={page}"
                )
                data, response_headers = _json_request(url, headers)
                if not isinstance(data, dict):
                    return {"status": "malformed_runs_response", "clean": False, "error": "runs response is not an object"}
                total_count = data.get("total_count")
                batch = data.get("workflow_runs")
                if not isinstance(total_count, int) or not isinstance(batch, list):
                    return {"status": "malformed_runs_response", "clean": False, "error": "runs payload missing total_count or workflow_runs"}
                if expected_total is None:
                    expected_total = total_count
                elif total_count != expected_total:
                    return {"status": "changing_total_count", "clean": False, "error": "total_count shifted during pagination"}

                for run in batch:
                    if not isinstance(run, dict) or not isinstance(run.get("id"), int):
                        return {"status": "malformed_run", "clean": False, "error": "run item malformed"}
                    active_runs.append({
                        "id": run["id"],
                        "status": run.get("status"),
                        "head_sha": run.get("head_sha"),
                        "created_at": run.get("created_at"),
                    })
                    status_count += 1

                next_url = _next_link(_header(response_headers, "link"))
                if status_count == expected_total or not batch:
                    break
                page += 1
    except Exception as exc:
        return {
            "status": "active_runs_check_failed",
            "workflow": BDL_WORKFLOW,
            "clean": False,
            "error": f"Failed checking active runs for {BDL_WORKFLOW}: {exc}",
        }

    clean = (len(active_runs) == 0)
    return {
        "status": "verified" if clean else "active_runs_detected",
        "workflow": BDL_WORKFLOW,
        "workflow_state": state,
        "disabled": True,
        "active_runs_count": len(active_runs),
        "active_runs": active_runs,
        "clean": clean,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--receipt-path", type=Path, default=Path("bdl-reset-preconditions.json"))
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN")
    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "repo": args.repo,
        "preconditions_met": True,
        "checks": {},
    }

    producer_check = check_bdl_producer(args.repo, token)
    report["checks"]["bdl_producer"] = producer_check

    if not producer_check.get("clean"):
        report["preconditions_met"] = False
        report["blocking_reason"] = producer_check.get("error") or f"Found {producer_check.get('active_runs_count')} active/queued runs"

    rendered = json.dumps(report, indent=2, sort_keys=True)
    args.receipt_path.parent.mkdir(parents=True, exist_ok=True)
    args.receipt_path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)

    if not report["preconditions_met"]:
        print("BDL reset preconditions NOT met: active producer or enabled workflow", file=sys.stderr)
        return 1

    print("BDL reset preconditions verified: workflow disabled and no active runs", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
