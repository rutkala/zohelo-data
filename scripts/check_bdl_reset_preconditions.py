#!/usr/bin/env python3
"""Verify pre-reset preconditions: BDL producer workflow is disabled and has no active runs."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import urllib.error
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


def check_bdl_producer(repo: str, token: str | None) -> dict:
    headers = _github_headers(token)

    # 1. Check workflow state
    wf_url = f"https://api.github.com/repos/{repo}/actions/workflows/{BDL_WORKFLOW}"
    req = urllib.request.Request(wf_url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            wf_data = json.loads(response.read().decode("utf-8"))
            state = wf_data.get("state")
            is_disabled = state in ("disabled_manually", "disabled_inactivity")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            state = "not_found"
            is_disabled = True
        else:
            return {
                "status": f"http_{exc.code}",
                "workflow": BDL_WORKFLOW,
                "disabled": False,
                "error": str(exc),
            }
    except Exception as exc:
        return {
            "status": "request_failed",
            "workflow": BDL_WORKFLOW,
            "disabled": False,
            "error": str(exc),
        }

    # 2. Check for active or pending runs of this workflow
    active_runs = []
    for status in sorted(ACTIVE_STATUSES):
        runs_url = f"https://api.github.com/repos/{repo}/actions/workflows/{BDL_WORKFLOW}/runs?status={status}"
        runs_req = urllib.request.Request(runs_url, headers=headers)
        try:
            with urllib.request.urlopen(runs_req, timeout=15) as response:
                runs_data = json.loads(response.read().decode("utf-8"))
                for run in runs_data.get("workflow_runs", []):
                    active_runs.append({
                        "id": run.get("id"),
                        "status": run.get("status"),
                        "head_sha": run.get("head_sha"),
                        "created_at": run.get("created_at"),
                    })
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                break
            return {
                "status": f"http_{exc.code}",
                "workflow": BDL_WORKFLOW,
                "disabled": is_disabled,
                "active_runs_count": -1,
                "error": str(exc),
            }
        except Exception as exc:
            return {
                "status": "request_failed",
                "workflow": BDL_WORKFLOW,
                "disabled": is_disabled,
                "active_runs_count": -1,
                "error": str(exc),
            }

    clean = is_disabled and len(active_runs) == 0
    return {
        "status": "verified" if clean else "producer_active_or_enabled",
        "workflow": BDL_WORKFLOW,
        "workflow_state": state,
        "disabled": is_disabled,
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
        reasons = []
        if not producer_check.get("disabled"):
            reasons.append(f"workflow {BDL_WORKFLOW} is not disabled (state: {producer_check.get('workflow_state')})")
        if producer_check.get("active_runs_count", 0) > 0:
            reasons.append(f"found {producer_check['active_runs_count']} active/queued runs of {BDL_WORKFLOW}")
        report["blocking_reason"] = "; ".join(reasons)

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
