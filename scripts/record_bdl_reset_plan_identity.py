#!/usr/bin/env python3
"""Record immutable identity for a successfully generated GUS BDL reset plan."""
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import re
import sys

WORKFLOW_PATH = ".github/workflows/reset-bdl-campaign.yml"


def canonical_plan_hash(plan: dict) -> str:
    canonical = dict(plan)
    canonical.pop("plan_sha256", None)
    return sha256(json.dumps(
        canonical, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--workflow-commit", required=True)
    parser.add_argument("--output", default="plan-identity.json")
    args = parser.parse_args()

    if not re.fullmatch(r"[1-9][0-9]*", args.run_id):
        raise SystemExit("workflow run ID must be decimal")
    if not re.fullmatch(r"[0-9a-f]{40}", args.workflow_commit):
        raise SystemExit("workflow commit must be a lowercase 40-character SHA")
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", args.repository):
        raise SystemExit("repository must be owner/name")

    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    if not isinstance(plan, dict) or plan.get("status") != "planned":
        raise SystemExit("plan artifact is not a successful planned BDL reset")

    digest = canonical_plan_hash(plan)
    if plan.get("root_id") != plan.get("expected_root_id"):
        raise SystemExit("plan root pins disagree")

    identity = {
        "format_version": 1,
        "repository": args.repository,
        "workflow_path": WORKFLOW_PATH,
        "workflow_run_id": args.run_id,
        "workflow_commit": args.workflow_commit,
        "plan_id": plan.get("plan_id"),
        "plan_sha256": digest,
        "root_id": plan.get("root_id"),
    }
    Path(args.output).write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
