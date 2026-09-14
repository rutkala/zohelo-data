#!/usr/bin/env python3
"""Validate migration workflow inputs and always write a local receipt."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import re

MUTATING = {"apply", "resume", "rollback"}

def validate(operation, confirmed, compatibility_commit, plan_run_id, plan_id, plan_sha256):
    errors = []
    if operation in MUTATING:
        if not confirmed:
            errors.append("explicit confirmation is required")
        if not re.fullmatch(r"[0-9a-f]{40}", compatibility_commit):
            errors.append("compatibility commit must be a lowercase 40-character SHA")
        if not re.fullmatch(r"[1-9][0-9]*", plan_run_id):
            errors.append("plan run ID must be a positive decimal integer")
        if not re.fullmatch(r"plan-[a-z0-9-]{1,80}", plan_id):
            errors.append("plan ID is malformed")
        if not re.fullmatch(r"[0-9a-f]{64}", plan_sha256):
            errors.append("plan SHA-256 must be lowercase hexadecimal")
    return errors

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", required=True)
    parser.add_argument("--confirmed", required=True)
    parser.add_argument("--compatibility-commit", default="")
    parser.add_argument("--plan-run-id", default="")
    parser.add_argument("--plan-id", default="")
    parser.add_argument("--plan-sha256", default="")
    parser.add_argument("--output", default="dispatch-preconditions.json")
    args = parser.parse_args()
    errors = validate(
        args.operation, args.confirmed == "true", args.compatibility_commit,
        args.plan_run_id, args.plan_id, args.plan_sha256,
    )
    result = {
        "status": "dispatch_inputs_verified" if not errors else "dispatch_inputs_rejected",
        "operation": args.operation, "errors": errors,
    }
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if errors else 0

if __name__ == "__main__":
    raise SystemExit(main())
