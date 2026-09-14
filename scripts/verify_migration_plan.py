#!/usr/bin/env python3
"""Verify that a migration plan came from the exact successful reviewed workflow run."""
from __future__ import annotations
import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

WORKFLOW_PATH = ".github/workflows/migrate-drive-layout.yml"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
HASH64 = re.compile(r"^[0-9a-f]{64}$")
RUN_ID = re.compile(r"^[1-9][0-9]*$")

class ProvenanceError(RuntimeError):
    pass

def _json_request(url: str, token: str) -> dict[str, Any]:
    request = Request(url, headers={
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer " + token,
        "X-GitHub-Api-Version": "2022-11-28",
    })
    try:
        with urlopen(request, timeout=20) as response:
            value = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise ProvenanceError("Could not verify reviewed workflow run: " + str(exc)) from exc
    if not isinstance(value, dict):
        raise ProvenanceError("Reviewed workflow run response is malformed")
    return value

def _load_object(path: str | Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        raise ProvenanceError("Could not read " + label + ": " + str(exc)) from exc
    if not isinstance(value, dict):
        raise ProvenanceError(label + " must be a JSON object")
    return value

def canonical_plan_hash(plan: dict[str, Any]) -> str:
    canonical = dict(plan)
    embedded = canonical.pop("plan_sha256", None)
    digest = sha256(json.dumps(
        canonical, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()
    if embedded != digest:
        raise ProvenanceError("Plan embedded hash does not match its canonical contents")
    return digest

def verify_provenance(
    *, plan: dict[str, Any], identity: dict[str, Any], run_id: str,
    expected_root_id: str, expected_plan_id: str, expected_plan_sha256: str, repository: str,
    executing_commit: str, token: str,
) -> dict[str, Any]:
    if not RUN_ID.fullmatch(run_id):
        raise ProvenanceError("Plan run ID must be a positive decimal integer")
    if not SHA40.fullmatch(executing_commit):
        raise ProvenanceError("Executing workflow commit must be a lowercase 40-character SHA")
    if not HASH64.fullmatch(expected_plan_sha256):
        raise ProvenanceError("Reviewed plan hash must be a lowercase SHA-256")
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", repository):
        raise ProvenanceError("Repository must be owner/name")
    digest = canonical_plan_hash(plan)
    expected_identity = {
        "format_version": 1,
        "repository": repository,
        "workflow_path": WORKFLOW_PATH,
        "workflow_run_id": run_id,
        "workflow_commit": executing_commit,
        "plan_id": plan.get("plan_id"),
        "plan_sha256": digest,
        "root_id": expected_root_id,
    }
    if identity != expected_identity:
        raise ProvenanceError("Plan identity does not exactly match the reviewed plan and dispatch")
    if (
        plan.get("status") != "planned"
        or plan.get("root_id") != expected_root_id
        or plan.get("expected_root_id") != expected_root_id
        or digest != expected_plan_sha256
        or plan.get("plan_id") != expected_plan_id
        or not isinstance(plan.get("plan_id"), str)
        or not plan["plan_id"]
    ):
        raise ProvenanceError("Reviewed plan pins do not match the migration dispatch")
    run = _json_request(
        "https://api.github.com/repos/" + repository + "/actions/runs/" + run_id,
        token,
    )
    run_repository = run.get("repository")
    actual = {
        "id": str(run.get("id")) if isinstance(run.get("id"), int) else None,
        "repository": run_repository.get("full_name") if isinstance(run_repository, dict) else None,
        "path": run.get("path"),
        "event": run.get("event"),
        "head_branch": run.get("head_branch"),
        "status": run.get("status"),
        "conclusion": run.get("conclusion"),
        "head_sha": run.get("head_sha"),
    }
    required = {
        "id": run_id, "repository": repository, "path": WORKFLOW_PATH,
        "event": "workflow_dispatch", "head_branch": "main",
        "status": "completed", "conclusion": "success",
        "head_sha": executing_commit,
    }
    if actual != required:
        raise ProvenanceError("Plan run is not the exact successful main migration workflow")
    return {
        "status": "reviewed_plan_verified",
        "repository": repository,
        "workflow_path": WORKFLOW_PATH,
        "workflow_run_id": run_id,
        "workflow_commit": executing_commit,
        "plan_id": plan["plan_id"],
        "plan_sha256": digest,
        "root_id": expected_root_id,
    }

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-root-id", required=True)
    parser.add_argument("--expected-plan-id", required=True)
    parser.add_argument("--expected-plan-sha256", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--executing-commit", required=True)
    parser.add_argument("--output", default="plan-provenance.json")
    args = parser.parse_args()
    try:
        result = verify_provenance(
            plan=_load_object(args.plan, "reviewed plan"),
            identity=_load_object(args.identity, "plan identity"),
            run_id=args.run_id,
            expected_root_id=args.expected_root_id,
            expected_plan_id=args.expected_plan_id,
            expected_plan_sha256=args.expected_plan_sha256,
            repository=args.repository,
            executing_commit=args.executing_commit,
            token=os.environ.get("GITHUB_TOKEN", ""),
        )
    except ProvenanceError as exc:
        result = {"status": "reviewed_plan_rejected", "error": str(exc)}
        Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
        print(json.dumps(result, indent=2, sort_keys=True), file=sys.stderr)
        return 1
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
