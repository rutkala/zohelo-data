#!/usr/bin/env python3
"""Check repository-owned GitHub Actions safety invariants.

This intentionally checks a small policy surface that GitHub's YAML parser does
not enforce: immutable action references, explicit least-privilege permissions,
bounded runtimes, and main-branch guards around manually dispatched jobs that
receive secrets or write-capable tokens.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Iterable

import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIRECTORY = REPOSITORY_ROOT / ".github" / "workflows"
FULL_COMMIT_REF = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
MAIN_REF_EXPRESSION = re.compile(
    r"github\.ref\s*==\s*(['\"])refs/heads/main\1"
)


def _mapping(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _walk(value: Any) -> Iterable[tuple[str | None, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            yield str(key), child
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield None, child
            yield from _walk(child)


def _all_text(value: Any) -> str:
    return "\n".join(
        str(child)
        for _, child in _walk(value)
        if isinstance(child, str)
    )


def _effective_permissions(
    workflow_permissions: dict[str, Any] | None, job: dict[str, Any]
) -> dict[str, Any] | None:
    if "permissions" in job:
        return _mapping(job.get("permissions"))
    return workflow_permissions


def _has_write_permission(permissions: dict[str, Any] | None) -> bool:
    return bool(permissions) and any(value == "write" for value in permissions.values())


def _check_action_reference(path: Path, reference: str, errors: list[str]) -> None:
    if reference.startswith("./"):
        return
    if reference.startswith("docker://"):
        if "@sha256:" not in reference:
            errors.append(f"{path}: container action must use an immutable sha256 digest: {reference}")
        return
    if not FULL_COMMIT_REF.fullmatch(reference):
        errors.append(f"{path}: action must use a full 40-character commit SHA: {reference}")


def validate_workflow(path: Path, document: Any) -> list[str]:
    """Return policy violations for one BaseLoader-parsed workflow."""

    errors: list[str] = []
    workflow = _mapping(document)
    if workflow is None:
        return [f"{path}: workflow root must be a mapping"]

    triggers = _mapping(workflow.get("on"))
    if triggers is None:
        errors.append(f"{path}: 'on' must be an explicit event mapping")
        triggers = {}
    if "pull_request_target" in triggers:
        errors.append(f"{path}: pull_request_target is not permitted")

    jobs = _mapping(workflow.get("jobs"))
    if not jobs:
        errors.append(f"{path}: jobs must be a non-empty mapping")
        return errors

    workflow_permissions = _mapping(workflow.get("permissions"))
    if workflow.get("permissions") is not None and workflow_permissions is None:
        errors.append(f"{path}: workflow permissions must be an explicit mapping")
    if _has_write_permission(workflow_permissions):
        errors.append(f"{path}: write permissions must be scoped to the job that needs them")

    has_manual_dispatch = "workflow_dispatch" in triggers
    is_pull_request = "pull_request" in triggers

    for job_name, raw_job in jobs.items():
        job = _mapping(raw_job)
        if job is None:
            errors.append(f"{path}: job {job_name!r} must be a mapping")
            continue

        permissions = _effective_permissions(workflow_permissions, job)
        if "permissions" in job and _mapping(job.get("permissions")) is None:
            errors.append(f"{path}: job {job_name!r} permissions must be an explicit mapping")
        if permissions is None:
            errors.append(f"{path}: job {job_name!r} has no explicit effective permissions")
        elif is_pull_request and _has_write_permission(permissions):
            errors.append(f"{path}: pull-request job {job_name!r} must not have write permissions")

        if "uses" not in job:
            timeout = job.get("timeout-minutes")
            if timeout is None:
                errors.append(f"{path}: job {job_name!r} must set timeout-minutes")
            else:
                try:
                    timeout_value = int(timeout)
                except (TypeError, ValueError):
                    errors.append(f"{path}: job {job_name!r} timeout-minutes must be an integer")
                else:
                    if not 1 <= timeout_value <= 360:
                        errors.append(f"{path}: job {job_name!r} timeout-minutes must be between 1 and 360")

        job_text = _all_text(job)
        is_sensitive = (
            "secrets." in job_text
            or job.get("secrets") == "inherit"
            or _has_write_permission(permissions)
        )
        if has_manual_dispatch and is_sensitive:
            condition = str(job.get("if", ""))
            if not MAIN_REF_EXPRESSION.search(condition):
                errors.append(
                    f"{path}: manually dispatched sensitive job {job_name!r} must require the main ref"
                )

        for key, value in _walk(job):
            if key != "uses" or not isinstance(value, str):
                continue
            _check_action_reference(path, value, errors)
            if value.startswith("actions/checkout@"):
                for step in job.get("steps", []):
                    if not isinstance(step, dict) or step.get("uses") != value:
                        continue
                    checkout_options = _mapping(step.get("with")) or {}
                    if checkout_options.get("persist-credentials") != "false":
                        errors.append(
                            f"{path}: checkout in job {job_name!r} must disable persisted credentials"
                        )

    return errors


def check_repository(workflow_directory: Path = WORKFLOW_DIRECTORY) -> list[str]:
    errors: list[str] = []
    workflow_paths = sorted(
        [*workflow_directory.glob("*.yml"), *workflow_directory.glob("*.yaml")]
    )
    if not workflow_paths:
        return [f"{workflow_directory}: no workflow files found"]

    for path in workflow_paths:
        try:
            document = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
        except (OSError, yaml.YAMLError) as exc:
            errors.append(f"{path}: cannot parse workflow: {exc}")
            continue
        errors.extend(validate_workflow(path, document))
    return errors


def main() -> int:
    errors = check_repository()
    if errors:
        print("GitHub Actions policy check failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("GitHub Actions policy check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
