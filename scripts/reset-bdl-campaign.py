#!/usr/bin/env python3
"""Safe administrative CLI to plan, apply, resume, or verify a GUS BDL-only reset on Google Drive.

Default operation is read-only plan. Mutating operations (apply, resume) require:
- Explicit --confirm
- Validated --plan-file from a reviewed read-only plan artifact
- Matching --plan-sha256
- Expected storage root safety pin (--expected-root-id)
- Verification that BDL writer workflow is disabled and has no active/queued runs
- Execution in main Actions runtime with zohelo-production-data concurrency

All deletions use recoverable Google Drive trash only (NEVER permanent delete).
"""
from __future__ import annotations

import argparse
from hashlib import sha256
import json
import logging
import os
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from bdl_reset import (
    ActiveProducerError,
    AmbiguityError,
    BdlResetEngine,
    BdlResetError,
    DriftError,
    SafetyPinError,
    _canonical_digest,
)
from storage_manager import StorageManager

DEFAULT_ROOT_ID = "1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safe, reviewable GUS BDL-only reset on Google Drive",
    )
    parser.add_argument(
        "--operation",
        choices=["plan", "apply", "resume", "verify"],
        default="plan",
        help="plan is read-only (default); apply and resume mutate Drive via recoverable trash",
    )
    parser.add_argument(
        "--expected-root-id",
        default=DEFAULT_ROOT_ID,
        help="Safety pin: expected Google Drive root ID",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Explicit confirmation required for mutating operations (apply, resume)",
    )
    parser.add_argument(
        "--plan-file",
        type=Path,
        help="Path to reviewed plan.json (required for apply, resume, verify)",
    )
    parser.add_argument(
        "--plan-sha256",
        help="Expected immutable reviewed plan SHA-256 digest",
    )
    parser.add_argument(
        "--journal-path",
        type=Path,
        default=Path("bdl-reset-journal.json"),
        help="Path to local journal receipt JSON",
    )
    parser.add_argument(
        "--skip-producer-check",
        action="store_true",
        help="Skip active producer check (permitted ONLY in local test environments)",
    )
    return parser.parse_args()


def write_json(path: str | Path, value: dict) -> None:
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_failure_receipt(args: argparse.Namespace, exc: Exception, status: str) -> dict:
    receipt = {
        "status": status,
        "operation": args.operation,
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "journal_path": str(args.journal_path),
    }
    write_json("bdl-reset-error.json", receipt)
    write_json("operation-result.json", receipt)
    return receipt


def load_reviewed_plan(args: argparse.Namespace) -> dict:
    if args.plan_file is None:
        raise BdlResetError(f"--plan-file is required for operation '{args.operation}'")
    if not args.plan_file.is_file():
        raise BdlResetError(f"Reviewed plan file does not exist: {args.plan_file}")
    try:
        plan = json.loads(args.plan_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise BdlResetError(f"Could not read reviewed plan file: {exc}") from exc

    if not isinstance(plan, dict) or plan.get("status") != "planned":
        raise BdlResetError("Reviewed plan is malformed or status is not 'planned'")
    if plan.get("expected_root_id") != args.expected_root_id or plan.get("root_id") != args.expected_root_id:
        raise SafetyPinError("Reviewed plan root pins do not match --expected-root-id")

    digest = _canonical_digest(plan)
    if not args.plan_sha256:
        raise BdlResetError(f"--plan-sha256 is required for operation '{args.operation}'")
    if args.plan_sha256 != digest:
        raise DriftError(f"Provided --plan-sha256 does not match reviewed plan file digest '{digest}'")

    return plan


def main() -> int:
    args = parse_args()
    logging.disable(logging.CRITICAL)

    mutating = args.operation in {"apply", "resume"}
    if mutating and not args.confirm:
        failure = write_failure_receipt(
            args,
            BdlResetError("Explicit --confirm is required for mutating operations"),
            "operation_rejected",
        )
        print(json.dumps(failure, indent=2, sort_keys=True), file=sys.stderr)
        return 1

    try:
        # Resolve storage manager without interactive auth and without creating roots
        storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
        engine = BdlResetEngine(
            storage=storage,
            expected_root_id=args.expected_root_id,
            journal_local_path=args.journal_path,
        )

        github_token = os.environ.get("GITHUB_TOKEN")

        if args.operation == "plan":
            result = engine.plan()
            write_json("plan.json", result)
            write_json("operation-result.json", result)
        elif args.operation in {"apply", "resume"}:
            plan = load_reviewed_plan(args)
            result = engine.apply(
                plan=plan,
                confirmed=True,
                resume=args.operation == "resume",
                expected_sha256=args.plan_sha256,
                skip_producer_check=args.skip_producer_check,
                github_token=github_token,
            )
            write_json("recovery-receipt.json", result)
            write_json("operation-result.json", result)
        elif args.operation == "verify":
            plan = load_reviewed_plan(args)
            result = engine.verify_post_reset(plan)
            write_json("verification.json", result)
            write_json("operation-result.json", result)
        else:
            raise BdlResetError(f"Unknown operation: {args.operation}")

        rendered = json.dumps(result, indent=2, sort_keys=True)
        print(rendered)

        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(summary).open("a", encoding="utf-8") as handle:
                handle.write(f"\n## GUS BDL Reset ({args.operation})\n\n```json\n{rendered}\n```\n")

        if output := os.environ.get("GITHUB_OUTPUT"):
            with Path(output).open("a", encoding="utf-8") as handle:
                handle.write(f"status={result.get('status', 'unknown')}\n")

        return 0

    except (SafetyPinError, DriftError, AmbiguityError, ActiveProducerError, BdlResetError) as exc:
        failure = write_failure_receipt(args, exc, "bdl_reset_failed")
        print(json.dumps(failure, indent=2, sort_keys=True), file=sys.stderr)
        return 1
    except Exception as exc:
        failure = write_failure_receipt(args, exc, "unexpected_failure")
        print(json.dumps(failure, indent=2, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
