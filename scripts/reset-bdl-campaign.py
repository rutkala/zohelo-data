#!/usr/bin/env python3
"""Safe administrative CLI to plan, apply, resume, or verify a GUS BDL-only reset on Google Drive.

Default operation is read-only plan. Mutating operations (apply, resume) require:
- Explicit --confirm
- Validated --plan-file from a reviewed read-only plan artifact
- Matching --plan-sha256
- Expected storage root safety pin (--expected-root-id)
- Verification that BDL writer workflow is disabled and has no active/queued runs
- Execution in main Actions runtime with zohelo-production-data concurrency

All deletions mutate exact BDL root folders only via recoverable Google Drive trash.
Descendants inherit trashed status automatically.
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
    RuntimeGuardError,
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


def format_concise_summary(operation: str, result: dict) -> str:
    """Format concise Markdown summary avoiding 36k-line payload in CI output."""
    plan_id = result.get("plan_id", "N/A")
    plan_sha = result.get("plan_sha256", "N/A")
    status = result.get("status", "N/A")

    lines = [
        f"## GUS BDL Reset ({operation})",
        "",
        f"- **Status:** `{status}`",
        f"- **Plan ID:** `{plan_id}`",
        f"- **Plan SHA-256:** `{plan_sha}`",
    ]

    if "counts_by_root" in result:
        lines.append("")
        lines.append("| Root Path | Object Count | Total Bytes |")
        lines.append("|---|---|---|")
        for root_key, count in sorted(result["counts_by_root"].items()):
            b = result.get("bytes_by_root", {}).get(root_key, 0)
            lines.append(f"| `{root_key}` | {count:,} | {b:,} |")
        lines.append("")
        lines.append(f"**Total Objects:** {result.get('total_object_count', 0):,} | **Total Bytes:** {result.get('total_bytes', 0):,}")
        lines.append(f"**Non-BDL Baseline Preserved:** {result.get('non_bdl_baseline_count', 0)} structures")
    elif "roots_trashed" in result:
        lines.append(f"- **Roots Trashed:** {result.get('roots_trashed')}")
        lines.append(f"- **Descendants Inherited Trash:** {result.get('total_descendants_inherited_trash', 0):,}")
        lines.append(f"- **Remote Plan Folder:** `{result.get('remote_plan_folder')}`")

    lines.append("")
    lines.append("Full artifact details written to `plan.json` / `recovery-receipt.json`.")
    return "\n".join(lines) + "\n"


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

        # Render concise output to stdout and step summary
        summary_md = format_concise_summary(args.operation, result)
        print(summary_md)

        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(summary).open("a", encoding="utf-8") as handle:
                handle.write(summary_md)

        if output := os.environ.get("GITHUB_OUTPUT"):
            with Path(output).open("a", encoding="utf-8") as handle:
                handle.write(f"status={result.get('status', 'unknown')}\n")
                handle.write(f"plan_id={result.get('plan_id', '')}\n")
                handle.write(f"plan_sha256={result.get('plan_sha256', '')}\n")

        return 0

    except (SafetyPinError, DriftError, AmbiguityError, ActiveProducerError, RuntimeGuardError, BdlResetError) as exc:
        failure = write_failure_receipt(args, exc, "bdl_reset_failed")
        print(json.dumps(failure, indent=2, sort_keys=True), file=sys.stderr)
        return 1
    except Exception as exc:
        failure = write_failure_receipt(args, exc, "unexpected_failure")
        print(json.dumps(failure, indent=2, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
