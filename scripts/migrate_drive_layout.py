#!/usr/bin/env python3
"""CLI tool for planning, applying, resuming, rolling back, and verifying Google Drive consolidation.

Default execution is strictly bounded read-only plan (--dry-run).
Explicit --apply requires opt-in confirmation and safety pin --expected-root-id.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from drive_migration import (  # noqa: E402
    DriveMigrationEngine,
    DriftError,
    MigrationError,
    SafetyPinError,
)
from storage_manager import StorageManager  # noqa: E402

PRODUCTION_ROOT_ID = "1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operation",
        choices=["plan", "apply", "resume", "rollback", "verify"],
        default="plan",
        help="Operation to perform: plan (dry run), apply (execute), resume (interrupted), rollback (reverse), verify (inspect). Default: plan.",
    )
    parser.add_argument(
        "--expected-root-id",
        default=PRODUCTION_ROOT_ID,
        required=False,
        help=f"Expected Drive root ID safety pin (default: {PRODUCTION_ROOT_ID}).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Explicit confirmation required to perform mutating operations under --operation apply.",
    )
    parser.add_argument(
        "--journal-path",
        type=Path,
        default=None,
        help="Optional local path to read or write migration journal.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.disable(logging.CRITICAL)

    if args.operation == "apply" and not args.apply:
        print(
            json.dumps({
                "status": "apply_rejected",
                "error": "Mutating operation 'apply' requires explicit --apply confirmation flag.",
            }, indent=2),
            file=sys.stderr,
        )
        return 1

    try:
        storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
        engine = DriveMigrationEngine(
            storage=storage,
            expected_root_id=args.expected_root_id,
            journal_local_path=args.journal_path,
        )

        if args.operation == "plan":
            result = engine.plan()
        elif args.operation == "apply":
            result = engine.apply()
        elif args.operation == "resume":
            result = engine.apply(resume=True)
        elif args.operation == "rollback":
            result = engine.rollback()
        elif args.operation == "verify":
            result = engine.verify()
        else:
            raise ValueError(f"Unknown operation: {args.operation}")

        rendered = json.dumps(result, indent=2, sort_keys=True)
        print(rendered)

        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(summary).open("a", encoding="utf-8") as handle:
                handle.write(f"\n\n## Drive Layout Migration ({args.operation})\n\n```json\n{rendered}\n```\n")

        if output := os.environ.get("GITHUB_OUTPUT"):
            with Path(output).open("a", encoding="utf-8") as handle:
                status = result.get("status", "unknown")
                handle.write(f"status={status}\n")

        return 0

    except (SafetyPinError, DriftError, MigrationError) as exc:
        err_report = {
            "status": "migration_failed",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
        print(json.dumps(err_report, indent=2), file=sys.stderr)
        return 1
    except Exception as exc:
        err_report = {
            "status": "unexpected_failure",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }
        print(json.dumps(err_report, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
