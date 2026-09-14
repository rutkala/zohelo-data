#!/usr/bin/env python3
"""Plan, apply, resume, roll back, or verify the reviewed Drive migration."""
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
from drive_migration import DriveMigrationEngine, DriftError, MigrationError, SafetyPinError
from storage_manager import StorageManager

PRODUCTION_ROOT_ID = "1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", choices=["plan", "apply", "resume", "rollback", "verify"], default="plan")
    parser.add_argument("--expected-root-id", default=PRODUCTION_ROOT_ID)
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Deprecated apply confirmation alias.")
    parser.add_argument("--plan-file", type=Path)
    parser.add_argument("--plan-sha256", help="Expected immutable reviewed plan SHA-256.")
    parser.add_argument("--journal-path", type=Path)
    return parser.parse_args()

def plan_digest(plan: dict) -> str:
    copy = dict(plan)
    supplied = copy.pop("plan_sha256", None)
    digest = sha256(json.dumps(copy, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    if supplied and supplied != digest:
        raise MigrationError("Reviewed plan hash does not match its immutable contents")
    return digest

def load_reviewed_plan(args: argparse.Namespace) -> dict:
    if args.plan_file is None:
        raise MigrationError("Apply requires --plan-file from a reviewed read-only plan artifact")
    if not args.plan_file.is_file():
        raise MigrationError("Reviewed plan file does not exist: " + str(args.plan_file))
    try:
        plan = json.loads(args.plan_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise MigrationError("Could not read reviewed plan file: " + str(exc)) from exc
    if not isinstance(plan, dict) or plan.get("status") != "planned":
        raise MigrationError("Reviewed plan is malformed or not a planned migration")
    if plan.get("expected_root_id") != args.expected_root_id or plan.get("root_id") != args.expected_root_id:
        raise MigrationError("Reviewed plan root pins do not match --expected-root-id")
    digest = plan_digest(plan)
    if not args.plan_sha256:
        raise MigrationError("Apply requires --plan-sha256 for the reviewed plan")
    if args.plan_sha256 != digest:
        raise MigrationError("Provided --plan-sha256 does not match reviewed plan")
    plan["plan_sha256"] = digest
    return plan

def write_json(path: str, value: dict) -> None:
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")

def main() -> int:
    args = parse_args()
    logging.disable(logging.CRITICAL)
    mutating = args.operation in {"apply", "resume", "rollback"}
    confirmed = args.confirm or (args.operation == "apply" and args.apply)
    if mutating and not confirmed:
        print(json.dumps({"status": "operation_rejected", "error": "Explicit --confirm is required"}, indent=2), file=sys.stderr)
        return 1
    try:
        storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
        engine = DriveMigrationEngine(storage=storage, expected_root_id=args.expected_root_id, journal_local_path=args.journal_path)
        if args.operation == "plan":
            result = engine.plan()
            write_json("plan.json", result)
        elif args.operation == "apply":
            result = engine.apply(plan=load_reviewed_plan(args), confirmed=True)
            write_json("journal.json", result.get("journal", result))
        elif args.operation == "resume":
            result = engine.apply(resume=True, confirmed=True)
            write_json("journal.json", result.get("journal", result))
        elif args.operation == "rollback":
            result = engine.rollback(confirmed=True)
            write_json("journal.json", result.get("journal", result))
        else:
            result = engine.verify()
            write_json("verification.json", result)
        write_json("operation-result.json", result)
        rendered = json.dumps(result, indent=2, sort_keys=True)
        print(rendered)
        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(summary).open("a", encoding="utf-8") as handle:
                handle.write("\n## Drive Layout Migration (" + args.operation + ")\n\n" + rendered + "\n")
        if output := os.environ.get("GITHUB_OUTPUT"):
            with Path(output).open("a", encoding="utf-8") as handle:
                handle.write("status=" + result.get("status", "unknown") + "\n")
        return 0
    except (SafetyPinError, DriftError, MigrationError) as exc:
        write_json("migration-error.json", {"status": "migration_failed", "error_type": type(exc).__name__, "error_message": str(exc)})
        print(Path("migration-error.json").read_text(), file=sys.stderr)
        return 1
    except Exception as exc:
        write_json("migration-error.json", {"status": "unexpected_failure", "error_type": type(exc).__name__, "error_message": str(exc)})
        print(Path("migration-error.json").read_text(), file=sys.stderr)
        return 1
if __name__ == "__main__":
    raise SystemExit(main())
