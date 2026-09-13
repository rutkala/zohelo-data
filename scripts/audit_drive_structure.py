#!/usr/bin/env python3
"""CLI entrypoint for Google Drive structure and release pointer audit.

Executes read-only inspection of zohelo-data Drive hierarchy, checks publication
pointers and manifest checksums, measures release cadence and storage, and outputs
a structured audit report.

No Drive modifications, folder creations, or large data downloads are performed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from drive_audit import run_full_drive_audit


def _check_credentials_available() -> bool:
    """Check whether Google Drive credentials are provided in environment."""
    has_oauth = bool(
        os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
        and os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
        and os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN")
    )
    has_sa = bool(os.environ.get("GCP_SERVICE_ACCOUNT_JSON"))
    return has_oauth or has_sa


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a bounded, read-only audit of the production Google Drive layout."
    )
    parser.add_argument(
        "--root-name",
        type=str,
        default="zohelo-data",
        help="Target Drive root folder name (default: zohelo-data)",
    )
    parser.add_argument(
        "--root-id",
        type=str,
        default=None,
        help="Optional target Drive root folder ID (bypasses name search)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path to write the JSON audit report",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=10_000,
        help="Maximum files to inspect across bounded traversal (default: 10000)",
    )
    parser.add_argument(
        "--max-requests",
        type=int,
        default=250,
        help="Maximum list requests to perform (default: 250)",
    )
    parser.add_argument(
        "--max-seconds",
        type=float,
        default=120.0,
        help=(
            "Cooperative elapsed budget checked between Drive calls; use an outer "
            "process timeout for blocked HTTP transports (default: 120.0)"
        ),
    )
    args = parser.parse_args()

    logging.disable(logging.CRITICAL)

    # Check credentials before attempting Google API connection
    if not _check_credentials_available():
        blocker_report = {
            "status": "blocked_missing_credentials",
            "read_only": True,
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "target_root_name": args.root_name,
            "target_root_id": args.root_id,
            "explanation": (
                "Google Drive credentials not found in runtime environment. "
                "Neither GOOGLE_OAUTH_* (CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN) "
                "nor GCP_SERVICE_ACCOUNT_JSON is set. Live audit requires authorized "
                "read-only Drive access. Run in an environment with Drive authentication configured."
            ),
            "evidence_status": "live_validation_pending",
        }
        print(json.dumps(blocker_report, indent=2, sort_keys=True))
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(blocker_report, indent=2, sort_keys=True), encoding="utf-8")
        return 1

    try:
        from storage_manager import StorageManager
        manager = StorageManager(
            backend="gdrive",
            allow_interactive_auth=False,
            root_name=args.root_name,
            root_id=args.root_id,
        )
        report = run_full_drive_audit(
            manager.drive_service,
            root_id=args.root_id,
            root_name=args.root_name,
            max_files=args.max_files,
            max_requests=args.max_requests,
            max_seconds=args.max_seconds,
        )
    except Exception as exc:
        error_report = {
            "status": "audit_execution_failed",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "read_only": True,
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        print(json.dumps(error_report, indent=2, sort_keys=True))
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(error_report, indent=2, sort_keys=True), encoding="utf-8")
        return 1

    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")

    # If GitHub Step Summary is present, append Markdown summary
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write(
                f"\n### Google Drive Structure Audit ({report.get('audited_at_utc')})\n\n"
                f"- **Status:** `{report.get('status')}`\n"
                f"- **Duration:** {report.get('duration_seconds')}s\n"
                f"- **NBP pointer verified:** `{report.get('nbp_current_pointer', {}).get('status')}`\n"
                f"- **BDL pointer verified:** `{report.get('bdl_current_pointer', {}).get('status')}`\n\n"
                "```json\n" + rendered + "\n```\n"
            )

    return 0 if report.get("status") == "audit_manifest_and_metadata_verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
