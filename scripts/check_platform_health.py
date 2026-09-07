#!/usr/bin/env python3
"""Report retained-state capacity and optional read-only Drive storage metadata."""
import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from capacity_report import account_storage_quota, capacity_report, inventory_project


def _emit_report(report):
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write(
                "\n### Platform capacity and retained storage\n\n```json\n" + rendered +
                "\n```\n\nRead-only observations. Project inventory and account quota are separate; "
                "unknown or incomplete sizes remain explicit. No deletion, compaction, future "
                "capacity guarantee, or freshness/recovery assertion is implied.\n"
            )


def read_drive_health(*, storage_factory=None, max_files=10_000):
    # Keep local-state review independent of Google clients/authentication.
    from ingestion.drive_state_store import DriveStateStore
    from ingestion.nbp_state import load_state, source_specs_from_config
    from storage_manager import StorageManager
    manager = (storage_factory or StorageManager)(backend="gdrive", allow_interactive_auth=False)
    root = manager.resolve_root(create=False)
    folders = manager._list_exact_folders("ingestion-control", parent_id=root)
    if len(folders) != 1:
        raise ValueError("Exactly one existing ingestion-control folder is required")
    control = folders[0]["id"]
    store = DriveStateStore(manager, root, control)
    loaded = load_state(store, control, source_specs_from_config(ROOT / "config/nbp-platform.yaml"))
    if loaded.snapshot_file_id is None:
        raise ValueError("No verified current ingestion-state snapshot exists")
    return {
        "state_capacity": capacity_report(loaded.state),
        "account_storage": account_storage_quota(manager.drive_service),
        "project_inventory": inventory_project(manager.drive_service.files(), root, max_files=max_files),
    }


def main():
    parser = argparse.ArgumentParser(description=(
        "Read retained-state capacity without creating, updating, compacting or deleting files. "
        "Local review needs no credentials. Drive review requires existing configured credentials "
        "and reads quota fields plus bounded project metadata; it does not request account identity."
    ))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--local-state", type=Path, help="Read a saved ingestion state JSON snapshot")
    mode.add_argument("--read-drive", action="store_true", help="Explicitly read the configured Drive project")
    parser.add_argument("--max-files", type=int, default=10_000,
                        help="Drive metadata entry bound, 1–10000 (default 10000)")
    args = parser.parse_args()
    if not 1 <= args.max_files <= 10_000:
        parser.error("--max-files must be between 1 and 10000")
    logging.disable(logging.CRITICAL)
    try:
        if args.local_state:
            if args.local_state.stat().st_size > 12_000_000:
                raise ValueError("Local state exceeds the bounded 12 MB diagnostic input limit")
            report = {"state_capacity": capacity_report(json.loads(args.local_state.read_text(encoding="utf-8"))),
                      "account_storage": {"status": "not_requested"},
                      "project_inventory": {"status": "not_requested"}}
        else:
            report = read_drive_health(max_files=args.max_files)
    except Exception as exc:
        # Error payloads from cloud clients may contain identifiers; only fixed
        # diagnostic status and exception class are emitted by this entry point.
        _emit_report({"status": "health_check_failed", "error_type": type(exc).__name__,
                      "read_only": True})
        return 1
    report.update(checked_at_utc=datetime.now(timezone.utc).isoformat(), read_only=True,
                  future_capacity_guarantee=False)
    _emit_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
