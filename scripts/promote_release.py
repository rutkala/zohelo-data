"""Safely promote one retained release after full validation and an exact current pin."""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from drive_release_store import DriveReleaseStore  # noqa: E402
from release_protocol import promote_retained_release  # noqa: E402
from release_validation import validate_staged_release  # noqa: E402
from storage_manager import StorageManager  # noqa: E402


def promote(*, target_release_id: str, expected_current_release_id: str) -> dict:
    storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
    storage.authorize_writes()
    root_id = storage.resolve_root(create=False)
    result = promote_retained_release(
        DriveReleaseStore(storage, root_id),
        root_id,
        target_release_id=target_release_id,
        expected_current_release_id=expected_current_release_id,
        pre_promote_validator=validate_staged_release,
    )
    report = {
        key: result[key]
        for key in (
            "status", "target_release_id", "previous_release_id",
            "pointer_file_id", "audit_file_id",
        )
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered, flush=True)
    if output := os.environ.get("GITHUB_OUTPUT"):
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"release_id={report['target_release_id']}\n")
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as handle:
            handle.write("\n\n## Retained release promotion\n\n```json\n" + rendered + "\n```\n")
    return report


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-release-id", required=True)
    parser.add_argument("--expected-current-release-id", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    try:
        arguments = _arguments()
        promote(
            target_release_id=arguments.target_release_id,
            expected_current_release_id=arguments.expected_current_release_id,
        )
    except Exception as exc:
        print(json.dumps({
            "status": "retained_release_promotion_failed",
            "error_type": type(exc).__name__,
            "message": "Promotion was not confirmed; inspect validation and current-release identity.",
        }), flush=True)
        raise SystemExit(1)
