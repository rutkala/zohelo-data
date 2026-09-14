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

from bdl_release_validation import validate_staged_bdl_release  # noqa: E402
from drive_release_store import DriveReleaseStore  # noqa: E402
from layout_resolution import resolve_source_release_root  # noqa: E402
from medallion_navigation import (finalize_source_medallion_navigation, sync_source_medallion_navigation)  # noqa: E402
from release_protocol import promote_retained_release, read_release_manifest  # noqa: E402
from release_validation import validate_staged_release  # noqa: E402
from storage_manager import StorageManager  # noqa: E402
from wdi_release_validation import validate_staged_wdi_release  # noqa: E402


def _get_validator(source: str):
    if source == "nbp":
        return validate_staged_release
    elif source == "bdl":
        return validate_staged_bdl_release
    elif source == "wdi":
        return validate_staged_wdi_release
    raise ValueError(f"Unknown source '{source}'; expected nbp, bdl, or wdi")


def promote(*, target_release_id: str, expected_current_release_id: str, source: str = "nbp") -> dict:
    storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
    storage.authorize_writes()
    root_id = storage.resolve_root(create=False)
    release_root, direct_releases = resolve_source_release_root(
        storage, root_id, source, is_writer=True
    )
    validator = _get_validator(source)
    release_store = DriveReleaseStore(storage, release_root)
    result = promote_retained_release(
        release_store,
        release_root,
        target_release_id=target_release_id,
        expected_current_release_id=expected_current_release_id,
        pre_promote_validator=validator,
        before_pointer_write=(
            lambda release_store, pointer: sync_source_medallion_navigation(
                storage, root_id, source,
                read_release_manifest(release_store, pointer), finalize=False,
            )
        ) if direct_releases else None,
        after_pointer_write=(
            lambda release_store, pointer: finalize_source_medallion_navigation(
                storage, root_id, source,
                read_release_manifest(release_store, pointer),
            )
        ) if direct_releases else None,
        direct_releases=direct_releases,
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
    parser.add_argument("--source", choices=["nbp", "bdl", "wdi"], default="nbp")
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
            source=arguments.source,
        )
    except Exception as exc:
        print(json.dumps({
            "status": "retained_release_promotion_failed",
            "error_type": type(exc).__name__,
            "message": "Promotion was not confirmed; inspect validation and current-release identity.",
        }), flush=True)
        raise SystemExit(1)
