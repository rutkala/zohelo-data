"""Prepare, stream-publish, and freshly verify the retained DBW modeled release."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import tempfile
from functools import partial

from dbw_platform import build_candidate
from dbw_release_validation import validate_staged_dbw_release
from dbw_retained_source import load_retained_source_descriptor
from drive_release_store import DriveReleaseStore
from layout_resolution import LayoutResolutionError, resolve_source_release_root
from medallion_navigation import (
    finalize_source_medallion_navigation,
    sync_source_medallion_navigation,
)
from release_protocol import (
    PlatformReleaseWriter,
    ReleaseProtocolError,
    read_current_release_manifest,
    read_release_manifest,
)
from retained_dbw_publication import REVIEWED_INVENTORY_SHA256
from runtime_metadata import _code_sha
from storage_manager import StorageManager

REPO_ROOT = Path(__file__).resolve().parents[1]
RETAINED_POINTER_FILE_ID = "1vl3ryLavKp2nyRzjcZsNLMeUxjzIrcMU"
DBW_RELEASE_SCOPE = "dbw_platform"


def _require_actions_main(
    *,
    expected_code_sha: str,
    allow_production_write: bool,
    require_write: bool,
) -> str:
    code_sha = _code_sha()
    if require_write and not allow_production_write:
        raise PermissionError("DBW modeled publication requires --allow-production-write")
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise PermissionError("DBW modeled publication must use the serialized main-branch Actions workflow")
    if expected_code_sha != code_sha:
        raise PermissionError("DBW modeled publication requires the exact checked-out Git SHA")
    if (
        require_write
        and os.environ.get("ZOHELO_ALLOW_PRODUCTION_WRITES", "").lower() != "true"
    ):
        raise PermissionError("DBW modeled publication requires explicit production-write opt-in")
    return code_sha


def _retained_files(store: DriveReleaseStore, directory: Path) -> tuple[Path, Path, dict]:
    """Read the exact canonical retained pointer and its checksum-pinned manifest."""
    pointer_raw = store.read(RETAINED_POINTER_FILE_ID)
    try:
        pointer = json.loads(pointer_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Canonical retained DBW pointer is not valid JSON") from exc
    manifest_id = pointer.get("manifest_file_id") if isinstance(pointer, dict) else None
    if not isinstance(manifest_id, str) or not manifest_id:
        raise ValueError("Canonical retained DBW pointer has no manifest identity")
    manifest_raw = store.read(manifest_id)
    pointer_path = directory / "retained-pointer.json"
    manifest_path = directory / "retained-manifest.json"
    pointer_path.write_bytes(pointer_raw)
    manifest_path.write_bytes(manifest_raw)
    descriptor, _ = load_retained_source_descriptor(
        pointer_path,
        manifest_path,
        pointer_file_id=RETAINED_POINTER_FILE_ID,
        expected_inventory_sha256=REVIEWED_INVENTORY_SHA256,
    )
    return pointer_path, manifest_path, descriptor


def _current_pointer(store: DriveReleaseStore) -> dict:
    ids = store.find("current-release.json", store.root_id)
    if len(ids) != 1:
        raise ReleaseProtocolError("DBW current-release pointer is absent or ambiguous")
    raw = store.read(ids[0])
    try:
        pointer = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseProtocolError("DBW current-release pointer is not valid JSON") from exc
    if not isinstance(pointer, dict):
        raise ReleaseProtocolError("DBW current-release pointer must be an object")
    return pointer


def _finalize_existing_navigation(
    storage: StorageManager,
    root_id: str,
    manifest: dict,
) -> None:
    """Repair a prior post-promotion navigation failure before returning success."""
    sync_source_medallion_navigation(
        storage, root_id, "dbw", manifest, finalize=False
    )
    finalize_source_medallion_navigation(storage, root_id, "dbw", manifest)


def _require_expected_release(manifest: dict, expected_release_id: str | None) -> None:
    if (
        not isinstance(expected_release_id, str)
        or manifest.get("release_id") != expected_release_id
    ):
        raise ReleaseProtocolError(
            "Current DBW release differs from the release published by this workflow"
        )


def _read_optional_current_release(store: DriveReleaseStore) -> dict | None:
    """Return no release only when the publication root has no pointer yet."""
    pointer_ids = store.find("current-release.json", store.root_id)
    if not pointer_ids:
        return None
    return read_current_release_manifest(store, store.root_id)


def run(
    *,
    drive_root_id: str,
    expected_code_sha: str,
    allow_production_write: bool = False,
    verify_current: bool = False,
    expected_release_id: str | None = None,
) -> dict:
    code_sha = _require_actions_main(
        expected_code_sha=expected_code_sha,
        allow_production_write=allow_production_write,
        require_write=not verify_current,
    )
    storage = StorageManager(
        allow_interactive_auth=False,
        root_id=drive_root_id,
    )
    root_id = storage.resolve_root(create=False)
    with tempfile.TemporaryDirectory(prefix="zohelo-dbw-platform-") as temporary:
        workspace = Path(temporary)
        read_store = DriveReleaseStore(storage, root_id)
        pointer_path, manifest_path, retained_source = _retained_files(
            read_store, workspace
        )
        if verify_current:
            release_root, _ = resolve_source_release_root(
                storage, root_id, "dbw", is_writer=False
            )
            release_store = DriveReleaseStore(storage, release_root)
            manifest = read_current_release_manifest(
                release_store, release_store.root_id
            )
            if manifest.get("release_scope") != DBW_RELEASE_SCOPE:
                raise ReleaseProtocolError(
                    "Current DBW pointer does not identify a modeled DBW release"
                )
            _require_expected_release(manifest, expected_release_id)
            report = validate_staged_dbw_release(
                release_store,
                _current_pointer(release_store),
                retained_pointer_file_id=RETAINED_POINTER_FILE_ID,
            )
            return {
                "status": "fresh_dbw_modeled_release_verified",
                "release_id": report["release_id"],
                "datasets": len(report["datasets"]),
                "observation_rows": report["observation_rows"],
                "retained_release_id": report["retained_release_id"],
            }

        try:
            existing_root, existing_direct_releases = resolve_source_release_root(
                storage, root_id, "dbw", is_writer=False
            )
        except LayoutResolutionError:
            existing_root = None
            existing_direct_releases = False
        if existing_root is not None:
            existing_store = DriveReleaseStore(storage, existing_root)
            existing = _read_optional_current_release(existing_store)
            if existing is not None and (
                existing.get("release_scope") == DBW_RELEASE_SCOPE
                and existing.get("code_sha") == code_sha
                and existing.get("inputs") == [retained_source]
            ):
                report = validate_staged_dbw_release(
                    existing_store,
                    _current_pointer(existing_store),
                    retained_pointer_file_id=RETAINED_POINTER_FILE_ID,
                )
                if existing_direct_releases:
                    storage.authorize_writes()
                    _finalize_existing_navigation(storage, root_id, existing)
                return {
                    "status": "dbw_modeled_release_unchanged",
                    "release_id": report["release_id"],
                    "datasets": len(report["datasets"]),
                    "observation_rows": report["observation_rows"],
                    "retained_release_id": report["retained_release_id"],
                }

        from scripts.prepare_retained_dbw_release import prepare

        prepared = prepare(storage, workspace / "prepared")
        if prepared.get("status") != "verified":
            raise RuntimeError("DBW retained input preparation did not verify")
        storage.authorize_writes()
        release_root, direct_releases = resolve_source_release_root(
            storage, root_id, "dbw", is_writer=True
        )
        release_store = DriveReleaseStore(storage, release_root)
        validator = partial(
            validate_staged_dbw_release,
            retained_pointer_file_id=RETAINED_POINTER_FILE_ID,
        )
        writer = PlatformReleaseWriter(
            release_store,
            release_store.root_id,
            release_scope=DBW_RELEASE_SCOPE,
            code_sha=code_sha,
            inputs=[retained_source],
            pre_promote_validator=validator,
            before_pointer_write=(
                lambda store, pointer: sync_source_medallion_navigation(
                    storage,
                    root_id,
                    "dbw",
                    read_release_manifest(store, pointer),
                    finalize=False,
                )
            ) if direct_releases else None,
            after_pointer_write=(
                lambda store, pointer: finalize_source_medallion_navigation(
                    storage,
                    root_id,
                    "dbw",
                    read_release_manifest(store, pointer),
                )
            ) if direct_releases else None,
            direct_releases=direct_releases,
        )
        modeled_workspace = workspace / "modeled"
        modeled_workspace.mkdir()
        candidate = build_candidate(
            Path(prepared["data_root"]),
            REVIEWED_INVENTORY_SHA256,
            modeled_workspace,
            retained_pointer=pointer_path,
            retained_manifest=manifest_path,
            retained_pointer_file_id=RETAINED_POINTER_FILE_ID,
            retained_audit_dir=workspace / "prepared" / "audit",
            dataset_sink=writer.add_dataset,
        )
        if candidate["inputs"] != [retained_source]:
            raise RuntimeError("DBW build changed its retained source identity")
        result = writer.finalize(
            artifacts=candidate["artifacts"],
            measurements=candidate["measurements"],
        )
        return {
            "status": "dbw_modeled_release_published",
            "release_id": result["release_id"],
            "datasets": candidate["measurements"]["dataset_count"],
            "output_files": candidate["measurements"]["output_files"],
            "output_bytes": candidate["measurements"]["output_bytes"],
            "retained_snapshot_id": retained_source["snapshot_id"],
            "coverage_status": retained_source["coverage_status"],
            "lineage_status": retained_source["lineage_status"],
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--drive-root-id", required=True)
    parser.add_argument("--expected-code-sha", required=True)
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--verify-current", action="store_true")
    parser.add_argument("--expected-release-id")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        report = run(
            drive_root_id=args.drive_root_id,
            expected_code_sha=args.expected_code_sha,
            allow_production_write=args.allow_production_write,
            verify_current=args.verify_current,
            expected_release_id=args.expected_release_id,
        )
    except Exception as exc:
        print(json.dumps({
            "status": "dbw_modeled_release_failed",
            "error_type": type(exc).__name__,
            "detail": str(exc),
            "occurred_at_utc": datetime.now(timezone.utc).isoformat(),
        }), flush=True)
        return 1
    rendered = json.dumps(report, sort_keys=True)
    print(rendered, flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as handle:
            handle.write("## DBW modeled release\n\n```json\n" + rendered + "\n```\n")
    output = os.environ.get("GITHUB_OUTPUT")
    if output and isinstance(report.get("release_id"), str):
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"release_id={report['release_id']}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
