#!/usr/bin/env python3
"""Publish one audited retained-DBW Bronze increment."""
from __future__ import annotations
import argparse, json, os, subprocess, sys, tempfile
from datetime import datetime, timezone
from contextlib import ExitStack
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from retained_bronze_store import RetainedBronzeDriveStore
from ingestion.source_campaign_store import DriveCampaignStore, LocalCampaignStore
from retained_dbw_publication import SOURCE_ID, publish_retained_bronze, publish_retained_bronze_until_complete, validate_audit
from storage_manager import StorageManager
from retained_publication_lock import GitPublicationLock

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--backend", choices=("local", "drive"), required=True)
    parser.add_argument("--local-root", type=Path)
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--drive-root-id")
    parser.add_argument("--expected-code-sha")
    parser.add_argument("--max-indicators", type=int, default=8)
    parser.add_argument("--until-complete", action="store_true")
    parser.add_argument("--recover-stale-owner")
    parser.add_argument("--recovery-identity")
    args = parser.parse_args()
    with ExitStack() as stack:
        code_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        if args.backend == "local":
            if args.local_root is None: parser.error("--local-root is required for local backend")
            store = LocalCampaignStore(args.local_root, SOURCE_ID)
        else:
            if not args.allow_production_write or os.environ.get("ZOHELO_ALLOW_PRODUCTION_WRITES", "").lower() != "true":
                parser.error("Drive publication requires --allow-production-write and ZOHELO_ALLOW_PRODUCTION_WRITES=true")
            if not args.drive_root_id:
                parser.error("Drive publication requires an explicit --drive-root-id")
            if args.expected_code_sha != code_sha:
                parser.error("Drive publication requires --expected-code-sha matching the checked-out revision")
            if subprocess.check_output(
                ["git", "status", "--porcelain", "--untracked-files=normal"], cwd=ROOT, text=True
            ).strip():
                parser.error("Drive publication requires a clean reviewed checkout")
            # Authenticate the reviewed snapshot before constructing a mutating Drive store.
            validate_audit(args.audit_dir, require_reviewed_snapshot=True)
            publication_lock = stack.enter_context(GitPublicationLock(ROOT, code_sha, args.drive_root_id))
            store = RetainedBronzeDriveStore(
                StorageManager(allow_interactive_auth=False, root_id=args.drive_root_id),
                args.audit_dir, publication_lock.guard,
            )
            store.retained_publication_guard = publication_lock.guard
        if args.recover_stale_owner:
            if not args.recovery_identity:
                parser.error("--recover-stale-owner requires --recovery-identity")
            store.recover_publication_owner(args.recover_stale_owner, args.recovery_identity)
            print(json.dumps({"recovered_owner": args.recover_stale_owner, "recovered_by": args.recovery_identity}, sort_keys=True))
            return
        def report_increment(value):
            progress = {"format_version": 1, "source_id": SOURCE_ID,
                "snapshot_id": value["snapshot_id"], "published_indicator_count": value["published_indicator_count"],
                "pending_indicator_count": value["pending_indicator_count"],
                "observation_row_count": value["datasets"][0]["row_count"],
                "publication_format_version": value["format_version"],
                "operation": "register_existing_bronze_and_prepare_oversized_query_parts",
                "updated_at_utc": datetime.now(timezone.utc).isoformat()}
            if value["format_version"] == 2:
                index = json.loads(store.read_landing_object(value["indicator_index"]))
                progress["referenced_original_observation_files"] = sum(
                    part["name"] == f"part_{item['indicator_id']}.parquet"
                    for item in index["indicators"] for part in item["parts"]
                )
                progress["observation_query_parts"] = sum(
                    part["name"].startswith("fragment-")
                    for item in index["indicators"] for part in item["parts"]
                )
            args.workspace.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile("w", dir=args.workspace, delete=False, encoding="utf-8") as stream:
                json.dump(progress, stream, sort_keys=True); stream.flush(); os.fsync(stream.fileno()); temporary=Path(stream.name)
            os.replace(temporary, args.workspace / "progress.json")
            print(json.dumps(progress, sort_keys=True), flush=True)
        if args.until_complete:
            result = publish_retained_bronze_until_complete(
                store, args.audit_dir, args.workspace, code_sha,
                max_indicators=args.max_indicators, on_increment=report_increment
            )
        else:
            result = publish_retained_bronze(store, args.audit_dir, args.workspace, code_sha, max_indicators=args.max_indicators)
            report_increment(result)
        print(json.dumps({k: result[k] for k in ("snapshot_id", "published_indicator_count", "pending_indicator_count")}, sort_keys=True))

if __name__ == "__main__": main()
