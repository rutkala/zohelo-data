"""Collect and verify full official source distributions into private Landing."""
import argparse
import json
import os
from pathlib import Path
import tempfile

from source_campaign import load_settings, production_storage
from ingestion.full_source_campaign import SOURCES, coverage, run_full_campaign


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, choices=SOURCES)
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--verify-current", action="store_true")
    parser.add_argument("--session-seconds", type=int, default=1200)
    parser.add_argument("--max-requests", type=int, default=24)
    args = parser.parse_args()
    if not 60 <= args.session_seconds <= 2400 or not 1 <= args.max_requests <= 100:
        parser.error("Use a 60–2400 second batch and 1–100 requests; checkpoints continue next run")
    settings = load_settings(args.source)
    if not args.verify_current and not settings.get("enabled", True):
        print(json.dumps({"source_id": args.source, "status": "disabled"}), flush=True)
        return 0
    from ingestion.source_campaign_store import DriveCampaignStore, _CampaignStore
    from ingestion.drive_state_store import DriveStateStore
    from ingestion.bulk_transport import BulkDriveRawStore
    storage = production_storage(args.allow_production_write)
    root = storage.resolve_root(create=False)
    campaign_id = args.source + "_bulk"
    control = storage.get_or_create_nested_folder(
        ["06_control", "source_campaigns", campaign_id], root_id=root)
    landing = storage.resolve_zone("landing", create=True)
    raw_root = storage.get_or_create_nested_folder([args.source, "bulk"], root_id=landing)
    transport = DriveStateStore(storage, root, control, allow_landing_pointer=True)
    store = _CampaignStore(transport, campaign_id, control, raw_root)
    raw_store = BulkDriveRawStore(storage, args.source, responses_root_id=raw_root)
    from ingestion.bulk_publication import publish_bulk_index, verify_bulk_index
    if args.verify_current:
        state = store.load()
        if not state or not state.get("receipts"):
            raise RuntimeError("No full distribution has been accepted")
        # A fresh worker verifies the latest receipt and streams the exact raw
        # object independently. This is sampled restore, not an all-file audit.
        receipt = store.read_receipt(state["receipts"][-1])
        with tempfile.TemporaryDirectory(prefix="zohelo-bulk-verify-") as directory:
            raw_store.read_to_file(receipt["raw"], Path(directory) / "restored.download")
        index = verify_bulk_index(store)
        if index is None:
            raise RuntimeError("Full-distribution index has not been published")
        print(json.dumps({"status": "fresh_full_distribution_verified", **coverage(state)}), flush=True)
        return 0
    quota_store = DriveCampaignStore(storage, args.source)
    with tempfile.TemporaryDirectory(prefix="zohelo-full-source-") as directory:
        report = run_full_campaign(
            store, raw_store, quota_store, args.source, settings, Path(directory),
            max_seconds=args.session_seconds, max_requests=args.max_requests,
            code_sha=os.environ.get("GITHUB_SHA", "unknown"),
            publish=publish_bulk_index,
            on_progress=lambda value: print(json.dumps(value, sort_keys=True), flush=True))
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered, flush=True)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as handle:
            handle.write(f"## Full distributions: {args.source}\n\n````json\n{rendered}\n````\n")
    return int(bool(report["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
