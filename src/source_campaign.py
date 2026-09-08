"""Human-operable entrypoint for bounded source ingestion campaigns."""
import argparse
from datetime import datetime
import importlib
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import yaml

from ingestion.source_campaign import run_campaign


ROOT = Path(__file__).resolve().parents[1]
SOURCE_IDS = ("world_bank_wdi", "gus_bdl", "eurostat")


def production_storage(allow_write):
    if not allow_write:
        raise PermissionError("Drive campaign writes require --allow-production-write")
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise PermissionError("Production campaigns must use the serialized main-branch Actions workflow")
    from storage_manager import StorageManager
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    return storage


def load_settings(source_id, config_path=ROOT / "config/source-campaigns.yaml"):
    document = yaml.safe_load(Path(config_path).read_text())
    if document.get("schema_version") != 1 or source_id not in document.get("sources", {}):
        raise ValueError("Unknown campaign configuration/source")
    settings = {**document["defaults"], **document["sources"][source_id]}
    for key in ("max_requests", "max_run_seconds", "http_timeout_seconds", "max_response_bytes",
                "max_retained_raw_bytes", "max_pending_tasks", "discovery_pause_threshold",
                "max_recent_roots", "max_completed_tasks", "min_request_interval_seconds", "max_inline_wait_seconds"):
        if type(settings.get(key)) is not int or settings[key] <= 0:
            raise ValueError(f"Invalid positive campaign setting: {key}")
    if settings["max_response_bytes"] > 8 * 1024 * 1024 or settings["max_run_seconds"] > 600:
        raise ValueError("Campaign exceeds response/time envelope")
    if settings["max_requests"] > 100 or settings["discovery_pause_threshold"] >= settings["max_pending_tasks"]:
        raise ValueError("Campaign request or discovery envelope is invalid")
    for window in settings["quota_windows"]:
        if any(type(window.get(k)) is not int or window[k] <= 0 for k in ("seconds", "requests")):
            raise ValueError("Invalid quota window")
    return settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=SOURCE_IDS)
    parser.add_argument("--initialize-drive", action="store_true")
    parser.add_argument("--verify-current", action="store_true")
    parser.add_argument("--retry-validation-failures", action="store_true")
    parser.add_argument("--backend", choices=("local", "drive"), default="local")
    parser.add_argument("--local-root", type=Path)
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--pause-history", action="store_true")
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()
    if args.retry_validation_failures and (args.initialize_drive or args.verify_current):
        parser.error("Validation retry is a collection operation")
    if args.initialize_drive:
        storage = production_storage(args.allow_production_write)
        quota = storage.drive_service.about().get(fields="storageQuota").execute(num_retries=2).get("storageQuota", {})
        limit, usage = quota.get("limit"), quota.get("usage")
        # Bound one complete parallel run, including two maximum-size state snapshots
        # per attempt and extra initialization/receipt headroom. This is a preflight,
        # not a reservation against unrelated account writers.
        reserve = 16 * 1024 * 1024
        for source_id in SOURCE_IDS:
            settings = load_settings(source_id)
            if settings["enabled"]:
                reserve += settings["max_requests"] * (settings["max_response_bytes"] + 8 * 1024 * 1024)
                reserve += 8 * 1024 * 1024
        if limit is not None and usage is not None and int(limit) - int(usage) < reserve:
            raise RuntimeError("Available Drive quota is below the configured campaign-run reserve")
        from ingestion.source_campaign_store import DriveCampaignStore
        # One serialized coordinator creates shared parents before parallel provider jobs.
        for source_id in SOURCE_IDS:
            if load_settings(source_id)["enabled"]:
                DriveCampaignStore(storage, source_id)
        print(json.dumps({"status": "source_campaign_paths_ready", "sources": list(SOURCE_IDS),
                          "storage_quota_reported": limit is not None and usage is not None}))
        return 0
    if args.source is None:
        parser.error("--source is required except with --initialize-drive")
    settings = load_settings(args.source)
    if not settings["enabled"]:
        print(json.dumps({"source_id": args.source, "reason": "operator_paused"}))
        return 0
    adapter = importlib.import_module(settings["adapter"])
    if adapter.SOURCE_ID != args.source:
        raise ValueError("Adapter source identity mismatch")
    if args.backend == "drive":
        # Current protocol relies on workflow serialization, not a Drive CAS primitive.
        from ingestion.source_campaign_store import DriveCampaignStore
        storage = production_storage(args.allow_production_write)
        store = DriveCampaignStore(storage, args.source)
    else:
        if args.local_root is None:
            raise ValueError("Local ingestion requires an explicit --local-root")
        from ingestion.source_campaign_store import LocalCampaignStore
        store = LocalCampaignStore(args.local_root, args.source)
    if args.retry_validation_failures:
        from ingestion.campaign_recovery import retry_validation_failures
        print(json.dumps(retry_validation_failures(store, os.environ.get("GITHUB_SHA", "local"))), flush=True)
    if args.verify_current:
        state = store.load()
        if not state or not state.get("receipts"):
            raise RuntimeError("No accepted campaign receipt is available to verify")
        rejected = []
        for descriptor in state.get("rejected_receipts", [])[-3:]:
            receipt = store.read_receipt(descriptor)
            rejected.append({key: receipt.get(key) for key in
                             ("task_id", "http_status", "error_type", "detail", "failed_at_utc")})
        print(json.dumps({"source_id": args.source, "recent_rejections": rejected}), flush=True)
        lanes = {}
        # Select the newest accepted receipt per lane from the verified state. Reading
        # receipt metadata is bounded to the newest 12 receipts. Only one raw
        # response per represented lane is restored; this is sampled replay.
        for descriptor in reversed(state["receipts"][-12:]):
            receipt = store.read_receipt(descriptor)
            lane = receipt["task"]["lane"]
            if lane in lanes:
                continue
            raw = store.read_raw(receipt["raw"])
            observed = datetime.fromisoformat(receipt["retrieved_at_utc"]).astimezone(ZoneInfo(settings["timezone"])).date()
            result = adapter.interpret(receipt["task"], raw, observed)
            if not receipt.get("accepted") or result["record_count"] != receipt["record_count"]:
                raise RuntimeError("Restored response does not reproduce its accepted receipt")
            lanes[lane] = {"raw_bytes": len(raw), "record_count": result["record_count"]}
            if len(lanes) >= 3:
                break
        print(json.dumps({"source_id": args.source, "status": "fresh_restore_verified", "lanes": lanes,
                          "accepted_responses": state["accepted_responses"], "pending_tasks": len(state["pending"])}))
        return 0
    today = datetime.now(ZoneInfo(settings["timezone"])).date()
    report = run_campaign(store, adapter, today, settings, history_enabled=not args.pause_history,
                          code_sha=os.environ.get("GITHUB_SHA", "local"))
    rendered = json.dumps(report, sort_keys=True, indent=2)
    print(rendered, flush=True)
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(rendered + "\n")
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as handle:
            handle.write(f"## Source campaign: {args.source}\n\n")
            handle.write("Durable Landing collection; source scope remains incomplete. Received records include metadata and repeated representations.\n\n")
            handle.write("```json\n" + rendered + "\n```\n")
    if "capacity_pause" in report["reason"]:
        print("::warning::Source campaign reached a documented capacity boundary; existing evidence is retained.")
    return 1 if report["failed_requests"] else 0


if __name__ == "__main__":
    sys.exit(main())
