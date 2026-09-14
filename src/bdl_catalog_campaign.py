"""Run resumable GUS BDL API catalogue discovery without numerical observations."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from ingestion.campaign_session import run_collection_session
from ingestion.source_campaign import fetch
from ingestion.source_campaign_store import DriveCampaignStore
from ingestion.source_credentials import make_authenticated_fetch
from ingestion.sources import gus_bdl_catalog as adapter
from source_campaign import load_settings, production_storage, publish_batch


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycles", type=int, default=6)
    parser.add_argument("--session-seconds", type=int, default=1200)
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()
    if not 1 <= args.cycles <= 6 or not 60 <= args.session_seconds <= 1800:
        parser.error("Use 1–6 cycles and a 60–1800 second session budget")

    storage = production_storage(args.allow_production_write)
    store = DriveCampaignStore(storage, adapter.SOURCE_ID)
    settings = load_settings(adapter.SOURCE_ID)
    report = run_collection_session(
        store,
        adapter,
        settings,
        today_factory=lambda: datetime.now(ZoneInfo(settings["timezone"])).date(),
        publish=publish_batch,
        max_cycles=args.cycles,
        max_seconds=args.session_seconds,
        history_enabled=False,
        code_sha=os.environ.get("GITHUB_SHA", "local"),
        fetcher=make_authenticated_fetch(adapter.SOURCE_ID, fetch),
        on_cycle=lambda cycle: print(json.dumps({"catalogue_batch_completed": cycle}, sort_keys=True), flush=True),
    )
    state = store.load()
    if state is None:
        raise RuntimeError("BDL catalogue campaign did not create durable state")
    pending = state.get("pending", [])
    non_catalogue = [task.get("id") for task in pending if task.get("kind") not in {
        "dictionary", "years", "subjects", "subject_detail", "units", "variables"
    }]
    if non_catalogue:
        raise RuntimeError("BDL catalogue state contains forbidden numerical-data tasks")
    summary = {
        **report,
        "catalogue_pending_tasks": len(pending),
        "catalogue_complete": len(pending) == 0 and report.get("failed_requests", 0) == 0,
        "total_accepted_responses": state.get("accepted_responses", 0),
        "total_record_count": state.get("record_count", 0),
        "mode": "metadata_only_api",
    }
    rendered = json.dumps(summary, sort_keys=True, indent=2)
    print(rendered, flush=True)
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(rendered + "\n", encoding="utf-8")
    return 1 if report.get("failed_requests", 0) else 0


if __name__ == "__main__":
    sys.exit(main())
