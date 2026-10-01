#!/usr/bin/env python3
"""Verify migrated Eurostat/WDI campaign state through the private R2 migration index."""
from __future__ import annotations

import json

from ingestion.bulk_publication import verify_bulk_index
from ingestion.full_source_campaign import coverage
from ingestion.landing_publication import verify_landing
from ingestion.source_campaign_store import R2CampaignStore

SOURCES=("eurostat","world_bank_wdi")


def summarize_state(state):
    if state is None:
        return None
    return {
        "source_id":state.get("source_id"),
        "provider_id":state.get("provider_id"),
        "accepted_responses":state.get("accepted_responses"),
        "pending":len(state.get("pending",[])),
        "completed":len(state.get("completed",{})),
        "receipts":len(state.get("receipts",[])),
    }


def main()->int:
    result={}
    for source in SOURCES:
        standard=R2CampaignStore(source,publication_only=True)
        standard_state=standard.load()
        standard_landing=verify_landing(standard)

        bulk_id=source+"_bulk"
        bulk=R2CampaignStore(bulk_id,publication_only=True)
        bulk_state=bulk.load()
        bulk_index=verify_bulk_index(bulk,require_current=True)

        result[source]={
            "standard_state":summarize_state(standard_state),
            "standard_landing":{
                "snapshot_id":standard_landing.get("snapshot_id"),
                "source_receipt_count":standard_landing.get("source_receipt_count"),
                "row_count":standard_landing.get("row_count"),
                "files":len(standard_landing.get("files",[])),
            } if isinstance(standard_landing,dict) else None,
            "bulk_state":summarize_state(bulk_state),
            "bulk_coverage":coverage(bulk_state) if isinstance(bulk_state,dict) else None,
            "bulk_index":{
                "snapshot_id":bulk_index.get("snapshot_id"),
                "source_receipt_count":bulk_index.get("source_receipt_count"),
                "row_count":bulk_index.get("row_count"),
                "files":len(bulk_index.get("files",[])),
            } if isinstance(bulk_index,dict) else None,
        }
    print(json.dumps({"result":"pass","operation":"verify-migrated-r2-campaign-state","sources":result},sort_keys=True))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
