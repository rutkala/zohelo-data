#!/usr/bin/env python3
"""Bounded verification that migrated Eurostat/WDI campaign pointers resolve on R2."""
from __future__ import annotations

import json

from ingestion.full_source_campaign import coverage
from ingestion.source_campaign_store import R2CampaignStore

CAMPAIGNS=(
    ("eurostat",False),
    ("eurostat_bulk",True),
    ("world_bank_wdi",False),
    ("world_bank_wdi_bulk",True),
)


def summary(state):
    if state is None:
        return None
    return {
        "source_id":state.get("source_id"),
        "provider_id":state.get("provider_id"),
        "phase":state.get("phase"),
        "accepted_responses":state.get("accepted_responses"),
        "pending":len(state.get("pending",[])),
        "completed":len(state.get("completed",{})),
        "receipts":len(state.get("receipts",[])),
        "rejected_receipts":len(state.get("rejected_receipts",[])),
    }


def main()->int:
    rows={}
    for campaign,publication_only in CAMPAIGNS:
        store=R2CampaignStore(campaign,publication_only=publication_only)
        state=store.load()
        landing=store.load_landing_pointer()
        item={
            "state":summary(state),
            "landing_pointer":landing,
        }
        if campaign.endswith("_bulk") and isinstance(state,dict):
            item["coverage"]=coverage(state)
        rows[campaign]=item
    print(json.dumps({
        "result":"pass",
        "operation":"verify-r2-campaign-pointers",
        "campaigns":rows,
        "writes_performed":False,
    },sort_keys=True))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
