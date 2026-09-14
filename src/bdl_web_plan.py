"""Plan complete GUS BDL Web UI historical bootstrap from the API metadata catalogue."""
from __future__ import annotations

import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any

from bdl_bulk_plan import _bulk_roots, _catalogue_candidates, _durable_status, _require_production_context
from ingestion.landing_publication import verify_landing
from ingestion.source_campaign import validate_state
from ingestion.source_campaign_store import DriveCampaignStore
from storage_manager import StorageManager


def _catalogue_subjects(store: DriveCampaignStore, state: dict[str, Any]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for descriptor in state.get("receipts", []):
        receipt = store.read_receipt(descriptor)
        task = receipt.get("task", {})
        cursor = task.get("cursor", {}) if isinstance(task, dict) else {}
        if task.get("kind") != "subject_detail" or cursor.get("lang") != "pl":
            continue
        raw = store.read_raw(receipt["raw"])
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("BDL subject-detail catalogue response is invalid JSON") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("BDL subject-detail catalogue response is not an object")
        identifier = payload.get("id")
        if identifier != cursor.get("entity_id") or not isinstance(identifier, str):
            raise RuntimeError("BDL subject-detail response identity does not match its receipt")
        record = {
            "subject_id": identifier,
            "parent_subject_id": payload.get("parentId"),
            "subject_name": payload.get("name"),
            "has_variables": payload.get("hasVariables"),
            "last_update": payload.get("lastUpdate"),
            "years": payload.get("years"),
        }
        existing = by_id.get(identifier)
        if existing is not None and existing != record:
            raise RuntimeError(f"Conflicting BDL subject detail for {identifier}")
        by_id[identifier] = record
    return [by_id[key] for key in sorted(by_id)]


def plan() -> dict[str, Any]:
    _require_production_context()
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    bulk_id, control_id = _bulk_roots(storage)
    landed, _ignored_processed = _durable_status(storage, bulk_id, control_id)
    store = DriveCampaignStore(storage, "gus_bdl")
    state = store.load()
    if state is None:
        raise RuntimeError("No metadata-only BDL catalogue state is available")
    validate_state(state, "gus_bdl")
    forbidden = [
        task.get("id") for task in state.get("pending", [])
        if task.get("kind") not in {"dictionary", "years", "subjects", "subject_detail", "units", "variables"}
    ]
    if forbidden:
        raise RuntimeError("BDL bootstrap catalogue contains forbidden observation tasks")
    catalogue_complete = not state.get("pending")
    landing_manifest = verify_landing(store)
    landing_current = bool(
        landing_manifest
        and landing_manifest.get("accepted_response_count") == state.get("accepted_responses")
        and landing_manifest.get("published_response_count") == state.get("accepted_responses")
        and landing_manifest.get("pending_publication_count") == 0
    )
    subjects = _catalogue_subjects(store, state)
    candidates, invalid = _catalogue_candidates(subjects)
    catalogue_payload = json.dumps(subjects, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    catalogue_sha = sha256(catalogue_payload).hexdigest()
    remaining = [item for item in candidates if item["subgroup_id"] not in landed]
    candidate = remaining[0] if catalogue_complete and landing_current and not invalid and remaining else None
    if not catalogue_complete:
        status = "catalogue_incomplete"
    elif not landing_current:
        status = "catalogue_landing_incomplete"
    elif invalid:
        status = "catalogue_invalid"
    elif remaining:
        status = "candidate"
    else:
        status = "complete"
    return {
        "status": status,
        "catalogue_sha256": catalogue_sha,
        "catalogue_subjects": len(subjects),
        "catalogue_subgroups": len(candidates) + len(invalid),
        "catalogue_valid_subgroups": len(candidates),
        "catalogue_invalid_subgroups": len(invalid),
        "invalid_subgroup_examples": invalid[:20],
        "catalogue_complete": catalogue_complete,
        "catalogue_landing_current": landing_current,
        "catalogue_pending_tasks": len(state.get("pending", [])),
        "catalogue_accepted_responses": state.get("accepted_responses", 0),
        "landed_subgroups": len(landed & {item["subgroup_id"] for item in candidates}),
        "remaining_subgroups": len(remaining),
        "candidate": candidate,
        "bulk_root_id": bulk_id,
        "control_root_id": control_id,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = plan()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    if result["status"] in {"catalogue_invalid", "catalogue_landing_incomplete"}:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
