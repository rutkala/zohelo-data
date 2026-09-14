"""Run the resumable GUS BDL historical Web bootstrap without API observation ingestion."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

from bdl_bulk_ingest import ingest_archive
from bdl_bulk_plan import (
    _bulk_roots,
    _catalogue_candidates,
    _durable_status,
    _subject_discovery_status,
)
from ingestion.source_campaign import validate_state
from ingestion.source_campaign_store import DriveCampaignStore
from storage_manager import StorageManager

REPO_ROOT = Path(__file__).resolve().parents[1]
PORTAL_ROOT = REPO_ROOT / "portal"
WORKER = PORTAL_ROOT / "scripts" / "bdl-web-bulk-worker.mjs"
ACCEPTED_WORKER_STATUSES = {"downloaded_relational_export", "downloaded_generated_export"}


def _require_production_context() -> None:
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise PermissionError("BDL Web bootstrap must run in serialized main-branch GitHub Actions")


def _clean_ephemeral(workspace: Path) -> None:
    for path in workspace.glob("download-*.zip"):
        path.unlink(missing_ok=True)
    for name in ("worker-result.json", "landing-summary.json"):
        (workspace / name).unlink(missing_ok=True)


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _subject_records(task: dict[str, Any], payload: Any) -> list[dict[str, Any]]:
    cursor = task.get("cursor") if isinstance(task, dict) else None
    if isinstance(cursor, dict) and cursor.get("lang") not in (None, "pl"):
        return []
    kind = task.get("kind") if isinstance(task, dict) else None
    rows: list[Any] = []
    if kind == "subject_detail" and isinstance(payload, dict):
        rows = [payload]
    elif kind == "subjects":
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            for key in ("results", "items", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    rows = value
                    break
    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        identifier = row.get("id")
        if not isinstance(identifier, str) or len(identifier) < 2 or identifier[0] not in "KGP" or not identifier[1:].isdigit():
            continue
        parent = row.get("parentId", row.get("parentSubjectId"))
        if parent == "":
            parent = None
        result.append(
            {
                "subject_id": identifier,
                "parent_subject_id": parent,
                "subject_name": row.get("name"),
                "has_variables": row.get("hasVariables"),
            }
        )
    return result


def _campaign_subject_catalogue(store: DriveCampaignStore, state: dict[str, Any]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for descriptor in state.get("receipts", []):
        receipt = store.read_receipt(descriptor)
        task = receipt.get("task") if isinstance(receipt, dict) else None
        if not isinstance(task, dict) or task.get("kind") not in {"subjects", "subject_detail"}:
            continue
        raw_descriptor = receipt.get("raw")
        if not isinstance(raw_descriptor, dict):
            continue
        try:
            payload = json.loads(store.read_raw(raw_descriptor).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        for record in _subject_records(task, payload):
            existing = by_id.get(record["subject_id"])
            if existing is not None:
                old_parent = existing.get("parent_subject_id")
                new_parent = record.get("parent_subject_id")
                if old_parent not in (None, "") and new_parent not in (None, "") and old_parent != new_parent:
                    raise RuntimeError(f"Conflicting BDL subject hierarchy for {record['subject_id']}")
                merged = dict(existing)
                for key, value in record.items():
                    if value not in (None, ""):
                        merged[key] = value
                by_id[record["subject_id"]] = merged
            else:
                by_id[record["subject_id"]] = record
    return [by_id[key] for key in sorted(by_id)]


def plan() -> dict[str, Any]:
    """Plan Web ingestion from durable campaign catalogue, never from modeled releases."""
    _require_production_context()
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    bulk_id, control_id = _bulk_roots(storage)
    landed, processed = _durable_status(storage, bulk_id, control_id)
    store = DriveCampaignStore(storage, "gus_bdl")
    state = store.load()
    if state is None:
        raise RuntimeError("No durable BDL campaign state is available for Web bootstrap planning")
    validate_state(state, "gus_bdl")
    subjects = _campaign_subject_catalogue(store, state)
    candidates, invalid = _catalogue_candidates(subjects)
    discovery = _subject_discovery_status(state)
    remaining = [item for item in candidates if item["subgroup_id"] not in processed]
    candidate = remaining[0] if remaining else None
    if candidate:
        status = "candidate"
    elif not discovery["exhausted"]:
        status = "catalogue_incomplete"
    elif invalid:
        status = "catalogue_invalid"
    else:
        status = "complete"
    return {
        "status": status,
        "catalogue_source": "campaign_receipts",
        "catalogue_subjects": len(subjects),
        "catalogue_subgroups": len(candidates) + len(invalid),
        "catalogue_valid_subgroups": len(candidates),
        "catalogue_invalid_subgroups": len(invalid),
        "subject_catalogue_exhausted": discovery["exhausted"],
        "pending_subject_catalogue_tasks": discovery["pending_tasks"],
        "invalid_subgroup_examples": invalid[:20],
        "landed_subgroups": len(landed),
        "processed_subgroups": len(processed),
        "remaining_subgroups": len(remaining),
        "candidate": candidate,
        "bulk_root_id": bulk_id,
        "control_root_id": control_id,
    }


def run(*, workspace: Path, max_seconds: int) -> dict:
    _require_production_context()
    if max_seconds < 300:
        raise ValueError("BDL Web bootstrap runtime must be at least five minutes")
    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    completed_this_run = 0
    last_plan: dict | None = None

    while time.monotonic() - started < max_seconds:
        last_plan = plan()
        _write_json(workspace / "plan.json", last_plan)
        status = last_plan.get("status")
        if status == "complete":
            break
        if status != "candidate":
            raise RuntimeError(f"BDL Web bootstrap planner blocked with status {status!r}: {json.dumps(last_plan, ensure_ascii=False, sort_keys=True)}")

        candidate = last_plan.get("candidate")
        if not isinstance(candidate, dict):
            raise RuntimeError("BDL Web bootstrap candidate is malformed")
        subgroup_id = candidate.get("subgroup_id")
        subgroup_url = candidate.get("url")
        subgroup_name = candidate.get("subgroup_name") or subgroup_id
        if not isinstance(subgroup_id, str) or not isinstance(subgroup_url, str):
            raise RuntimeError("BDL Web bootstrap candidate identity is malformed")

        _clean_ephemeral(workspace)
        env = dict(os.environ)
        env.update(
            BDL_BULK_SUBGROUP_ID=subgroup_id,
            BDL_BULK_SUBGROUP_NAME=str(subgroup_name),
            BDL_BULK_URL=subgroup_url,
            BDL_BULK_OUT_DIR=str(workspace),
        )
        print(json.dumps({"status": "bdl_web_subgroup_started", "subgroup_id": subgroup_id, "completed_this_run": completed_this_run}), flush=True)
        subprocess.run(
            ["node", str(WORKER)],
            cwd=PORTAL_ROOT,
            env=env,
            check=True,
            timeout=min(1800, max(300, int(max_seconds - (time.monotonic() - started)))),
        )
        worker_result_path = workspace / "worker-result.json"
        if not worker_result_path.is_file():
            raise RuntimeError("BDL Web worker did not produce worker-result.json")
        worker_result = json.loads(worker_result_path.read_text(encoding="utf-8"))
        if worker_result.get("status") not in ACCEPTED_WORKER_STATUSES:
            raise RuntimeError(f"BDL Web worker did not complete subgroup {subgroup_id}: {worker_result.get('status')}")

        archives = sorted(workspace.glob("download-*.zip"))
        if len(archives) != 1:
            raise RuntimeError(f"BDL Web worker must produce exactly one ZIP for {subgroup_id}; found {len(archives)}")
        landed_summary = ingest_archive(archives[0], subgroup_id, True)
        _write_json(workspace / "landing-summary.json", landed_summary)
        if landed_summary.get("status") != "bdl_web_bulk_landed":
            raise RuntimeError(f"BDL Web landing did not verify for {subgroup_id}")
        completed_this_run += 1
        print(json.dumps({"status": "bdl_web_subgroup_landed", "subgroup_id": subgroup_id, "completed_this_run": completed_this_run, "row_count": landed_summary.get("row_count")}), flush=True)

        if max_seconds - (time.monotonic() - started) < 180:
            break

    final_plan = plan()
    _write_json(workspace / "final-plan.json", final_plan)
    report = {
        "status": "bdl_web_bootstrap_run_complete",
        "completed_this_run": completed_this_run,
        "elapsed_seconds": round(time.monotonic() - started, 2),
        "planner_status": final_plan.get("status"),
        "catalogue_subgroups": final_plan.get("catalogue_subgroups"),
        "landed_subgroups": final_plan.get("landed_subgroups"),
        "remaining_subgroups": final_plan.get("remaining_subgroups"),
    }
    _write_json(workspace / "bootstrap-summary.json", report)
    print(json.dumps(report, sort_keys=True), flush=True)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--max-seconds", type=int, default=18_600)
    args = parser.parse_args()
    run(workspace=args.workspace, max_seconds=args.max_seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main())
