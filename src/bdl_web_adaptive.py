"""On-demand, resumable BDL Web-history campaign with lossless selection splitting.

Only UI routing/selection metadata and native bytes are handled here. Existing
raw exports are retained. Legacy subgroup receipts are not upgraded into exact
selection coverage: missing subgroups run first, then legacy coverage is rebuilt.
There is no scheduling, self-dispatch, API observation call or data processing.
"""
from __future__ import annotations

import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
import signal
import subprocess
import time

import bdl_web_partitions as parts
from bdl_web_queue import DriveControl, apply_discovery, new_state, now, rendered, invoke

ROOT = Path(__file__).resolve().parents[1]


class WorkerFailure(RuntimeError):
    def __init__(self, message, failure_class):
        super().__init__(message)
        self.failure_class = failure_class


def invoke_selection(item, node, workspace, timeout):
    request = {"subgroup_id": item["subgroup_id"], "url": item["url"],
               "selection_id": node["id"], "scope": node["scope"]}
    request_path = workspace / "selection-task.json"
    result_path = workspace / "selection-result.json"
    request_path.write_bytes(parts.canonical(request))
    result_path.unlink(missing_ok=True)
    # Do not pass Drive OAuth credentials or GitHub tokens to the browser process.
    names = ("PATH", "HOME", "TMPDIR", "LD_LIBRARY_PATH", "PLAYWRIGHT_BROWSERS_PATH",
             "GUS_BDL_WEB_EMAIL", "GUS_BDL_WEB_PASSWORD")
    env = {key: os.environ[key] for key in names if key in os.environ}
    env.update(BDL_WEB_TASK_PATH=str(request_path), BDL_BULK_OUT_DIR=str(workspace))
    process = subprocess.Popen(["node", str(ROOT / "portal/scripts/bdl-web-selection-worker.mjs")],
                               cwd=ROOT / "portal", env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, start_new_session=True)
    timed_out = False
    try:
        process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
    result = json.loads(result_path.read_text()) if result_path.is_file() else {}
    if timed_out:
        kind = "provider_timeout" if result.get("stage") in {"table", "download"} else "browser_infrastructure"
        raise WorkerFailure("Bounded Web worker timeout at " + str(result.get("stage", "startup")), kind)
    if process.returncode:
        raise WorkerFailure(str(result.get("error", "Web worker stopped without a result"))[:1800],
                            result.get("failure_class", "browser_infrastructure"))
    if result.get("subgroup_id") != item["subgroup_id"] or result.get("selection_id") != node["id"]:
        raise WorkerFailure("Web result identity mismatch", "ui_protocol")
    return result


def check_stored_receipt(storage, plan, node, receipt):
    """Receipt bytes were already verified by DriveControl; verify its native object."""
    probe = json.loads(json.dumps(plan))
    parts.accept_download(probe, probe["nodes"][node["id"]], receipt)
    obj = receipt["archive_object"]
    current = storage.drive_service.files().get(
        fileId=obj["id"], fields="id,size,md5Checksum,appProperties,trashed").execute(num_retries=4)
    if (current.get("trashed") or int(current.get("size", -1)) != obj["size"]
            or current.get("md5Checksum") != obj.get("md5")
            or (current.get("appProperties") or {}).get("sha256") != obj["sha256"]):
        raise RuntimeError("Retained BDL partition object no longer matches its receipt")


def persist_download(storage, session, landing_root, control, plan, node, result, workspace, part_store):
    from bdl_bulk_ingest import _hash_file, _upload_file, _upload_manifest
    descriptor = result.get("archive", {})
    name = descriptor.get("filename", "")
    local_name = descriptor.get("local_filename", "")
    if not name or any(c in name + local_name for c in ("/", "\\", "\0", "\r", "\n")):
        raise RuntimeError("Unsafe native filename")
    archive = workspace / local_name
    if (not archive.is_file() or archive.stat().st_size != descriptor.get("bytes")
            or _hash_file(archive, "sha256") != descriptor.get("sha256")):
        raise RuntimeError("Native Web transfer identity did not verify")
    folder = storage.get_or_create_nested_folder(
        ["gus_bdl", "web_bulk", plan["subgroup_id"], descriptor["sha256"]],
        root_id=landing_root, write_session=session)
    obj = _upload_file(storage, archive, name=name, parent_id=folder,
                       sha256_hex=descriptor["sha256"], md5_hex=_hash_file(archive, "md5"), kind="source_native")
    obj = {key: value for key, value in obj.items() if key != "reused"}
    receipt = {"format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
               "record_type": "native_partition_receipt", "subgroup_id": plan["subgroup_id"],
               "selection_id": node["id"], "selection": node["scope"],
               "dimension_inventory_sha256": parts.digest(plan["dimension_inventory"]),
               "landing_scope": "native_bytes_only", "content_validation": "not_performed",
               "archive_object": obj, "completed_at_utc": now()}
    receipt["manifest_object"] = _upload_manifest(storage, receipt, parent_id=control)
    # This is a PART receipt. Never call the old subgroup completion-marker writer here.
    part_store.save(receipt)
    check_stored_receipt(storage, plan, node, receipt)
    return receipt


def process_result(plan, node, result):
    kind = node["scope"]["kind"]
    if result.get("status") != kind:
        raise WorkerFailure("Unexpected Web selection result", "ui_protocol")
    if kind == "dimensions":
        parts.accept_dimensions(plan, node, result["dimensions"])
    elif kind == "layouts":
        parts.accept_layouts(plan, node, result["layouts"])
    elif kind == "territories":
        parts.accept_territories(plan, node, result["items"], result["advertised_count"])


def campaign_summary(state, legacy, files, transferred, reason):
    known = set(state["candidates"])
    records = state.get("selection_plans", {})
    complete = {k for k, v in records.items() if v["summary"]["complete"]}
    verified = {k for k, v in state["candidates"].items() if v.get("web_catalogue_verified")}
    catalogue_complete = bool(known) and not state["discovery_pending"] and known == verified and not state["catalogue_errors"]
    done = catalogue_complete and known <= complete
    return {"status": "complete" if done else "incomplete", "updated_at_utc": now(),
            "completion_scope": "all_planned_web_selections", "content_validation": "not_performed",
            "known_subgroups": len(known), "selection_complete_subgroups": len(known & complete),
            "legacy_native_subgroups_retained": len(legacy), "legacy_scope_unreconciled": len((known & legacy) - complete),
            "remaining_subgroups": len(known - complete), "catalogue_exhausted": catalogue_complete,
            "pending_catalogue_tables": len(state["discovery_pending"]), "catalogue_errors": state["catalogue_errors"],
            "new_files_this_run": files, "new_bytes_this_run": transferred,
            "blocked_selections": sum(v["summary"]["blocked_selections"] for v in records.values()),
            "run_stop_reason": reason, "recurring_schedule": False}


def run(workspace, max_seconds=18600, seed=None):
    from bdl_bulk_plan import _bulk_roots, _durable_status
    from bdl_web_bootstrap import _require_production_context, _clean_ephemeral
    from storage_manager import StorageManager
    _require_production_context()
    if not 300 <= max_seconds <= 18600:
        raise ValueError("Web campaign runtime must be 300..18600 seconds")
    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    session = storage.begin_write_session()
    landing_root = storage.resolve_zone("landing", create=False)
    bulk, control = _bulk_roots(storage)
    store = DriveControl(storage, control, "web-queue-v1.json")
    state = store.load() or new_state(seed)
    state.setdefault("selection_plans", {})
    legacy, _ = _durable_status(storage, bulk, control)
    start = time.monotonic()
    files = transferred = consecutive_failures = 0
    reason = "running"
    attempted_groups, discovery_attempted = set(), set()

    def report():
        value = campaign_summary(state, legacy, files, transferred, reason)
        (workspace / "bootstrap-summary.json").write_bytes(rendered(value))
        print(json.dumps(value, ensure_ascii=False), flush=True)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            Path(os.environ["GITHUB_STEP_SUMMARY"]).write_text(
                "# BDL Web initial history — on demand\n\n"
                f"Campaign: **{value['status']}**; exact-selection complete subgroups: **{value['selection_complete_subgroups']} / {value['known_subgroups']}**.\n\n"
                f"New native files this run: **{files}**; bytes: **{transferred:,}**. "
                f"Existing native subgroups retained: {len(legacy)}; their scope is not assumed verified.\n\n"
                f"Catalogue pending: {value['pending_catalogue_tables']}; blocked selections: {value['blocked_selections']}. "
                f"Stop reason: {reason}. No recurring schedule or automatic successor run is configured.\n\n"
                "Native bytes only. No archive extraction, CSV parsing, row counting, Parquet, API observations or medallion processing. "
                "Completed selections are transport coverage, not validation of their numerical contents.\n")
        return value

    def save_plan(plan_store, plan):
        value = parts.summary(plan)
        plan_store.save(plan)
        state["selection_plans"][plan["subgroup_id"]] = {
            "id": plan_store.file_id, "sha256": sha256(plan_store.observed).hexdigest(), "summary": value}
        store.save(state)
        print(json.dumps({"status": "bdl_selection_progress", "subgroup_id": plan["subgroup_id"], **value}), flush=True)
        report()

    try:
        store.save(state)
        report()
        while max_seconds - (time.monotonic() - start) >= 300:
            catalogue_task = next((t for t in state["discovery_pending"] if t["url"] not in discovery_attempted), None)
            if catalogue_task:
                discovery_attempted.add(catalogue_task["url"])
                env = {k: os.environ[k] for k in ("PATH", "HOME", "LD_LIBRARY_PATH", "PLAYWRIGHT_BROWSERS_PATH") if k in os.environ}
                env.update(BDL_CATALOGUE_TASK=json.dumps(catalogue_task), BDL_CATALOGUE_OUTPUT=str(workspace / "catalogue-task.json"))
                try:
                    result = invoke("bdl-web-catalogue.mjs", env, 240, workspace / "catalogue-task.json")
                    state = apply_discovery(state, catalogue_task, result)
                except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as exc:
                    state["catalogue_errors"][catalogue_task["url"]] = str(exc)[:1200]
                store.save(state)
            eligible = [c for k, c in state["candidates"].items() if k not in attempted_groups
                        and not state["selection_plans"].get(k, {}).get("summary", {}).get("complete")]
            if not eligible:
                if catalogue_task:
                    continue
                reason = "remaining_selections_blocked" if state["candidates"] else "catalogue_unresolved"
                break
            item = min(eligible, key=lambda c: (c["subgroup_id"] != "P1313", c["subgroup_id"] in legacy, int(c["subgroup_id"][1:])))
            subgroup = item["subgroup_id"]
            attempted_groups.add(subgroup)
            plan_store = DriveControl(storage, control, f"web-parts-{subgroup}-v1.json")
            plan = plan_store.load() or parts.new_plan(subgroup, item["url"])
            if plan["subgroup_id"] != subgroup or plan["url"] != item["url"]:
                raise RuntimeError("Partition plan no longer matches the Web catalogue")
            parts.validate(plan)
            for node in plan["nodes"].values():
                if node["status"] == "blocked":
                    node.update(status="pending", attempts=0)
            save_plan(plan_store, plan)
            while max_seconds - (time.monotonic() - start) >= 300:
                node = parts.next_task(plan, set())
                save_plan(plan_store, plan)  # Persist any pre-request dimension split.
                if node is None:
                    break
                _clean_ephemeral(workspace)
                part_store = None
                if node["scope"]["kind"] == "download":
                    part_store = DriveControl(storage, control, f"web-part-{subgroup}-{node['id']}.json")
                    retained = part_store.load()
                    if retained:
                        check_stored_receipt(storage, plan, node, retained)
                        parts.accept_download(plan, node, retained)
                        save_plan(plan_store, plan)
                        continue
                print(json.dumps({"status": "bdl_web_selection_started", "subgroup_id": subgroup,
                                  "selection_id": node["id"], "kind": node["scope"]["kind"],
                                  "territories": len(node["scope"].get("territories") or [])}), flush=True)
                try:
                    remaining = int(max_seconds - (time.monotonic() - start))
                    result = invoke_selection(item, node, workspace, min(600, remaining - 60))
                    process_result(plan, node, result)
                except WorkerFailure as exc:
                    consecutive_failures += 1
                    disposition = parts.failed(plan, node, str(exc), exc.failure_class)
                    state["failures"][subgroup] = {"last_attempt_utc": now(), "error": str(exc),
                                                   "selection_id": node["id"], "failure_class": exc.failure_class}
                    (workspace / f"failure-{subgroup}-{node['id']}.json").write_bytes(rendered(state["failures"][subgroup]))
                    save_plan(plan_store, plan)
                    if disposition == "stop" or consecutive_failures >= 20:
                        reason = "operator_repair_required"
                        raise
                    if disposition == "retry":
                        time.sleep(min(20, max(0, max_seconds - (time.monotonic() - start) - 300)))
                    continue
                if node["scope"]["kind"] == "download":
                    # Storage errors are deliberately OUTSIDE the provider retry/split handler.
                    receipt = persist_download(storage, session, landing_root, control, plan, node, result, workspace, part_store)
                    parts.accept_download(plan, node, receipt)
                    consecutive_failures = 0
                    files += 1
                    transferred += receipt["archive_object"]["size"]
                    (workspace / f"landed-{subgroup}-{node['id']}.json").write_bytes(rendered(receipt))
                if parts.summary(plan)["complete"]:
                    state["failures"].pop(subgroup, None)
                save_plan(plan_store, plan)
        if reason == "running":
            reason = "runtime_budget_reached"
        return report()
    finally:
        report()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--max-seconds", type=int, default=18600)
    parser.add_argument("--seed", type=Path)
    args = parser.parse_args()
    result = run(args.workspace, args.max_seconds, args.seed)
    return 1 if result["status"] != "complete" and result["new_files_this_run"] == 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
