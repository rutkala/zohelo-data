"""On-demand, resumable BDL Web-history campaign with lossless selection splitting.

Only UI routing/selection metadata and native bytes are handled here. Existing
raw exports are retained. Legacy subgroup receipts are not upgraded into exact
selection coverage: missing subgroups run first, then legacy coverage is rebuilt.
There is no scheduling, self-dispatch, API observation call or data processing.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
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


def authorize_production_runner(storage, allow_codespace: bool = False) -> str:
    """Verify execution environment, approved revision, Drive root, credentials, and exclusive ownership."""
    is_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
    is_main = os.environ.get("GITHUB_REF") == "refs/heads/main"
    codespace_opt_in = allow_codespace or os.environ.get("ZOHELO_ALLOW_CODESPACE_EXECUTION", "").lower() == "true"

    if is_actions:
        if not is_main:
            raise PermissionError("GitHub Actions BDL runner must run on refs/heads/main")
        host_kind = "github_actions"
    elif codespace_opt_in:
        try:
            diff = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
            if diff and os.environ.get("ZOHELO_ALLOW_UNCOMMITTED_DEV", "").lower() != "true":
                raise PermissionError("Codespace production execution requires a clean working tree; commit changes or set ZOHELO_ALLOW_UNCOMMITTED_DEV=true")
        except (subprocess.SubprocessError, OSError):
            pass
        host_kind = "codespace"
    else:
        raise PermissionError(
            "BDL Web full-history runner must run in serialized main-branch GitHub Actions "
            "or with explicit Codespace production authorization (ZOHELO_ALLOW_CODESPACE_EXECUTION=true or --allow-codespace)"
        )

    missing = []
    if not os.environ.get("GUS_BDL_WEB_EMAIL"):
        missing.append("GUS_BDL_WEB_EMAIL")
    if not os.environ.get("GUS_BDL_WEB_PASSWORD"):
        missing.append("GUS_BDL_WEB_PASSWORD")
    if missing:
        raise PermissionError(f"Missing required Web credentials: {', '.join(missing)}")

    storage.authorize_writes()

    if host_kind == "codespace" and os.environ.get("GITHUB_TOKEN"):
        try:
            import urllib.request
            repo = os.environ.get("GITHUB_REPOSITORY", "rutkala/zohelo-data")
            url = f"https://api.github.com/repos/{repo}/actions/workflows/bdl-web-bootstrap.yml/runs?status=in_progress"
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "zohelo-data-runner"
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                if data.get("total_count", 0) > 0:
                    raise RuntimeError("Active GitHub Actions BDL writer is running; cannot start concurrent Codespace writer")
        except Exception as exc:
            if isinstance(exc, RuntimeError):
                raise

    return host_kind


def acquire_writer_lock(storage, control, host_kind: str) -> tuple[DriveControl, dict[str, Any]]:
    lock_store = DriveControl(storage, control, "bdl-writer-lock.json")
    existing = lock_store.load()
    if existing and existing.get("status") == "active":
        heartbeat = existing.get("heartbeat_utc") or existing.get("started_at_utc", "")
        if heartbeat:
            try:
                from datetime import datetime, timezone
                hb_time = datetime.fromisoformat(heartbeat.replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - hb_time).total_seconds()
                if age < 600 and (existing.get("host") != host_kind or existing.get("pid") != os.getpid()):
                    raise RuntimeError(
                        f"Another active writer owns the BDL campaign: host={existing.get('host')}, "
                        f"pid={existing.get('pid')}, last heartbeat {age:.0f}s ago ({heartbeat})"
                    )
            except (ValueError, TypeError):
                pass
    lock_data = {
        "format_version": 1,
        "source_id": "gus_bdl",
        "transport": "web_ui",
        "record_type": "bdl_writer_lock",
        "host": host_kind,
        "pid": os.getpid(),
        "started_at_utc": now(),
        "heartbeat_utc": now(),
        "status": "active",
    }
    lock_store.save(lock_data)
    return lock_store, lock_data


def update_writer_heartbeat(lock_store, lock_data):
    if lock_store and lock_data:
        try:
            lock_data["heartbeat_utc"] = now()
            lock_store.save(lock_data)
        except Exception:
            pass


def release_writer_lock(lock_store, lock_data):
    if lock_store and lock_data:
        try:
            lock_data["status"] = "released"
            lock_data["released_at_utc"] = now()
            lock_store.save(lock_data)
        except Exception:
            pass


def campaign_summary(state, legacy, files, transferred, reason, mode="resume"):
    known = set(state["candidates"])
    records = state.get("selection_plans", {})
    complete = {k for k, v in records.items() if v.get("summary", {}).get("complete")}
    verified = {k for k, v in state["candidates"].items() if v.get("web_catalogue_verified")}
    catalogue_complete = bool(known) and not state["discovery_pending"] and known == verified and not state["catalogue_errors"]

    pass_outcomes = state.get("pass_outcomes", {})
    unvisited = known - set(pass_outcomes.keys())
    unresolved_partial = {k for k, v in pass_outcomes.items() if v.get("status") == "partial"}
    pass_complete = catalogue_complete and not unvisited and not unresolved_partial and not state.get("in_flight")

    has_failures = bool(state.get("failures")) or any(
        v.get("status") == "failed" for v in pass_outcomes.values()
    ) or any(v.get("summary", {}).get("blocked_selections", 0) > 0 for v in records.values())

    load_complete = pass_complete and (known <= complete) and not has_failures

    if reason in {"operator_stop", "interrupted", "runtime_budget_reached"}:
        status = "interrupted"
    elif load_complete:
        status = "load_complete"
    elif pass_complete:
        status = "pass_complete"
    else:
        status = "incomplete"

    return {
        "status": status,
        "pass_complete": pass_complete,
        "load_complete": load_complete,
        "pass_id": state.get("pass_id"),
        "mode": mode,
        "updated_at_utc": now(),
        "completion_scope": "all_planned_web_selections",
        "content_validation": "not_performed",
        "known_subgroups": len(known),
        "visited_subgroups_this_pass": len(known & set(pass_outcomes.keys())),
        "unvisited_subgroups_this_pass": len(unvisited),
        "selection_complete_subgroups": len(known & complete),
        "legacy_native_subgroups_retained": len(legacy),
        "legacy_scope_unreconciled": len((known & legacy) - complete),
        "remaining_subgroups": len(known - complete),
        "catalogue_exhausted": catalogue_complete,
        "pending_catalogue_tables": len(state["discovery_pending"]),
        "catalogue_errors": state["catalogue_errors"],
        "new_files_this_run": files,
        "new_bytes_this_run": transferred,
        "blocked_selections": sum(v.get("summary", {}).get("blocked_selections", 0) for v in records.values()),
        "run_stop_reason": reason,
        "recurring_schedule": False,
    }


def run(workspace, max_seconds=None, seed=None, mode="resume", concurrency=1, allow_codespace=False):
    from bdl_bulk_plan import _bulk_roots, _durable_status
    from bdl_web_bootstrap import _clean_ephemeral
    from storage_manager import StorageManager

    if max_seconds is not None and max_seconds < 1:
        raise ValueError("max_seconds must be positive when provided")
    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    session = storage.begin_write_session()
    landing_root = storage.resolve_zone("landing", create=False)
    bulk, control = _bulk_roots(storage)

    host_kind = authorize_production_runner(storage, allow_codespace=allow_codespace)
    lock_store, lock_data = acquire_writer_lock(storage, control, host_kind)

    store = DriveControl(storage, control, "web-queue-v1.json")
    if mode == "reload":
        existing_state = store.load() or {}
        state = new_state(seed)
        if not seed and existing_state.get("candidates"):
            state["candidates"] = deepcopy(existing_state["candidates"])
            state["discovery_pending"] = deepcopy(existing_state.get("discovery_pending", []))
            state["discovery_completed"] = deepcopy(existing_state.get("discovery_completed", {}))
            state["catalogue_errors"] = deepcopy(existing_state.get("catalogue_errors", {}))
        state["pass_id"] = f"pass-{int(time.time())}"
        state["pass_started_at_utc"] = now()
        state["pass_outcomes"] = {}
        state["in_flight"] = []
    else:
        state = store.load() or new_state(seed)
        state.setdefault("pass_id", "pass-initial")
        state.setdefault("pass_outcomes", {})
        state.setdefault("in_flight", [])
    state.setdefault("selection_plans", {})
    state.setdefault("failures", {})

    legacy, _ = _durable_status(storage, bulk, control)
    start = time.monotonic()
    files = transferred = consecutive_failures = 0
    reason = "running"
    discovery_attempted = set()

    def report():
        value = campaign_summary(state, legacy, files, transferred, reason, mode=mode)
        (workspace / "bootstrap-summary.json").write_bytes(rendered(value))
        print(json.dumps(value, ensure_ascii=False), flush=True)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            Path(os.environ["GITHUB_STEP_SUMMARY"]).write_text(
                "# BDL Web full-history campaign\n\n"
                f"Campaign pass: **{value['status']}**; pass complete: **{value['pass_complete']}**; full load complete: **{value['load_complete']}**.\n\n"
                f"Visited this pass: **{value['visited_subgroups_this_pass']} / {value['known_subgroups']}**; "
                f"selection-complete subgroups: **{value['selection_complete_subgroups']}**.\n\n"
                f"New native files this run: **{files}**; bytes: **{transferred:,}**. "
                f"Existing native subgroups retained: {len(legacy)}.\n\n"
                f"Catalogue pending: {value['pending_catalogue_tables']}; blocked selections: {value['blocked_selections']}. "
                f"Stop reason: {reason}. Mode: {mode}. Host: {host_kind}.\n\n"
                "Native bytes only. No archive extraction, CSV parsing, row counting, Parquet, API observations or medallion processing. "
                "Completed selections are transport coverage, not validation of their numerical contents.\n"
            )
        return value

    def save_plan(plan_store, plan):
        value = parts.summary(plan)
        plan_store.save(plan)
        state["selection_plans"][plan["subgroup_id"]] = {
            "id": plan_store.file_id, "sha256": sha256(plan_store.observed).hexdigest(), "summary": value
        }
        store.save(state)
        print(json.dumps({"status": "bdl_selection_progress", "subgroup_id": plan["subgroup_id"], **value}), flush=True)
        report()

    try:
        store.save(state)
        report()
        while True:
            if max_seconds is not None and (time.monotonic() - start) >= max_seconds:
                reason = "interrupted"
                break

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
                update_writer_heartbeat(lock_store, lock_data)

            pass_outcomes = state["pass_outcomes"]
            unvisited = [c for k, c in state["candidates"].items() if k not in pass_outcomes]

            if not unvisited:
                partials = [state["candidates"][k] for k, v in pass_outcomes.items() if v.get("status") == "partial"]
                if partials:
                    item = min(partials, key=lambda c: int(c["subgroup_id"][1:]))
                else:
                    if catalogue_task:
                        continue
                    catalogue_complete = bool(state["candidates"]) and not state["discovery_pending"] and not state["catalogue_errors"]
                    if catalogue_complete:
                        all_complete = set(state["candidates"]) <= {k for k, v in state.get("selection_plans", {}).items() if v.get("summary", {}).get("complete")}
                        has_failures = bool(state.get("failures")) or any(v.get("summary", {}).get("blocked_selections", 0) > 0 for v in state.get("selection_plans", {}).values())
                        reason = "load_complete" if (all_complete and not has_failures) else "pass_complete"
                    else:
                        reason = "catalogue_unresolved"
                    break
            else:
                item = min(unvisited, key=lambda c: (c["subgroup_id"] != "P1313", c["subgroup_id"] in legacy, int(c["subgroup_id"][1:])))
            subgroup = item["subgroup_id"]
            state["in_flight"] = [subgroup]
            store.save(state)
            update_writer_heartbeat(lock_store, lock_data)

            plan_store = DriveControl(storage, control, f"web-parts-{subgroup}-v1.json")
            existing_plan = plan_store.load()
            if mode == "resume" and existing_plan and parts.summary(existing_plan)["complete"]:
                try:
                    for node in existing_plan["nodes"].values():
                        if node.get("status") == "landed" and "receipt" in node:
                            check_stored_receipt(storage, existing_plan, node, node["receipt"])
                    state["pass_outcomes"][subgroup] = {
                        "status": "reused",
                        "files": len([n for n in existing_plan["nodes"].values() if n.get("status") == "landed"]),
                        "verified_at_utc": now()
                    }
                    save_plan(plan_store, existing_plan)
                    consecutive_failures = 0
                    state["in_flight"] = []
                    store.save(state)
                    continue
                except Exception:
                    existing_plan = None

            if mode == "resume" and existing_plan:
                summ = parts.summary(existing_plan)
                if summ["files"] == 0 and (summ["blocked_selections"] > 0 or subgroup in state.get("failures", {})):
                    plan = None
                else:
                    plan = existing_plan
            else:
                plan = None
            if plan is None:
                _clean_ephemeral(workspace)
                whole_node = parts.task("download", dimensions={}, layout=None, territories=["all"])
                print(json.dumps({"status": "bdl_web_whole_subgroup_attempt", "subgroup_id": subgroup}), flush=True)
                try:
                    rem = int(max_seconds - (time.monotonic() - start) - 60) if max_seconds is not None else 600
                    timeout = min(600, rem) if rem > 60 else 60
                    result = invoke_selection(item, whole_node, workspace, timeout)
                    if result.get("status") == "download":
                        part_store = DriveControl(storage, control, f"web-part-{subgroup}-whole.json")
                        receipt = persist_download(storage, session, landing_root, control, {"subgroup_id": subgroup, "dimension_inventory": []}, whole_node, result, workspace, part_store)
                        whole_plan = parts.new_whole_plan(subgroup, item["url"], root_task=whole_node)
                        parts.accept_download(whole_plan, whole_plan["nodes"][whole_plan["root"]], receipt)
                        save_plan(plan_store, whole_plan)
                        files += 1
                        transferred += receipt["archive_object"]["size"]
                        consecutive_failures = 0
                        state["pass_outcomes"][subgroup] = {"status": "landed", "files": 1, "bytes": receipt["archive_object"]["size"], "completed_at_utc": now()}
                        state["failures"].pop(subgroup, None)
                        state["in_flight"] = []
                        store.save(state)
                        continue
                except WorkerFailure as exc:
                    if exc.failure_class in {"authentication_or_site", "rate_limit", "storage_error"}:
                        state["failures"][subgroup] = {"last_attempt_utc": now(), "error": str(exc), "failure_class": exc.failure_class}
                        state["pass_outcomes"][subgroup] = {"status": "failed", "error": str(exc)}
                        state["in_flight"] = []
                        store.save(state)
                        reason = "interrupted"
                        raise
                    plan = parts.new_plan(subgroup, item["url"])
                    save_plan(plan_store, plan)
                except Exception:
                    plan = parts.new_plan(subgroup, item["url"])
                    save_plan(plan_store, plan)

            parts.validate(plan)
            for node in plan["nodes"].values():
                if node["status"] == "blocked":
                    node.update(status="pending", attempts=0)
            save_plan(plan_store, plan)

            subgroup_tasks_processed = 0
            subgroup_failures = 0
            max_subgroup_slice = 15
            max_subgroup_failures = 5
            while subgroup_tasks_processed < max_subgroup_slice:
                if max_seconds is not None and (time.monotonic() - start) >= max_seconds:
                    break
                node = parts.next_task(plan, set())
                save_plan(plan_store, plan)
                if node is None:
                    break
                subgroup_tasks_processed += 1
                _clean_ephemeral(workspace)
                part_store = None
                if node["scope"]["kind"] == "download":
                    part_store = DriveControl(storage, control, f"web-part-{subgroup}-{node['id']}.json")
                    retained = part_store.load() if mode == "resume" else None
                    if retained:
                        try:
                            check_stored_receipt(storage, plan, node, retained)
                            parts.accept_download(plan, node, retained)
                            save_plan(plan_store, plan)
                            continue
                        except Exception:
                            retained = None

                print(json.dumps({"status": "bdl_web_selection_started", "subgroup_id": subgroup,
                                  "selection_id": node["id"], "kind": node["scope"]["kind"],
                                  "territories": len(node["scope"].get("territories") or [])}), flush=True)
                try:
                    rem = int(max_seconds - (time.monotonic() - start) - 60) if max_seconds is not None else 600
                    timeout = min(600, rem) if rem > 60 else 60
                    result = invoke_selection(item, node, workspace, timeout)
                    process_result(plan, node, result)
                except WorkerFailure as exc:
                    consecutive_failures += 1
                    subgroup_failures += 1
                    disposition = parts.failed(plan, node, str(exc), exc.failure_class)
                    state["failures"][subgroup] = {"last_attempt_utc": now(), "error": str(exc),
                                                   "selection_id": node["id"], "failure_class": exc.failure_class}
                    (workspace / f"failure-{subgroup}-{node['id']}.json").write_bytes(rendered(state["failures"][subgroup]))
                    save_plan(plan_store, plan)
                    if consecutive_failures >= 20:
                        reason = "interrupted"
                        raise
                    if subgroup_failures >= max_subgroup_failures or disposition == "stop":
                        break
                    if disposition == "retry":
                        time.sleep(2)
                    continue

                if node["scope"]["kind"] == "download":
                    receipt = persist_download(storage, session, landing_root, control, plan, node, result, workspace, part_store)
                    parts.accept_download(plan, node, receipt)
                    consecutive_failures = 0
                    subgroup_failures = 0
                    files += 1
                    transferred += receipt["archive_object"]["size"]
                    (workspace / f"landed-{subgroup}-{node['id']}.json").write_bytes(rendered(receipt))

                save_plan(plan_store, plan)

            summary_val = parts.summary(plan)
            if summary_val["complete"]:
                consecutive_failures = 0
                state["failures"].pop(subgroup, None)
                state["pass_outcomes"][subgroup] = {"status": "landed", "files": summary_val["files"], "bytes": summary_val["bytes"], "completed_at_utc": now()}
            elif summary_val.get("blocked_selections", 0) > 0:
                state["pass_outcomes"][subgroup] = {"status": "failed", "blocked_selections": summary_val["blocked_selections"], "error": state.get("failures", {}).get(subgroup, {}).get("error", "blocked")}
            else:
                state["pass_outcomes"][subgroup] = {"status": "partial", "files": summary_val["files"], "outstanding_selections": summary_val["outstanding_selections"], "updated_at_utc": now()}

            state["in_flight"] = []
            store.save(state)
            update_writer_heartbeat(lock_store, lock_data)

        return report()
    finally:
        if lock_store and lock_data:
            release_writer_lock(lock_store, lock_data)
        if "state" in locals() and "legacy" in locals():
            report()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--max-seconds", type=int, default=None)
    parser.add_argument("--seed", type=Path)
    parser.add_argument("--mode", choices=["resume", "reload"], default="resume")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--allow-codespace", action="store_true", default=False)
    args = parser.parse_args()
    result = run(
        args.workspace,
        max_seconds=args.max_seconds,
        seed=args.seed,
        mode=args.mode,
        concurrency=args.concurrency,
        allow_codespace=args.allow_codespace,
    )
    return 0 if result["status"] in {"pass_complete", "load_complete", "complete"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
