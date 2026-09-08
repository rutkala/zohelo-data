"""Bounded source campaigns: exact responses, independent lanes and durable quotas.

This module collects Landing evidence. It does not promote an NBP release or perform
business transformations. All network attempts are reserved durably before execution.
"""
from copy import deepcopy
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import math
import time
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


LANES = ("recent", "history", "discovery", "reconcile")
# A busy recent lane cannot consume the history allocation, or vice versa.
LANE_CYCLE = ("recent", "history", "recent", "history", "discovery", "reconcile")
STATE_VERSION = 1


class CampaignError(ValueError):
    pass


class CapacityPause(CampaignError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def validate_task(task):
    if not isinstance(task, dict) or not isinstance(task.get("id"), str) or not 0 < len(task["id"]) <= 500:
        raise CampaignError("Task requires a bounded stable ID")
    if task.get("lane") not in LANES or not isinstance(task.get("kind"), str) or not task["kind"]:
        raise CampaignError("Task lane/kind is invalid")
    if not isinstance(task.get("cursor"), dict) or len(canonical(task)) > 16_384:
        raise CampaignError("Task cursor is invalid or oversized")
    if "recurrence_key" in task and not isinstance(task["recurrence_key"], str):
        raise CampaignError("Invalid recurrence key")


def new_state(source_id, today):
    return {"schema_version": STATE_VERSION, "source_id": source_id, "onboarding_date": today.isoformat(),
            "pending": [], "completed": {}, "recent_roots": {}, "quota_attempts": [],
            "receipts": [], "rejected_receipts": [], "raw_bytes": 0, "accepted_responses": 0, "record_count": 0,
            "lane_position": 0, "coverage_status": "incomplete", "catalogue_totals": {}, "last_error": None}


def validate_state(state, source_id):
    if state.get("schema_version") != STATE_VERSION or state.get("source_id") != source_id:
        raise CampaignError("Campaign state identity/version mismatch")
    for name, kind in (("pending", list), ("completed", dict), ("recent_roots", dict),
                       ("quota_attempts", list), ("receipts", list)):
        if not isinstance(state.get(name), kind):
            raise CampaignError("Campaign state shape mismatch")
    ids = []
    for task in state["pending"]:
        validate_task(task)
        ids.append(task["id"])
    if len(set(ids)) != len(ids) or set(ids) & state["completed"].keys():
        raise CampaignError("Pending tasks duplicate accepted work")


def add_tasks(state, tasks, adapter, today, settings):
    existing = {t["id"] for t in state["pending"]} | state["completed"].keys()
    for original in tasks:
        task = deepcopy(original)
        validate_task(task)
        key = task.get("recurrence_key")
        if key and task["lane"] == "recent" and adapter.refresh_task(task, today) is not None:
            state["recent_roots"][key] = task
        if task["id"] not in existing:
            state["pending"].append(task)
            existing.add(task["id"])
    if len(state["pending"]) > settings["max_pending_tasks"]:
        raise CapacityPause("Pending task capacity reached; discovery must wait for admitted work")
    if len(state["recent_roots"]) > settings["max_recent_roots"]:
        raise CapacityPause("Recent-series capacity requires sharding/change-feed design before expansion")


def prepare_state(store, adapter, today, settings):
    state = store.load()
    if state is None:
        state = new_state(adapter.SOURCE_ID, today)
        add_tasks(state, adapter.initial_tasks(today), adapter, today, settings)
    validate_state(state, adapter.SOURCE_ID)
    if today.isoformat() < state["onboarding_date"]:
        raise CampaignError("Campaign date cannot precede onboarding")
    migrate = getattr(adapter, "migrate_state", None)
    if migrate is not None:
        migrated = migrate(deepcopy(state), today)
        if not isinstance(migrated, dict):
            raise CampaignError("Adapter planning migration must return state")
        # Planning corrections may supersede queued work, but cannot rewrite
        # received evidence, completed work, quota or provider cooldown history.
        for key in set(state) | set(migrated):
            if key not in {"pending", "plan_dispositions"} and state.get(key) != migrated.get(key):
                raise CampaignError("Adapter planning migration changed protected campaign evidence")
        validate_state(migrated, adapter.SOURCE_ID)
        state = migrated
    # Pending earlier generations finish before a new generation is enqueued.
    busy = {t.get("recurrence_key") for t in state["pending"] if t["lane"] == "recent"}
    templates = list(state["recent_roots"].items())
    for key, template in templates:
        if key not in busy:
            refreshed = adapter.refresh_task(template, today)
            if refreshed is not None:
                add_tasks(state, [refreshed], adapter, today, settings)
    seeds = [task for task in adapter.recent_tasks(today) if task.get("recurrence_key") not in busy]
    add_tasks(state, seeds, adapter, today, settings)
    store.save(state)
    return state


def choose_task(state, settings, now, *, history_enabled=True):
    for offset in range(len(LANE_CYCLE)):
        slot = (state["lane_position"] + offset) % len(LANE_CYCLE)
        lane = LANE_CYCLE[slot]
        if lane in {"history", "reconcile"} and not history_enabled:
            continue
        # Discovery is backpressured, while admitted history and recent work continue.
        if lane == "discovery" and (len(state["pending"]) >= settings["discovery_pause_threshold"]
                                     or len(state["recent_roots"]) >= settings["max_recent_roots"] - 100):
            continue
        for task in state["pending"]:
            if task["lane"] == lane and task.get("retry_at", 0) <= now:
                state["lane_position"] = (slot + 1) % len(LANE_CYCLE)
                return task
    return None


def quota_wait(state, settings, now):
    windows = settings["quota_windows"]
    longest = max(w["seconds"] for w in windows)
    state["quota_attempts"] = [t for t in state["quota_attempts"] if t > now - longest]
    waits = []
    for window in windows:
        attempts = sorted(t for t in state["quota_attempts"] if t > now - window["seconds"])
        if len(attempts) >= window["requests"]:
            waits.append(attempts[-window["requests"]] + window["seconds"] - now)
    if state["quota_attempts"]:
        waits.append(max(state["quota_attempts"]) + settings["min_request_interval_seconds"] - now)
    return max([0, *waits])


def retry_delay(headers, now):
    raw = headers.get("retry-after", "")
    try:
        delay = float(raw)
    except (TypeError, ValueError):
        try:
            delay = parsedate_to_datetime(raw).timestamp() - now
        except (TypeError, ValueError, OverflowError):
            delay = 900
    # Do not shorten a provider's Retry-After, even if longer than a workflow.
    return max(60, delay) if math.isfinite(delay) else 900


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def fetch(request_spec, allowed_hosts, *, max_bytes, timeout):
    url = request_spec["url"]
    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname not in allowed_hosts or parsed.username
            or parsed.password or parsed.fragment or parsed.port not in (None, 443)):
        raise CampaignError("Source request is outside the approved HTTPS host scope")
    params = request_spec.get("params", {})
    if not isinstance(params, dict):
        raise CampaignError("Request params must be a mapping")
    if params:
        url += ("&" if parsed.query else "?") + urlencode(params, doseq=True)
    request = Request(url, headers={"Accept": "application/json, application/xml, text/plain;q=0.8",
                                  "User-Agent": "zohelo-data/1.0 (+https://github.com/rutkala/zohelo-data)"})
    try:
        response = build_opener(NoRedirect()).open(request, timeout=timeout)
    except HTTPError as error:
        response = error
    with response:
        headers = {k.lower(): v for k, v in response.headers.items()}
        if headers.get("content-length", "").isdigit() and int(headers["content-length"]) > max_bytes:
            raise CampaignError("Source response exceeds configured byte limit")
        body = response.read(max_bytes + 1)
        if len(body) > max_bytes:
            raise CampaignError("Source response exceeds configured byte limit")
        return response.status, body, headers


def run_campaign(store, adapter, today, settings, *, history_enabled=True, fetcher=fetch,
                 clock=time.time, sleeper=time.sleep, code_sha="unknown"):
    started = clock()
    state = prepare_state(store, adapter, today, settings)
    run = {"source_id": adapter.SOURCE_ID, "requests": 0, "accepted_responses": 0,
           "records_received": 0, "retrieved_bytes": 0, "failed_requests": 0, "by_lane": {},
           "by_kind": {}, "errors": [], "reason": "no_due_tasks"}
    while run["requests"] < settings["max_requests"]:
        now = clock()
        if now - started >= settings["max_run_seconds"]:
            run["reason"] = "time_budget"
            break
        if now < state.get("provider_retry_at", 0):
            run["reason"] = "provider_retry_after"
            run["retry_after_seconds"] = round(state["provider_retry_at"] - now, 1)
            break
        if len(state["completed"]) >= settings["max_completed_tasks"]:
            run["reason"] = "state_capacity_pause"
            break
        if state["raw_bytes"] + settings["max_response_bytes"] > settings["max_retained_raw_bytes"]:
            run["reason"] = "storage_capacity_pause"
            break
        task = choose_task(state, settings, now, history_enabled=history_enabled)
        if task is None:
            break
        wait = quota_wait(state, settings, now)
        if wait > settings["max_inline_wait_seconds"] or now + wait - started >= settings["max_run_seconds"]:
            run["reason"] = "quota_wait"
            run["retry_after_seconds"] = round(wait, 1)
            break
        if wait:
            sleeper(wait)
        now = clock()
        request_spec = adapter.request_for(task)
        # A killed worker has consumed a conservative reservation; it cannot reset quota.
        state["quota_attempts"].append(now)
        state["last_attempt_utc"] = datetime.fromtimestamp(now, timezone.utc).isoformat()
        store.save(state)
        run["requests"] += 1
        body, headers, status = b"", {}, 0
        descriptor = None
        try:
            status, body, headers = fetcher(request_spec, adapter.ALLOWED_HOSTS,
                                            max_bytes=settings["max_response_bytes"],
                                            timeout=settings["http_timeout_seconds"])
            if body:
                descriptor = store.put_raw(body, {"task_id": task["id"], "source_id": adapter.SOURCE_ID})
                state["raw_bytes"] += len(body)
                run["retrieved_bytes"] += len(body)
            if status != 200:
                raise CampaignError(f"Source HTTP status {status}; task retained")
            result = adapter.interpret(task, body, today)
            if (not isinstance(result, dict) or type(result.get("record_count")) is not int
                    or result["record_count"] < 0 or not isinstance(result.get("next_tasks"), list)):
                raise CampaignError("Adapter returned an invalid validation result")
            candidate = deepcopy(state)
            candidate["pending"] = [t for t in candidate["pending"] if t["id"] != task["id"]]
            candidate["completed"][task["id"]] = state["last_attempt_utc"]
            add_tasks(candidate, result["next_tasks"], adapter, today, settings)
            receipt = {"schema_version": 1, "source_id": adapter.SOURCE_ID, "task": task,
                       "request": request_spec, "http_status": status, "headers": {k: headers[k] for k in
                           ("etag", "last-modified", "content-type") if k in headers},
                       "retrieved_at_utc": state["last_attempt_utc"], "raw": descriptor,
                       "record_count": result["record_count"], "metadata": result.get("metadata", {}),
                       "code_sha": code_sha, "accepted": True}
            receipt_descriptor = store.put_receipt(receipt)
            candidate["receipts"].append({"task_id": task["id"], **receipt_descriptor})
            if task["lane"] == "discovery":
                for field in ("api_total", "api_total_records", "total_records", "totalRecords"):
                    total = result.get("metadata", {}).get(field)
                    if type(total) is int and total >= 0:
                        candidate.setdefault("catalogue_totals", {})[task["kind"]] = total
            candidate["accepted_responses"] += 1
            candidate["record_count"] += result["record_count"]
            candidate["last_success_utc"] = state["last_attempt_utc"]
            candidate["last_error"] = None
        except (CampaignError, ValueError, KeyError, TypeError, OSError) as error:
            # Transport/store promotion errors must not be disguised as a source retry.
            if getattr(error, "uncertain", False):
                raise
            failure = {"task_id": task["id"], "http_status": status, "raw": descriptor,
                       "error_type": type(error).__name__, "detail": str(error)[:300],
                       "failed_at_utc": state["last_attempt_utc"]}
            state["last_error"] = failure
            run["errors"].append({key: value for key, value in failure.items() if key != "raw"})
            rejected = store.put_receipt({"schema_version": 1, "accepted": False, "source_id": adapter.SOURCE_ID,
                                          "task": task, "request": request_spec, "code_sha": code_sha, **failure})
            state.setdefault("rejected_receipts", []).append({"task_id": task["id"], **rejected})
            task["failures"] = task.get("failures", 0) + 1
            task["retry_at"] = clock() + max(retry_delay(headers, clock()), 86400 if task["failures"] >= 3 else 0)
            if status in (429, 503):
                state["provider_retry_at"] = task["retry_at"]
            store.save(state)
            run["failed_requests"] += 1
            run["reason"] = "source_error"
            # A bad/retired series is isolated; rate/auth/service trouble stops this provider.
            if status not in (200, 400, 404, 422) or run["failed_requests"] >= 3:
                break
            continue
        # Promotion failures propagate. Do not resume with an uncertain in-memory state.
        store.save(candidate)
        state = candidate
        run["accepted_responses"] += 1
        run["records_received"] += result["record_count"]
        for group, key in (("by_lane", task["lane"]), ("by_kind", task["kind"])):
            run[group][key] = run[group].get(key, 0) + 1
        run["reason"] = "request_budget"
    run.update({"pending_tasks": len(state["pending"]), "registered_recent_roots": len(state["recent_roots"]),
                "total_accepted_responses": state["accepted_responses"], "total_raw_bytes": state["raw_bytes"],
                "coverage_status": "incomplete", "publication_layer": "01_landing",
                "last_success_utc": state.get("last_success_utc"), "last_error": state.get("last_error"),
                "catalogue_totals": state.get("catalogue_totals", {}),
                "pending_retry_tasks": sum(bool(t.get("failures")) for t in state["pending"]),
                "superseded_plan_tasks": len(state.get("plan_dispositions", {})),
                "repeated_failure_tasks": sum(t.get("failures", 0) >= 3 for t in state["pending"]),
                "elapsed_seconds": round(clock() - started, 2)})
    # record_count is representations received, including metadata/rechecks, not distinct facts.
    return run
