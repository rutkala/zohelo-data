"""Durable full-distribution backfills alongside the existing API campaigns.

Raw archives remain exact provider distributions. A completed distribution is a
validated raw object, never a claim that its observations are already modeled.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import gzip
from pathlib import Path
import shutil
import time

from ingestion.source_campaign import canonical, new_state, quota_wait, retry_delay, state_digest


SOURCES = ("world_bank_wdi", "eurostat")
HOSTS = {
    "world_bank_wdi": ("databank.worldbank.org", "databankfiles.worldbank.org",
                       "datacatalogfiles.worldbank.org", "api.worldbank.org"),
    "eurostat": ("ec.europa.eu",),
}

MAX_PARTITION_PLAN_BYTES = 512 * 1024
MAX_PROTOCOL_BYTES = 16 * 1024 * 1024
# Raw/state progress is still committed for every object. Coalesce only the
# consumer index to avoid a cumulative scan, tail rewrite and pointer promotion
# for each of the tens of thousands of catalogue distributions.
INDEX_BATCH_DISTRIBUTIONS = 8
INDEX_BATCH_SECONDS = 120


def task_for(distribution, *, lane="history", generation=None):
    spec = deepcopy(distribution)
    identity = {key: spec.get(key) for key in ("dataset_id", "url", "params", "version", "kind")}
    if generation is not None:
        identity["generation"] = generation
    return {"id": "distribution:" + sha256(canonical(identity)).hexdigest(),
            "lane": lane, "kind": spec["kind"], "cursor": spec}


INVENTORY_KINDS = ("eurostat_inventory", "eurostat_inventory_codelist", "eurostat_inventory_metadata")
EUROSTAT_CATALOGUE_KINDS = frozenset({
    "eurostat_tsv_gzip",
    "eurostat_structure_sdmx",
    "eurostat_codelist_tsv",
    "eurostat_metadata_zip",
})
INVENTORY_DISTRIBUTION_KINDS = {
    "eurostat_inventory": frozenset({"eurostat_tsv_gzip", "eurostat_structure_sdmx"}),
    "eurostat_inventory_codelist": frozenset({"eurostat_codelist_tsv"}),
    "eurostat_inventory_metadata": frozenset({"eurostat_metadata_zip"}),
}


def offer(state, distributions, *, replace_kinds=()):
    distributions = [deepcopy(item) for item in distributions]
    replace_kinds = frozenset(replace_kinds)
    current_keys = {
        item["kind"] + ":" + item["dataset_id"] for item in distributions
        if item["kind"] in replace_kinds
    }
    if replace_kinds:
        state["recent_roots"] = {
            key: value for key, value in state["recent_roots"].items()
            if value.get("kind") not in replace_kinds or key in current_keys
        }
        state["pending"] = [
            task for task in state["pending"]
            if task.get("cursor", {}).get("kind") not in replace_kinds
            or (
                task["cursor"]["kind"] + ":" + task["cursor"]["dataset_id"]
                in current_keys
            )
        ]
    present = {item["id"] for item in state["pending"]} | state["completed"].keys()
    for distribution in distributions:
        catalogue_key = distribution["kind"] + ":" + distribution["dataset_id"]
        previous = state["recent_roots"].get(catalogue_key)
        task = task_for(distribution, lane="recent" if previous is not None else "history")
        # Latest catalogue membership is retained independently of the queue.
        state["recent_roots"][catalogue_key] = deepcopy(distribution)
        if task["id"] not in present:
            state["pending"].append(task)
            present.add(task["id"])


def _durable_catalogue_distribution(distribution, inventory_receipt):
    """Keep queue entries small; the exact full inventory remains in raw Landing."""
    result = deepcopy(distribution)
    metadata = result.get("catalogue_metadata")
    if isinstance(metadata, dict):
        result["catalogue_metadata"] = {
            "title": metadata.get("title"),
            "source_inventory_receipt": deepcopy(inventory_receipt),
        }
    return result


def prepare(store, source_id, today, initial_distributions):
    state = getattr(store, "load_cached", store.load)()
    if state is None:
        state = new_state(source_id + "_bulk", today)
        state["provider_id"] = source_id
    if state.get("source_id") != source_id + "_bulk" or state.get("provider_id") != source_id:
        raise ValueError("Full-distribution state source identity mismatch")
    original = state_digest(state)
    if state.get("last_catalogue_date") != today.isoformat():
        roots = initial_distributions(source_id)
        existing = {t["id"] for t in state["pending"]} | state["completed"].keys()
        for distribution in roots:
            task = task_for(distribution, lane="discovery", generation=today.isoformat())
            if task["id"] not in existing:
                state["pending"].append(task)
        state["last_catalogue_date"] = today.isoformat()
    if original != state_digest(state):
        store.save(state)
    return state


def coverage(state):
    catalogue = {
        key: value for key, value in state.get("recent_roots", {}).items()
        if isinstance(value, dict) and value.get("kind") in EUROSTAT_CATALOGUE_KINDS
    }
    finished = state.get("completed", {})
    if state.get("provider_id") == "world_bank_wdi":
        latest = state.get("latest_wdi")
        admitted = 1
        complete = int(
            isinstance(latest, dict)
            and latest.get("catalogue_date") == state.get("last_catalogue_date")
            and state.get("last_catalogue_success") == state.get("last_catalogue_date")
        )
    else:
        admitted = len(catalogue)
        complete = sum(task_for(d)["id"] in finished for d in catalogue.values())
    inventories_ready = state.get("provider_id") == "world_bank_wdi" or all(
        state.get("inventory_success", {}).get(kind) == state.get("last_catalogue_date") for kind in INVENTORY_KINDS)
    return {
        "source_id": state.get("provider_id"), "layer": "01_landing",
        "scope": "exact official distributions from the last validated catalogue",
        "catalogue_distributions": admitted,
        "validated_current_distributions": complete,
        "pending_tasks": len(state.get("pending", [])),
        "failed_pending_tasks": sum(bool(t.get("failures")) for t in state.get("pending", [])),
        "accepted_distributions": state.get("accepted_responses", 0),
        "received_raw_bytes": state.get("raw_bytes", 0),
        "coverage_status": "complete_current_catalogue" if admitted and complete == admitted and inventories_ready else "incomplete",
        "inventories_current": inventories_ready,
        "catalogue_checked_on": state.get("last_catalogue_success"),
        "modeling_status": "raw_distributions_only",
    }


def _next_task(state, now):
    # New inventory is cheap and admits both newly published and historical data.
    # Alternate changed data/history; neither backlog blocks the other permanently.
    lanes = ("discovery", "recent", "history", "history")
    for offset in range(len(lanes)):
        slot = (state.get("lane_position", 0) + offset) % len(lanes)
        for task in state["pending"]:
            if task["lane"] == lanes[slot] and task.get("retry_at", 0) <= now:
                state["lane_position"] = (slot + 1) % len(lanes)
                return task
    return None


def _protocol_bytes(path, max_bytes=65536):
    with Path(path).open("rb") as handle:
        compressed = handle.read(2) == b"\x1f\x8b"
    opener = gzip.open if compressed else open
    with opener(path, "rb") as handle:
        body = handle.read(max_bytes + 1)
    if len(body) > max_bytes:
        raise ValueError("Eurostat protocol envelope is excessive")
    return body


def _guard_partition_plan(plan):
    size = len(canonical(plan))
    if size > MAX_PARTITION_PLAN_BYTES:
        raise ValueError(
            "Eurostat partition recovery plan exceeds the 512 KiB durable-task "
            f"resource budget ({size} bytes); split the catalogue distribution"
        )
    return plan


def _pending_task(state, task_id):
    for task in state["pending"]:
        if task["id"] == task_id:
            return task
    raise ValueError("The selected full-distribution task is no longer pending")


def _partition_distribution(spec, partition_id, request):
    """Give every accepted leaf a stable identity while retaining its parent."""
    return {
        **deepcopy(spec),
        "dataset_id": (
            f"{spec['dataset_id']}::partition::"
            f"{partition_id.removeprefix('partition:')}"
        ),
        "original_dataset_id": spec["dataset_id"],
        "partition_id": partition_id,
        "url": request["url"],
        "params": deepcopy(request.get("params", {})),
    }


def _next_recovery_request(task, spec):
    """Return the authoritative request plus its durable transition context."""
    partition_async = task.get("partition_async")
    if partition_async:
        return partition_async["plan"]["next_request"], {
            "kind": "partition",
            "partition_id": partition_async["partition_id"],
            "async": True,
        }
    if task.get("recovery"):
        return task["recovery"]["next_request"], {"kind": "parent", "async": True}
    plan = task.get("partition_recovery")
    if not plan:
        return {"url": spec["url"], "params": spec.get("params", {})}, {"kind": "parent"}
    phase = plan["phase"]
    requests = plan["structure_requests"]
    if phase == "need_dataflow":
        return requests["dataflow"], {"kind": "structure", "name": "dataflow"}
    if phase == "need_datastructure":
        return requests["datastructure"], {"kind": "structure", "name": "datastructure"}
    if phase == "need_constraint":
        fetched = task.get("partition_inputs", {}).get("codelists", {})
        for key in sorted(requests.get("codelists", {})):
            if key not in fetched:
                return requests["codelists"][key], {
                    "kind": "structure", "name": "codelist", "key": key,
                }
        return requests["contentconstraint"], {"kind": "structure", "name": "constraint"}
    if phase == "partitions_pending":
        from ingestion.eurostat_bulk_recovery import pending_partition_requests
        pending = pending_partition_requests(plan)
        if not pending:
            raise ValueError("Eurostat partition plan has no pending leaf")
        return pending[0]["request"], {
            "kind": "partition", "partition_id": pending[0]["partition_id"],
        }
    raise ValueError(f"Eurostat partition recovery has unusable phase {phase!r}")


def _record_protocol(store, raw_store, state, task, spec, path, request, transport,
                     now, code_sha, kind, context):
    raw = raw_store.put_file(path, {
        "source_id": state["provider_id"], "dataset_id": spec["dataset_id"], "kind": kind,
    })
    receipt = store.put_receipt({
        "schema_version": 1,
        "source_id": state["provider_id"],
        "accepted": False,
        "kind": kind,
        "parent_task_id": task["id"],
        "request": request,
        "context": context,
        "transport": transport,
        "raw": raw,
        "retrieved_at_utc": datetime.fromtimestamp(now, timezone.utc).isoformat(),
        "code_sha": code_sha,
    })
    state["rejected_receipts"].append({"task_id": task["id"], **receipt})
    return raw


def _apply_413_response(store, raw_store, state, task, spec, path, request,
                        transport, context, now, code_sha):
    from ingestion.eurostat_bulk_recovery import start_413_recovery, split_partition_on_413
    if state["provider_id"] != "eurostat" or context["kind"] not in {"parent", "partition"}:
        raise ValueError("HTTP 413 is unsupported for this full-distribution request")
    raw = _record_protocol(
        store, raw_store, state, task, spec, path, request, transport,
        now, code_sha, "http_413_protocol", context,
    )
    if context["kind"] == "partition":
        task["partition_recovery"] = _guard_partition_plan(
            split_partition_on_413(
                task["partition_recovery"], context["partition_id"], raw,
            )
        )
        task.pop("partition_async", None)
    else:
        task["partition_recovery"] = _guard_partition_plan(
            start_413_recovery(spec, raw)
        )
        if task.get("recovery"):
            task["parent_async_protocol"] = task.pop("recovery")
    task["retry_at"] = 0
    state["last_error"] = None


def _restore_protocol_body(raw_store, descriptor, workdir, label):
    path = Path(workdir) / f"restore-{sha256(canonical(descriptor)).hexdigest()}-{label}.xml"
    try:
        raw_store.read_to_file(descriptor, path)
        return _protocol_bytes(path, MAX_PROTOCOL_BYTES)
    finally:
        path.unlink(missing_ok=True)


def _advance_partition_structure(task, name, key, body, raw, raw_store, workdir):
    from ingestion.eurostat_bulk_recovery import apply_dataflow, apply_datastructure, build_partitions
    plan = task["partition_recovery"]
    if name == "dataflow":
        task["partition_recovery"] = _guard_partition_plan(apply_dataflow(plan, body, raw))
    elif name == "datastructure":
        task["partition_recovery"] = _guard_partition_plan(apply_datastructure(plan, body, raw))
    elif name == "codelist":
        task.setdefault("partition_inputs", {}).setdefault("codelists", {})[key] = raw
    elif name == "constraint":
        descriptors = task.get("partition_inputs", {}).get("codelists", {})
        bodies = {
            item_key: _restore_protocol_body(raw_store, descriptor, workdir, item_key)
            for item_key, descriptor in descriptors.items()
        }
        task["partition_recovery"] = _guard_partition_plan(build_partitions(
            plan, body, bodies, raw, descriptors,
        ))
        task.pop("partition_inputs", None)
    else:  # pragma: no cover - contexts are constructed above
        raise ValueError("Unknown Eurostat partition structure transition")


def _mark_catalogue_success(candidate, today):
    current = candidate.setdefault("inventory_success", {})
    if all(current.get(kind) == today.isoformat() for kind in INVENTORY_KINDS):
        candidate["last_catalogue_success"] = today.isoformat()


def run_full_campaign(store, raw_store, quota_store, source_id, settings, workdir, *,
                      max_seconds=1200, max_requests=24, code_sha="unknown",
                      clock=time.time, fetcher=None, inspector=None, planner=None, inventory_planner=None,
                      publish=None, on_progress=None):
    from ingestion.bulk_transport import (fetch_to_file, DriveIntegrityError,
                                          DriveCapacityError, LocalDiskCapacityError)
    from ingestion.source_campaign_store import CampaignStoreError
    from ingestion.full_source_adapters import initial_distributions, inspect_distribution, distributions_from_inventory
    fetcher = fetcher or fetch_to_file
    inspector = inspector or inspect_distribution
    planner = planner or initial_distributions
    inventory_planner = inventory_planner or distributions_from_inventory
    if source_id not in SOURCES:
        raise ValueError("Unknown full-distribution source")
    if settings.get("enabled") is False:
        return {
            "requests": 0,
            "accepted": 0,
            "failures": [],
            "reason": "source_disabled",
            "source_id": source_id,
            "layer": "01_landing",
            "scope": "exact official distributions from the last validated catalogue",
            "catalogue_distributions": 0,
            "validated_current_distributions": 0,
            "pending_tasks": 0,
            "failed_pending_tasks": 0,
            "accepted_distributions": 0,
            "received_raw_bytes": 0,
            "coverage_status": "disabled",
            "inventories_current": False,
            "catalogue_checked_on": None,
            "modeling_status": "raw_distributions_only",
            "elapsed_seconds": 0.0,
        }
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    started = clock()
    today = datetime.fromtimestamp(started, timezone.utc).date()
    state = prepare(store, source_id, today, planner)
    if publish and state.get("receipts"):
        publish(store, code_sha)
    last_index_at = clock()
    pending_index = 0
    first_index = not state.get("receipts")
    report = {"requests": 0, "accepted": 0, "failures": [], "reason": "no_due_tasks"}
    while report["requests"] < max_requests and clock() - started < max_seconds:
        now = clock()
        task = _next_task(state, now)
        if task is None:
            break
        # Both paths use this same provider quota ledger and Actions concurrency
        # group. A backfill never resets the API's rate/cooldown history.
        quota_state = getattr(quota_store, "load_cached", quota_store.load)()
        if quota_state is None:
            raise ValueError("Initialize the provider API quota ledger before its bulk campaign")
        cooldown = max(quota_state.get("provider_retry_at", 0), state.get("provider_retry_at", 0))
        if now < cooldown:
            report["reason"] = "provider_retry_after"
            break
        wait = quota_wait(quota_state, settings, now)
        if wait > 0:
            report["reason"] = "quota_wait"
            break
        quota_state["quota_attempts"].append(now)
        quota_state["last_attempt_utc"] = datetime.fromtimestamp(now, timezone.utc).isoformat()
        quota_store.save(quota_state)
        report["requests"] += 1
        spec = task["cursor"]
        path = workdir / (task["id"].split(":")[1] + ".download")
        try:
            # Free local disk, rather than a fixed response/lifetime cap, bounds
            # one transfer. Raw bytes are streamed; archive members are not extracted.
            free = shutil.disk_usage(workdir).free
            reserve = max(64 * 1024 * 1024, free // 20)
            if free <= reserve:
                raise OSError("Insufficient runner disk headroom for a full distribution")
            request, context = _next_recovery_request(task, spec)
            transport = fetcher(request, path, HOSTS[source_id], timeout=180, max_bytes=free - reserve,
                                accepted_statuses=(200, 202, 413))
            status_code = transport.get("status_code", 200)
            if status_code == 413:
                _apply_413_response(
                    store, raw_store, state, task, spec, path, request,
                    transport, context, now, code_sha,
                )
                store.save(state)
                report["reason"] = "partition_recovery"
                if on_progress:
                    on_progress({
                        "distribution_partitioning": spec["dataset_id"],
                        "phase": task["partition_recovery"]["phase"],
                        **coverage(state),
                    })
                continue

            if context["kind"] == "structure":
                raw = _record_protocol(
                    store, raw_store, state, task, spec, path, request, transport,
                    now, code_sha, "partition_structure", context,
                )
                body = _protocol_bytes(path, MAX_PROTOCOL_BYTES)
                _advance_partition_structure(
                    task, context["name"], context.get("key"), body, raw,
                    raw_store, workdir,
                )
                task["retry_at"] = 0
                state["last_error"] = None
                store.save(state)
                report["reason"] = "partition_recovery"
                if on_progress:
                    on_progress({
                        "distribution_partitioning": spec["dataset_id"],
                        "phase": task["partition_recovery"]["phase"],
                        **coverage(state),
                    })
                continue

            inspected = inspector(path, spec["kind"])
            if inspected.get("http_status") == 413:
                _apply_413_response(
                    store, raw_store, state, task, spec, path, request,
                    transport, context, now, code_sha,
                )
                store.save(state)
                report["reason"] = "partition_recovery"
                if on_progress:
                    on_progress({
                        "distribution_partitioning": spec["dataset_id"],
                        "phase": task["partition_recovery"]["phase"],
                        **coverage(state),
                    })
                continue
            accepted_protocol_raw = None
            if inspected.get("status") in {
                "async", "available", "expired", "async_error", "no_results",
            }:
                from ingestion.eurostat_bulk_recovery import start_async_recovery, advance_async_recovery
                if source_id != "eurostat" or context["kind"] not in {"parent", "partition"}:
                    raise ValueError("Asynchronous preparation is unsupported for this request")
                raw = _record_protocol(
                    store, raw_store, state, task, spec, path, request, transport,
                    now, code_sha, "async_protocol", context,
                )
                if inspected.get("status") == "no_results" and not context.get("async"):
                    if context["kind"] != "partition":
                        raise ValueError("Only a constrained partition can prove an empty result")
                    accepted_protocol_raw = raw
                    inspected = {
                        "status": "complete",
                        "empty_partition": True,
                        "protocol_inspection": inspected,
                    }
                    recovery = None
                else:
                    body = _protocol_bytes(path)
                    if context["kind"] == "partition":
                        if context.get("async"):
                            recovery = advance_async_recovery(
                                task["partition_async"]["plan"], body, raw,
                            )
                        else:
                            recovery = start_async_recovery(
                                _partition_distribution(
                                    spec, context["partition_id"], request,
                                ), body, raw,
                            )
                        task["partition_async"] = {
                            "partition_id": context["partition_id"], "plan": recovery,
                        }
                    else:
                        if context.get("async"):
                            recovery = advance_async_recovery(task["recovery"], body, raw)
                        else:
                            recovery = start_async_recovery(spec, body, raw)
                        task["recovery"] = recovery
                if recovery is not None and recovery["phase"] == "empty_partition":
                    if context["kind"] != "partition":
                        raise ValueError("Only a constrained partition can prove an empty result")
                    accepted_protocol_raw = raw
                    inspected = {
                        "status": "complete",
                        "empty_partition": True,
                        "protocol_inspection": inspected,
                    }
                elif recovery is not None:
                    task["retry_at"] = now + 60
                    store.save(state)
                    report["reason"] = "asynchronous_preparation"
                    if on_progress:
                        on_progress({"distribution_preparing": spec["dataset_id"], "phase": recovery["phase"], **coverage(state)})
                    continue
            if inspected.get("status") != "complete":
                raise ValueError("Distribution is not data yet: " + str(inspected.get("status")))
            if context["kind"] == "partition":
                partition_id = context["partition_id"]
                partition = task["partition_recovery"]["partitions"][partition_id]
                accepted_distribution = _partition_distribution(spec, partition_id, partition["request"])
                accepted_distribution["partition_request"] = deepcopy(partition["request"])
                accepted_distribution["partition_selection"] = deepcopy(partition["selections"])
                accepted_task_id = f"{task['id']}::{partition_id}"
            else:
                partition_id = None
                accepted_distribution = spec
                accepted_task_id = task["id"]
            raw = accepted_protocol_raw or raw_store.put_file(path, {
                "source_id": source_id,
                "dataset_id": accepted_distribution["dataset_id"],
            })
            receipt = {"schema_version": 1, "source_id": source_id, "accepted": True,
                       "kind": "full_distribution", "distribution": accepted_distribution,
                       "parent_task_id": task["id"],
                       "retrieved_at_utc": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                       "raw": raw, "transport": transport, "inspection": inspected,
                       "protocol": (task.get("partition_async", {}).get("plan")
                                    or task.get("recovery") or task.get("parent_async_protocol")),
                       "code_sha": code_sha}
            receipt_descriptor = store.put_receipt(receipt)
            candidate = deepcopy(state)
            candidate_task = _pending_task(candidate, task["id"])
            candidate["completed"][accepted_task_id] = {
                "dataset_id": accepted_distribution["dataset_id"],
                "original_dataset_id": spec["dataset_id"],
                "raw": raw,
                "receipt": receipt_descriptor,
            }
            candidate["receipts"].append({"task_id": accepted_task_id, **receipt_descriptor})
            candidate["raw_bytes"] += raw["size_bytes"]
            candidate["accepted_responses"] += 1
            if partition_id is not None:
                from ingestion.eurostat_bulk_recovery import accept_partition
                candidate_task["partition_recovery"] = _guard_partition_plan(
                    accept_partition(
                        candidate_task["partition_recovery"], partition_id,
                        receipt_descriptor,
                    )
                )
                candidate_task.pop("partition_async", None)
                candidate_task["retry_at"] = 0
                if candidate_task["partition_recovery"]["phase"] == "complete":
                    aggregate_receipt = store.put_receipt({
                        "schema_version": 1,
                        "source_id": source_id,
                        "accepted": True,
                        "kind": "partition_aggregate",
                        "parent_task_id": task["id"],
                        "distribution": spec,
                        "partition_recovery": candidate_task["partition_recovery"],
                        "retrieved_at_utc": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                        "code_sha": code_sha,
                    })
                    leaf_count = sum(
                        item["status"] == "accepted"
                        for item in candidate_task["partition_recovery"]["partitions"].values()
                    )
                    candidate["completed"][task["id"]] = {
                        "dataset_id": spec["dataset_id"],
                        "partitioned": True,
                        "partition_count": leaf_count,
                        "aggregate_receipt": aggregate_receipt,
                    }
                    candidate["pending"] = [
                        item for item in candidate["pending"] if item["id"] != task["id"]
                    ]
            else:
                candidate["pending"] = [
                    item for item in candidate["pending"] if item["id"] != task["id"]
                ]
            if spec["kind"] in INVENTORY_KINDS:
                distributions = [
                    _durable_catalogue_distribution(item, receipt_descriptor)
                    for item in inventory_planner(spec["kind"], path.read_bytes())
                ]
                offer(
                    candidate, distributions,
                    replace_kinds=INVENTORY_DISTRIBUTION_KINDS[spec["kind"]],
                )
                candidate.setdefault("inventory_success", {})[spec["kind"]] = today.isoformat()
                _mark_catalogue_success(candidate, today)
            elif spec["kind"] == "wdi_zip":
                candidate["completed"][task["id"]]["catalogue_date"] = today.isoformat()
                candidate["latest_wdi"] = candidate["completed"][task["id"]]
                candidate["last_catalogue_success"] = today.isoformat()
            candidate["last_error"] = None
        except Exception as exc:
            # Never continue after an uncertain durable write/promotion.
            if getattr(exc, "uncertain", False) or isinstance(exc, (CampaignStoreError, DriveIntegrityError,
                                                                    DriveCapacityError, LocalDiskCapacityError)):
                raise
            failure = {"task_id": task["id"], "dataset_id": spec["dataset_id"],
                       "error_type": type(exc).__name__, "detail": str(exc)[:300]}
            task["failures"] = task.get("failures", 0) + 1
            headers = getattr(exc, "headers", {})
            task["retry_at"] = now + max(retry_delay(headers, now), min(86400, 60 * 2 ** min(task["failures"], 10)))
            status = getattr(exc, "status_code", getattr(exc, "status", None))
            if status in (429, 503):
                quota_state["provider_retry_at"] = task["retry_at"]
                quota_store.save(quota_state)
            state["last_error"] = failure
            store.save(state)
            report["failures"].append(failure)
            if on_progress:
                on_progress({"distribution_failed": failure, **coverage(state)})
            report["reason"] = "source_error"
            if status in (429, 503) or len(report["failures"]) >= 3:
                break
            continue
        finally:
            path.unlink(missing_ok=True)
        store.save(candidate)
        state = candidate
        report["accepted"] += 1
        pending_index += 1
        if publish and (first_index or pending_index >= INDEX_BATCH_DISTRIBUTIONS
                        or clock() - last_index_at >= INDEX_BATCH_SECONDS):
            publish(store, code_sha)
            pending_index = 0
            first_index = False
            last_index_at = clock()
        report["reason"] = "batch_budget"
        if on_progress:
            on_progress({
                "distribution_completed": accepted_distribution["dataset_id"],
                **coverage(state),
            })
    # Normal budget/quota stops and known source failures publish every retained
    # increment before returning. Uncertain storage failures propagate above;
    # the next fresh worker publishes that durable backlog before new collection.
    if publish and pending_index:
        publish(store, code_sha)
    return {**report, **coverage(state), "elapsed_seconds": round(clock() - started, 2)}
