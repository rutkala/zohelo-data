"""Read-only capacity facts; this module never deletes or compacts retained data."""
from __future__ import annotations

from collections import deque
import json
import sys
import time

from ingestion.nbp_state import list_successful_response_descriptors


RETENTION_POLICY = "retain_all_no_automatic_deletion"
FOLDER_MIME = "application/vnd.google-apps.folder"


def _bytes(value):
    """Drive may omit sizes; absence and malformed values are not zero bytes."""
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and value.isdigit():
        value = int(value)
    return value if isinstance(value, int) and value >= 0 else None


def _bound(used, limit, threshold):
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise ValueError("Capacity limits must be positive integers")
    return {
        "used": used, "limit": limit,
        "headroom": limit - used if used is not None else None,
        "percent_used": round(100 * used / limit, 2) if used is not None else None,
        "status": ("unknown" if used is None else "exceeded" if used > limit
                   else "warning" if used >= limit * threshold else "within_limit"),
    }


def capacity_report(state, *, max_observation_batches=2048, max_raw_bytes=256 * 1024 * 1024,
                    max_state_bytes=8_000_000, warning_threshold=0.70):
    """Report current state and effective build bounds without forecasting growth.

    The current build guard counts every descriptor's declared bytes, including
    repeated references to one raw file. Unique raw bytes are a separate storage
    fact and must not be substituted for that guard's working-set limit.
    """
    if not 0 < warning_threshold <= 1:
        raise ValueError("Warning threshold must be greater than zero and at most one")
    descriptors = list_successful_response_descriptors(state)
    canonical = json.dumps(state, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False, allow_nan=False).encode("utf-8")
    declared_known = 0
    unknown_sizes = 0
    unknown_ids = 0
    files = {}
    conflicting_ids = set()
    for item in descriptors:
        size = _bytes(item.get("size_bytes"))
        if size is None or size == 0:
            unknown_sizes += 1
            size = None
        else:
            declared_known += size
        file_id = item.get("raw_file_id")
        if not isinstance(file_id, str) or not file_id:
            unknown_ids += 1
            continue
        if file_id in files and files[file_id] != size:
            conflicting_ids.add(file_id)
        files[file_id] = size
    unique_known = sum(size for file_id, size in files.items()
                       if size is not None and file_id not in conflicting_ids)
    declared = declared_known if not unknown_sizes else None
    unique = unique_known if not (unknown_sizes or unknown_ids or conflicting_ids) else None
    bounds = {
        "observation_batches": _bound(len(descriptors), max_observation_batches, warning_threshold),
        "declared_raw_bytes": _bound(declared, max_raw_bytes, warning_threshold),
        "canonical_state_bytes": _bound(len(canonical), max_state_bytes, warning_threshold),
    }
    warnings = [f"{name}_{entry['status']}" for name, entry in bounds.items()
                if entry["status"] != "within_limit"]
    if unknown_ids:
        warnings.append("raw_file_identity_unknown")
    if conflicting_ids:
        warnings.append("raw_file_size_conflict")
    return {
        "status": "attention_required" if warnings else "within_current_bounds",
        "scope": "capacity_observation_not_freshness_or_recovery_validation",
        "retention_policy": RETENTION_POLICY,
        "warning_threshold_percent": round(warning_threshold * 100, 2),
        "successful_descriptor_count": len(descriptors),
        "declared_raw_bytes": declared,
        "known_declared_raw_bytes_lower_bound": declared_known,
        "unknown_size_descriptor_count": unknown_sizes,
        "unique_raw_file_count": len(files) if not unknown_ids else None,
        "unique_raw_file_bytes": unique,
        "known_unique_raw_file_bytes_lower_bound": unique_known,
        "conflicting_raw_file_size_count": len(conflicting_ids),
        "canonical_state_bytes": len(canonical),
        "capacity": bounds,
        "warnings": warnings,
        "forecast": "not_estimated_no_measured_growth_rate",
        "required_action_at_warning": "review_capacity_and_reference_safe_compaction_before_expansion",
    }


def process_memory_report():
    """Linux high-water values are separate; their sum is not a tree peak."""
    if not sys.platform.startswith("linux"):
        return {"status": "unavailable_on_this_platform", "process_tree_peak_bytes": None}
    import resource
    return {
        "status": "observed", "measurement": "linux_getrusage_ru_maxrss_kib_times_1024",
        "parent_process_high_water_bytes": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024,
        "largest_completed_child_high_water_bytes": int(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss) * 1024,
        "process_tree_peak_bytes": None,
        "interpretation": "individual_high_water_values_not_a_sum_or_concurrent_process_tree_peak",
    }


def account_storage_quota(drive_service):
    """Ask only for quota fields; account identity and credentials are excluded."""
    try:
        response = drive_service.about().get(fields="storageQuota").execute(num_retries=2)
        quota = response.get("storageQuota", {})
        limit, usage = _bytes(quota.get("limit")), _bytes(quota.get("usage"))
        return {
            "status": "available" if quota else "unavailable",
            "scope": "authenticated_account_storage_not_project_allocation",
            "limit_bytes": limit, "usage_bytes": usage,
            "available_bytes": max(0, limit - usage) if limit is not None and usage is not None else None,
            "drive_usage_bytes": _bytes(quota.get("usageInDrive")),
            "drive_trash_usage_bytes": _bytes(quota.get("usageInDriveTrash")),
            "missing_limit_meaning": "unknown_not_a_claim_of_unlimited_capacity",
        }
    except Exception:
        return {"status": "unavailable", "scope": "authenticated_account_storage_not_project_allocation",
                "limit_bytes": None, "usage_bytes": None, "available_bytes": None}


def _category(path):
    if path and path[0] == "ingestion-control":
        if len(path) > 1 and path[1] in {"states", "attempts", "raw-references"}:
            return "ingestion_" + path[1].replace("-", "_")
        return "ingestion_control"
    if path and path[0] in {"01_landing", "02_bronze", "03_silver", "04_gold", "05_archive",
                            "releases", "promotion-audits"}:
        return path[0].replace("-", "_")
    return "other_project_files"


def inventory_project(files, root_id, *, max_files=10_000, max_list_requests=250,
                      max_seconds=120, clock=time.monotonic):
    """Bounded, metadata-only traversal. Never follow shortcuts outside the root.

    The result is an observation over several requests, not a transactional Drive
    snapshot. Incomplete traversal and missing sizes remain explicit.
    """
    if not isinstance(root_id, str) or not root_id:
        raise ValueError("A selected project root is required")
    if not (0 < max_files <= 10_000 and max_list_requests > 0 and max_seconds > 0):
        raise ValueError("Inventory limits must be positive and max_files cannot exceed 10000")
    started = clock()
    pending = deque([(root_id, ())])
    seen = {root_id}
    inspected = requests = folder_count = file_count = known_bytes = unknown_sizes = 0
    groups = {}
    incomplete_reasons = set()
    stop = False
    while pending and not stop:
        folder_id, folder_path = pending.popleft()
        token = None
        seen_tokens = set()
        while True:
            if inspected >= max_files or requests >= max_list_requests or clock() - started >= max_seconds:
                incomplete_reasons.add("inventory_budget_reached")
                stop = True
                break
            quoted = folder_id.replace("\\", "\\\\").replace("'", "\\'")
            requests += 1
            try:
                response = files.list(
                    q=f"'{quoted}' in parents and trashed=false", spaces="drive",
                    fields="nextPageToken,incompleteSearch,files(id,name,mimeType,size)",
                    pageSize=min(1000, max_files - inspected), pageToken=token,
                ).execute(num_retries=2)
            except Exception:
                incomplete_reasons.add("drive_metadata_request_failed")
                stop = True
                break
            if response.get("incompleteSearch"):
                incomplete_reasons.add("drive_incomplete_search")
            for item in response.get("files", []):
                if inspected >= max_files:
                    incomplete_reasons.add("inventory_budget_reached")
                    stop = True
                    break
                inspected += 1
                file_id = item.get("id")
                if not isinstance(file_id, str) or not file_id:
                    incomplete_reasons.add("missing_file_identity")
                    continue
                if file_id in seen:
                    continue
                seen.add(file_id)
                path = (*folder_path, item.get("name", ""))
                group = groups.setdefault(_category(path), {"file_count": 0, "folder_count": 0,
                                                            "known_file_bytes": 0, "unknown_size_file_count": 0})
                if item.get("mimeType") == FOLDER_MIME:
                    folder_count += 1
                    group["folder_count"] += 1
                    pending.append((file_id, path))
                    continue
                file_count += 1
                group["file_count"] += 1
                size = _bytes(item.get("size"))
                if size is None:
                    unknown_sizes += 1
                    group["unknown_size_file_count"] += 1
                else:
                    known_bytes += size
                    group["known_file_bytes"] += size
            token = response.get("nextPageToken")
            if not token or stop:
                break
            if token in seen_tokens:
                incomplete_reasons.add("repeated_page_token")
                stop = True
                break
            seen_tokens.add(token)
    complete = not incomplete_reasons and not pending
    return {
        "status": "complete" if complete else "incomplete", "metadata_only": True,
        "consistency": "observed_over_multiple_requests_not_transactional",
        "scope": "selected_project_folder_descendants_no_shortcut_traversal",
        "inspected_entries": inspected, "file_count": file_count, "folder_count": folder_count,
        "known_file_bytes_lower_bound": known_bytes, "unknown_size_file_count": unknown_sizes,
        "total_file_bytes": known_bytes if complete and not unknown_sizes else None,
        "list_requests": requests, "duration_seconds": round(clock() - started, 3),
        "limits": {"entries": max_files, "list_requests": max_list_requests, "seconds": max_seconds},
        "incomplete_reasons": sorted(incomplete_reasons), "categories": groups,
        "retention_policy": RETENTION_POLICY,
        "growth_rate": "not_measured_single_inventory_observation",
    }
