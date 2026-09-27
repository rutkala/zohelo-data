"""Explicit recovery controls for source-validation failures.

This module only changes durable campaign scheduling state.  It does not call a
source adapter, perform HTTP requests, or relax provider quota/cooldown state.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping


MAX_REJECTED_RECEIPTS_TO_INSPECT = 20
MAX_RECOVERY_AUDITS = 20
MAX_CODE_SHA_LENGTH = 128
MAX_TASK_ID_LENGTH = 500
_TASK_IDENTITY_FIELDS = ("id", "lane", "kind", "cursor", "recurrence_key")


class CampaignRecoveryError(ValueError):
    """Campaign state or receipt evidence is unsafe for explicit recovery."""


def retry_validation_failures(store: Any, code_sha: str) -> dict[str, Any]:
    """Make eligible validation failures immediately due after corrected code.

    Only the latest retained rejection for each task is considered, and at most
    the newest 20 rejected receipts are read.  A successful control write is
    recorded in bounded state audit metadata before a caller can start requests.
    """
    corrected_code_sha = _code_sha(code_sha, "corrected code SHA")
    loaded = store.load()
    if loaded is None:
        source_id = getattr(store, "source_id", None)
        if not isinstance(source_id, str) or not source_id:
            raise CampaignRecoveryError("Campaign store has an invalid source identity")
        return {
            "source_id": source_id,
            "task_ids": [],
            "reset_count": 0,
        }
    if not isinstance(loaded, dict):
        raise CampaignRecoveryError("Campaign recovery requires existing durable state")
    state = deepcopy(loaded)
    source_id = state.get("source_id")
    if not isinstance(source_id, str) or not source_id:
        raise CampaignRecoveryError("Campaign state has an invalid source identity")
    store_source_id = getattr(store, "source_id", source_id)
    if store_source_id != source_id:
        raise CampaignRecoveryError("Campaign store and state source identities differ")

    pending = state.get("pending")
    completed = state.get("completed")
    rejected_descriptors = state.get("rejected_receipts", [])
    audits = state.get("validation_retry_audits", [])
    if not isinstance(pending, list) or not isinstance(completed, dict):
        raise CampaignRecoveryError("Campaign task state has an invalid shape")
    if not isinstance(rejected_descriptors, list) or not isinstance(audits, list):
        raise CampaignRecoveryError("Campaign recovery evidence has an invalid shape")

    pending_by_id: dict[str, dict[str, Any]] = {}
    for pending_task in pending:
        if not isinstance(pending_task, dict):
            raise CampaignRecoveryError("Pending campaign task is invalid")
        task_id = pending_task.get("id")
        if not _valid_task_id(task_id) or task_id in pending_by_id:
            raise CampaignRecoveryError("Pending campaign task identity is invalid")
        pending_by_id[task_id] = pending_task

    already_controlled = _controlled_tasks(audits, corrected_code_sha)
    latest_by_task: dict[str, Mapping[str, Any]] = {}
    newest = rejected_descriptors[-MAX_REJECTED_RECEIPTS_TO_INSPECT:]
    for descriptor in reversed(newest):
        if not isinstance(descriptor, dict):
            raise CampaignRecoveryError("Rejected receipt descriptor is invalid")
        descriptor_task_id = descriptor.get("task_id")
        if not _valid_task_id(descriptor_task_id):
            raise CampaignRecoveryError("Rejected receipt task identity is invalid")
        if descriptor_task_id in latest_by_task:
            continue
        receipt = store.read_receipt(descriptor)
        if not isinstance(receipt, Mapping):
            raise CampaignRecoveryError("Rejected receipt is not an object")
        if receipt.get("source_id") != source_id:
            raise CampaignRecoveryError("Rejected receipt source identity does not match state")
        receipt_task = receipt.get("task")
        if not isinstance(receipt_task, Mapping) or receipt_task.get("id") != descriptor_task_id:
            raise CampaignRecoveryError("Rejected receipt task identity is inconsistent")
        latest_by_task.setdefault(descriptor_task_id, receipt)

    affected: list[str] = []
    for task_id, receipt in latest_by_task.items():
        pending_task = pending_by_id.get(task_id)
        if pending_task is None or task_id in completed or task_id in already_controlled:
            continue
        if not _same_task_identity(receipt["task"], pending_task):
            continue
        retry_at = pending_task.get("retry_at")
        if isinstance(retry_at, bool) or not isinstance(retry_at, (int, float)) or retry_at <= 0:
            continue
        prior_code_sha = receipt.get("code_sha")
        if not _valid_prior_code_sha(prior_code_sha) or prior_code_sha == corrected_code_sha:
            continue
        if (
            receipt.get("accepted") is not False
            or type(receipt.get("http_status")) is not int
            or receipt["http_status"] != 200
            or receipt.get("error_type") != "ValueError"
        ):
            continue
        pending_task["retry_at"] = 0
        affected.append(task_id)

    affected.sort()
    if affected:
        audit = {
            "schema_version": 1,
            "kind": "validation_failure_retry",
            "corrected_code_sha": corrected_code_sha,
            "task_ids": affected,
        }
        state["validation_retry_audits"] = [*audits[-(MAX_RECOVERY_AUDITS - 1):], audit]
        # Store promotion errors, including uncertain writes, deliberately propagate.
        store.save(state)

    return {
        "source_id": source_id,
        "task_ids": affected,
        "reset_count": len(affected),
    }


def _controlled_tasks(audits: list[Any], code_sha: str) -> set[str]:
    controlled: set[str] = set()
    for audit in audits[-MAX_RECOVERY_AUDITS:]:
        if not isinstance(audit, Mapping) or audit.get("corrected_code_sha") != code_sha:
            continue
        task_ids = audit.get("task_ids")
        if not isinstance(task_ids, list):
            raise CampaignRecoveryError("Campaign recovery audit has invalid task IDs")
        for task_id in task_ids:
            if not _valid_task_id(task_id):
                raise CampaignRecoveryError("Campaign recovery audit task identity is invalid")
            controlled.add(task_id)
    return controlled


def _same_task_identity(receipt_task: Mapping[str, Any], pending_task: Mapping[str, Any]) -> bool:
    return all(receipt_task.get(field) == pending_task.get(field) for field in _TASK_IDENTITY_FIELDS)


def _code_sha(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_CODE_SHA_LENGTH
        or any(ord(character) < 32 for character in value)
    ):
        raise CampaignRecoveryError(f"Invalid {label}")
    return value


def _valid_prior_code_sha(value: Any) -> bool:
    try:
        _code_sha(value, "receipt code SHA")
    except CampaignRecoveryError:
        return False
    return True


def _valid_task_id(value: Any) -> bool:
    return isinstance(value, str) and 0 < len(value) <= MAX_TASK_ID_LENGTH


__all__ = ["CampaignRecoveryError", "retry_validation_failures"]
