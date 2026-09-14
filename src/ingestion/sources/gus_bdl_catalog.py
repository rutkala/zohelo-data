"""Metadata-only GUS BDL adapter used to plan Web UI historical bootstrap.

It deliberately never creates numerical-data tasks. Historical observations are owned by
the authenticated Web UI bootstrap; the API is used here only to enumerate current BDL
subjects/subgroups, variables, administrative units and dictionaries.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any

from ingestion.sources import gus_bdl as base

SOURCE_ID = base.SOURCE_ID
ALLOWED_HOSTS = base.ALLOWED_HOSTS

_INITIAL_KINDS = {"dictionary", "years", "subjects", "units"}
_ALLOWED_KINDS = {"dictionary", "years", "subjects", "subject_detail", "units", "variables"}


def _filter(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for task in tasks:
        if task.get("kind") not in _ALLOWED_KINDS:
            continue
        # Variables must be scoped to a discovered BDL subject. The base adapter's
        # global root seed would duplicate the complete subject-scoped catalogue.
        if task.get("kind") == "variables" and "subject_id" not in task.get("cursor", {}):
            continue
        if task["id"] not in seen:
            result.append(deepcopy(task))
            seen.add(task["id"])
    return result


def initial_tasks(today: date) -> list[dict[str, Any]]:
    return [
        deepcopy(task)
        for task in base.initial_tasks(today)
        if task.get("kind") in _INITIAL_KINDS
    ]


def recent_tasks(today: date) -> list[dict[str, Any]]:
    return []


def refresh_task(task: dict[str, Any], today: date) -> None:
    return None


def migrate_state(state: dict[str, Any], today: date) -> dict[str, Any]:
    return base.migrate_state(state, today)


def request_for(task: dict[str, Any]) -> dict[str, Any]:
    if task.get("kind") not in _ALLOWED_KINDS:
        raise ValueError("Metadata-only BDL catalogue refuses numerical-data task")
    return base.request_for(task)


def interpret(task: dict[str, Any], body: bytes, today: date) -> dict[str, Any]:
    if task.get("kind") not in _ALLOWED_KINDS:
        raise ValueError("Metadata-only BDL catalogue received numerical-data task")
    result = deepcopy(base.interpret(task, body, today))
    result["next_tasks"] = _filter(result.get("next_tasks", []))
    metadata = dict(result.get("metadata", {}))
    metadata["catalogue_only"] = True
    result["metadata"] = metadata
    return result
