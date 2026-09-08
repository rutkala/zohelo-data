"""Task planning and response validation for the public GUS BDL API.

The shared ingestion runner owns HTTP, retry, quota, and exact-byte retention.
This module only describes one-page GET tasks and validates enough of each JSON
response to make discovery resumable without interpreting business measures.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
import math
import re
from typing import Any


SOURCE_ID = "gus_bdl"
ALLOWED_HOSTS = ("bdl.stat.gov.pl",)

_BASE_URL = "https://bdl.stat.gov.pl/api/v1"
_DISCOVERY_PAGE_SIZE = 20
_DATA_PAGE_SIZE = 100
_RECENT_YEARS = 5
_SEED_VARIABLES = (
    {
        "id": 72305,
        "name_pl": "ludność ogółem",
        "name_en": "total population",
    },
)
_LANGUAGES = ("pl", "en")
_DICTIONARIES = ("aggregates", "attributes", "levels", "measures")
_PAGED_KINDS = {"subjects", "units", "localities", "variables", "data_by_variable"}
_DETAIL_KINDS = {"subject_detail", "unit_detail", "locality_detail", "variable_detail"}
_SUBJECT_ID_RE = re.compile(r"^[A-Z][0-9]+$")
_UNIT_ID_RE = re.compile(r"^[0-9]{12}$")
_LOCALITY_ID_RE = re.compile(r"^[0-9]{12}-[0-9]{7}$")
_OBSOLETE_ROOT_LOCALITY_IDS = {
    "discovery:localities:pl:root:p000000": "pl",
    "discovery:localities:en:root:p000000": "en",
}
_LOCALITY_DISPOSITION_REASON = "obsolete_root_locality_request"
_LOCALITY_DISPOSITION_EVIDENCE = (
    "Production returned HTTP 400; the official /units/localities contract "
    "requires a municipality parent-id."
)
_LOCALITY_CONTRACT_REVISION = "gus-bdl-parent-scoped-localities-2026-09-08"


def _task_id(lane: str, kind: str, cursor: dict[str, Any]) -> str:
    if kind == "dictionary":
        return f"{lane}:dictionary:{cursor['resource']}:{cursor['lang']}"
    if kind == "years":
        return f"{lane}:years"
    if kind in {"subjects", "units", "localities", "variables"}:
        parent = cursor.get("parent_id", "root")
        return f"{lane}:{kind}:{cursor['lang']}:{parent}:p{cursor['page']:06d}"
    if kind in _DETAIL_KINDS:
        return f"{lane}:{kind}:{cursor['lang']}:{cursor['entity_id']}"
    if kind == "data_by_variable":
        years = cursor.get("years")
        window = "all" if years is None else f"{years[0]}-{years[-1]}"
        generation = f":asof-{cursor['asof']}" if lane == "recent" else ""
        return (
            f"{lane}:data-by-variable:v{cursor['variable_id']}:"
            f"y{window}{generation}:p{cursor['page']:06d}"
        )
    raise ValueError(f"Unsupported GUS BDL task kind: {kind}")


def _task(lane: str, kind: str, cursor: dict[str, Any]) -> dict[str, Any]:
    task = {
        "id": _task_id(lane, kind, cursor),
        "lane": lane,
        "kind": kind,
        "cursor": cursor,
    }
    if kind == "data_by_variable" and lane == "recent":
        task["recurrence_key"] = f"variable:{cursor['variable_id']}"
    return task


def _recent_data_task(variable_id: int, today: date, *, page: int = 0) -> dict[str, Any]:
    first_year = today.year - (_RECENT_YEARS - 1)
    return _task(
        "recent",
        "data_by_variable",
        {
            "variable_id": variable_id,
            "years": list(range(first_year, today.year + 1)),
            "page": page,
            "page_size": _DATA_PAGE_SIZE,
            "root": page == 0,
            "asof": today.isoformat(),
        },
    )


def _history_data_task(variable_id: int, *, page: int = 0) -> dict[str, Any]:
    return _task(
        "history",
        "data_by_variable",
        {
            "variable_id": variable_id,
            "years": None,
            "page": page,
            "page_size": _DATA_PAGE_SIZE,
            "root": page == 0,
        },
    )


def recent_tasks(today: date) -> list[dict[str, Any]]:
    """Return the small built-in recurring campaign.

    The runner also retains recent roots produced while catalogue pages are
    admitted. It must capacity-bound that campaign under the shared quota.
    """
    _require_date(today)
    return [_recent_data_task(item["id"], today) for item in _SEED_VARIABLES]


def initial_tasks(today: date) -> list[dict[str, Any]]:
    """Seed bilingual catalogue discovery plus one useful data series."""
    _require_date(today)
    tasks: list[dict[str, Any]] = []
    for resource in _DICTIONARIES:
        for lang in _LANGUAGES:
            tasks.append(_task("discovery", "dictionary", {"resource": resource, "lang": lang}))
    tasks.append(_task("discovery", "years", {}))
    # Statistical localities have no valid root listing. They are discovered
    # from municipality (level 6) units through the required parent-id filter.
    for kind in ("subjects", "units", "variables"):
        for lang in _LANGUAGES:
            tasks.append(
                _task(
                    "discovery",
                    kind,
                    {"lang": lang, "page": 0, "page_size": _DISCOVERY_PAGE_SIZE},
                )
            )
    for item in _SEED_VARIABLES:
        for lang in _LANGUAGES:
            tasks.append(
                _task(
                    "discovery",
                    "variable_detail",
                    {"lang": lang, "entity_id": item["id"]},
                )
            )
        tasks.append(_history_data_task(item["id"]))
    tasks.extend(recent_tasks(today))
    return _unique_tasks(tasks)


def migrate_state(state: dict[str, Any], today: date) -> dict[str, Any]:
    """Retire only the two invalid pre-parent locality root tasks.

    The runner validates the returned state and persists it. This hook records
    an auditable planning disposition without converting rejected work into a
    completed request or touching retained response evidence.
    """
    _require_date(today)
    if not isinstance(state, dict):
        raise TypeError("GUS BDL campaign state must be an object")
    migrated = deepcopy(state)
    if migrated.get("source_id") != SOURCE_ID:
        raise ValueError("GUS BDL state migration requires source_id gus_bdl")
    pending = migrated.get("pending")
    if not isinstance(pending, list):
        raise ValueError("GUS BDL state pending tasks must be a list")

    obsolete: list[dict[str, Any]] = []
    retained: list[Any] = []
    for task in pending:
        task_id = task.get("id") if isinstance(task, dict) else None
        if task_id not in _OBSOLETE_ROOT_LOCALITY_IDS:
            retained.append(task)
            continue
        _validate_obsolete_locality_task(task, _OBSOLETE_ROOT_LOCALITY_IDS[task_id])
        obsolete.append(task)

    if not obsolete:
        return migrated

    dispositions = migrated.get("plan_dispositions")
    if dispositions is None:
        dispositions = {}
        migrated["plan_dispositions"] = dispositions
    if not isinstance(dispositions, dict):
        raise ValueError("GUS BDL state plan_dispositions must be an object")
    for task in obsolete:
        task_id = task["id"]
        disposition = {
            "original_task": deepcopy(task),
            "reason": _LOCALITY_DISPOSITION_REASON,
            "evidence": _LOCALITY_DISPOSITION_EVIDENCE,
            "contract_revision": _LOCALITY_CONTRACT_REVISION,
        }
        existing = dispositions.get(task_id)
        if existing is not None and existing != disposition:
            raise ValueError(f"GUS BDL state has conflicting disposition for {task_id}")
        dispositions[task_id] = disposition
    migrated["pending"] = retained
    return migrated


def refresh_task(task: dict[str, Any], today: date) -> dict[str, Any] | None:
    """Return a current-window generation for a retained recent root."""
    _require_date(today)
    _validate_task(task)
    cursor = task["cursor"]
    if (
        task["lane"] != "recent"
        or task["kind"] != "data_by_variable"
        or cursor.get("page") != 0
        or cursor.get("root") is not True
    ):
        return None
    return _recent_data_task(cursor["variable_id"], today)


def request_for(task: dict[str, Any]) -> dict[str, Any]:
    """Map a task to one public HTTPS GET request."""
    _validate_task(task)
    kind = task["kind"]
    cursor = task["cursor"]
    params: dict[str, Any] = {"format": "json"}

    if kind == "dictionary":
        resource = cursor["resource"]
        if resource not in _DICTIONARIES:
            raise ValueError(f"Unsupported GUS BDL dictionary: {resource}")
        params["lang"] = cursor["lang"]
        path = f"/{resource}"
    elif kind == "years":
        path = "/years"
    elif kind in {"subjects", "units", "variables"}:
        path = f"/{kind}"
        params.update(
            {
                "lang": cursor["lang"],
                "page": cursor["page"],
                "page-size": cursor["page_size"],
                "sort": "Id",
            }
        )
        if "parent_id" in cursor:
            params["parent-id"] = cursor["parent_id"]
    elif kind == "localities":
        path = "/units/localities"
        params.update(
            {
                "lang": cursor["lang"],
                "page": cursor["page"],
                "page-size": cursor["page_size"],
                "sort": "id",
                "parent-id": cursor["parent_id"],
            }
        )
    elif kind in _DETAIL_KINDS:
        prefix = {
            "subject_detail": "/subjects",
            "unit_detail": "/units",
            "locality_detail": "/units/localities",
            "variable_detail": "/variables",
        }[kind]
        path = f"{prefix}/{cursor['entity_id']}"
        params["lang"] = cursor["lang"]
    elif kind == "data_by_variable":
        path = f"/data/by-variable/{cursor['variable_id']}"
        params.update(
            {"page": cursor["page"], "page-size": cursor["page_size"], "lang": "pl"}
        )
        if cursor["years"] is not None:
            params["year"] = cursor["years"]
    else:
        raise ValueError(f"Unsupported GUS BDL task kind: {kind}")
    return {"url": _BASE_URL + path, "params": params}


def interpret(task: dict[str, Any], body: bytes, today: date) -> dict[str, Any]:
    """Validate a retained JSON page and return deterministic follow-up tasks."""
    _require_date(today)
    _validate_task(task)
    if not isinstance(body, bytes):
        raise TypeError("GUS BDL response body must be bytes")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("GUS BDL response is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("GUS BDL response must be a JSON object")

    kind = task["kind"]
    if kind in _PAGED_KINDS:
        result = _interpret_page(task, payload, today)
    elif kind == "dictionary":
        result = _interpret_dictionary(task, payload)
    elif kind == "years":
        result = _interpret_years(payload)
    elif kind in _DETAIL_KINDS:
        result = _interpret_detail(task, payload)
    else:  # guarded by _validate_task; retained for defensive clarity
        raise ValueError(f"Unsupported GUS BDL task kind: {kind}")
    return result


def _interpret_page(
    task: dict[str, Any], payload: dict[str, Any], today: date
) -> dict[str, Any]:
    results = _list_field(payload, "results")
    total = _nonnegative_int(payload, "totalRecords")
    cursor = task["cursor"]

    # SingleVariableData does not promise response page/pageSize members;
    # the retained request cursor remains the
    # paging authority. If the API does echo either member, validate it strictly
    # so an explicit wrong page can never be accepted.
    if task["kind"] == "data_by_variable":
        page = _optional_paging_int(payload, "page", cursor["page"], positive=False)
        page_size = _optional_paging_int(
            payload, "pageSize", cursor["page_size"], positive=True
        )
    else:
        page = _nonnegative_int(payload, "page")
        page_size = _positive_int(payload, "pageSize")
        if page != cursor["page"]:
            raise ValueError("GUS BDL response page does not match the task cursor")
        if page_size != cursor["page_size"]:
            raise ValueError(
                f"GUS BDL response pageSize {page_size} differs from requested "
                f"{cursor['page_size']}"
            )
    if len(results) > total:
        raise ValueError("GUS BDL response contains more results than totalRecords")
    complete_child_listing = (
        task["kind"] == "subjects"
        and "parent_id" in cursor
        and len(results) == total
        and len(results) > page_size
    )
    if len(results) > page_size and not complete_child_listing:
        raise ValueError("GUS BDL response contains more results than pageSize")
    links = payload.get("links")
    if links is not None and not isinstance(links, dict):
        raise ValueError("GUS BDL links must be an object when present")

    next_tasks: list[dict[str, Any]] = []
    if not complete_child_listing and (page + 1) * page_size < total:
        next_cursor = dict(cursor)
        next_cursor["page"] = page + 1
        if task["kind"] == "data_by_variable":
            next_cursor["root"] = False
        next_tasks.append(_task(task["lane"], task["kind"], next_cursor))

    kind = task["kind"]
    ids: list[Any] = []
    if kind == "data_by_variable":
        observation_count = _validate_data_page(task, payload, results)
        metadata = {
            "api_total_records": total,
            "api_page": page,
            "api_page_size": page_size,
            "variable_id": payload["variableId"],
            "measure_unit_id": payload.get("measureUnitId"),
            "aggregate_id": payload.get("aggregateId"),
            "last_update": payload.get("lastUpdate"),
            "observation_count": observation_count,
            "record_grain": "variable_id + bdl_unit_id + returned_year + attr_id",
        }
        return {
            "record_count": observation_count,
            "next_tasks": _unique_tasks(next_tasks),
            "metadata": metadata,
        }

    for item in results:
        if not isinstance(item, dict):
            raise ValueError(f"GUS BDL {kind} results must contain objects")
        entity_id = item.get("id")
        _validate_catalog_item(kind, item)
        if (
            kind == "subjects"
            and "parent_id" in cursor
            and item.get("parentId") != cursor["parent_id"]
        ):
            raise ValueError("GUS BDL subject result escaped the requested parent")
        ids.append(entity_id)
    if len(set(ids)) != len(ids):
        raise ValueError(f"GUS BDL {kind} page contains duplicate result identifiers")

    # The Polish pass owns expansion so bilingual pages do not emit duplicate
    # data campaigns. English pages still paginate and retain source labels.
    if cursor["lang"] == "pl":
        if kind == "subjects":
            for item in results:
                entity_id = item["id"]
                for lang in _LANGUAGES:
                    next_tasks.append(
                        _task(
                            "discovery",
                            "subject_detail",
                            {"lang": lang, "entity_id": entity_id},
                        )
                    )
                children = item.get("children")
                if children is not None:
                    if not isinstance(children, list) or any(
                        not isinstance(child, str) or not child for child in children
                    ):
                        raise ValueError("GUS BDL subject children must be string identifiers")
                    if children:
                        for lang in _LANGUAGES:
                            next_tasks.append(
                                _task(
                                    "discovery",
                                    "subjects",
                                    {
                                        "lang": lang,
                                        "parent_id": entity_id,
                                        "page": 0,
                                        "page_size": _DISCOVERY_PAGE_SIZE,
                                    },
                                )
                            )
        elif kind in {"units", "localities"}:
            detail_kind = "unit_detail" if kind == "units" else "locality_detail"
            for item in results:
                if item.get("hasDescription") is True:
                    for lang in _LANGUAGES:
                        next_tasks.append(
                            _task(
                                "discovery",
                                detail_kind,
                                {"lang": lang, "entity_id": item["id"]},
                            )
                        )
                if kind == "units" and item["level"] == 6:
                    for lang in _LANGUAGES:
                        next_tasks.append(
                            _task(
                                "discovery",
                                "localities",
                                {
                                    "lang": lang,
                                    "parent_id": item["id"],
                                    "page": 0,
                                    "page_size": _DISCOVERY_PAGE_SIZE,
                                },
                            )
                        )
        elif kind == "variables":
            for item in results:
                variable_id = item["id"]
                next_tasks.append(
                    _task(
                        "discovery",
                        "variable_detail",
                        {"lang": "en", "entity_id": variable_id},
                    )
                )
                next_tasks.append(_recent_data_task(variable_id, today))
                next_tasks.append(_history_data_task(variable_id))

    return {
        "record_count": len(results),
        "next_tasks": _unique_tasks(next_tasks),
        "metadata": {
            "api_total_records": total,
            "api_page": page,
            "api_page_size": page_size,
            "api_pagination_mode": (
                "complete_child_list"
                if complete_child_listing
                else "paged"
            ),
            "language": cursor["lang"],
            "result_ids": ids,
        },
    }


def _validate_data_page(
    task: dict[str, Any], payload: dict[str, Any], results: list[Any]
) -> int:
    variable_id = payload.get("variableId")
    if not _is_int(variable_id) or variable_id != task["cursor"]["variable_id"]:
        raise ValueError(f"GUS BDL data variableId {variable_id!r} differs from requested {task['cursor']['variable_id']}")
    for optional_integer in ("measureUnitId", "aggregateId"):
        value = payload.get(optional_integer)
        if value is not None and (not _is_int(value) or value < 0):
            raise ValueError(f"GUS BDL data {optional_integer} must be a non-negative integer; received {value!r}")
    last_update = payload.get("lastUpdate")
    if last_update is not None and not isinstance(last_update, str):
        raise ValueError("GUS BDL data lastUpdate must be a string or null")

    count = 0
    seen: set[tuple[int, str, str, int | None]] = set()
    for unit in results:
        if not isinstance(unit, dict):
            raise ValueError("GUS BDL data results must contain unit objects")
        unit_id = unit.get("id")
        if not _matches(_UNIT_ID_RE, unit_id):
            raise ValueError("GUS BDL data unit id must be a 12-digit BDL identifier")
        if unit.get("name") is not None and not isinstance(unit["name"], str):
            raise ValueError("GUS BDL data unit name must be a string or null")
        values = _list_field(unit, "values")
        for observation in values:
            if not isinstance(observation, dict):
                raise ValueError("GUS BDL unit values must contain objects")
            year = observation.get("year")
            if not isinstance(year, (str, int)) or isinstance(year, bool) or str(year) == "":
                raise ValueError("GUS BDL observation year must be a string or integer")
            attr_id = observation.get("attrId")
            if attr_id is not None and (not _is_int(attr_id) or attr_id < 0):
                raise ValueError(f"GUS BDL observation attrId must be a non-negative integer or null; received {attr_id!r}")
            # Live compact JSON uses ``val``; the published OpenAPI schema says
            # ``value``. Accept either spelling and leave the exact body intact.
            has_val = "val" in observation
            has_value = "value" in observation
            if has_val == has_value:
                raise ValueError("GUS BDL observation must contain exactly one of val or value")
            numeric_value = observation["val"] if has_val else observation["value"]
            if not isinstance(numeric_value, (int, float)) or isinstance(numeric_value, bool):
                raise ValueError(f"GUS BDL observation value must be numeric; received type {type(numeric_value).__name__}")
            precision = observation.get("precision")
            if precision is not None and (not _is_int(precision) or precision < 0):
                raise ValueError(f"GUS BDL observation precision must be a non-negative integer; received {precision!r}")
            key = (variable_id, unit_id, str(year), attr_id)
            if key in seen:
                raise ValueError("GUS BDL data page contains a duplicate observation grain")
            seen.add(key)
            count += 1
    return count


def _validate_catalog_item(kind: str, item: dict[str, Any]) -> None:
    entity_id = item.get("id")
    if kind == "variables":
        if not _is_int(entity_id) or entity_id < 0:
            raise ValueError("GUS BDL variable result id must be a non-negative integer")
        subject_id = item.get("subjectId")
        if not _matches(_SUBJECT_ID_RE, subject_id):
            raise ValueError("GUS BDL variable subjectId is invalid")
        for name in ("n1", "n2", "n3", "n4", "n5", "measureUnitName"):
            if item.get(name) is not None and not isinstance(item[name], str):
                raise ValueError(f"GUS BDL variable {name} must be a string or null")
        for name in ("level", "measureUnitId"):
            if not _is_int(item.get(name)) or item[name] < 0:
                raise ValueError(f"GUS BDL variable {name} must be a non-negative integer")
        return

    pattern = {
        "subjects": _SUBJECT_ID_RE,
        "units": _UNIT_ID_RE,
        "localities": _LOCALITY_ID_RE,
    }[kind]
    if not _matches(pattern, entity_id):
        raise ValueError(f"GUS BDL {kind} result id is invalid")
    if item.get("name") is not None and not isinstance(item["name"], str):
        raise ValueError(f"GUS BDL {kind} result name must be a string or null")
    if kind == "subjects":
        if not isinstance(item.get("hasVariables"), bool):
            raise ValueError("GUS BDL subject hasVariables must be boolean")
        parent_id = item.get("parentId")
        if parent_id is not None and not _matches(_SUBJECT_ID_RE, parent_id):
            raise ValueError("GUS BDL subject parentId is invalid")
        levels = item.get("levels")
        if levels is not None and (
            not isinstance(levels, list) or any(not _is_int(level) or level < 0 for level in levels)
        ):
            raise ValueError("GUS BDL subject levels must be non-negative integers")
        children = item.get("children")
        if children is not None and (
            not isinstance(children, list)
            or any(not _matches(_SUBJECT_ID_RE, child) for child in children)
        ):
            raise ValueError("GUS BDL subject children must be valid identifiers")
    else:
        parent_id = item.get("parentId")
        if parent_id is not None and not _matches(_UNIT_ID_RE, parent_id):
            raise ValueError(f"GUS BDL {kind} parentId is invalid")
        if not _is_int(item.get("level")) or item["level"] < 0:
            raise ValueError(f"GUS BDL {kind} level must be a non-negative integer")
        if item.get("kind") is not None and not isinstance(item["kind"], str):
            raise ValueError(f"GUS BDL {kind} kind must be a string or null")
        if not isinstance(item.get("hasDescription"), bool):
            raise ValueError(f"GUS BDL {kind} hasDescription must be boolean")


def _interpret_dictionary(
    task: dict[str, Any], payload: dict[str, Any]
) -> dict[str, Any]:
    results = _list_field(payload, "results")
    total = _nonnegative_int(payload, "totalRecords")
    if len(results) > total:
        raise ValueError("GUS BDL dictionary result count exceeds totalRecords")
    for item in results:
        if not isinstance(item, dict) or "id" not in item:
            raise ValueError("GUS BDL dictionary results must be identified objects")
    return {
        "record_count": len(results),
        "next_tasks": [],
        "metadata": {
            "api_total_records": total,
            "resource": task["cursor"]["resource"],
            "language": task["cursor"]["lang"],
        },
    }


def _interpret_years(payload: dict[str, Any]) -> dict[str, Any]:
    results = _list_field(payload, "results")
    total = _nonnegative_int(payload, "totalRecords")
    for item in results:
        if not isinstance(item, dict) or not _is_int(item.get("id")):
            raise ValueError("GUS BDL years must contain objects with integer ids")
    return {
        "record_count": len(results),
        "next_tasks": [],
        "metadata": {"api_total_records": total},
    }


def _interpret_detail(task: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    expected = task["cursor"]["entity_id"]
    actual = payload.get("id")
    if actual != expected:
        raise ValueError("GUS BDL detail id does not match the task cursor")
    if task["kind"] == "variable_detail":
        years = payload.get("years")
        if years is not None and (
            not isinstance(years, list) or any(not _is_int(year) for year in years)
        ):
            raise ValueError("GUS BDL variable years must be an integer list or null")
        for name in ("subjectId", "n1", "n2", "n3", "n4", "n5", "description"):
            if payload.get(name) is not None and not isinstance(payload[name], str):
                raise ValueError(f"GUS BDL variable {name} must be a string or null")
    elif task["kind"] == "subject_detail":
        children = payload.get("children")
        if children is not None and (
            not isinstance(children, list)
            or any(not isinstance(child, str) or not child for child in children)
        ):
            raise ValueError("GUS BDL subject children must be string identifiers")
    else:
        if not isinstance(actual, str) or not actual:
            raise ValueError("GUS BDL unit detail id must be a non-empty string")
        level = payload.get("level")
        if level is not None and (not _is_int(level) or level < 0):
            raise ValueError("GUS BDL unit detail level must be a non-negative integer or null")
    next_tasks: list[dict[str, Any]] = []
    if task["kind"] == "unit_detail" and payload.get("level") == 6:
        for lang in _LANGUAGES:
            next_tasks.append(
                _task(
                    "discovery",
                    "localities",
                    {
                        "lang": lang,
                        "parent_id": actual,
                        "page": 0,
                        "page_size": _DISCOVERY_PAGE_SIZE,
                    },
                )
            )
    return {
        "record_count": 1,
        "next_tasks": next_tasks,
        "metadata": {
            "entity_id": actual,
            "language": task["cursor"]["lang"],
            "resource": task["kind"],
        },
    }


def _validate_task(task: dict[str, Any]) -> None:
    if not isinstance(task, dict):
        raise TypeError("GUS BDL task must be an object")
    for field in ("id", "lane", "kind", "cursor"):
        if field not in task:
            raise ValueError(f"GUS BDL task is missing {field}")
    if task["lane"] not in {"recent", "history", "discovery", "reconcile"}:
        raise ValueError("GUS BDL task lane is invalid")
    if not isinstance(task["cursor"], dict):
        raise ValueError("GUS BDL task cursor must be an object")
    kind = task["kind"]
    if kind not in _PAGED_KINDS | _DETAIL_KINDS | {"dictionary", "years"}:
        raise ValueError(f"Unsupported GUS BDL task kind: {kind}")
    expected_fields = {"id", "lane", "kind", "cursor"}
    if kind == "data_by_variable" and task["lane"] == "recent":
        expected_fields.add("recurrence_key")
    runtime_fields = {"failures", "retry_at"}
    if not expected_fields <= set(task) or not (set(task) - expected_fields) <= runtime_fields:
        raise ValueError("GUS BDL task has unexpected or missing fields")
    failures = task.get("failures")
    if failures is not None and (not _is_int(failures) or failures < 0):
        raise ValueError("GUS BDL task failures must be a non-negative integer")
    retry_at = task.get("retry_at")
    if retry_at is not None and (
        not isinstance(retry_at, (int, float))
        or isinstance(retry_at, bool)
        or not math.isfinite(retry_at)
        or retry_at < 0
    ):
        raise ValueError("GUS BDL task retry_at must be a finite non-negative epoch")
    if task["id"] != _task_id(task["lane"], kind, task["cursor"]):
        raise ValueError("GUS BDL task id is not canonical for its cursor")
    if kind in {"subjects", "units", "localities", "variables"} | _DETAIL_KINDS:
        if task["cursor"].get("lang") not in _LANGUAGES:
            raise ValueError("GUS BDL task language must be pl or en")
    cursor_fields = set(task["cursor"])
    if kind == "dictionary":
        if cursor_fields != {"resource", "lang"} or task["lane"] != "discovery":
            raise ValueError("GUS BDL dictionary task shape is invalid")
        if task["cursor"]["lang"] not in _LANGUAGES:
            raise ValueError("GUS BDL dictionary language must be pl or en")
    elif kind == "years":
        if cursor_fields or task["lane"] != "discovery":
            raise ValueError("GUS BDL years task shape is invalid")
    elif kind in {"subjects", "units"}:
        if cursor_fields not in (
            {"lang", "page", "page_size"},
            {"lang", "page", "page_size", "parent_id"},
        ) or task["lane"] != "discovery":
            raise ValueError(f"GUS BDL {kind} task shape is invalid")
        parent_id = task["cursor"].get("parent_id")
        pattern = _SUBJECT_ID_RE if kind == "subjects" else _UNIT_ID_RE
        if parent_id is not None and not _matches(pattern, parent_id):
            raise ValueError(f"GUS BDL {kind} parent id is invalid")
    elif kind == "localities":
        if (
            cursor_fields != {"lang", "parent_id", "page", "page_size"}
            or task["lane"] != "discovery"
            or not _matches(_UNIT_ID_RE, task["cursor"].get("parent_id"))
        ):
            raise ValueError("GUS BDL localities task shape is invalid")
    elif kind == "variables":
        if cursor_fields != {"lang", "page", "page_size"} or task["lane"] != "discovery":
            raise ValueError(f"GUS BDL {kind} task shape is invalid")
    elif kind in _DETAIL_KINDS:
        if cursor_fields != {"lang", "entity_id"} or task["lane"] != "discovery":
            raise ValueError(f"GUS BDL {kind} task shape is invalid")
        entity_id = task["cursor"]["entity_id"]
        if kind == "variable_detail":
            if not _is_int(entity_id) or entity_id < 0:
                raise ValueError("GUS BDL variable detail id is invalid")
        else:
            pattern = {
                "subject_detail": _SUBJECT_ID_RE,
                "unit_detail": _UNIT_ID_RE,
                "locality_detail": _LOCALITY_ID_RE,
            }[kind]
            if not _matches(pattern, entity_id):
                raise ValueError(f"GUS BDL {kind} id is invalid")
    if kind in _PAGED_KINDS:
        _cursor_nonnegative_int(task["cursor"], "page")
        _cursor_positive_int(task["cursor"], "page_size")
    if kind == "data_by_variable":
        expected_cursor_fields = {"variable_id", "years", "page", "page_size", "root"}
        if task["lane"] == "recent":
            expected_cursor_fields.add("asof")
        if cursor_fields != expected_cursor_fields or task["lane"] not in {
            "recent",
            "history",
            "reconcile",
        }:
            raise ValueError("GUS BDL data task shape is invalid")
        variable_id = task["cursor"].get("variable_id")
        if not _is_int(variable_id) or variable_id < 0:
            raise ValueError("GUS BDL variable_id must be a non-negative integer")
        years = task["cursor"].get("years")
        if years is not None and (
            not isinstance(years, list)
            or not years
            or any(not _is_int(year) for year in years)
            or years != sorted(set(years))
        ):
            raise ValueError("GUS BDL data years must be null or a sorted unique integer list")
        root = task["cursor"].get("root")
        if not isinstance(root, bool) or root != (task["cursor"]["page"] == 0):
            raise ValueError("GUS BDL data root must be true only on page zero")
        if task["lane"] == "recent":
            asof = task["cursor"].get("asof")
            try:
                parsed_asof = date.fromisoformat(asof)
            except (TypeError, ValueError) as exc:
                raise ValueError("GUS BDL recent task asof must be an ISO date") from exc
            if parsed_asof.isoformat() != asof:
                raise ValueError("GUS BDL recent task asof must be a canonical ISO date")
        if (
            task["lane"] == "recent"
            and task.get("recurrence_key") != f"variable:{variable_id}"
        ):
            raise ValueError("GUS BDL recent task requires its stable recurrence_key")


def _unique_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for task in tasks:
        by_id[task["id"]] = task
    return list(by_id.values())


def _validate_obsolete_locality_task(task: dict[str, Any], lang: str) -> None:
    expected_fields = {"id", "lane", "kind", "cursor"}
    runtime_fields = {"failures", "retry_at"}
    if set(task) - expected_fields - runtime_fields:
        raise ValueError("Obsolete GUS BDL locality task has unexpected fields")
    if not expected_fields <= set(task):
        raise ValueError("Obsolete GUS BDL locality task is missing fields")
    if task["lane"] != "discovery" or task["kind"] != "localities":
        raise ValueError("Obsolete GUS BDL locality task has unexpected identity")
    if task["cursor"] != {"lang": lang, "page": 0, "page_size": _DISCOVERY_PAGE_SIZE}:
        raise ValueError("Obsolete GUS BDL locality task has unexpected cursor")
    failures = task.get("failures")
    if failures is not None and (not _is_int(failures) or failures < 0):
        raise ValueError("Obsolete GUS BDL locality task failures are invalid")
    retry_at = task.get("retry_at")
    if retry_at is not None and (
        not isinstance(retry_at, (int, float))
        or isinstance(retry_at, bool)
        or not math.isfinite(retry_at)
        or retry_at < 0
    ):
        raise ValueError("Obsolete GUS BDL locality task retry_at is invalid")


def _require_date(value: date) -> None:
    if not isinstance(value, date):
        raise TypeError("today must be a date")


def _list_field(payload: dict[str, Any], field: str) -> list[Any]:
    value = payload.get(field)
    if not isinstance(value, list):
        raise ValueError(f"GUS BDL response {field} must be a list")
    return value


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _matches(pattern: re.Pattern[str], value: Any) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _nonnegative_int(payload: dict[str, Any], field: str) -> int:
    value = payload.get(field)
    if not _is_int(value) or value < 0:
        raise ValueError(f"GUS BDL response {field} must be a non-negative integer")
    return value


def _positive_int(payload: dict[str, Any], field: str) -> int:
    value = payload.get(field)
    if not _is_int(value) or value <= 0:
        raise ValueError(f"GUS BDL response {field} must be a positive integer")
    return value


def _optional_paging_int(
    payload: dict[str, Any], field: str, expected: int, *, positive: bool
) -> int:
    if field not in payload:
        return expected
    value = payload[field]
    valid = _is_int(value) and (value > 0 if positive else value >= 0)
    if not valid:
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"GUS BDL response {field} must be a {qualifier} integer")
    if value != expected:
        raise ValueError(f"GUS BDL response {field} does not match the task cursor")
    return value


def _cursor_nonnegative_int(cursor: dict[str, Any], field: str) -> int:
    value = cursor.get(field)
    if not _is_int(value) or value < 0:
        raise ValueError(f"GUS BDL task {field} must be a non-negative integer")
    return value


def _cursor_positive_int(cursor: dict[str, Any], field: str) -> int:
    value = cursor.get(field)
    if not _is_int(value) or value <= 0:
        raise ValueError(f"GUS BDL task {field} must be a positive integer")
    return value
