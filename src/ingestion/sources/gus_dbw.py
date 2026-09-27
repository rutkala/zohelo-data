"""Task planning and response validation for GUS Dziedzinowe Bazy Wiedzy (DBW).

This module defines task planning, request formation, and response interpretation
for the official Statistics Poland (GUS) Subject-Matter Knowledge Databases (DBW).
It complies with ADR 0009: landing is native transfer only; parsing and validation
here serve only to plan subsequent discovery tasks and verify transport completeness
without transforming data or inferring downstream business semantics.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
import re
from typing import Any


SOURCE_ID = "gus_dbw"
ALLOWED_HOSTS = ("api-dbw.stat.gov.pl", "dbw.stat.gov.pl")

API_BASE = "https://api-dbw.stat.gov.pl/api"
_DEFAULT_PAGE_SIZE = 100
_DICTIONARY_PAGE_SIZE = 100
_OBSERVATION_PAGE_SIZE = 100
_LANGUAGES = ("pl", "en")

_DICTIONARIES = (
    "date-dictionary",
    "periods-dictionary",
    "way-of-presentation",
    "no-value-dictionary",
    "confidentionality-dictionary",
    "flag-dictionary",
)

_KINDS = {
    "dictionary",
    "areas",
    "area_variables",
    "variable_meta",
    "variable_section_periods",
    "variable_section_position",
    "variable_data_section",
    "bulk_download",
}


class GusDbwSourceError(ValueError):
    """Raised when a task or DBW response violates the source contract."""


def _require_date(value: date) -> None:
    if not isinstance(value, date):
        raise TypeError("A valid calendar date is required")


def _task_id(lane: str, kind: str, cursor: dict[str, Any]) -> str:
    lang = cursor.get("lang", "pl")
    if kind == "dictionary":
        resource = cursor["resource"]
        page = cursor.get("page", 1)
        return f"{lane}:dictionary:{resource}:{lang}:p{page:06d}"
    if kind == "areas":
        return f"{lane}:areas:{lang}"
    if kind == "area_variables":
        area_id = cursor["area_id"]
        return f"{lane}:area-variables:{area_id}:{lang}"
    if kind == "variable_meta":
        var_id = cursor["variable_id"]
        return f"{lane}:variable-meta:{var_id}:{lang}"
    if kind == "variable_section_periods":
        page = cursor.get("page", 1)
        return f"{lane}:section-periods:{lang}:p{page:06d}"
    if kind == "variable_section_position":
        sec_id = cursor["section_id"]
        return f"{lane}:section-position:{sec_id}:{lang}"
    if kind == "variable_data_section":
        var_id = cursor["variable_id"]
        sec_id = cursor["section_id"]
        year = cursor["year"]
        period = cursor["period"]
        page = cursor.get("page", 1)
        return f"{lane}:data-section:v{var_id}:s{sec_id}:y{year}:p{period}:{lang}:pg{page:06d}"
    if kind == "bulk_download":
        url = cursor["url"]
        import hashlib
        h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
        name = cursor.get("filename", "package")
        return f"{lane}:bulk:{name}:{h}"
    raise GusDbwSourceError(f"Unsupported DBW task kind: {kind}")


def _task(lane: str, kind: str, cursor: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _task_id(lane, kind, cursor),
        "lane": lane,
        "kind": kind,
        "cursor": cursor,
    }


def initial_tasks(today: date) -> list[dict[str, Any]]:
    """Return bootstrap discovery tasks for DBW taxonomy, dictionaries, and sections."""
    _require_date(today)
    tasks: list[dict[str, Any]] = []

    # 1. Dictionaries in PL and EN
    for dict_name in _DICTIONARIES:
        for lang in _LANGUAGES:
            tasks.append(_task("discovery", "dictionary", {
                "resource": dict_name,
                "page": 1,
                "page_size": _DICTIONARY_PAGE_SIZE,
                "lang": lang,
            }))

    # 2. Complete Areas hierarchy in PL and EN
    for lang in _LANGUAGES:
        tasks.append(_task("discovery", "areas", {
            "lang": lang,
        }))

    # 3. Variable section periods (initial page)
    for lang in _LANGUAGES:
        tasks.append(_task("discovery", "variable_section_periods", {
            "page": 1,
            "page_size": _DEFAULT_PAGE_SIZE,
            "lang": lang,
        }))

    return tasks


def recent_tasks(today: date) -> list[dict[str, Any]]:
    """Return recurring tasks to refresh the catalogue and top-level areas."""
    _require_date(today)
    tasks: list[dict[str, Any]] = []
    for lang in _LANGUAGES:
        tasks.append(_task("recent", "areas", {"lang": lang}))
    for dict_name in _DICTIONARIES:
        tasks.append(_task("recent", "dictionary", {
            "resource": dict_name,
            "page": 1,
            "page_size": _DICTIONARY_PAGE_SIZE,
            "lang": "pl",
        }))
    return tasks


def refresh_task(task: dict[str, Any], today: date) -> dict[str, Any] | None:
    """Refresh a recurring recent task for a new collection date."""
    _require_date(today)
    kind = task.get("kind")
    cursor = task.get("cursor", {})
    if kind == "areas":
        return _task("recent", "areas", {"lang": cursor.get("lang", "pl")})
    if kind == "dictionary" and cursor.get("page") == 1:
        return _task("recent", "dictionary", {
            "resource": cursor["resource"],
            "page": 1,
            "page_size": cursor.get("page_size", _DICTIONARY_PAGE_SIZE),
            "lang": cursor.get("lang", "pl"),
        })
    return None


def request_for(task: dict[str, Any]) -> dict[str, Any]:
    """Build the exact GET request specification for one DBW task."""
    kind = task.get("kind")
    cursor = task.get("cursor", {})
    lang = cursor.get("lang", "pl")

    if kind == "dictionary":
        resource = cursor["resource"]
        page = cursor.get("page", 1)
        page_size = cursor.get("page_size", _DICTIONARY_PAGE_SIZE)
        return {
            "url": f"{API_BASE}/dictionaries/{resource}",
            "params": {
                "page": str(page),
                "page-size": str(page_size),
                "lang": lang,
            },
        }

    if kind == "areas":
        return {
            "url": f"{API_BASE}/area/area-area",
            "params": {
                "lang": lang,
            },
        }

    if kind == "area_variables":
        area_id = cursor["area_id"]
        return {
            "url": f"{API_BASE}/area/area-variable",
            "params": {
                "id-obszaru": str(area_id),
                "lang": lang,
            },
        }

    if kind == "variable_meta":
        var_id = cursor["variable_id"]
        return {
            "url": f"{API_BASE}/variable/variable-meta",
            "params": {
                "id-zmiennej": str(var_id),
                "lang": lang,
            },
        }

    if kind == "variable_section_periods":
        page = cursor.get("page", 1)
        page_size = cursor.get("page_size", _DEFAULT_PAGE_SIZE)
        return {
            "url": f"{API_BASE}/variable/variable-section-periods",
            "params": {
                "numer-strony": str(page),
                "ile-na-stronie": str(page_size),
                "lang": lang,
            },
        }

    if kind == "variable_section_position":
        section_id = cursor["section_id"]
        return {
            "url": f"{API_BASE}/variable/variable-section-position",
            "params": {
                "id-przekroj": str(section_id),
                "lang": lang,
            },
        }

    if kind == "variable_data_section":
        return {
            "url": f"{API_BASE}/variable/variable-data-section",
            "params": {
                "id-zmienna": str(cursor["variable_id"]),
                "id-przekroj": str(cursor["section_id"]),
                "id-rok": str(cursor["year"]),
                "id-okres": str(cursor["period"]),
                "numer-strony": str(cursor.get("page", 1)),
                "ile-na-stronie": str(cursor.get("page_size", _OBSERVATION_PAGE_SIZE)),
                "lang": lang,
            },
        }

    if kind == "bulk_download":
        return {
            "url": cursor["url"],
            "params": {},
        }

    raise GusDbwSourceError(f"Unsupported task kind for request_for: {kind}")


def interpret(task: dict[str, Any], body: bytes, today: date) -> dict[str, Any]:
    """Validate a DBW response structurally and return planned follow-up tasks."""
    _require_date(today)
    if not isinstance(body, (bytes, bytearray)) or not body:
        raise GusDbwSourceError("DBW response body must be non-empty bytes")

    kind = task.get("kind")
    lane = task.get("lane", "discovery")
    cursor = task.get("cursor", {})
    lang = cursor.get("lang", "pl")

    if kind == "bulk_download":
        return {
            "status": "accepted",
            "bytes_received": len(body),
            "records": 1,
            "tasks": [],
        }

    # All API responses are expected to be valid JSON
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise GusDbwSourceError(f"DBW response is not valid JSON: {exc}") from exc

    follow_up_tasks: list[dict[str, Any]] = []
    records = 0

    if kind == "dictionary":
        records = _interpret_dictionary(lane, cursor, payload, follow_up_tasks)
    elif kind == "areas":
        records = _interpret_areas(lane, lang, payload, follow_up_tasks)
    elif kind == "area_variables":
        records = _interpret_area_variables(lane, lang, cursor, payload, follow_up_tasks)
    elif kind == "variable_meta":
        records = _interpret_variable_meta(lane, lang, cursor, payload)
    elif kind == "variable_section_periods":
        records = _interpret_section_periods(lane, lang, cursor, payload, follow_up_tasks)
    elif kind == "variable_section_position":
        records = _interpret_section_position(lane, lang, cursor, payload)
    elif kind == "variable_data_section":
        records = _interpret_data_section(lane, lang, cursor, payload, follow_up_tasks)
    else:
        raise GusDbwSourceError(f"Unsupported task kind for interpret: {kind}")

    return {
        "status": "accepted",
        "bytes_received": len(body),
        "records": records,
        "tasks": follow_up_tasks,
    }


def _interpret_dictionary(
    lane: str, cursor: dict[str, Any], payload: Any, follow_ups: list[dict[str, Any]]
) -> int:
    resource = cursor["resource"]
    page = cursor.get("page", 1)
    page_size = cursor.get("page_size", _DICTIONARY_PAGE_SIZE)
    lang = cursor.get("lang", "pl")

    items = []
    total = None

    if isinstance(payload, list):
        items = payload
        total = len(items)
    elif isinstance(payload, dict):
        items = payload.get("data") or payload.get("results") or payload.get("items") or []
        total = payload.get("total") or payload.get("totalRecords") or payload.get("count")

    count = len(items)
    # Check if there is another page
    if total is not None and (page * page_size) < total and count > 0:
        follow_ups.append(_task(lane, "dictionary", {
            "resource": resource,
            "page": page + 1,
            "page_size": page_size,
            "lang": lang,
        }))
    return count


def _interpret_areas(
    lane: str, lang: str, payload: Any, follow_ups: list[dict[str, Any]]
) -> int:
    if not isinstance(payload, list):
        raise GusDbwSourceError("DBW area-area response must be an array")

    area_ids: set[int] = set()

    def _walk(nodes: list[Any]) -> None:
        for node in nodes:
            if isinstance(node, dict):
                # An area node has an ID (can be int or string)
                raw_id = node.get("id") or node.get("id_obszaru") or node.get("id-obszaru")
                if raw_id is not None:
                    try:
                        # Extract integer if present
                        m = re.search(r"\d+", str(raw_id))
                        if m:
                            area_ids.add(int(m.group()))
                    except (ValueError, TypeError):
                        pass
                children = node.get("children") or node.get("podgrupy") or []
                if isinstance(children, list):
                    _walk(children)

    _walk(payload)

    # For each discovered area ID, emit task to discover variables
    for aid in sorted(area_ids):
        follow_ups.append(_task(lane, "area_variables", {
            "area_id": aid,
            "lang": lang,
        }))

    return len(payload)


def _interpret_area_variables(
    lane: str, lang: str, cursor: dict[str, Any], payload: Any, follow_ups: list[dict[str, Any]]
) -> int:
    variables = payload if isinstance(payload, list) else (payload.get("data") or payload.get("results") or [])
    if not isinstance(variables, list):
        raise GusDbwSourceError("DBW area-variable response must contain a list of variables")

    for item in variables:
        if isinstance(item, dict):
            var_id = item.get("id") or item.get("id-zmienna") or item.get("id_zmienna") or item.get("id-zmiennej")
            if var_id is not None:
                try:
                    numeric_id = int(str(var_id).strip())
                    follow_ups.append(_task(lane, "variable_meta", {
                        "variable_id": numeric_id,
                        "lang": lang,
                    }))
                except (ValueError, TypeError):
                    pass
    return len(variables)


def _interpret_variable_meta(
    lane: str, lang: str, cursor: dict[str, Any], payload: Any
) -> int:
    if not isinstance(payload, dict):
        raise GusDbwSourceError("DBW variable-meta response must be an object")
    return 1


def _interpret_section_periods(
    lane: str, lang: str, cursor: dict[str, Any], payload: Any, follow_ups: list[dict[str, Any]]
) -> int:
    page = cursor.get("page", 1)
    page_size = cursor.get("page_size", _DEFAULT_PAGE_SIZE)

    items = []
    total_pages = None

    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = payload.get("data") or payload.get("results") or payload.get("items") or []
        total_pages = payload.get("totalPages") or payload.get("total_pages") or payload.get("liczba-stron")

    for item in items:
        if isinstance(item, dict):
            sec_id = item.get("id-przekroj") or item.get("id_przekroj") or item.get("idPrzekroj")
            if sec_id is not None:
                try:
                    num_sec = int(str(sec_id).strip())
                    follow_ups.append(_task(lane, "variable_section_position", {
                        "section_id": num_sec,
                        "lang": lang,
                    }))
                except (ValueError, TypeError):
                    pass

    # Follow up with next page if available
    count = len(items)
    if (total_pages is not None and page < total_pages) or count == page_size:
        follow_ups.append(_task(lane, "variable_section_periods", {
            "page": page + 1,
            "page_size": page_size,
            "lang": lang,
        }))

    return count


def _interpret_section_position(
    lane: str, lang: str, cursor: dict[str, Any], payload: Any
) -> int:
    if not isinstance(payload, (list, dict)):
        raise GusDbwSourceError("DBW variable-section-position response must be an array or object")
    return len(payload) if isinstance(payload, list) else len(payload.get("data", [payload]))


def _interpret_data_section(
    lane: str, lang: str, cursor: dict[str, Any], payload: Any, follow_ups: list[dict[str, Any]]
) -> int:
    page = cursor.get("page", 1)
    page_size = cursor.get("page_size", _OBSERVATION_PAGE_SIZE)

    items = []
    total_pages = None

    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = payload.get("data") or payload.get("results") or payload.get("items") or []
        total_pages = payload.get("totalPages") or payload.get("total_pages") or payload.get("liczba-stron")

    count = len(items)
    if (total_pages is not None and page < total_pages) or count == page_size:
        next_cursor = deepcopy(cursor)
        next_cursor["page"] = page + 1
        follow_ups.append(_task(lane, "variable_data_section", next_cursor))

    return count
