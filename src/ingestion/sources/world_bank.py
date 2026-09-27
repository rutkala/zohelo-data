"""Task planning and response validation for World Bank WDI API v2.

The shared ingestion framework owns HTTP, retries, exact-byte persistence and the
durable queue.  This module only creates serializable tasks and validates enough
of each JSON page to plan subsequent work.
"""

from __future__ import annotations

from datetime import date
import json
import re
from typing import Any, Mapping


SOURCE_ID = "world_bank_wdi"
WDI_API_SOURCE_ID = "2"
ALLOWED_HOSTS = ("api.worldbank.org",)

API_BASE = "https://api.worldbank.org/v2"
HISTORY_START_YEAR = 1960
RECENT_RECHECK_YEARS = 5
INDICATOR_DISCOVERY_PAGE_SIZE = 25
COUNTRY_DISCOVERY_PAGE_SIZE = 500
OBSERVATION_PAGE_SIZE = 1_000

_INDICATOR_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SEED_INDICATORS = (
    "SP.POP.TOTL",
    "NY.GDP.MKTP.CD",
    "SH.XPD.CHEX.GD.ZS",
)
_KINDS = {
    "indicator_catalog",
    "country_catalog",
    "indicator_history",
    "indicator_recent",
}


class WorldBankSourceError(ValueError):
    """Raised when a task or WDI response violates the source contract."""


def initial_tasks(today: date) -> list[dict[str, Any]]:
    """Return bootstrap discovery plus immediately useful recent/history work."""
    _require_date(today)
    tasks = [_indicator_catalog_task(1), _country_catalog_task(1)]
    for indicator in _SEED_INDICATORS:
        tasks.append(_recent_task(indicator, today, 1))
        tasks.append(_history_task(indicator, today.year, 1))
    return tasks


def recent_tasks(today: date) -> list[dict[str, Any]]:
    """Return deterministic recurring roots for the bootstrap indicators.

    The framework also registers recent roots emitted by indicator discovery and
    calls :func:`refresh_task` for them, so the catalogue is not limited to these
    seeds after bootstrap.
    """
    _require_date(today)
    return [_recent_task(indicator, today, 1) for indicator in _SEED_INDICATORS]


def refresh_task(task: dict[str, Any], today: date) -> dict[str, Any] | None:
    """Refresh a registered recent first-page root for a new collection date."""
    _require_date(today)
    kind, cursor = _validated_task(task)
    if kind != "indicator_recent" or cursor["page"] != 1:
        return None
    return _recent_task(cursor["indicator"], today, 1)


def request_for(task: dict[str, Any]) -> dict[str, Any]:
    """Build one GET request for exactly one API page."""
    kind, cursor = _validated_task(task)
    common = {
        "format": "json",
        "page": str(cursor["page"]),
    }
    if kind == "indicator_catalog":
        return {
            "url": f"{API_BASE}/indicator",
            "params": {
                **common,
                "source": WDI_API_SOURCE_ID,
                "per_page": str(INDICATOR_DISCOVERY_PAGE_SIZE),
            },
        }
    if kind == "country_catalog":
        return {
            "url": f"{API_BASE}/country",
            "params": {**common, "per_page": str(COUNTRY_DISCOVERY_PAGE_SIZE)},
        }

    indicator = cursor["indicator"]
    params = {
        **common,
        "source": WDI_API_SOURCE_ID,
        "date": f"{cursor['start_year']}:{cursor['end_year']}",
        "footnote": "y",
        "per_page": str(OBSERVATION_PAGE_SIZE),
    }
    return {
        "url": f"{API_BASE}/country/all/indicator/{indicator}",
        "params": params,
    }


def interpret(task: dict[str, Any], body: bytes, today: date) -> dict[str, Any]:
    """Validate a WDI page and return bounded follow-up tasks."""
    _require_date(today)
    kind, cursor = _validated_task(task)
    header, rows = _paged_json(body)
    page = _page_number(header, "page", minimum=1)
    pages = _page_number(header, "pages", minimum=0)
    per_page = _page_number(header, "per_page", minimum=1)
    total = _page_number(header, "total", minimum=0)
    if page != cursor["page"]:
        raise WorldBankSourceError(
            f"response page {page} does not match requested page {cursor['page']}"
        )
    if pages == 0 and total != 0:
        raise WorldBankSourceError("zero response pages require a zero total")
    if pages and page > pages:
        raise WorldBankSourceError("response page exceeds the declared page count")
    if len(rows) > per_page or len(rows) > total:
        raise WorldBankSourceError("response row count conflicts with pagination metadata")
    if total and not rows:
        raise WorldBankSourceError("non-empty result metadata cannot have an empty page")
    if kind in {"indicator_catalog", "country_catalog"} and not rows:
        raise WorldBankSourceError("WDI discovery cannot complete from an empty catalog")

    next_tasks: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {
        "api_page": page,
        "api_pages": pages,
        "api_per_page": per_page,
        "api_total": total,
        "api_last_updated": _optional_text(header, "lastupdated"),
        "wdi_api_source_id": WDI_API_SOURCE_ID,
        "task_kind": kind,
    }

    if kind == "indicator_catalog":
        indicators = _validate_indicators(rows)
        for indicator in indicators:
            next_tasks.append(_recent_task(indicator, today, 1))
            next_tasks.append(_history_task(indicator, today.year, 1))
        if page < pages:
            next_tasks.append(_indicator_catalog_task(page + 1))
        metadata["discovered_indicator_count"] = len(indicators)
    elif kind == "country_catalog":
        economy_count, aggregate_count = _validate_countries(rows)
        if page < pages:
            next_tasks.append(_country_catalog_task(page + 1))
        metadata["country_entity_count"] = economy_count
        metadata["aggregate_entity_count"] = aggregate_count
    else:
        _validate_observations(
            rows,
            cursor["indicator"],
            cursor["start_year"],
            cursor["end_year"],
        )
        if page < pages:
            next_tasks.append(_next_observation_page(task, cursor, page + 1))
        metadata.update(
            {
                "indicator_id": cursor["indicator"],
                "period_start_year": cursor["start_year"],
                "period_end_year": cursor["end_year"],
            }
        )
        if kind == "indicator_recent":
            metadata["as_of_date"] = cursor["asof"]

    return {
        "record_count": len(rows),
        "next_tasks": next_tasks,
        "metadata": metadata,
    }


def _indicator_catalog_task(page: int) -> dict[str, Any]:
    return _task("discovery", "indicator_catalog", {"page": page})


def _country_catalog_task(page: int) -> dict[str, Any]:
    return _task("discovery", "country_catalog", {"page": page})


def _history_task(indicator: str, end_year: int, page: int) -> dict[str, Any]:
    _require_indicator(indicator)
    cursor = {
        "indicator": indicator,
        "start_year": HISTORY_START_YEAR,
        "end_year": _require_year(end_year),
        "page": _require_page(page),
    }
    return _task("history", "indicator_history", cursor)


def _recent_task(indicator: str, today: date, page: int) -> dict[str, Any]:
    _require_date(today)
    _require_indicator(indicator)
    end_year = today.year
    cursor = {
        "indicator": indicator,
        "start_year": end_year - RECENT_RECHECK_YEARS + 1,
        "end_year": end_year,
        "asof": today.isoformat(),
        "page": _require_page(page),
    }
    return _task("recent", "indicator_recent", cursor)


def _task(lane: str, kind: str, cursor: dict[str, Any]) -> dict[str, Any]:
    if kind in {"indicator_catalog", "country_catalog"}:
        task_id = f"wdi:{kind}:p{cursor['page']:06d}"
    elif kind == "indicator_history":
        task_id = (
            f"wdi:history:{cursor['indicator']}:{cursor['start_year']}-"
            f"{cursor['end_year']}:p{cursor['page']:06d}"
        )
    else:
        task_id = (
            f"wdi:recent:{cursor['indicator']}:{cursor['asof']}:"
            f"p{cursor['page']:06d}"
        )
    task = {"id": task_id, "lane": lane, "kind": kind, "cursor": cursor}
    if kind == "indicator_recent":
        task["recurrence_key"] = f"indicator:{cursor['indicator']}"
    return task


def _next_observation_page(
    task: Mapping[str, Any], cursor: Mapping[str, Any], page: int
) -> dict[str, Any]:
    if task["kind"] == "indicator_history":
        return _history_task(cursor["indicator"], cursor["end_year"], page)
    asof = date.fromisoformat(cursor["asof"])
    return _recent_task(cursor["indicator"], asof, page)


def _validated_task(task: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    if not isinstance(task, Mapping):
        raise WorldBankSourceError("task must be an object")
    kind = task.get("kind")
    cursor = task.get("cursor")
    if kind not in _KINDS or not isinstance(cursor, Mapping):
        raise WorldBankSourceError("task kind or cursor is invalid")
    expected_fields = {"id", "lane", "kind", "cursor"}
    if kind == "indicator_recent":
        expected_fields.add("recurrence_key")
    optional_runtime_fields = {"failures", "retry_at"}
    if not expected_fields <= set(task) or not (
        set(task) - expected_fields
    ) <= optional_runtime_fields:
        raise WorldBankSourceError("task has unexpected or missing fields")
    failures = task.get("failures")
    if failures is not None and (
        isinstance(failures, bool) or not isinstance(failures, int) or failures < 1
    ):
        raise WorldBankSourceError("task failure count is invalid")
    retry_at = task.get("retry_at")
    if retry_at is not None and (
        isinstance(retry_at, bool)
        or not isinstance(retry_at, (int, float))
        or retry_at < 0
    ):
        raise WorldBankSourceError("task retry time is invalid")
    page = _require_page(cursor.get("page"))

    if kind in {"indicator_catalog", "country_catalog"}:
        if task.get("lane") != "discovery" or set(cursor) != {"page"}:
            raise WorldBankSourceError("discovery task shape is invalid")
        canonical = _task("discovery", kind, {"page": page})
    else:
        indicator = _require_indicator(cursor.get("indicator"))
        start_year = _require_year(cursor.get("start_year"))
        end_year = _require_year(cursor.get("end_year"))
        if start_year > end_year:
            raise WorldBankSourceError("task year range is reversed")
        if kind == "indicator_history":
            if task.get("lane") != "history" or set(cursor) != {
                "indicator",
                "start_year",
                "end_year",
                "page",
            }:
                raise WorldBankSourceError("history task shape is invalid")
            canonical = _history_task(indicator, end_year, page)
            if start_year != HISTORY_START_YEAR:
                raise WorldBankSourceError("history task has an unsupported start year")
        else:
            if task.get("lane") != "recent" or set(cursor) != {
                "indicator",
                "start_year",
                "end_year",
                "asof",
                "page",
            }:
                raise WorldBankSourceError("recent task shape is invalid")
            try:
                asof = date.fromisoformat(cursor["asof"])
            except (TypeError, ValueError):
                raise WorldBankSourceError("recent task asof must be an ISO date") from None
            canonical = _recent_task(indicator, asof, page)
            if start_year != canonical["cursor"]["start_year"] or end_year != asof.year:
                raise WorldBankSourceError("recent task year range does not match asof")

    if task.get("id") != canonical["id"]:
        raise WorldBankSourceError("task id does not match its cursor")
    if task.get("recurrence_key") != canonical.get("recurrence_key"):
        raise WorldBankSourceError("task recurrence key does not match its cursor")
    return kind, dict(cursor)


def _paged_json(body: bytes) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not isinstance(body, bytes) or not body:
        raise WorldBankSourceError("response body must be non-empty bytes")
    try:
        document = json.loads(
            body.decode("utf-8"), parse_constant=_reject_non_json_number
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorldBankSourceError("response is not valid UTF-8 JSON") from exc
    if (
        not isinstance(document, list)
        or len(document) != 2
        or not isinstance(document[0], dict)
        or not isinstance(document[1], list)
    ):
        raise WorldBankSourceError("response is not a paginated World Bank JSON result")
    if "message" in document[0]:
        raise WorldBankSourceError("World Bank API returned an error message")
    if any(not isinstance(row, dict) for row in document[1]):
        raise WorldBankSourceError("response rows must be objects")
    return document[0], document[1]


def _reject_non_json_number(value: str) -> None:
    raise WorldBankSourceError(f"response contains invalid JSON number {value}")


def _validate_indicators(rows: list[dict[str, Any]]) -> list[str]:
    indicators: list[str] = []
    seen: set[str] = set()
    for row in rows:
        indicator = _require_indicator(row.get("id"))
        if not isinstance(row.get("name"), str):
            raise WorldBankSourceError("indicator name must be text")
        source = row.get("source")
        if not isinstance(source, Mapping) or str(source.get("id")) != WDI_API_SOURCE_ID:
            raise WorldBankSourceError("indicator does not belong to WDI source 2")
        for key in ("unit", "sourceNote", "sourceOrganization"):
            if key in row and row[key] is not None and not isinstance(row[key], str):
                raise WorldBankSourceError(f"indicator {key} must be text when present")
        if (
            "topics" in row
            and row["topics"] is not None
            and not isinstance(row["topics"], list)
        ):
            raise WorldBankSourceError("indicator topics must be a list when present")
        if indicator in seen:
            raise WorldBankSourceError("indicator page contains a duplicate id")
        seen.add(indicator)
        indicators.append(indicator)
    return indicators


def _validate_countries(rows: list[dict[str, Any]]) -> tuple[int, int]:
    economies = aggregates = 0
    seen: set[str] = set()
    for row in rows:
        entity_id = row.get("id")
        if not isinstance(entity_id, str) or not entity_id:
            raise WorldBankSourceError("country entity id must be non-empty text")
        if entity_id in seen:
            raise WorldBankSourceError("country page contains a duplicate entity id")
        seen.add(entity_id)
        if not isinstance(row.get("name"), str):
            raise WorldBankSourceError("country entity name must be text")
        region = row.get("region")
        if not isinstance(region, Mapping) or not isinstance(region.get("id"), str):
            raise WorldBankSourceError("country entity region metadata is missing")
        if region["id"] == "NA":
            aggregates += 1
        else:
            economies += 1
    return economies, aggregates


def _validate_observations(
    rows: list[dict[str, Any]], indicator: str, start_year: int, end_year: int
) -> None:
    for row in rows:
        row_indicator = row.get("indicator")
        country = row.get("country")
        if (
            not isinstance(row_indicator, Mapping)
            or row_indicator.get("id") != indicator
            or not isinstance(country, Mapping)
            or not isinstance(country.get("id"), str)
            or not country["id"]
        ):
            raise WorldBankSourceError("observation source keys are invalid")
        row_date = row.get("date")
        if not isinstance(row_date, str) or not row_date.isdecimal():
            raise WorldBankSourceError("WDI observation period must be a calendar year")
        if not start_year <= int(row_date) <= end_year:
            raise WorldBankSourceError("observation period is outside the requested range")
        if "value" not in row:
            raise WorldBankSourceError("observation value field is missing")
        value = row.get("value")
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, (int, float))
        ):
            raise WorldBankSourceError("observation value must be numeric or null")
        country_iso3 = row.get("countryiso3code")
        if country_iso3 is not None and not isinstance(country_iso3, str):
            raise WorldBankSourceError(
                "observation countryiso3code must be text when present"
            )
        for key in ("unit", "obs_status", "footnote"):
            if key in row and row[key] is not None and not isinstance(row[key], str):
                raise WorldBankSourceError(f"observation {key} must be text when present")
        decimal = row.get("decimal")
        if decimal is not None and (
            isinstance(decimal, bool) or not isinstance(decimal, int)
        ):
            raise WorldBankSourceError("observation decimal must be an integer when present")


def _page_number(header: Mapping[str, Any], name: str, *, minimum: int) -> int:
    value = header.get(name)
    if isinstance(value, str) and value.isdecimal():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise WorldBankSourceError(f"response {name} is invalid")
    return value


def _optional_text(header: Mapping[str, Any], name: str) -> str | None:
    value = header.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise WorldBankSourceError(f"response {name} must be text when present")
    return value


def _require_date(value: Any) -> date:
    if not isinstance(value, date):
        raise WorldBankSourceError("today must be a date")
    return value


def _require_indicator(value: Any) -> str:
    if not isinstance(value, str) or not _INDICATOR_RE.fullmatch(value):
        raise WorldBankSourceError("indicator id is invalid")
    return value


def _require_year(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1800 <= value <= 9999:
        raise WorldBankSourceError("year is invalid")
    return value


def _require_page(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise WorldBankSourceError("page must be a positive integer")
    return value
