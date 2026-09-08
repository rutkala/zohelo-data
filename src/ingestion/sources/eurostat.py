"""Bounded Eurostat discovery and JSON-stat starter-slice planning.

The shared scheduler owns HTTP retries, quotas, exact-response retention, and durable
state.  This module only describes safe GET requests and validates their payloads.
"""
from __future__ import annotations

from datetime import date
import itertools
import json
import math
import re
from typing import Any, Mapping


SOURCE_ID = "eurostat"
ALLOWED_HOSTS = ("ec.europa.eu",)

STATISTICS_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}"
)
CATALOGUE_TOC_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt"
)

# The first admitted scope is deliberately easy to audit for Eurostat's commercial-
# reuse geography condition.  EFTA and candidate countries can be added after their
# maintained membership list is introduced; the notice permits them but a stale list
# would be unsafe.  Aggregates and non-country codes are not silently admitted.
ALLOWED_GEOS = (
    "PL", "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "EL", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PT",
    "RO", "SK", "SI", "ES", "SE",
)

STARTER_DATASETS: dict[str, dict[str, Any]] = {
    "demo_pjan": {
        "title": "Population on 1 January: total population",
        "frequency": "annual",
        "history_start_year": 1960,
        "window_years": 10,
        "filters": {"sex": "T", "age": "TOTAL"},
    },
    "nama_10_gdp": {
        "title": "GDP at current market prices, million euro",
        "frequency": "annual",
        "history_start_year": 1975,
        "window_years": 10,
        "filters": {"unit": "CP_MEUR", "na_item": "B1GQ"},
    },
    "prc_hicp_minr": {
        "title": "HICP food and non-alcoholic beverages, 2025=100",
        "frequency": "monthly",
        "history_start_year": 1996,
        "window_years": 5,
        "filters": {"unit": "I25", "coicop18": "CP01"},
    },
}

_TASK_ID_RE = re.compile(r"^[a-z0-9:_-]{1,200}$")


class EurostatSourceError(ValueError):
    """A task or response is outside the admitted Eurostat contract."""


def _period(year: int, frequency: str, *, end: bool) -> str:
    if frequency == "annual":
        return str(year)
    if frequency == "monthly":
        return f"{year}-{'12' if end else '01'}"
    raise EurostatSourceError("unsupported starter frequency")


def _data_task(
    dataset: str,
    geo: str,
    lane: str,
    start_year: int,
    end_year: int,
    *,
    asof: str | None = None,
) -> dict[str, Any]:
    spec = _starter_spec(dataset, geo)
    if lane not in ("recent", "history", "reconcile"):
        raise EurostatSourceError("Eurostat data task has an unsupported lane")
    if not isinstance(start_year, int) or not isinstance(end_year, int):
        raise EurostatSourceError("Eurostat task years must be integers")
    if start_year < spec["history_start_year"] or end_year < start_year:
        raise EurostatSourceError("Eurostat task has an invalid time window")
    maximum_years = 3 if lane == "recent" else spec["window_years"]
    if end_year - start_year + 1 > maximum_years:
        raise EurostatSourceError("Eurostat task exceeds its bounded time window")
    start = _period(start_year, spec["frequency"], end=False)
    end = _period(end_year, spec["frequency"], end=True)
    if lane == "recent":
        try:
            if asof is None or date.fromisoformat(asof).isoformat() != asof:
                raise ValueError
        except (TypeError, ValueError):
            raise EurostatSourceError("Eurostat recent task requires a canonical asof date") from None
    elif asof is not None:
        raise EurostatSourceError("only recent Eurostat tasks have an asof date")
    task_id = f"eurostat:{lane}:{dataset}:{geo}:{start}:{end}"
    cursor = {
        "dataset": dataset,
        "geo": geo,
        "start_period": start,
        "end_period": end,
        "filters": dict(spec["filters"]),
    }
    if asof is not None:
        task_id += f":asof-{asof}"
        cursor["asof"] = asof
    task = {
        "id": task_id.lower(),
        "lane": lane,
        "kind": "jsonstat",
        "cursor": cursor,
    }
    if lane == "recent":
        task["recurrence_key"] = f"{dataset}:{geo}"
    return task


def initial_tasks(today: date) -> list[dict[str, Any]]:
    """Start catalogue metadata discovery and every admitted full-history campaign."""
    _require_date(today)
    tasks: list[dict[str, Any]] = [{
        "id": "eurostat:discovery:catalogue-toc:en",
        "lane": "discovery",
        "kind": "catalogue_toc",
        "cursor": {"language": "en"},
    }]
    for geo in ALLOWED_GEOS:
        for dataset, spec in STARTER_DATASETS.items():
            first_end = min(today.year, spec["history_start_year"] + spec["window_years"] - 1)
            tasks.append(_data_task(dataset, geo, "history", spec["history_start_year"], first_end))
    return tasks


def recent_tasks(today: date) -> list[dict[str, Any]]:
    """Return bounded recurring roots for recent data and revision refreshes."""
    _require_date(today)
    tasks = []
    for geo in ALLOWED_GEOS:
        for dataset, spec in STARTER_DATASETS.items():
            start_year = max(spec["history_start_year"], today.year - 2)
            tasks.append(_data_task(
                dataset, geo, "recent", start_year, today.year, asof=today.isoformat()
            ))
    return tasks


def refresh_task(task: Mapping[str, Any], today: date) -> dict[str, Any]:
    """Recreate one recurring recent root for a new scheduler date."""
    _require_date(today)
    _validate_task_shell(task)
    if task["lane"] != "recent" or task["kind"] != "jsonstat":
        raise EurostatSourceError("only recent Eurostat data roots can recur")
    cursor = _data_cursor(task)
    expected_key = f"{cursor['dataset']}:{cursor['geo']}"
    if task.get("recurrence_key") != expected_key:
        raise EurostatSourceError("Eurostat recent task has an invalid recurrence_key")
    spec = STARTER_DATASETS[cursor["dataset"]]
    return _data_task(
        cursor["dataset"],
        cursor["geo"],
        "recent",
        max(spec["history_start_year"], today.year - 2),
        today.year,
        asof=today.isoformat(),
    )


def request_for(task: Mapping[str, Any]) -> dict[str, Any]:
    """Build one allowlisted GET request without credentials or arbitrary URLs."""
    _validate_task_shell(task)
    if task["kind"] == "catalogue_toc":
        if (
            task["id"] != "eurostat:discovery:catalogue-toc:en"
            or task["lane"] != "discovery"
            or task["cursor"] != {"language": "en"}
        ):
            raise EurostatSourceError("invalid Eurostat catalogue task")
        return {"url": CATALOGUE_TOC_URL, "params": {"lang": "en"}}
    if task["kind"] != "jsonstat":
        raise EurostatSourceError("unsupported Eurostat task kind")
    cursor = _data_cursor(task)
    params = {
        "lang": "en",
        "format": "JSON",
        "geo": cursor["geo"],
        "sinceTimePeriod": cursor["start_period"],
        "untilTimePeriod": cursor["end_period"],
        **cursor["filters"],
    }
    return {
        "url": STATISTICS_URL.format(dataset=cursor["dataset"]),
        "params": params,
    }


def interpret(
    task: Mapping[str, Any], body: bytes, today: date
) -> dict[str, Any]:
    """Validate one retained response and schedule the next bounded history window."""
    _require_date(today)
    _validate_task_shell(task)
    if not isinstance(body, bytes):
        raise EurostatSourceError("Eurostat response body must be exact bytes")
    if task["kind"] == "catalogue_toc":
        return _interpret_catalogue(task, body)
    if task["kind"] != "jsonstat":
        raise EurostatSourceError("unsupported Eurostat response kind")
    cursor = _data_cursor(task)
    cube = _parse_jsonstat(body)
    _validate_requested_slice(cursor, cube)
    decoded = decode_jsonstat(body)
    next_tasks: list[dict[str, Any]] = []
    if task["lane"] == "history":
        spec = STARTER_DATASETS[cursor["dataset"]]
        end_year = int(cursor["end_period"][:4])
        if end_year < today.year:
            next_start = end_year + 1
            next_end = min(today.year, next_start + spec["window_years"] - 1)
            next_tasks.append(_data_task(
                cursor["dataset"], cursor["geo"], "history", next_start, next_end
            ))
    dimensions = cube["dimension"]
    unit_codes = _category_codes(dimensions["unit"], "unit") if "unit" in dimensions else []
    return {
        "record_count": sum(row["value"] is not None for row in decoded),
        "next_tasks": next_tasks,
        "metadata": {
            "dataset": cursor["dataset"],
            "geo": cursor["geo"],
            "dimensions": list(cube["id"]),
            "dimension_sizes": dict(zip(cube["id"], cube["size"])),
            "cell_count": len(decoded),
            "missing_count": sum(row["value"] is None for row in decoded),
            "status_count": sum(row["status"] is not None for row in decoded),
            "unit_codes": unit_codes,
            "updated": cube.get("updated"),
            "source": cube.get("source"),
        },
    }


def decode_jsonstat(body: bytes) -> list[dict[str, Any]]:
    """Decode every cube position, including missing cells, to its complete native key."""
    cube = _parse_jsonstat(body)
    ids = cube["id"]
    dimensions = cube["dimension"]
    code_lists = [_category_codes(dimensions[name], name) for name in ids]
    total = math.prod(cube["size"])
    values = _position_map(cube["value"], total, "value")
    statuses = _position_map(cube.get("status", {}), total, "status")
    rows = []
    for position, coordinates in enumerate(itertools.product(*code_lists)):
        rows.append({
            "dimensions": dict(zip(ids, coordinates)),
            "value": values.get(position),
            "status": statuses.get(position),
            "is_missing": position not in values or values[position] is None,
        })
    return rows


def _parse_jsonstat(body: bytes) -> dict[str, Any]:
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EurostatSourceError("Eurostat response is not UTF-8 JSON") from exc
    if not isinstance(document, dict):
        raise EurostatSourceError("Eurostat JSON-stat response must be an object")
    if "error" in document or "warning" in document:
        raise EurostatSourceError("Eurostat returned an error or asynchronous warning")
    if document.get("version") != "2.0" or document.get("class") != "dataset":
        raise EurostatSourceError("Eurostat response is not a JSON-stat 2.0 dataset")
    ids, sizes, dimensions = document.get("id"), document.get("size"), document.get("dimension")
    if (
        not isinstance(ids, list)
        or not ids
        or len(set(ids)) != len(ids)
        or any(not isinstance(item, str) or not item for item in ids)
        or not isinstance(sizes, list)
        or len(sizes) != len(ids)
        or any(not isinstance(size, int) or isinstance(size, bool) or size < 1 for size in sizes)
        or not isinstance(dimensions, dict)
        or set(dimensions) != set(ids)
    ):
        raise EurostatSourceError("Eurostat JSON-stat dimensions are inconsistent")
    for name, size in zip(ids, sizes):
        if len(_category_codes(dimensions[name], name)) != size:
            raise EurostatSourceError(f"Eurostat dimension {name} size is inconsistent")
    if "value" not in document:
        raise EurostatSourceError("Eurostat JSON-stat response has no value collection")
    total = math.prod(sizes)
    _position_map(document["value"], total, "value")
    _position_map(document.get("status", {}), total, "status")
    return document


def _category_codes(dimension: Any, name: str) -> list[str]:
    if not isinstance(dimension, dict) or not isinstance(dimension.get("category"), dict):
        raise EurostatSourceError(f"Eurostat dimension {name} has no category")
    index = dimension["category"].get("index")
    if isinstance(index, list):
        if any(not isinstance(code, str) or not code for code in index) or len(set(index)) != len(index):
            raise EurostatSourceError(f"Eurostat dimension {name} has invalid category codes")
        return list(index)
    if isinstance(index, dict):
        if (
            any(not isinstance(code, str) or not code for code in index)
            or any(not isinstance(pos, int) or isinstance(pos, bool) for pos in index.values())
            or set(index.values()) != set(range(len(index)))
        ):
            raise EurostatSourceError(f"Eurostat dimension {name} has invalid category positions")
        return [code for code, _ in sorted(index.items(), key=lambda item: item[1])]
    raise EurostatSourceError(f"Eurostat dimension {name} has no category index")


def _position_map(collection: Any, total: int, label: str) -> dict[int, Any]:
    if isinstance(collection, list):
        if len(collection) != total:
            raise EurostatSourceError(f"Eurostat {label} array length is inconsistent")
        return dict(enumerate(collection))
    if isinstance(collection, dict):
        result = {}
        for raw_position, item in collection.items():
            if not isinstance(raw_position, str) or not raw_position.isdecimal():
                raise EurostatSourceError(f"Eurostat {label} has a non-numeric position")
            position = int(raw_position)
            if position < 0 or position >= total or position in result:
                raise EurostatSourceError(f"Eurostat {label} position is outside the cube")
            result[position] = item
        return result
    raise EurostatSourceError(f"Eurostat {label} must be an array or sparse object")


def _interpret_catalogue(task: Mapping[str, Any], body: bytes) -> dict[str, Any]:
    if (
        task["id"] != "eurostat:discovery:catalogue-toc:en"
        or task["lane"] != "discovery"
        or task["cursor"] != {"language": "en"}
    ):
        raise EurostatSourceError("invalid Eurostat catalogue task")
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise EurostatSourceError("Eurostat catalogue is not UTF-8 text") from exc
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines or not any("dataset" in line.lower() or "data" in line.lower() for line in lines[:10]):
        raise EurostatSourceError("Eurostat catalogue has an unrecognized TOC header")
    return {
        "record_count": max(0, len(lines) - 1),
        "next_tasks": [],
        "metadata": {
            "catalogue": "toc",
            "language": "en",
            "activation": "metadata-only",
        },
    }


def _validate_requested_slice(cursor: Mapping[str, Any], cube: Mapping[str, Any]) -> None:
    dimensions = cube["dimension"]
    expected = {"geo": cursor["geo"], **cursor["filters"]}
    for name, code in expected.items():
        if name not in dimensions or _category_codes(dimensions[name], name) != [code]:
            raise EurostatSourceError(f"Eurostat response escaped requested {name} slice")
    if "time" not in dimensions:
        raise EurostatSourceError("Eurostat response has no time dimension")
    for period in _category_codes(dimensions["time"], "time"):
        if not cursor["start_period"] <= period <= cursor["end_period"]:
            raise EurostatSourceError("Eurostat response escaped requested time window")


def _starter_spec(dataset: str, geo: str) -> dict[str, Any]:
    if not isinstance(dataset, str) or dataset.upper().startswith("DS-"):
        raise EurostatSourceError("Comext/PRODCOM datasets require a separate rights-approved contract")
    if dataset not in STARTER_DATASETS:
        raise EurostatSourceError("Eurostat dataset is not in the admitted starter scope")
    if geo not in ALLOWED_GEOS:
        raise EurostatSourceError("Eurostat geography is not in the admitted EU-country scope")
    return STARTER_DATASETS[dataset]


def _data_cursor(task: Mapping[str, Any]) -> dict[str, Any]:
    cursor = task.get("cursor")
    required = {"dataset", "geo", "start_period", "end_period", "filters"}
    if task.get("lane") == "recent":
        required.add("asof")
    if not isinstance(cursor, dict) or set(cursor) != required:
        raise EurostatSourceError("Eurostat data cursor has an invalid shape")
    spec = _starter_spec(cursor.get("dataset"), cursor.get("geo"))
    if cursor.get("filters") != spec["filters"]:
        raise EurostatSourceError("Eurostat starter filters cannot be widened")
    if not all(isinstance(cursor.get(key), str) for key in ("start_period", "end_period")):
        raise EurostatSourceError("Eurostat time cursor must contain strings")
    pattern = r"^\d{4}$" if spec["frequency"] == "annual" else r"^\d{4}-(?:01|12)$"
    if not re.fullmatch(pattern, cursor["start_period"]) or not re.fullmatch(pattern, cursor["end_period"]):
        raise EurostatSourceError("Eurostat time cursor is not a canonical window")
    if spec["frequency"] == "monthly" and not (
        cursor["start_period"].endswith("-01") and cursor["end_period"].endswith("-12")
    ):
        raise EurostatSourceError("Eurostat monthly windows must use calendar-year boundaries")
    start_year, end_year = int(cursor["start_period"][:4]), int(cursor["end_period"][:4])
    canonical = _data_task(
        cursor["dataset"],
        cursor["geo"],
        task["lane"],
        start_year,
        end_year,
        asof=cursor.get("asof"),
    )
    if task["id"] != canonical["id"]:
        raise EurostatSourceError("Eurostat task id does not match its cursor")
    if task["lane"] == "recent":
        if task.get("recurrence_key") != canonical["recurrence_key"]:
            raise EurostatSourceError("Eurostat recent task has an invalid recurrence_key")
    elif "recurrence_key" in task:
        raise EurostatSourceError("only recent Eurostat tasks have recurrence keys")
    return cursor


def _validate_task_shell(task: Mapping[str, Any]) -> None:
    if not isinstance(task, Mapping):
        raise EurostatSourceError("Eurostat task must be an object")
    required = {"id", "lane", "kind", "cursor"}
    optional = {"recurrence_key", "failures", "retry_at"}
    if not required.issubset(task) or set(task) - (required | optional):
        raise EurostatSourceError("Eurostat task has an invalid shape")
    if not isinstance(task["id"], str) or not _TASK_ID_RE.fullmatch(task["id"]):
        raise EurostatSourceError("Eurostat task id is invalid")
    if task["lane"] not in ("recent", "history", "discovery", "reconcile"):
        raise EurostatSourceError("Eurostat task lane is invalid")
    if "failures" in task and (
        not isinstance(task["failures"], int)
        or isinstance(task["failures"], bool)
        or task["failures"] < 0
    ):
        raise EurostatSourceError("Eurostat task failures must be a nonnegative integer")
    if "retry_at" in task and (
        not isinstance(task["retry_at"], (int, float))
        or isinstance(task["retry_at"], bool)
        or not math.isfinite(task["retry_at"])
        or task["retry_at"] < 0
    ):
        raise EurostatSourceError("Eurostat task retry_at must be a finite timestamp")


def _require_date(value: date) -> None:
    if not isinstance(value, date):
        raise EurostatSourceError("Eurostat planning date must be a date")
