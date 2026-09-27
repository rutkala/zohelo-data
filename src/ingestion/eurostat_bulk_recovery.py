"""Pure Eurostat asynchronous and HTTP 413 recovery planning.

The caller owns transport and durable raw-object writes.  Every transition takes
the descriptor of the exact response envelope that triggered it and retains that
descriptor in the returned state.  Request URLs are constructed locally from
validated Eurostat identities; URLs supplied inside provider payloads are never
followed.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from hashlib import sha256
import json
import re
from typing import Any, Mapping
from urllib.parse import parse_qsl, quote, unquote_to_bytes, urlsplit, urlunsplit
import xml.etree.ElementTree as ET

from ingestion.full_source_adapters import parse_eurostat_async


EUROSTAT_HOST = "ec.europa.eu"
EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"
EUROSTAT_COMEXT_BASE = (
    "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1"
)
EUROSTAT_ASYNC_STATUS = (
    "https://ec.europa.eu/eurostat/api/dissemination/1.0/async/status/{request_id}"
)
EUROSTAT_ASYNC_DATA = (
    "https://ec.europa.eu/eurostat/api/dissemination/1.0/async/data/{request_id}"
)

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_ASYNC_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,127}$")
_DATA_QUERY_KEYS = {"format", "lang", "compressed"}
_ASYNC_STATUSES = {
    "SUBMITTED", "PROCESSING", "AVAILABLE", "EXPIRED", "UNKNOWN_REQUEST",
    "ERROR", "DATA_NOT_YET_AVAILABLE", "NO_RESULTS",
}


class EurostatBulkRecoveryError(ValueError):
    """Eurostat recovery metadata cannot prove a safe complete next step."""


def start_async(
    initial_distribution: Mapping[str, Any],
    envelope_body: bytes | str,
    raw_envelope: Mapping[str, Any],
) -> dict[str, Any]:
    """Runner-facing alias for :func:`start_async_recovery`."""
    return start_async_recovery(initial_distribution, envelope_body, raw_envelope)


def advance_async(
    plan: Mapping[str, Any],
    envelope_body: bytes | str,
    raw_envelope: Mapping[str, Any],
) -> dict[str, Any]:
    """Runner-facing alias for :func:`advance_async_recovery`."""
    return advance_async_recovery(plan, envelope_body, raw_envelope)


def start_async_recovery(
    initial_distribution: Mapping[str, Any],
    envelope_body: bytes | str,
    raw_envelope: Mapping[str, Any],
) -> dict[str, Any]:
    """Start recovery from an exact asynchronous response envelope."""
    initial = _initial_distribution(initial_distribution)
    parsed = _parse_async(envelope_body, initial)
    if parsed.get("request_id") is None:
        raise EurostatBulkRecoveryError(
            "initial Eurostat async envelope has no request id"
        )
    plan = {
        "format_version": 1,
        "kind": "eurostat_async_recovery",
        "initial_distribution": initial,
        "request_id": parsed["request_id"],
        "provider_status": parsed["provider_status"],
        "phase": "poll",
        "submission_count": 1,
        "raw_envelopes": [_raw_evidence(raw_envelope)],
        "next_request": {},
    }
    _set_async_next(plan)
    _validate_async_plan(plan)
    return plan


def advance_async_recovery(
    plan: Mapping[str, Any],
    envelope_body: bytes | str,
    raw_envelope: Mapping[str, Any],
) -> dict[str, Any]:
    """Apply one retained status or re-submission envelope to an async plan."""
    current = _validated_async_copy(plan)
    if current["phase"] == "empty_partition":
        raise EurostatBulkRecoveryError("empty async partition is already terminal")
    parsed = _parse_async(envelope_body, current["initial_distribution"])
    request_id = parsed.get("request_id") or current["request_id"]
    if current["phase"] == "resubmit":
        current["submission_count"] += 1
    elif request_id != current["request_id"]:
        raise EurostatBulkRecoveryError("Eurostat async request id changed while polling")
    current["request_id"] = request_id
    current["provider_status"] = parsed["provider_status"]
    current["raw_envelopes"].append(_raw_evidence(raw_envelope))
    _set_async_next(current)
    _validate_async_plan(current)
    return current


def start_413_recovery(
    initial_distribution: Mapping[str, Any], raw_failure: Mapping[str, Any]
) -> dict[str, Any]:
    """Return the authoritative structure requests needed after HTTP 413."""
    initial = _initial_distribution(initial_distribution)
    dataset_id = initial["dataset_id"]
    base = _base_for_dataset(dataset_id)
    plan = {
        "format_version": 1,
        "kind": "eurostat_413_recovery",
        "initial_distribution": initial,
        "phase": "need_dataflow",
        "structure_requests": {
            "dataflow": {
                "url": f"{base}/dataflow/ESTAT/{dataset_id}/1.0",
                "params": {"references": "none"},
            },
            "contentconstraint": {
                "url": f"{base}/contentconstraint/ESTAT/{dataset_id}/1.0",
                "params": {"references": "none"},
            },
        },
        "structure_ref": None,
        "dimensions": [],
        "domains": {},
        "time_periods": [],
        "partitions": {},
        "raw_evidence": [_raw_evidence(raw_failure)],
    }
    _validate_413_plan(plan, allow_incomplete=True)
    return plan


def apply_dataflow(
    plan: Mapping[str, Any], body: bytes | str, raw_dataflow: Mapping[str, Any]
) -> dict[str, Any]:
    """Read the dataflow's versioned DSD reference before planning key order."""
    candidate = _validated_413_copy(plan, allow_incomplete=True)
    if candidate["phase"] != "need_dataflow":
        raise EurostatBulkRecoveryError("Eurostat recovery is not waiting for a dataflow")
    reference = _parse_dataflow(body, candidate["initial_distribution"]["dataset_id"])
    base = _base_for_dataset(candidate["initial_distribution"]["dataset_id"])
    candidate["structure_ref"] = reference
    candidate["structure_requests"]["datastructure"] = {
        "url": (
            f"{base}/datastructure/{reference['agency_id']}/"
            f"{reference['id']}/{reference['version']}"
        ),
        "params": {"references": "children"},
    }
    candidate["phase"] = "need_datastructure"
    candidate["raw_evidence"].append(_raw_evidence(raw_dataflow))
    _validate_413_plan(candidate, allow_incomplete=True)
    return candidate


def apply_datastructure(
    plan: Mapping[str, Any], body: bytes | str, raw_datastructure: Mapping[str, Any]
) -> dict[str, Any]:
    """Read ordered DSD dimensions and construct any required codelist requests."""
    candidate = _validated_413_copy(plan, allow_incomplete=True)
    if candidate["phase"] != "need_datastructure":
        raise EurostatBulkRecoveryError(
            "Eurostat recovery is not waiting for a datastructure"
        )
    dimensions, embedded = _parse_datastructure(body, candidate["structure_ref"])
    base = _base_for_dataset(candidate["initial_distribution"]["dataset_id"])
    codelist_requests: dict[str, dict[str, Any]] = {}
    for dimension in dimensions:
        reference = dimension.get("codelist")
        if reference is None:
            continue
        key = _reference_key(reference)
        if key not in embedded:
            codelist_requests[key] = {
                "url": (
                    f"{base}/codelist/{reference['agency_id']}/"
                    f"{reference['id']}/{reference['version']}"
                ),
                "params": {"references": "none"},
            }
    candidate["dimensions"] = dimensions
    candidate["embedded_codelists"] = embedded
    candidate["structure_requests"]["codelists"] = codelist_requests
    candidate["phase"] = "need_constraint"
    candidate["raw_evidence"].append(_raw_evidence(raw_datastructure))
    _validate_413_plan(candidate, allow_incomplete=True)
    return candidate


def build_partitions(
    plan: Mapping[str, Any],
    constraint_body: bytes | str,
    codelist_bodies: Mapping[str, bytes | str],
    raw_constraint: Mapping[str, Any],
    raw_codelists: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build complete, disjoint time partitions from DSD and constraint domains."""
    candidate = _validated_413_copy(plan, allow_incomplete=True)
    if candidate["phase"] != "need_constraint":
        raise EurostatBulkRecoveryError("Eurostat recovery is not waiting for constraints")
    if not isinstance(codelist_bodies, Mapping):
        raise EurostatBulkRecoveryError("codelist bodies must be a mapping")
    supplied: dict[str, list[str]] = {}
    for body in codelist_bodies.values():
        supplied.update(_parse_codelists(body))
    available = dict(candidate.get("embedded_codelists", {}))
    available.update(supplied)
    constrained, time_periods = _parse_constraint(
        constraint_body, candidate["initial_distribution"]["dataset_id"]
    )
    dimension_ids = [item["id"] for item in candidate["dimensions"]]
    unknown = set(constrained) - set(dimension_ids) - {"TIME_PERIOD"}
    if unknown:
        raise EurostatBulkRecoveryError(
            "content constraint contains unknown dimensions: " + ", ".join(sorted(unknown))
        )
    domains: dict[str, list[str]] = {}
    for dimension in candidate["dimensions"]:
        dimension_id = dimension["id"]
        if dimension["role"] == "time":
            continue
        values = constrained.get(dimension_id)
        reference = dimension.get("codelist")
        allowed = None if reference is None else available.get(_reference_key(reference))
        if values is None:
            values = allowed
        elif allowed is not None and not set(values).issubset(allowed):
            raise EurostatBulkRecoveryError(
                f"constraint values for {dimension_id} are absent from its codelist"
            )
        if not values:
            raise EurostatBulkRecoveryError(
                f"no complete allowed-value domain for dimension {dimension_id}"
            )
        domains[dimension_id] = _unique_values(values, dimension_id)
    if not time_periods:
        raise EurostatBulkRecoveryError("content constraint has no complete time domain")
    candidate["domains"] = domains
    candidate["time_periods"] = _unique_values(time_periods, "TIME_PERIOD")
    candidate["partitions"] = {}
    root_partition = _add_partition(candidate, {}, parent_id=None)
    candidate.pop("embedded_codelists", None)
    candidate["raw_evidence"].append(_raw_evidence(raw_constraint))
    if raw_codelists is not None:
        if not isinstance(raw_codelists, Mapping):
            raise EurostatBulkRecoveryError("raw codelist evidence must be a mapping")
        for key in sorted(raw_codelists):
            candidate["raw_evidence"].append(_raw_evidence(raw_codelists[key]))
    candidate["phase"] = "partitions_pending"
    _split_pending_partition(
        candidate,
        root_partition,
        candidate["raw_evidence"][0],
        append_evidence=False,
    )
    _refresh_partition_phase(candidate)
    _validate_413_plan(candidate, allow_incomplete=False)
    return candidate


def split_partition_on_413(
    plan: Mapping[str, Any],
    partition_id: str,
    raw_failure: Mapping[str, Any],
) -> dict[str, Any]:
    """Bisect one DSD-ordered constrained dimension after a repeated HTTP 413."""
    candidate = _validated_413_copy(plan, allow_incomplete=False)
    _split_pending_partition(candidate, partition_id, raw_failure, append_evidence=True)
    _refresh_partition_phase(candidate)
    _validate_413_plan(candidate, allow_incomplete=False)
    return candidate


def _split_pending_partition(
    candidate: dict[str, Any],
    partition_id: str,
    raw_failure: Mapping[str, Any],
    *,
    append_evidence: bool,
) -> None:
    parent = _pending_leaf(candidate, partition_id)
    split_dimension = None
    split_values: list[str] = []
    for dimension in candidate["dimensions"]:
        if dimension["role"] == "time":
            continue
        dimension_id = dimension["id"]
        values = parent["selections"].get(
            dimension_id, candidate["domains"][dimension_id]
        )
        if len(values) > 1:
            split_dimension = dimension_id
            split_values = values
            break
    if split_dimension is None:
        raise EurostatBulkRecoveryError(
            "413 partition cannot be split further within the complete constraint"
        )
    midpoint = len(split_values) // 2
    halves = (split_values[:midpoint], split_values[midpoint:])
    parent["status"] = "split"
    parent["raw_failure"] = _raw_evidence(raw_failure)
    for values in halves:
        selections = deepcopy(parent["selections"])
        selections[split_dimension] = list(values)
        _add_partition(candidate, selections, parent_id=partition_id)
    if append_evidence:
        candidate["raw_evidence"].append(_raw_evidence(raw_failure))


def accept_partition(
    plan: Mapping[str, Any], partition_id: str, receipt: Mapping[str, Any]
) -> dict[str, Any]:
    """Mark one leaf accepted; aggregate completion requires every leaf."""
    candidate = _validated_413_copy(plan, allow_incomplete=False)
    leaf = _pending_leaf(candidate, partition_id)
    leaf["status"] = "accepted"
    leaf["receipt"] = _raw_evidence(receipt)
    _refresh_partition_phase(candidate)
    _validate_413_plan(candidate, allow_incomplete=False)
    return candidate


def pending_partition_requests(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return deterministic pending leaf IDs and request descriptors."""
    candidate = _validated_413_copy(plan, allow_incomplete=False)
    return [
        {"partition_id": partition_id, "request": deepcopy(partition["request"])}
        for partition_id, partition in candidate["partitions"].items()
        if partition["status"] == "pending"
    ]


def _set_async_next(plan: dict[str, Any]) -> None:
    status = plan["provider_status"]
    request_id = plan["request_id"]
    if status in {"SUBMITTED", "PROCESSING", "DATA_NOT_YET_AVAILABLE"}:
        plan["phase"] = "poll"
        plan["next_request"] = {
            "url": EUROSTAT_ASYNC_STATUS.format(request_id=request_id), "params": {}
        }
    elif status == "AVAILABLE":
        plan["phase"] = "download"
        plan["next_request"] = {
            "url": EUROSTAT_ASYNC_DATA.format(request_id=request_id), "params": {}
        }
    elif status in {"EXPIRED", "UNKNOWN_REQUEST", "ERROR"}:
        plan["phase"] = "resubmit"
        initial = plan["initial_distribution"]
        plan["next_request"] = {
            "url": initial["url"], "params": deepcopy(initial.get("params", {}))
        }
    elif status == "NO_RESULTS":
        initial = plan["initial_distribution"]
        if "partition_id" not in initial or "original_dataset_id" not in initial:
            raise EurostatBulkRecoveryError(
                "NO_RESULTS cannot prove a complete unpartitioned dataset"
            )
        plan["phase"] = "empty_partition"
        plan["next_request"] = {}
    else:  # pragma: no cover - parsed status is checked before this helper
        raise EurostatBulkRecoveryError(f"unsupported Eurostat async status {status!r}")


def _parse_async(body: bytes | str, initial: Mapping[str, Any]) -> dict[str, Any]:
    try:
        parsed = parse_eurostat_async(
            body, {"url": initial["url"], "params": deepcopy(initial.get("params", {}))}
        )
    except Exception as exc:
        raise EurostatBulkRecoveryError(str(exc)) from exc
    provider_status = parsed.get("provider_status")
    if parsed.get("status") == "no_results":
        provider_status = "NO_RESULTS"
    if provider_status not in _ASYNC_STATUSES:
        raise EurostatBulkRecoveryError(
            f"unsupported Eurostat async status {provider_status!r}"
        )
    return {**parsed, "provider_status": provider_status}


def _validate_async_plan(plan: Mapping[str, Any]) -> None:
    required = {
        "format_version", "kind", "initial_distribution", "request_id",
        "provider_status", "phase", "submission_count", "raw_envelopes",
        "next_request",
    }
    if not isinstance(plan, Mapping) or set(plan) != required:
        raise EurostatBulkRecoveryError("async recovery plan fields are invalid")
    if plan["format_version"] != 1 or plan["kind"] != "eurostat_async_recovery":
        raise EurostatBulkRecoveryError("async recovery plan identity is invalid")
    initial = _initial_distribution(plan["initial_distribution"])
    request_id = plan["request_id"]
    if not isinstance(request_id, str) or not _ASYNC_ID_RE.fullmatch(request_id):
        raise EurostatBulkRecoveryError("async recovery request id is unsafe")
    if plan["provider_status"] not in _ASYNC_STATUSES:
        raise EurostatBulkRecoveryError("async recovery provider status is invalid")
    if type(plan["submission_count"]) is not int or plan["submission_count"] < 1:
        raise EurostatBulkRecoveryError("async submission count is invalid")
    if not isinstance(plan["raw_envelopes"], list) or not plan["raw_envelopes"]:
        raise EurostatBulkRecoveryError("async raw envelope history is missing")
    for item in plan["raw_envelopes"]:
        _raw_evidence(item)
    expected = dict(plan)
    expected["initial_distribution"] = initial
    _set_async_next(expected)
    if plan["phase"] != expected["phase"] or plan["next_request"] != expected["next_request"]:
        raise EurostatBulkRecoveryError("async recovery next request is not authoritative")


def _validated_async_copy(plan: Mapping[str, Any]) -> dict[str, Any]:
    _validate_async_plan(plan)
    return deepcopy(dict(plan))


def _initial_distribution(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise EurostatBulkRecoveryError("initial Eurostat distribution must be a mapping")
    result = deepcopy(dict(value))
    dataset_id = result.get("dataset_id")
    original_dataset_id = result.get("original_dataset_id")
    partition_id = result.get("partition_id")
    if original_dataset_id is None and partition_id is None:
        if not isinstance(dataset_id, str) or not _SAFE_ID_RE.fullmatch(dataset_id):
            raise EurostatBulkRecoveryError("initial Eurostat dataset id is unsafe")
        request_dataset_id = dataset_id
    else:
        if (
            not isinstance(original_dataset_id, str)
            or not _SAFE_ID_RE.fullmatch(original_dataset_id)
            or not isinstance(partition_id, str)
            or not re.fullmatch(r"partition:[0-9a-f]{64}", partition_id)
            or dataset_id
            != f"{original_dataset_id}::partition::{partition_id.removeprefix('partition:')}"
        ):
            raise EurostatBulkRecoveryError("initial Eurostat partition identity is unsafe")
        request_dataset_id = original_dataset_id
    version = result.get("version")
    if version is not None and (
        not isinstance(version, str) or not version or len(version) > 160
    ):
        raise EurostatBulkRecoveryError("initial Eurostat dataset version is invalid")
    params = result.get("params", {})
    if not isinstance(params, dict) or any(
        not isinstance(key, str) or not isinstance(item, str)
        for key, item in params.items()
    ):
        raise EurostatBulkRecoveryError("initial Eurostat request parameters are invalid")
    _validate_data_url(result.get("url"), request_dataset_id)
    return result


def _validate_data_url(value: Any, dataset_id: str) -> None:
    if not isinstance(value, str):
        raise EurostatBulkRecoveryError("initial Eurostat data URL is invalid")
    parsed = urlsplit(value)
    base = _base_for_dataset(dataset_id)
    expected_path = urlsplit(f"{base}/data/{dataset_id}").path
    if (
        parsed.scheme != "https" or parsed.hostname != EUROSTAT_HOST
        or parsed.port is not None or parsed.username is not None
        or parsed.password is not None or parsed.fragment
    ):
        raise EurostatBulkRecoveryError("initial Eurostat data URL is not authoritative")
    if parsed.path == expected_path:
        pass
    elif parsed.path.startswith(expected_path + "/"):
        key = parsed.path[len(expected_path) + 1 :]
        _validate_encoded_key(key)
    else:
        raise EurostatBulkRecoveryError("initial Eurostat data URL is not authoritative")
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if len(query) != len({key.lower() for key, _ in query}):
        raise EurostatBulkRecoveryError("initial Eurostat data URL repeats a parameter")
    if any(key.lower() not in _DATA_QUERY_KEYS for key, _ in query):
        raise EurostatBulkRecoveryError("initial Eurostat data URL has unsafe parameters")


def _validate_encoded_key(key: str) -> None:
    """Validate one canonical SDMX key without guessing its DSD dimension count."""
    if not key or "/" in key or not re.fullmatch(r"[A-Za-z0-9_~%+.-]+", key):
        raise EurostatBulkRecoveryError("Eurostat partition URL has an unsafe SDMX key")
    populated = False
    for dimension in key.split("."):
        if not dimension:  # an empty position is the SDMX wildcard for that dimension
            continue
        values = dimension.split("+")
        if any(not value for value in values):
            raise EurostatBulkRecoveryError("Eurostat partition URL has an invalid SDMX key")
        for encoded in values:
            try:
                decoded = unquote_to_bytes(encoded).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise EurostatBulkRecoveryError(
                    "Eurostat partition URL has a non-UTF-8 SDMX key"
                ) from exc
            if _encode(decoded) != encoded:
                raise EurostatBulkRecoveryError(
                    "Eurostat partition URL has a non-canonical SDMX key"
                )
            populated = True
    if not populated:
        raise EurostatBulkRecoveryError("Eurostat partition URL has an empty SDMX key")


def _base_for_dataset(dataset_id: str) -> str:
    return EUROSTAT_COMEXT_BASE if dataset_id.upper().startswith("DS-") else EUROSTAT_BASE


def _parse_dataflow(body: bytes | str, dataset_id: str) -> dict[str, str]:
    root = _xml(body, "Eurostat dataflow")
    flows = [item for item in root.iter() if _local(item.tag) == "Dataflow"]
    matching = [
        item for item in flows
        if (_attr(item, "id") or "").upper() == dataset_id.upper()
    ]
    if len(matching) != 1:
        raise EurostatBulkRecoveryError("dataflow does not uniquely match the dataset")
    references = []
    for structure in matching[0].iter():
        if _local(structure.tag) != "Structure":
            continue
        for item in structure.iter():
            if (
                _local(item.tag) == "Ref"
                and (_attr(item, "class") or "").lower() == "datastructure"
            ):
                references.append(item)
    if len(references) != 1:
        raise EurostatBulkRecoveryError("dataflow has no single versioned DSD reference")
    return _reference(references[0], "dataflow DSD")


def _parse_datastructure(
    body: bytes | str, reference: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    root = _xml(body, "Eurostat datastructure")
    structures = [item for item in root.iter() if _local(item.tag) == "DataStructure"]
    matching = [
        item for item in structures
        if _attr(item, "id") == reference.get("id")
        and _attr(item, "agencyID") == reference.get("agency_id")
        and _attr(item, "version") == reference.get("version")
    ]
    if len(matching) != 1:
        raise EurostatBulkRecoveryError("datastructure does not match dataflow reference")
    dimensions: list[dict[str, Any]] = []
    for item in matching[0].iter():
        kind = _local(item.tag)
        if kind not in {"Dimension", "TimeDimension"}:
            continue
        dimension_id = _attr(item, "id")
        position_text = _attr(item, "position")
        if not dimension_id or not _SAFE_ID_RE.fullmatch(dimension_id):
            raise EurostatBulkRecoveryError("datastructure dimension id is unsafe")
        try:
            position = int(position_text or "")
        except ValueError as exc:
            raise EurostatBulkRecoveryError("datastructure dimension position is invalid") from exc
        enumeration_refs = [
            child for child in item.iter()
            if (
                _local(child.tag) == "Ref"
                and (_attr(child, "class") or "").lower() == "codelist"
            )
        ]
        if len(enumeration_refs) > 1:
            raise EurostatBulkRecoveryError("dimension has ambiguous codelist references")
        dimensions.append(
            {
                "id": dimension_id,
                "position": position,
                "role": "time" if kind == "TimeDimension" else "series",
                "codelist": (
                    _reference(enumeration_refs[0], f"{dimension_id} codelist")
                    if enumeration_refs else None
                ),
            }
        )
    dimensions.sort(key=lambda item: item["position"])
    if not dimensions or [item["position"] for item in dimensions] != list(
        range(1, len(dimensions) + 1)
    ):
        raise EurostatBulkRecoveryError("datastructure dimensions are not fully ordered")
    times = [item for item in dimensions if item["role"] == "time"]
    if len(times) != 1 or times[0]["id"].upper() != "TIME_PERIOD":
        raise EurostatBulkRecoveryError("datastructure has no single TIME_PERIOD dimension")
    return dimensions, _parse_codelists(body)


def _parse_constraint(
    body: bytes | str, dataset_id: str
) -> tuple[dict[str, list[str]], list[str]]:
    root = _xml(body, "Eurostat content constraint")
    constraints = [item for item in root.iter() if _local(item.tag) == "ContentConstraint"]
    if len(constraints) != 1:
        raise EurostatBulkRecoveryError("content constraint is missing or ambiguous")
    flow_refs = [
        item for item in constraints[0].iter()
        if (
            _local(item.tag) == "Ref"
            and (_attr(item, "class") or "").lower() == "dataflow"
        )
    ]
    if not any(
        (_attr(item, "id") or "").upper() == dataset_id.upper()
        for item in flow_refs
    ):
        raise EurostatBulkRecoveryError("content constraint does not identify the dataflow")
    regions = [item for item in constraints[0].iter() if _local(item.tag) == "CubeRegion"]
    if len(regions) != 1 or (_attr(regions[0], "include") or "true").lower() != "true":
        raise EurostatBulkRecoveryError("only one inclusive cube constraint is supported")
    domains: dict[str, list[str]] = {}
    time_periods: list[str] = []
    for item in list(regions[0]):
        kind = _local(item.tag)
        if kind not in {"KeyValue", "TimeKeyValue"}:
            continue
        dimension_id = _attr(item, "id")
        if not dimension_id or not _SAFE_ID_RE.fullmatch(dimension_id):
            raise EurostatBulkRecoveryError("constraint dimension id is unsafe")
        values = [
            _text(child, f"constraint {dimension_id} value")
            for child in item.iter() if _local(child.tag) == "Value"
        ]
        ranges = [child for child in item.iter() if _local(child.tag) == "TimeRange"]
        if ranges:
            if kind != "TimeKeyValue" or len(ranges) != 1:
                raise EurostatBulkRecoveryError("constraint time range is ambiguous")
            start = _first_named_text(ranges[0], "StartPeriod")
            end = _first_named_text(ranges[0], "EndPeriod")
            values.extend(_expand_periods(start, end))
        values = _unique_values(values, dimension_id)
        if dimension_id in domains:
            raise EurostatBulkRecoveryError("constraint repeats a dimension")
        if dimension_id.upper() == "TIME_PERIOD" or kind == "TimeKeyValue":
            if time_periods:
                raise EurostatBulkRecoveryError("constraint repeats TIME_PERIOD")
            time_periods = values
        else:
            domains[dimension_id] = values
    return domains, time_periods


def _parse_codelists(body: bytes | str) -> dict[str, list[str]]:
    root = _xml(body, "Eurostat codelist")
    result: dict[str, list[str]] = {}
    for item in root.iter():
        if _local(item.tag) != "Codelist":
            continue
        reference = {
            "agency_id": _attr(item, "agencyID"),
            "id": _attr(item, "id"),
            "version": _attr(item, "version"),
        }
        if not all(
            isinstance(value, str) and _SAFE_ID_RE.fullmatch(value)
            for value in reference.values()
        ):
            raise EurostatBulkRecoveryError("codelist identity is unsafe")
        codes = [
            _attr(child, "id") for child in item.iter() if _local(child.tag) == "Code"
        ]
        result[_reference_key(reference)] = _unique_values(codes, reference["id"])
    return result


def _add_partition(
    plan: dict[str, Any], selections: dict[str, list[str]], parent_id: str | None
) -> str:
    partition_id = _partition_identifier(plan, selections)
    if partition_id in plan["partitions"]:
        raise EurostatBulkRecoveryError("partition plan contains duplicate coverage")
    plan["partitions"][partition_id] = {
        "parent_id": parent_id,
        "status": "pending",
        "selections": deepcopy(selections),
        "request": _partition_request(plan, selections),
    }
    return partition_id


def _partition_request(
    plan: Mapping[str, Any], selections: Mapping[str, list[str]]
) -> dict[str, Any]:
    initial = plan["initial_distribution"]
    series = [item for item in plan["dimensions"] if item["role"] == "series"]
    key_parts: list[str] = []
    for dimension in series:
        dimension_id = dimension["id"]
        values = selections.get(dimension_id)
        key_parts.append("" if values is None else "+".join(_encode(value) for value in values))
    key = ".".join(key_parts)
    if any(key_parts):
        parsed = urlsplit(initial["url"])
        url = urlunsplit(parsed._replace(path=parsed.path.rstrip("/") + "/" + key))
    else:
        url = initial["url"]
    params = deepcopy(initial.get("params", {}))
    return {"url": url, "params": params}


def _pending_leaf(plan: dict[str, Any], partition_id: str) -> dict[str, Any]:
    if not isinstance(partition_id, str):
        raise EurostatBulkRecoveryError("partition id is invalid")
    partition = plan["partitions"].get(partition_id)
    if not isinstance(partition, dict) or partition.get("status") != "pending":
        raise EurostatBulkRecoveryError("partition is not a pending leaf")
    return partition


def _refresh_partition_phase(plan: dict[str, Any]) -> None:
    leaves = [item for item in plan["partitions"].values() if item["status"] != "split"]
    if leaves and all(item["status"] == "accepted" for item in leaves):
        plan["phase"] = "complete"
    else:
        plan["phase"] = "partitions_pending"


def _partition_identifier(
    plan: Mapping[str, Any], selections: Mapping[str, list[str]]
) -> str:
    identity = {
        "dataset_id": plan["initial_distribution"]["dataset_id"],
        "version": plan["initial_distribution"].get("version"),
        "selections": selections,
    }
    return "partition:" + sha256(_canonical(identity)).hexdigest()


def _validate_413_plan(plan: Mapping[str, Any], *, allow_incomplete: bool) -> None:
    if not isinstance(plan, Mapping) or plan.get("format_version") != 1:
        raise EurostatBulkRecoveryError("413 recovery plan identity is invalid")
    if plan.get("kind") != "eurostat_413_recovery":
        raise EurostatBulkRecoveryError("413 recovery plan kind is invalid")
    initial = _initial_distribution(plan.get("initial_distribution"))
    expected = start_413_recovery_urls(initial)
    requests = plan.get("structure_requests")
    if not isinstance(requests, dict):
        raise EurostatBulkRecoveryError("413 structure requests are missing")
    if requests.get("dataflow") != expected["dataflow"] or requests.get(
        "contentconstraint"
    ) != expected["contentconstraint"]:
        raise EurostatBulkRecoveryError("413 structure request URL is not authoritative")
    reference = plan.get("structure_ref")
    if reference is not None:
        reference = _validated_reference(reference, "dataflow DSD")
        base = _base_for_dataset(initial["dataset_id"])
        expected_dsd = {
            "url": (
                f"{base}/datastructure/{reference['agency_id']}/"
                f"{reference['id']}/{reference['version']}"
            ),
            "params": {"references": "children"},
        }
        if requests.get("datastructure") != expected_dsd:
            raise EurostatBulkRecoveryError(
                "413 datastructure request URL is not authoritative"
            )
    dimensions = plan.get("dimensions", [])
    if not isinstance(dimensions, list):
        raise EurostatBulkRecoveryError("413 DSD dimensions are invalid")
    known_references: dict[str, dict[str, str]] = {}
    seen_dimensions: set[str] = set()
    positions: list[int] = []
    for item in dimensions:
        if (
            not isinstance(item, dict)
            or set(item) != {"id", "position", "role", "codelist"}
            or not isinstance(item["id"], str)
            or not _SAFE_ID_RE.fullmatch(item["id"])
            or item["id"] in seen_dimensions
            or type(item["position"]) is not int
            or item["position"] < 1
            or item["role"] not in {"series", "time"}
        ):
            raise EurostatBulkRecoveryError("413 DSD dimension metadata is invalid")
        seen_dimensions.add(item["id"])
        positions.append(item["position"])
        if item["codelist"] is not None:
            codelist = _validated_reference(item["codelist"], "DSD codelist")
            known_references[_reference_key(codelist)] = codelist
    if dimensions and positions != list(range(1, len(dimensions) + 1)):
        raise EurostatBulkRecoveryError("413 DSD dimension ordering is invalid")
    codelist_requests = requests.get("codelists", {})
    if not isinstance(codelist_requests, dict):
        raise EurostatBulkRecoveryError("413 codelist requests are invalid")
    for key, request in codelist_requests.items():
        codelist = known_references.get(key)
        if codelist is None:
            raise EurostatBulkRecoveryError("413 codelist request is not referenced by the DSD")
        base = _base_for_dataset(initial["dataset_id"])
        expected_request = {
            "url": (
                f"{base}/codelist/{codelist['agency_id']}/"
                f"{codelist['id']}/{codelist['version']}"
            ),
            "params": {"references": "none"},
        }
        if request != expected_request:
            raise EurostatBulkRecoveryError(
                "413 codelist request URL is not authoritative"
            )
    if not isinstance(plan.get("raw_evidence"), list) or not plan["raw_evidence"]:
        raise EurostatBulkRecoveryError("413 raw evidence is missing")
    for item in plan["raw_evidence"]:
        _raw_evidence(item)
    if allow_incomplete:
        return
    domains = plan.get("domains")
    periods = plan.get("time_periods")
    partitions = plan.get("partitions")
    if not dimensions or not isinstance(domains, dict):
        raise EurostatBulkRecoveryError("413 partition structure is missing")
    if not isinstance(periods, list) or not periods or not isinstance(partitions, dict):
        raise EurostatBulkRecoveryError("413 partition coverage is missing")
    series_ids = {item["id"] for item in dimensions if item["role"] == "series"}
    if set(domains) != series_ids:
        raise EurostatBulkRecoveryError("413 partition domains do not match the DSD")
    for dimension_id, values in domains.items():
        _unique_values(values, dimension_id)
    _unique_values(periods, "TIME_PERIOD")
    _validate_partition_tree(plan)
    for partition_id, partition in partitions.items():
        if not isinstance(partition_id, str) or not isinstance(partition, dict):
            raise EurostatBulkRecoveryError("413 partition entry is invalid")
        if partition.get("status") not in {"pending", "accepted", "split"}:
            raise EurostatBulkRecoveryError("413 partition status is invalid")
        expected_request = _partition_request(plan, partition.get("selections", {}))
        if partition.get("request") != expected_request:
            raise EurostatBulkRecoveryError("partition request URL is not authoritative")
    phase = plan.get("phase")
    leaves = [item for item in partitions.values() if item.get("status") != "split"]
    complete = bool(leaves) and all(item.get("status") == "accepted" for item in leaves)
    if phase != ("complete" if complete else "partitions_pending"):
        raise EurostatBulkRecoveryError("413 aggregate completion is inconsistent")


def _validate_partition_tree(plan: Mapping[str, Any]) -> None:
    partitions = plan["partitions"]
    domains = plan["domains"]
    series_ids = [
        item["id"] for item in plan["dimensions"] if item["role"] == "series"
    ]
    children: dict[str, list[str]] = {partition_id: [] for partition_id in partitions}
    roots: list[str] = []
    for partition_id, partition in partitions.items():
        selections = partition.get("selections")
        if not isinstance(selections, dict) or set(selections) - set(series_ids):
            raise EurostatBulkRecoveryError("partition selections do not match the DSD")
        if partition_id != _partition_identifier(plan, selections):
            raise EurostatBulkRecoveryError("partition identity does not match its coverage")
        for dimension_id in selections:
            values = _unique_values(selections[dimension_id], dimension_id)
            if not set(values).issubset(domains[dimension_id]):
                raise EurostatBulkRecoveryError("partition selects values outside its domain")
        parent_id = partition.get("parent_id")
        if parent_id is None:
            if selections:
                raise EurostatBulkRecoveryError("root partition has narrowed coverage")
            roots.append(partition_id)
        elif parent_id not in partitions or parent_id == partition_id:
            raise EurostatBulkRecoveryError("partition parent is missing or cyclic")
        else:
            children[parent_id].append(partition_id)
        status = partition.get("status")
        if status == "accepted":
            _raw_evidence(partition.get("receipt"))
        elif status == "split":
            _raw_evidence(partition.get("raw_failure"))
        elif status != "pending":
            raise EurostatBulkRecoveryError("partition status is invalid")
    if len(roots) != 1:
        raise EurostatBulkRecoveryError("partition tree must retain one full-history root")
    for partition_id, partition in partitions.items():
        child_ids = children[partition_id]
        if partition["status"] != "split":
            if child_ids:
                raise EurostatBulkRecoveryError("unsplit partition unexpectedly has children")
            continue
        if len(child_ids) != 2:
            raise EurostatBulkRecoveryError("split partition must retain exactly two children")
        parent_selections = partition["selections"]
        child_selections = [partitions[item]["selections"] for item in child_ids]
        split_dimensions = []
        for dimension_id in series_ids:
            parent_values = parent_selections.get(dimension_id, domains[dimension_id])
            child_values = [
                child.get(dimension_id, domains[dimension_id])
                for child in child_selections
            ]
            if child_values[0] == parent_values and child_values[1] == parent_values:
                continue
            if (
                set(child_values[0]).isdisjoint(child_values[1])
                and child_values[0] + child_values[1] == parent_values
            ):
                split_dimensions.append(dimension_id)
                continue
            raise EurostatBulkRecoveryError("split children have a hole or overlap")
        if len(split_dimensions) != 1:
            raise EurostatBulkRecoveryError("split children do not bisect one dimension")
    for partition_id in partitions:
        visited: set[str] = set()
        current = partition_id
        while current is not None:
            if current in visited:
                raise EurostatBulkRecoveryError("partition ancestry is cyclic")
            visited.add(current)
            current = partitions[current].get("parent_id")


def start_413_recovery_urls(initial_distribution: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Construct the two initial official SDMX 2.1 structure requests."""
    initial = _initial_distribution(initial_distribution)
    dataset_id = initial["dataset_id"]
    base = _base_for_dataset(dataset_id)
    return {
        "dataflow": {
            "url": f"{base}/dataflow/ESTAT/{dataset_id}/1.0",
            "params": {"references": "none"},
        },
        "contentconstraint": {
            "url": f"{base}/contentconstraint/ESTAT/{dataset_id}/1.0",
            "params": {"references": "none"},
        },
    }


def _validated_413_copy(plan: Mapping[str, Any], *, allow_incomplete: bool) -> dict[str, Any]:
    _validate_413_plan(plan, allow_incomplete=allow_incomplete)
    return deepcopy(dict(plan))


def _reference(element: ET.Element, label: str) -> dict[str, str]:
    result = {
        "agency_id": _attr(element, "agencyID"),
        "id": _attr(element, "id"),
        "version": _attr(element, "version"),
    }
    return _validated_reference(result, label)


def _validated_reference(value: Mapping[str, Any], label: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"agency_id", "id", "version"}:
        raise EurostatBulkRecoveryError(f"{label} identity is unsafe or incomplete")
    result = dict(value)
    if not all(
        isinstance(value, str) and _SAFE_ID_RE.fullmatch(value)
        for value in result.values()
    ):
        raise EurostatBulkRecoveryError(f"{label} identity is unsafe or incomplete")
    return result


def _reference_key(reference: Mapping[str, Any]) -> str:
    return f"{reference['agency_id']}:{reference['id']}:{reference['version']}"


def _expand_periods(start: str, end: str) -> list[str]:
    if start == end:
        return [_safe_value(start, "time period")]
    annual = re.compile(r"^(\d{4})$")
    monthly = re.compile(r"^(\d{4})-(\d{2})$")
    quarterly = re.compile(r"^(\d{4})-Q([1-4])$")
    semester = re.compile(r"^(\d{4})-S([12])$")
    daily = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
    for pattern, frequency in (
        (annual, 1), (semester, 2), (quarterly, 4), (monthly, 12)
    ):
        left, right = pattern.fullmatch(start), pattern.fullmatch(end)
        if left and right:
            left_slot = int(left.group(1)) * frequency + (
                int(left.group(2)) - 1 if frequency > 1 else 0
            )
            right_slot = int(right.group(1)) * frequency + (
                int(right.group(2)) - 1 if frequency > 1 else 0
            )
            if left_slot > right_slot:
                raise EurostatBulkRecoveryError("constraint time range is reversed")
            values = []
            for slot in range(left_slot, right_slot + 1):
                year, offset = divmod(slot, frequency)
                if frequency == 1:
                    values.append(f"{year:04d}")
                elif frequency == 12:
                    values.append(f"{year:04d}-{offset + 1:02d}")
                else:
                    marker = "S" if frequency == 2 else "Q"
                    values.append(f"{year:04d}-{marker}{offset + 1}")
            return values
    left_day, right_day = daily.fullmatch(start), daily.fullmatch(end)
    if left_day and right_day:
        try:
            current = date.fromisoformat(start)
            finish = date.fromisoformat(end)
        except ValueError as exc:
            raise EurostatBulkRecoveryError("constraint daily range is invalid") from exc
        if current > finish:
            raise EurostatBulkRecoveryError("constraint time range is reversed")
        values = []
        while current <= finish:
            values.append(current.isoformat())
            current += timedelta(days=1)
        return values
    raise EurostatBulkRecoveryError("constraint uses an unsupported time period range")


def _unique_values(values: Any, label: str) -> list[str]:
    if not isinstance(values, list):
        raise EurostatBulkRecoveryError(f"{label} values are invalid")
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        safe = _safe_value(value, f"{label} value")
        if safe in seen:
            continue
        seen.add(safe)
        result.append(safe)
    if not result:
        raise EurostatBulkRecoveryError(f"{label} has no allowed values")
    return result


def _safe_value(value: Any, label: str) -> str:
    if (
        not isinstance(value, str) or not value or len(value) > 256
        or any(ord(character) < 32 for character in value)
    ):
        raise EurostatBulkRecoveryError(f"{label} is unsafe")
    return value


def _encode(value: str) -> str:
    return quote(_safe_value(value, "SDMX key"), safe="").replace(".", "%2E")


def _raw_evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise EurostatBulkRecoveryError("raw recovery evidence must be a non-empty mapping")
    result = deepcopy(dict(value))
    try:
        json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise EurostatBulkRecoveryError("raw recovery evidence is not JSON-safe") from exc
    return result


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _xml(body: bytes | str, label: str) -> ET.Element:
    if isinstance(body, bytes):
        try:
            body = body.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise EurostatBulkRecoveryError(f"{label} is not UTF-8") from exc
    if not isinstance(body, str):
        raise EurostatBulkRecoveryError(f"{label} must be bytes or text")
    try:
        return ET.fromstring(body)
    except ET.ParseError as exc:
        raise EurostatBulkRecoveryError(f"{label} is not valid XML") from exc


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def _attr(element: ET.Element, name: str) -> str | None:
    for key, value in element.attrib.items():
        if _local(key).lower() == name.lower():
            return value
    return None


def _text(element: ET.Element, label: str) -> str:
    text = "".join(element.itertext()).strip()
    return _safe_value(text, label)


def _first_named_text(element: ET.Element, name: str) -> str:
    matches = [_text(item, name) for item in element.iter() if _local(item.tag) == name]
    if len(matches) != 1:
        raise EurostatBulkRecoveryError(f"constraint has no single {name}")
    return matches[0]


__all__ = [
    "EurostatBulkRecoveryError",
    "accept_partition",
    "advance_async",
    "advance_async_recovery",
    "apply_dataflow",
    "apply_datastructure",
    "build_partitions",
    "pending_partition_requests",
    "split_partition_on_413",
    "start_413_recovery",
    "start_413_recovery_urls",
    "start_async",
    "start_async_recovery",
]
