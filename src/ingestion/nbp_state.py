"""Durable, backend-agnostic state for bounded NBP dated-response ingestion.

This module deliberately has no HTTP or Google client dependency.  The runner supplies
exact response bytes and a store with the small Drive-shaped protocol below.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping, Protocol
from uuid import uuid4

MAX_CHUNK_DAYS = 93
DEFAULT_MAX_RESPONSE_BYTES = 12_000_000
MAX_STATE_BYTES = 8_000_000
MAX_RESPONSES = 25_000
NBP_SOURCE_IDS = frozenset({"nbp_exchange_rates_table_a", "nbp_exchange_rates_table_b", "nbp_exchange_rates_table_c", "nbp_gold_prices"})
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,255}$")
_SOURCE_RE = re.compile(r"^[a-z][a-z0-9_]{1,120}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class NBPStateError(RuntimeError):
    """A request, state object, or durable store result is unsafe."""


class UncertainStatePointerError(NBPStateError):
    """Drive did not prove whether the mutable state pointer was updated."""


class NBPStateStore(Protocol):
    def find(self, name: str, parent_id: str) -> list[str]: ...
    def create(self, name: str, data: bytes, parent_id: str) -> str: ...
    def read(self, file_id: str) -> bytes: ...
    def replace(self, file_id: str, data: bytes) -> None: ...
    def mkdir(self, name: str, parent_id: str) -> str: ...


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    historical_start_date: date
    endpoint_template: str
    params: dict[str, str]
    max_chunk_days: int = MAX_CHUNK_DAYS


@dataclass(frozen=True)
class RequestPlan:
    source_id: str
    requested_start_date: date
    requested_end_date: date
    mode: str
    endpoint_template: str
    params: dict[str, str]

    @property
    def request_url(self) -> str:
        return self.endpoint_template.format(
            start_date=self.requested_start_date.isoformat(),
            end_date=self.requested_end_date.isoformat(),
        )


@dataclass(frozen=True)
class ValidationResult:
    outcome: str
    response_sha256: str | None
    body_json: str | None
    observation_min_date: date | None
    observation_max_date: date | None
    observation_count: int


@dataclass(frozen=True)
class LoadedState:
    state: dict[str, Any]
    pointer_file_id: str | None
    pointer_raw: bytes | None
    snapshot_file_id: str | None


@dataclass(frozen=True)
class CommitResult:
    state: dict[str, Any]
    snapshot_file_id: str | None
    pointer_file_id: str | None
    pointer_raw: bytes | None
    attempt_file_id: str
    raw_file_id: str | None
    outcome: str
    advanced: bool
    validation_error: str | None = None


def source_specs_from_config(config: str | Path | Mapping[str, Any], source_ids: list[str] | None = None) -> dict[str, SourceSpec]:
    """Read only the explicitly supported configured NBP dated sources."""
    if isinstance(config, (str, Path)):
        try:
            import yaml  # project dependency; isolated from the pure state protocol
        except ImportError as exc:  # pragma: no cover
            raise NBPStateError("PyYAML is required to read a sources.yaml path") from exc
        with Path(config).open("r", encoding="utf-8") as handle:
            document = yaml.safe_load(handle)
    else:
        document = config
    if not isinstance(document, Mapping) or not isinstance(document.get("sources"), Mapping):
        raise NBPStateError("sources configuration must contain a sources object")
    available = document["sources"]
    selected = list(available) if source_ids is None else list(source_ids)
    if not selected:
        raise NBPStateError("at least one source must be selected")
    specs: dict[str, SourceSpec] = {}
    for source_id in selected:
        _require_source_id(source_id)
        if source_id not in NBP_SOURCE_IDS:
            raise NBPStateError(f"unsupported NBP source: {source_id}")
        raw = available.get(source_id)
        if not isinstance(raw, Mapping):
            raise NBPStateError(f"unknown configured source: {source_id}")
        full = raw.get("load_methods", {}).get("full") if isinstance(raw.get("load_methods"), Mapping) else None
        if not isinstance(full, Mapping):
            raise NBPStateError(f"{source_id} has no full dated-request configuration")
        template = full.get("endpoint_template")
        max_days = full.get("max_chunk_days")
        start = full.get("historical_start_date")
        params = full.get("params", {})
        if not isinstance(template, str) or "{start_date}" not in template or "{end_date}" not in template or not template.startswith("https://api.nbp.pl/api/"):
            raise NBPStateError(f"{source_id} has an unsafe dated endpoint template")
        if not isinstance(max_days, int) or isinstance(max_days, bool) or not 1 <= max_days <= MAX_CHUNK_DAYS:
            raise NBPStateError(f"{source_id} max_chunk_days must be 1..{MAX_CHUNK_DAYS}")
        if not isinstance(params, Mapping) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in params.items()):
            raise NBPStateError(f"{source_id} request params must be string pairs")
        specs[source_id] = SourceSpec(source_id, _parse_date(start, "historical_start_date"), template, dict(params), max_days)
    return specs


def new_state(specs: Mapping[str, SourceSpec], now_utc: datetime | None = None) -> dict[str, Any]:
    _validate_specs(specs)
    now = _timestamp(now_utc or datetime.now(timezone.utc))
    return {
        "format_version": 1,
        "state_id": str(uuid4()),
        "created_at_utc": now,
        "updated_at_utc": now,
        "global_sequence": 0,
        "sources": {
            source_id: {
                "historical_start_date": spec.historical_start_date.isoformat(),
                "last_attempt_at_utc": None,
                "last_attempt_file_id": None,
                "last_checked_through_date": None,
                "last_successful_ingestion_at_utc": None,
                "latest_observation_date": None,
                "historical_cursor": spec.historical_start_date.isoformat(),
                "completed_intervals": [],
                "successful_responses": [],
            }
            for source_id, spec in sorted(specs.items())
        },
    }


def plan_requests(
    state: Mapping[str, Any], specs: Mapping[str, SourceSpec], today_utc: date | datetime,
    *, recent_recheck_days: int = MAX_CHUNK_DAYS, max_catch_up_chunks: int = 64,
    max_historical_recheck_chunks: int = 1,
) -> list[RequestPlan]:
    """Plan requests through the supplied inclusive UTC cutoff date.

    Callers normally pass ``UTC today - 1 day`` so an unfinished publication day is
    not checkpointed. Plans recover old holes first, then recheck the recent overlap,
    then perform bounded historical rechecks. A runner persists each successful plan
    before asking for a fresh plan, so a restart resumes the exact hole.
    """
    _validate_state(state, specs)
    if not isinstance(recent_recheck_days, int) or not 1 <= recent_recheck_days <= MAX_CHUNK_DAYS:
        raise NBPStateError(f"recent_recheck_days must be 1..{MAX_CHUNK_DAYS}")
    if not isinstance(max_catch_up_chunks, int) or max_catch_up_chunks < 1 or not isinstance(max_historical_recheck_chunks, int) or max_historical_recheck_chunks < 0:
        raise NBPStateError("chunk budgets must be nonnegative bounded integers")
    if isinstance(today_utc, datetime):
        _timestamp(today_utc)
        today = today_utc.date()
    else:
        today = today_utc
    if not isinstance(today, date):
        raise NBPStateError("today_utc must be a UTC date or datetime")
    plans: list[RequestPlan] = []
    for source_id, spec in sorted(specs.items()):
        source = state["sources"][source_id]
        if today < spec.historical_start_date:
            continue
        recent_start = max(spec.historical_start_date, today - timedelta(days=recent_recheck_days - 1))
        old_end = recent_start - timedelta(days=1)
        holes = _uncovered_intervals(source["completed_intervals"], spec.historical_start_date, old_end)
        catch_up = _chunk_intervals(holes, spec, "catch_up", max_catch_up_chunks)
        plans.extend(catch_up)
        # Do not schedule a recent or history pass until every older hole selected for
        # recovery has actually been committed; otherwise a run budget can hide it.
        if catch_up:
            continue
        plans.extend(_chunk_intervals([(recent_start, today)], spec, "recent_recheck", 1))
        if old_end >= spec.historical_start_date:
            cursor = _parse_date(source["historical_cursor"], "historical_cursor")
            if cursor < spec.historical_start_date or cursor > old_end:
                cursor = spec.historical_start_date
            plans.extend(_chunk_intervals([(cursor, old_end)], spec, "historical_recheck", max_historical_recheck_chunks))
    return plans


def validate_nbp_response(source_id: str, http_status: int, body: bytes, *, max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES) -> ValidationResult:
    """Validate source fields without changing/normalising the retained bytes."""
    _require_source_id(source_id)
    if not isinstance(http_status, int) or isinstance(http_status, bool):
        raise NBPStateError("HTTP status must be an integer")
    if not isinstance(body, bytes):
        raise NBPStateError("response body must be bytes")
    if len(body) > max_response_bytes:
        raise NBPStateError("response exceeds the configured size limit")
    if http_status == 404:
        return ValidationResult("no_observations", sha256(body).hexdigest() if body else None, _decode_body(body) if body else None, None, None, 0)
    if http_status != 200:
        raise NBPStateError(f"response HTTP status is not a valid completed NBP result: {http_status}")
    text = _decode_body(body)
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, ValueError) as exc:
        raise NBPStateError("200 response is not valid JSON") from exc
    dates = _validate_payload(source_id, payload)
    return ValidationResult("data", sha256(body).hexdigest(), text, min(dates), max(dates), len(dates))


def store_raw_response(store: NBPStateStore, source_id: str, body: bytes, landing_source_folder_id: str) -> tuple[str, str]:
    """Persist immutable exact bytes at ``<sha256>.json`` or verify/reuse them."""
    _require_source_id(source_id)
    _require_id(landing_source_folder_id, "landing source folder id")
    if not isinstance(body, bytes) or not body:
        raise NBPStateError("raw response bytes must be nonempty")
    digest = sha256(body).hexdigest()
    name = f"{digest}.json"
    found = store.find(name, landing_source_folder_id)
    if len(found) > 1:
        raise NBPStateError("ambiguous content-addressed raw response")
    if found:
        file_id = _require_id(found[0], "raw file id")
        if _read(store, file_id, "raw response") != body:
            raise NBPStateError("existing raw response hash path has different bytes")
        return file_id, digest
    file_id = _require_id(store.create(name, body, landing_source_folder_id), "raw file id")
    if _read(store, file_id, "raw response") != body:
        raise NBPStateError("raw response did not read back exactly")
    return file_id, digest


def load_state(store: NBPStateStore, control_root_id: str, specs: Mapping[str, SourceSpec], *, now_utc: datetime | None = None) -> LoadedState:
    _require_id(control_root_id, "control root id")
    _validate_specs(specs)
    found = store.find("current-ingestion-state.json", control_root_id)
    if len(found) > 1:
        raise NBPStateError("ambiguous current-ingestion-state pointers")
    if not found:
        return LoadedState(new_state(specs, now_utc), None, None, None)
    pointer_id = _require_id(found[0], "current state pointer id")
    raw = _read(store, pointer_id, "current state pointer")
    pointer = _json_object(raw, "current state pointer")
    if pointer.get("format_version") != 1:
        raise NBPStateError("current state pointer has unsupported format")
    snapshot_id = _require_id(pointer.get("state_file_id"), "state file id")
    snapshot_raw = _read(store, snapshot_id, "state snapshot")
    if pointer.get("state_sha256") != sha256(snapshot_raw).hexdigest():
        raise NBPStateError("state snapshot checksum does not match current pointer")
    state = _json_object(snapshot_raw, "state snapshot")
    _validate_state(state, specs)
    return LoadedState(state, pointer_id, raw, snapshot_id)


def commit_response(
    store: NBPStateStore, control_root_id: str, loaded: LoadedState, specs: Mapping[str, SourceSpec], plan: RequestPlan,
    *, http_status: int, body: bytes, retrieved_at_utc: datetime, landing_source_folder_id: str | None = None,
    raw_file_id: str | None = None, run_id: str | None = None, retry_count: int = 0,
    started_at_utc: datetime | None = None, code_sha: str | None = None,
) -> CommitResult:
    """Append an attempt and, only for a durable valid result, advance state safely."""
    _require_id(control_root_id, "control root id")
    _validate_state(loaded.state, specs)
    _validate_plan(plan, specs)
    if not isinstance(retry_count, int) or isinstance(retry_count, bool) or retry_count < 0:
        raise NBPStateError("retry_count must be a nonnegative integer")
    finished = _timestamp(retrieved_at_utc)
    started = _timestamp(started_at_utc or retrieved_at_utc)
    if code_sha is not None and (not isinstance(code_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", code_sha)):
        raise NBPStateError("code_sha must be a lowercase 40-character Git SHA")
    try:
        validation = validate_nbp_response(plan.source_id, http_status, body)
        if validation.observation_min_date is not None and (
            validation.observation_min_date < plan.requested_start_date
            or validation.observation_max_date > plan.requested_end_date
        ):
            raise NBPStateError("source observations fall outside the requested interval")
    except NBPStateError as exc:
        # A malformed 200 is retained for replay, but it never becomes coverage.
        failed_raw_id = raw_file_id
        if (
            http_status == 200 and failed_raw_id is None and landing_source_folder_id is not None
            and isinstance(body, bytes) and body and len(body) <= DEFAULT_MAX_RESPONSE_BYTES
        ):
            failed_raw_id, _ = store_raw_response(store, plan.source_id, body, landing_source_folder_id)
        if failed_raw_id is not None:
            failed_raw_id = _require_id(failed_raw_id, "raw file id")
        attempt_id = _write_attempt(store, control_root_id, _attempt_record(plan, "parse_or_http_error", http_status, None, failed_raw_id, run_id, retry_count, started, finished, code_sha, response_size_bytes=len(body) if isinstance(body, bytes) else None, error=str(exc)))
        return _advance_failed_attempt(store, control_root_id, loaded, specs, plan.source_id, attempt_id, failed_raw_id, finished, str(exc))
    resolved_raw_id: str | None = None
    raw_reference_file_id: str | None = None
    # A 404 is a completed interval too. Preserve any returned bytes as request
    # evidence, while omitting it from the JSONL observation inventory.
    if validation.outcome == "data" or body:
        if raw_file_id is not None:
            resolved_raw_id = _require_id(raw_file_id, "raw file id")
            if _read(store, resolved_raw_id, "raw response") != body:
                raise NBPStateError("provided raw file does not contain the exact response bytes")
        else:
            if landing_source_folder_id is None:
                raise NBPStateError("landing_source_folder_id is required for retained response bytes")
            resolved_raw_id, digest = store_raw_response(store, plan.source_id, body, landing_source_folder_id)
            if digest != validation.response_sha256:  # defensive; should be impossible
                raise NBPStateError("raw response hash changed during persistence")
        resolved_raw_id, raw_reference_file_id = _ensure_raw_reference(
            store, control_root_id, plan.source_id, validation.response_sha256, resolved_raw_id, len(body)
        )
    candidate = json.loads(json.dumps(loaded.state))
    sequence = int(candidate["global_sequence"]) + 1
    descriptor = {
        "source_id": plan.source_id,
        "batch_id": f"nbp-response-{sequence}",
        "ingestion_sequence": sequence,
        "requested_start_date": plan.requested_start_date.isoformat(),
        "requested_end_date": plan.requested_end_date.isoformat(),
        "retrieved_at_utc": finished,
        "response_sha256": validation.response_sha256,
        "raw_file_id": resolved_raw_id,
        "raw_reference_file_id": raw_reference_file_id,
        "size_bytes": len(body),
        "outcome": validation.outcome,
        "observation_min_date": validation.observation_min_date.isoformat() if validation.observation_min_date else None,
        "observation_max_date": validation.observation_max_date.isoformat() if validation.observation_max_date else None,
    }
    attempt = _attempt_record(plan, validation.outcome, http_status, validation, resolved_raw_id, run_id, retry_count, started, finished, code_sha, response_size_bytes=len(body), sequence=sequence)
    attempt["raw_reference_file_id"] = raw_reference_file_id
    attempt_id = _write_attempt(store, control_root_id, attempt)
    source = candidate["sources"][plan.source_id]
    source["last_attempt_at_utc"] = finished
    source["last_attempt_file_id"] = attempt_id
    if validation.outcome == "data":
        source["last_successful_ingestion_at_utc"] = finished
    source["completed_intervals"] = _merge_intervals(source["completed_intervals"] + [{
        "start_date": plan.requested_start_date.isoformat(), "end_date": plan.requested_end_date.isoformat(), "outcome": validation.outcome,
        "ingestion_sequence": sequence, "attempt_file_id": attempt_id,
        "observation_min_date": descriptor["observation_min_date"], "observation_max_date": descriptor["observation_max_date"],
    }])
    source["last_checked_through_date"] = _contiguous_through(source["completed_intervals"], specs[plan.source_id].historical_start_date)
    if validation.observation_max_date is not None:
        existing = source.get("latest_observation_date")
        value = validation.observation_max_date.isoformat()
        source["latest_observation_date"] = max(existing, value) if existing else value
    if validation.outcome == "data":
        responses = source["successful_responses"]
        # Each request gets an attempt even when raw bytes repeat.  Do not collapse
        # descriptors: their intervals/retrieval evidence differ.
        responses.append(descriptor)
        if len(responses) > MAX_RESPONSES:
            raise NBPStateError("successful response inventory exceeds state safety bound")
    if plan.mode == "historical_recheck":
        # Cursor can temporarily be after the old range; planner wraps it safely.
        source["historical_cursor"] = (plan.requested_end_date + timedelta(days=1)).isoformat()
    candidate["global_sequence"] = sequence
    candidate["state_id"] = str(uuid4())
    candidate["updated_at_utc"] = finished
    _validate_state(candidate, specs)
    snapshot_id = _write_snapshot(store, control_root_id, candidate)
    pointer_id, pointer_raw = _advance_pointer(store, control_root_id, loaded, snapshot_id, candidate)
    return CommitResult(candidate, snapshot_id, pointer_id, pointer_raw, attempt_id, resolved_raw_id, validation.outcome, True)


def successful_response_observation_descriptors(state: Mapping[str, Any], source_id: str | None = None) -> list[dict[str, Any]]:
    """Descriptors for runners to download raw files and produce JSONL envelopes."""
    if not isinstance(state, Mapping) or not isinstance(state.get("sources"), Mapping):
        raise NBPStateError("state is not an ingestion state object")
    selected = [source_id] if source_id else sorted(state["sources"])
    descriptors: list[dict[str, Any]] = []
    for item_id in selected:
        _require_source_id(item_id)
        source = state["sources"].get(item_id)
        if not isinstance(source, Mapping):
            raise NBPStateError(f"state has no source {item_id}")
        for descriptor in source.get("successful_responses", []):
            if not isinstance(descriptor, Mapping) or descriptor.get("source_id") != item_id:
                raise NBPStateError("state contains an invalid response descriptor")
            # Exact downstream contract; runner adds body_json after downloading raw_file_id.
            descriptors.append({key: descriptor.get(key) for key in (
                "source_id", "batch_id", "ingestion_sequence", "requested_start_date", "requested_end_date",
                "retrieved_at_utc", "response_sha256", "raw_file_id", "size_bytes",
            )})
    return sorted(descriptors, key=lambda item: item["ingestion_sequence"])


def response_envelope(descriptor: Mapping[str, Any], body: bytes) -> dict[str, Any]:
    """Build the downstream JSONL envelope while retaining the original JSON text."""
    if not isinstance(descriptor, Mapping):
        raise NBPStateError("response descriptor must be an object")
    required = ("source_id", "batch_id", "ingestion_sequence", "requested_start_date", "requested_end_date", "retrieved_at_utc", "response_sha256", "raw_file_id")
    if any(descriptor.get(key) is None for key in required):
        raise NBPStateError("descriptor is incomplete for a data response")
    if sha256(body).hexdigest() != descriptor["response_sha256"]:
        raise NBPStateError("downloaded raw body checksum does not match descriptor")
    text = _decode_body(body)
    try:
        json.loads(text)
    except json.JSONDecodeError as exc:
        raise NBPStateError("downloaded raw body is not JSON") from exc
    return {key: descriptor[key] for key in required} | {"body_json": text}


# Friendly alias used by orchestration code.
list_successful_response_descriptors = successful_response_observation_descriptors


def _advance_failed_attempt(
    store: NBPStateStore, root: str, loaded: LoadedState, specs: Mapping[str, SourceSpec],
    source_id: str, attempt_id: str, raw_file_id: str | None, finished: str, validation_error: str,
) -> CommitResult:
    """Publish attempt metadata only; coverage and successful sequence are unchanged."""
    candidate = json.loads(json.dumps(loaded.state))
    source = candidate["sources"][source_id]
    source["last_attempt_at_utc"] = finished
    source["last_attempt_file_id"] = attempt_id
    candidate["state_id"] = str(uuid4())
    candidate["updated_at_utc"] = finished
    _validate_state(candidate, specs)
    snapshot_id = _write_snapshot(store, root, candidate)
    pointer_id, pointer_raw = _advance_pointer(store, root, loaded, snapshot_id, candidate)
    return CommitResult(candidate, snapshot_id, pointer_id, pointer_raw, attempt_id, raw_file_id, "parse_or_http_error", False, validation_error)


def _ensure_raw_reference(store: NBPStateStore, root: str, source_id: str, digest: str, raw_file_id: str, size_bytes: int) -> tuple[str, str]:
    """Create/read back the control reference without duplicating authoritative bytes."""
    _require_source_id(source_id)
    if not isinstance(digest, str) or not _SHA_RE.fullmatch(digest):
        raise NBPStateError("invalid raw response SHA-256")
    raw_file_id = _require_id(raw_file_id, "raw file id")
    references = _folder(store, "raw-references", root)
    source_folder = _folder(store, source_id, references)
    name = f"{digest}.json"
    found = store.find(name, source_folder)
    if len(found) > 1:
        raise NBPStateError("ambiguous raw response reference")
    if found:
        reference_id = _require_id(found[0], "raw reference file id")
        reference = _json_object(_read(store, reference_id, "raw response reference"), "raw response reference")
        if reference.get("source_id") != source_id or reference.get("response_sha256") != digest or not isinstance(reference.get("size_bytes"), int):
            raise NBPStateError("raw response reference does not match requested response")
        existing_raw = _require_id(reference.get("raw_file_id"), "referenced raw file id")
        # Read the selected identity to prove this still points at the immutable bytes.
        raw = _read(store, existing_raw, "referenced raw response")
        if len(raw) != reference["size_bytes"] or sha256(raw).hexdigest() != digest:
            raise NBPStateError("raw response reference no longer verifies")
        return existing_raw, reference_id
    reference = {"format_version": 1, "source_id": source_id, "response_sha256": digest, "raw_file_id": raw_file_id, "size_bytes": size_bytes}
    data = _json_bytes(reference)
    reference_id = _require_id(store.create(name, data, source_folder), "raw reference file id")
    if _read(store, reference_id, "raw response reference") != data:
        raise NBPStateError("raw response reference did not read back exactly")
    return raw_file_id, reference_id


def _write_attempt(store: NBPStateStore, root: str, attempt: dict[str, Any]) -> str:
    attempts = _folder(store, "attempts", root)
    data = _json_bytes(attempt)
    name = f"{attempt['attempt_id']}.json"
    file_id = _require_id(store.create(name, data, attempts), "attempt file id")
    if _read(store, file_id, "attempt") != data:
        raise NBPStateError("attempt did not read back exactly")
    return file_id


def _write_snapshot(store: NBPStateStore, root: str, state: dict[str, Any]) -> str:
    states = _folder(store, "states", root)
    data = _json_bytes(state)
    if len(data) > MAX_STATE_BYTES:
        raise NBPStateError("state snapshot exceeds safety bound")
    file_id = _require_id(store.create(f"{state['state_id']}.json", data, states), "state snapshot id")
    if _read(store, file_id, "state snapshot") != data:
        raise NBPStateError("state snapshot did not read back exactly")
    return file_id


def _advance_pointer(store: NBPStateStore, root: str, loaded: LoadedState, snapshot_id: str, state: dict[str, Any]) -> tuple[str, bytes]:
    current = _read_pointer(store, root)
    current_raw = current[1] if current else None
    if current_raw != loaded.pointer_raw:
        raise NBPStateError("current-ingestion-state pointer changed during state upload")
    snapshot_bytes = _json_bytes(state)
    pointer = {
        "format_version": 1, "state_file_id": snapshot_id, "state_sha256": sha256(snapshot_bytes).hexdigest(),
        "updated_at_utc": state["updated_at_utc"], "previous_state_file_id": loaded.snapshot_file_id,
    }
    data = _json_bytes(pointer)
    if current is None:
        pointer_id = _require_id(store.create("current-ingestion-state.json", data, root), "state pointer id")
        if _read(store, pointer_id, "state pointer") != data:
            raise UncertainStatePointerError("new current-ingestion-state pointer did not read back exactly")
        return pointer_id, data
    pointer_id = current[0]
    try:
        store.replace(pointer_id, data)
    except Exception as exc:
        try:
            after = _read(store, pointer_id, "state pointer")
        except Exception as read_exc:
            raise UncertainStatePointerError("state pointer update failed and could not be read back") from read_exc
        if after == data:
            return pointer_id, data
        raise UncertainStatePointerError("state pointer update outcome is uncertain; prior pointer remains usable") from exc
    after = _read(store, pointer_id, "state pointer")
    if after != data:
        raise UncertainStatePointerError("state pointer readback differs after update")
    return pointer_id, data


def _read_pointer(store: NBPStateStore, root: str) -> tuple[str, bytes, dict[str, Any]] | None:
    found = store.find("current-ingestion-state.json", root)
    if len(found) > 1:
        raise NBPStateError("ambiguous current-ingestion-state pointers")
    if not found:
        return None
    pointer_id = _require_id(found[0], "current state pointer id")
    raw = _read(store, pointer_id, "current state pointer")
    pointer = _json_object(raw, "current state pointer")
    if pointer.get("format_version") != 1 or not isinstance(pointer.get("state_sha256"), str) or not _SHA_RE.fullmatch(pointer["state_sha256"]):
        raise NBPStateError("current state pointer is invalid")
    _require_id(pointer.get("state_file_id"), "state file id")
    return pointer_id, raw, pointer


def _folder(store: NBPStateStore, name: str, root: str) -> str:
    found = store.find(name, root)
    if len(found) > 1:
        raise NBPStateError(f"ambiguous {name} folders")
    return _require_id(found[0], f"{name} folder id") if found else _require_id(store.mkdir(name, root), f"{name} folder id")


def _attempt_record(plan: RequestPlan, outcome: str, status: int, validation: ValidationResult | None, raw_file_id: str | None, run_id: str | None, retry_count: int, started: str, finished: str, code_sha: str | None, *, response_size_bytes: int | None = None, error: str | None = None, sequence: int | None = None) -> dict[str, Any]:
    return {
        "format_version": 1, "attempt_id": str(uuid4()), "run_id": run_id, "source_id": plan.source_id, "mode": plan.mode,
        "request_url": plan.request_url, "params": plan.params, "interval_start": plan.requested_start_date.isoformat(), "interval_end": plan.requested_end_date.isoformat(),
        "started_at_utc": started, "finished_at_utc": finished, "retry_count": retry_count, "final_http_status": status,
        "outcome": outcome, "ingestion_sequence": sequence, "response_sha256": validation.response_sha256 if validation else None,
        "raw_file_id": raw_file_id, "response_size_bytes": response_size_bytes,
        "observation_min_date": validation.observation_min_date.isoformat() if validation and validation.observation_min_date else None,
        "observation_max_date": validation.observation_max_date.isoformat() if validation and validation.observation_max_date else None,
        "code_sha": code_sha, "error": error,
    }


def _validate_payload(source_id: str, payload: Any) -> list[date]:
    if source_id == "nbp_gold_prices":
        if not isinstance(payload, list) or not payload:
            raise NBPStateError("gold response must be a nonempty array")
        dates: list[date] = []
        seen_dates: set[date] = set()
        for row in payload:
            if not isinstance(row, dict):
                raise NBPStateError("gold response rows must be objects")
            observed = _parse_date(row.get("data"), "gold data")
            if observed in seen_dates:
                raise NBPStateError("duplicate gold observation date")
            seen_dates.add(observed)
            dates.append(observed)
            _positive_number(row.get("cena"), "gold cena")
        return dates
    table = {"nbp_exchange_rates_table_a": "A", "nbp_exchange_rates_table_b": "B", "nbp_exchange_rates_table_c": "C"}.get(source_id)
    if table is None:
        raise NBPStateError(f"unsupported NBP source: {source_id}")
    if not isinstance(payload, list) or not payload:
        raise NBPStateError("exchange response must be a nonempty array")
    dates: list[date] = []
    # Legacy NBP responses can repeat a code under different country labels.
    # They are one analytical observation only when its measures and publication
    # provenance agree; raw bytes retain the distinct labels for dbt.
    observations: dict[tuple[date, str], tuple[tuple[float, ...], str | None, str | None]] = {}
    for publication in payload:
        if not isinstance(publication, dict) or publication.get("table") != table:
            raise NBPStateError(f"Table {table} response has an invalid publication")
        publication_no = publication.get("no")
        if publication_no is not None and not isinstance(publication_no, str):
            raise NBPStateError("publication no must be a string when supplied")
        effective = _parse_date(publication.get("effectiveDate"), "effectiveDate")
        if table == "C" and publication.get("tradingDate") is not None:
            _parse_date(publication["tradingDate"], "tradingDate")
        rates = publication.get("rates")
        if not isinstance(rates, list) or not rates:
            raise NBPStateError("publication rates must be a nonempty array")
        for rate in rates:
            if not isinstance(rate, dict):
                raise NBPStateError("rate must be an object")
            currency = rate.get("currency")
            if currency is not None and not isinstance(currency, str):
                raise NBPStateError("rate currency must be a string when supplied")
            code = rate.get("code")
            if not isinstance(code, str) or not code.strip():
                raise NBPStateError("rate code is required")
            if table == "C":
                bid, ask = _nonnegative_number(rate.get("bid"), "bid"), _nonnegative_number(rate.get("ask"), "ask")
                if bid > ask:
                    raise NBPStateError("bid must be less than or equal to ask")
                measures = (bid, ask)
                trading_date = publication.get("tradingDate")
            else:
                measures = (_nonnegative_number(rate.get("mid"), "mid"),)
                trading_date = None
            identity = (effective, code)
            evidence = (measures, publication_no, trading_date)
            previous = observations.get(identity)
            if previous is not None and previous != evidence:
                raise NBPStateError("conflicting table/effectiveDate/code in source response")
            observations[identity] = evidence
        dates.append(effective)
    return dates

def _nonnegative_number(value: Any, label: str) -> float:
    if (
        isinstance(value, bool) or not isinstance(value, (int, float))
        or not math.isfinite(float(value)) or float(value) < 0
    ):
        raise NBPStateError(f"{label} must be a finite nonnegative number")
    return float(value)


def _positive_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) or float(value) <= 0:
        raise NBPStateError(f"{label} must be a finite positive number")
    return float(value)


def _merge_intervals(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Preserve request evidence; only coalesce exact same-outcome adjacent coverage.
    ordered = sorted(items, key=lambda item: (item["start_date"], item["end_date"]))
    result: list[dict[str, Any]] = []
    for item in ordered:
        _parse_date(item["start_date"], "interval start"); _parse_date(item["end_date"], "interval end")
        if result and item["start_date"] == result[-1]["start_date"] and item["end_date"] == result[-1]["end_date"]:
            # Recent/reconciliation rechecks retain their response descriptor but
            # coverage needs one interval; keep the newest successful evidence.
            result[-1] = item
        else: result.append(item)
    return result


def _contiguous_through(intervals: list[dict[str, Any]], start: date) -> str | None:
    cursor = start
    for item in sorted(intervals, key=lambda row: (row["start_date"], row["end_date"])):
        low, high = _parse_date(item["start_date"], "interval start"), _parse_date(item["end_date"], "interval end")
        if high < cursor: continue
        if low > cursor: break
        cursor = max(cursor, high + timedelta(days=1))
    return (cursor - timedelta(days=1)).isoformat() if cursor > start else None


def _uncovered_intervals(intervals: list[dict[str, Any]], start: date, end: date) -> list[tuple[date, date]]:
    if end < start: return []
    cursor = start; result = []
    for item in sorted(intervals, key=lambda row: (row["start_date"], row["end_date"])):
        low, high = _parse_date(item["start_date"], "interval start"), _parse_date(item["end_date"], "interval end")
        if high < cursor: continue
        if low > end: break
        if low > cursor: result.append((cursor, min(end, low - timedelta(days=1))))
        cursor = max(cursor, high + timedelta(days=1))
        if cursor > end: break
    if cursor <= end: result.append((cursor, end))
    return result


def _chunk_intervals(intervals: list[tuple[date, date]], spec: SourceSpec, mode: str, budget: int) -> list[RequestPlan]:
    plans: list[RequestPlan] = []
    for low, high in intervals:
        cursor = low
        while cursor <= high and len(plans) < budget:
            end = min(high, cursor + timedelta(days=spec.max_chunk_days - 1))
            plans.append(RequestPlan(spec.source_id, cursor, end, mode, spec.endpoint_template, spec.params))
            cursor = end + timedelta(days=1)
        if len(plans) >= budget: break
    return plans


def _validate_plan(plan: RequestPlan, specs: Mapping[str, SourceSpec]) -> None:
    if not isinstance(plan, RequestPlan) or plan.source_id not in specs: raise NBPStateError("plan has an unknown source")
    spec = specs[plan.source_id]
    if plan.requested_end_date < plan.requested_start_date or (plan.requested_end_date - plan.requested_start_date).days + 1 > spec.max_chunk_days: raise NBPStateError("plan exceeds configured inclusive chunk limit")
    if plan.mode not in {"catch_up", "recent_recheck", "historical_recheck"}: raise NBPStateError("plan has an unsafe mode")
    if plan.endpoint_template != spec.endpoint_template or plan.params != spec.params: raise NBPStateError("plan does not match configured source specification")


def _validate_specs(specs: Mapping[str, SourceSpec]) -> None:
    if not isinstance(specs, Mapping) or not specs: raise NBPStateError("source specs are required")
    for source_id, spec in specs.items():
        _require_source_id(source_id)
        if (
            source_id not in NBP_SOURCE_IDS or not isinstance(spec, SourceSpec)
            or spec.source_id != source_id or not 1 <= spec.max_chunk_days <= MAX_CHUNK_DAYS
            or not spec.endpoint_template.startswith("https://api.nbp.pl/api/")
        ):
            raise NBPStateError("invalid source specification")


def _validate_state(state: Mapping[str, Any], specs: Mapping[str, SourceSpec]) -> None:
    if not isinstance(state, Mapping) or state.get("format_version") != 1 or not isinstance(state.get("global_sequence"), int) or state["global_sequence"] < 0: raise NBPStateError("invalid ingestion state")
    if not isinstance(state.get("state_id"), str) or not isinstance(state.get("sources"), Mapping) or set(state["sources"]) != set(specs): raise NBPStateError("state sources do not match configured sources")
    globally_seen_sequences: set[int] = set()
    for source_id, spec in specs.items():
        source = state["sources"][source_id]
        if not isinstance(source, Mapping) or source.get("historical_start_date") != spec.historical_start_date.isoformat() or not isinstance(source.get("completed_intervals"), list) or not isinstance(source.get("successful_responses"), list): raise NBPStateError(f"invalid state for {source_id}")
        _parse_date(source.get("historical_cursor"), "historical_cursor")
        for optional_date in ("last_checked_through_date", "latest_observation_date"):
            if source.get(optional_date) is not None:
                _parse_date(source[optional_date], optional_date)
        for optional_time in ("last_attempt_at_utc", "last_successful_ingestion_at_utc"):
            if source.get(optional_time) is not None:
                _parse_utc_timestamp(source[optional_time], optional_time)
        if source.get("last_attempt_file_id") is not None:
            _require_id(source["last_attempt_file_id"], "last attempt file id")
        last_sequence = 0
        for item in source["completed_intervals"]:
            if not isinstance(item, Mapping): raise NBPStateError("invalid completed interval")
            low, high = _parse_date(item.get("start_date"), "interval start"), _parse_date(item.get("end_date"), "interval end")
            if low > high or item.get("outcome") not in {"data", "no_observations"}: raise NBPStateError("invalid completed interval")
            if not isinstance(item.get("ingestion_sequence"), int) or item["ingestion_sequence"] < 1 or item["ingestion_sequence"] > state["global_sequence"]: raise NBPStateError("invalid completed interval sequence")
            _require_id(item.get("attempt_file_id"), "attempt file id")
        if len(source["successful_responses"]) > MAX_RESPONSES: raise NBPStateError("successful response inventory exceeds state safety bound")
        for descriptor in source["successful_responses"]:
            if not isinstance(descriptor, Mapping) or descriptor.get("source_id") != source_id: raise NBPStateError("invalid successful response descriptor")
            if not isinstance(descriptor.get("ingestion_sequence"), int) or descriptor["ingestion_sequence"] <= last_sequence or descriptor["ingestion_sequence"] > state["global_sequence"]: raise NBPStateError("response sequences must increase")
            last_sequence = descriptor["ingestion_sequence"]
            if last_sequence in globally_seen_sequences:
                raise NBPStateError("response ingestion sequences must be globally unique")
            globally_seen_sequences.add(last_sequence)
            for field in ("requested_start_date", "requested_end_date", "observation_min_date", "observation_max_date"):
                if descriptor.get(field) is not None: _parse_date(descriptor[field], field)
            _parse_utc_timestamp(descriptor.get("retrieved_at_utc"), "retrieved_at_utc")
            if not isinstance(descriptor.get("batch_id"), str) or not descriptor["batch_id"]: raise NBPStateError("invalid response batch ID")
            if not isinstance(descriptor.get("response_sha256"), str) or not _SHA_RE.fullmatch(descriptor["response_sha256"]): raise NBPStateError("invalid response checksum")
            _require_id(descriptor.get("raw_file_id"), "raw file id")


def _timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None: raise NBPStateError("timestamps must be timezone-aware UTC datetimes")
    if value.utcoffset() != timedelta(0): raise NBPStateError("timestamps must be UTC")
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _parse_date(value: Any, label: str) -> date:
    if not isinstance(value, str): raise NBPStateError(f"{label} must be an ISO date")
    try: return date.fromisoformat(value)
    except ValueError as exc: raise NBPStateError(f"{label} must be an ISO date") from exc

def _parse_utc_timestamp(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise NBPStateError(f"{label} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise NBPStateError(f"{label} must be a UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise NBPStateError(f"{label} must be a UTC timestamp")

def _decode_body(body: bytes) -> str:
    try: return body.decode("utf-8")
    except UnicodeDecodeError as exc: raise NBPStateError("response body must be UTF-8 JSON") from exc

def _json_object(raw: bytes, label: str) -> dict[str, Any]:
    try: value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc: raise NBPStateError(f"{label} is not JSON") from exc
    if not isinstance(value, dict): raise NBPStateError(f"{label} must be a JSON object")
    return value

def _json_bytes(value: Any) -> bytes: return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
def _read(store: NBPStateStore, file_id: str, label: str) -> bytes:
    value = store.read(file_id)
    if not isinstance(value, bytes): raise NBPStateError(f"{label} read did not return bytes")
    return value
def _require_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value): raise NBPStateError(f"unsafe {label}")
    return value
def _require_source_id(value: Any) -> str:
    if not isinstance(value, str) or not _SOURCE_RE.fullmatch(value): raise NBPStateError("unsafe source_id")
    return value
