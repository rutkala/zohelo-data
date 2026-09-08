"""Bounded durable transport for one non-NBP source ingestion campaign.

The public stores expose source-scoped state and exact raw response persistence.
State publication follows the repository's immutable-snapshot-plus-pointer model;
the only mutable object is ``current-ingestion-state.json`` inside the source's
own control directory.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from typing import Any, Protocol
from uuid import uuid4

MAX_STATE_BYTES = 4 * 1024 * 1024
MAX_STATE_MANIFEST_BYTES = 1024 * 1024
MAX_STATE_SHARD_BYTES = 512 * 1024
MAX_STATE_SHARDS = 4096
RECEIPT_SEGMENT_ITEMS = 128
# Cycle/corruption protection for linked receipt logs.  This is far beyond the
# current campaign scale and does not make the manifest grow with log lifetime.
MAX_RECEIPT_CHAIN_SEGMENTS = 1_000_000
MAX_RAW_BYTES = 8 * 1024 * 1024
MAX_RECEIPT_BYTES = MAX_STATE_BYTES
MAX_POINTER_BYTES = 16 * 1024
MAX_LANDING_FILE_BYTES = 8 * 1024 * 1024
MAX_LANDING_MANIFEST_BYTES = 1024 * 1024

_SOURCE_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,119}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STATE_FILE_RE = re.compile(
    r"^state-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$"
)
_STATE_OBJECT_RE = re.compile(
    r"^state-(manifest|pending|completed|recent-roots|receipts|rejected-receipts)-"
    r"([a-z0-9]+-)?[0-9a-f]{64}\.json$"
)
_POINTER_NAME = "current-ingestion-state.json"
_LANDING_POINTER_NAME = "current-landing.json"
_LANDING_OBJECT_RE = re.compile(
    r"^(?:fragment|manifest)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\.(?:parquet|json)$"
)

_SHARDED_FIELDS = (
    "pending",
    "completed",
    "recent_roots",
    "receipts",
    "rejected_receipts",
)
_MAP_FIELDS = {"completed", "recent_roots"}
_SHARD_KIND = {
    "pending": "pending",
    "completed": "completed",
    "recent_roots": "recent-roots",
    "receipts": "receipts",
    "rejected_receipts": "rejected-receipts",
}


class CampaignStoreError(RuntimeError):
    """Durable campaign data could not be safely verified or promoted."""

    uncertain = False


class CampaignCapacityError(CampaignStoreError):
    """A campaign object exceeds its explicit transport bound."""


class UncertainCampaignPointerError(CampaignStoreError):
    """The store cannot prove the outcome of the sole mutable operation."""

    uncertain = True


class UncertainCampaignWriteError(CampaignStoreError):
    """An immutable object write may have succeeded but cannot be verified."""

    uncertain = True


class _ObjectStore(Protocol):
    def find(self, name: str, parent_id: str) -> list[str]: ...

    def create(self, name: str, data: bytes, parent_id: str) -> str: ...

    def read(self, file_id: str) -> bytes: ...

    def replace(self, file_id: str, data: bytes) -> None: ...

    def mkdir(self, name: str, parent_id: str) -> str: ...


@dataclass(frozen=True)
class _PointerObservation:
    file_id: str
    raw: bytes
    value: dict[str, Any]


class _CampaignStore:
    """Backend-neutral campaign protocol over a Drive-shaped object store."""

    def __init__(
        self,
        store: _ObjectStore,
        source_id: str,
        control_root_id: str,
        responses_root_id: str,
        *,
        max_materialized_bytes: int | None = None,
    ) -> None:
        self._store = store
        self.source_id = _require_source_id(source_id)
        self._control_root_id = _require_object_id(control_root_id, "control root id")
        self._responses_root_id = _require_object_id(responses_root_id, "responses root id")
        if max_materialized_bytes is not None and (
            isinstance(max_materialized_bytes, bool)
            or not isinstance(max_materialized_bytes, int)
            or max_materialized_bytes < 1
        ):
            raise CampaignStoreError("materialized state budget must be a positive integer")
        self._max_materialized_bytes = max_materialized_bytes
        self._expected_pointer: _PointerObservation | None = None
        self._pointer_observed = False
        self._expected_landing_pointer: _PointerObservation | None = None
        self._landing_pointer_observed = False
        self._folder_ids: dict[tuple[str, str], str] = {}
        self._verified_state_objects: dict[str, dict[str, Any]] = {}
        self._entry_orders: dict[str, dict[str, int]] = {}
        self._materialized_state: dict[str, Any] | None = None

    def load(self) -> dict[str, Any] | None:
        """Load and verify the current immutable state snapshot, if it exists."""
        observed = self._read_pointer()
        if observed is None:
            self._expected_pointer = None
            self._pointer_observed = True
            self._materialized_state = None
            return None

        pointer = observed.value
        if pointer.get("format_version") == 2:
            state = self._load_sharded_state(pointer)
            self._expected_pointer = observed
            self._pointer_observed = True
            self._materialized_state = deepcopy(state)
            return state

        snapshot_name = pointer.get("state_file_name")
        if not isinstance(snapshot_name, str) or not _STATE_FILE_RE.fullmatch(snapshot_name):
            raise CampaignStoreError("current campaign pointer has an unsafe state file name")
        snapshot_id = _require_object_id(pointer.get("state_file_id"), "state snapshot id")
        matches = self._find(snapshot_name, self._states_root())
        if len(matches) != 1 or matches[0] != snapshot_id:
            raise CampaignStoreError(
                "current campaign snapshot is missing, ambiguous, or outside "
                "its source state folder"
            )
        expected_size = _bounded_size(
            pointer.get("state_size_bytes"), MAX_STATE_BYTES, "state snapshot"
        )
        expected_sha = _require_sha256(pointer.get("state_sha256"), "state snapshot SHA-256")
        raw = self._read(snapshot_id, "state snapshot")
        if len(raw) > MAX_STATE_BYTES:
            raise CampaignCapacityError("state snapshot exceeds the 4 MiB limit")
        if len(raw) != expected_size or sha256(raw).hexdigest() != expected_sha:
            raise CampaignStoreError("state snapshot does not match the current campaign pointer")
        state = _decode_object(raw, "state snapshot")
        if "source_id" in state and state["source_id"] != self.source_id:
            raise CampaignStoreError("state snapshot source identity does not match its store")

        self._expected_pointer = observed
        self._pointer_observed = True
        self._materialized_state = deepcopy(state)
        return state

    def load_cached(self) -> dict[str, Any] | None:
        """Reuse a verified materialization only while the exact pointer is current.

        This is intended for consecutive operations in one serialized worker.
        A fresh store still performs full namespace, size and hash verification,
        and :meth:`load` remains the explicit full-read path.
        """
        observed = self._read_pointer()
        if (
            self._pointer_observed
            and _same_pointer(observed, self._expected_pointer)
            and self._materialized_state is not None
        ):
            return deepcopy(self._materialized_state)
        return self.load()

    def save(self, state: dict[str, Any]) -> None:
        """Persist bounded immutable v2 shards, then safely promote their pointer.

        The caller-facing value remains the legacy materialized dictionary for
        now.  This limits integration churn while removing the unbounded remote
        rewrite.  Deployments may set ``max_materialized_bytes`` as a diagnosed
        resource guard; no arbitrary lifetime catalogue ceiling is implied.
        """
        self._validate_state_input(state)
        state_digest, state_size = _json_object_identity(state, "campaign state")
        if self._max_materialized_bytes is not None and state_size > self._max_materialized_bytes:
            raise CampaignCapacityError(
                "campaign state exceeds the configured materialized-runner resource budget"
            )

        if not self._pointer_observed:
            # A first save verifies any previous snapshot before it is allowed
            # to supersede that state.
            self.load()
        expected = self._expected_pointer
        if not _same_pointer(self._read_pointer(), expected):
            raise CampaignStoreError(
                "current campaign pointer changed since state was loaded or saved"
            )

        manifest_descriptor = self._write_sharded_state(
            state, state_digest=state_digest, state_size=state_size
        )
        pointer = {
            "format_version": 2,
            "source_id": self.source_id,
            "manifest_file_id": manifest_descriptor["id"],
            "manifest_file_name": manifest_descriptor["name"],
            "manifest_sha256": manifest_descriptor["sha256"],
            "manifest_size_bytes": manifest_descriptor["size_bytes"],
            # Transitional aliases let existing diagnostics identify and
            # inspect the immutable object without treating it as a v1 state.
            "state_file_id": manifest_descriptor["id"],
            "state_file_name": manifest_descriptor["name"],
            "state_sha256": manifest_descriptor["sha256"],
            "state_size_bytes": manifest_descriptor["size_bytes"],
        }
        pointer_raw = _json_object_bytes(pointer, "campaign state pointer")
        if len(pointer_raw) > MAX_POINTER_BYTES:  # defensive; normal pointers are tiny
            raise CampaignCapacityError("campaign state pointer exceeds its safety limit")

        # Re-saving an identical materialized state is a true no-op.  The
        # pointer observation above still detects a stale writer first.
        if expected is not None and expected.raw == pointer_raw:
            self._materialized_state = deepcopy(state)
            return

        # This is deliberately fresh and immediately precedes the only mutable
        # operation. Drive has no compare-and-swap, so the root workflow must
        # still serialize writers for each provider.
        try:
            current = self._read_pointer()
        except CampaignStoreError as exc:
            raise CampaignStoreError(
                "current campaign pointer changed during state upload"
            ) from exc
        if not _same_pointer(current, expected):
            raise CampaignStoreError("current campaign pointer changed during state upload")

        promoted = self._promote_pointer(expected, pointer_raw)
        self._expected_pointer = promoted
        self._pointer_observed = True
        self._materialized_state = deepcopy(state)

    def _validate_state_input(self, state: Any) -> None:
        """Reject malformed materialized collections before creating any shard."""
        if not isinstance(state, dict):
            raise CampaignStoreError("campaign state must be a JSON object")
        if "source_id" in state and state["source_id"] != self.source_id:
            raise CampaignStoreError("campaign state source identity does not match its store")
        for field in _SHARDED_FIELDS:
            if field not in state:
                continue
            value = state[field]
            if field in _MAP_FIELDS:
                if not isinstance(value, dict):
                    raise CampaignStoreError(f"campaign state {field} must be an object")
                if any(not isinstance(key, str) or not key for key in value):
                    raise CampaignStoreError(
                        f"campaign state {field} contains an invalid identity"
                    )
                continue
            if not isinstance(value, list):
                raise CampaignStoreError(f"campaign state {field} must be a list")
            if field == "pending":
                identities = []
                for item in value:
                    if (
                        not isinstance(item, dict)
                        or not isinstance(item.get("id"), str)
                        or not item["id"]
                    ):
                        raise CampaignStoreError(
                            "campaign pending task identity is invalid"
                        )
                    identities.append(item["id"])
                if len(identities) != len(set(identities)):
                    raise CampaignStoreError(
                        "campaign state pending contains duplicate identities"
                    )
            elif field in {"receipts", "rejected_receipts"}:
                receipt_ids: set[str] = set()
                for item in value:
                    if not isinstance(item, dict) or set(item) != {
                        "task_id",
                        "id",
                        "sha256",
                        "size_bytes",
                    }:
                        raise CampaignStoreError(
                            f"campaign state {field} receipt link fields are invalid"
                        )
                    task_id = item.get("task_id")
                    if not isinstance(task_id, str) or not task_id:
                        raise CampaignStoreError(
                            f"campaign state {field} task identity is invalid"
                        )
                    receipt_id = _require_object_id(
                        item.get("id"), f"campaign state {field} receipt id"
                    )
                    _require_sha256(
                        item.get("sha256"), f"campaign state {field} receipt SHA-256"
                    )
                    _bounded_size(
                        item.get("size_bytes"),
                        MAX_RECEIPT_BYTES,
                        f"campaign state {field} receipt",
                    )
                    if receipt_id in receipt_ids:
                        raise CampaignStoreError(
                            f"campaign state {field} contains duplicate receipt links"
                        )
                    receipt_ids.add(receipt_id)

    def _write_sharded_state(
        self,
        state: dict[str, Any],
        *,
        state_digest: str,
        state_size: int,
    ) -> dict[str, Any]:
        present_fields = [field for field in _SHARDED_FIELDS if field in state]
        collections: dict[str, list[dict[str, Any]]] = {}
        for field in _SHARDED_FIELDS:
            if field not in state:
                collections[field] = []
                self._entry_orders[field] = {}
                continue
            value = state[field]
            if field in _MAP_FIELDS:
                if not isinstance(value, dict):
                    raise CampaignStoreError(f"campaign state {field} must be an object")
                items = list(value.items())
                if any(not isinstance(key, str) or not key for key, _ in items):
                    raise CampaignStoreError(
                        f"campaign state {field} contains an invalid identity"
                    )
            else:
                if not isinstance(value, list):
                    raise CampaignStoreError(f"campaign state {field} must be a list")
                if field == "pending":
                    items = []
                    for item in value:
                        if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"]:
                            raise CampaignStoreError("campaign pending task identity is invalid")
                        items.append((item["id"], item))
                else:
                    items = [(str(index), item) for index, item in enumerate(value)]

            if field in {"receipts", "rejected_receipts"}:
                collections[field] = self._write_state_segments(field, value)
                self._entry_orders[field] = {
                    str(index): index for index in range(len(value))
                }
            else:
                entries, orders = self._ordered_state_entries(field, items)
                collections[field] = self._write_hash_shards(field, entries)
                self._entry_orders[field] = orders

        runtime = {key: value for key, value in state.items() if key not in _SHARDED_FIELDS}
        manifest = {
            "format_version": 2,
            "kind": "campaign_state_manifest",
            "source_id": self.source_id,
            "runtime": runtime,
            "present_fields": present_fields,
            "collections": collections,
            "materialized_state_sha256": state_digest,
            "materialized_state_size_bytes": state_size,
        }
        manifest_raw = _json_object_bytes(manifest, "campaign state manifest")
        if len(manifest_raw) > MAX_STATE_MANIFEST_BYTES:
            raise CampaignCapacityError(
                "campaign state manifest exceeds its 1 MiB safety limit; the legacy "
                "4 MiB monolithic-state allowance cannot hold unsharded runtime payloads"
            )
        return self._put_state_object("manifest", "root", manifest_raw)

    def _ordered_state_entries(
        self, field: str, items: list[tuple[str, Any]]
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        keys = [key for key, _ in items]
        if len(keys) != len(set(keys)):
            raise CampaignStoreError(f"campaign state {field} contains duplicate identities")
        previous = self._entry_orders.get(field, {})
        retained = [key for key in keys if key in previous]
        preserves_order = retained == sorted(retained, key=previous.__getitem__)
        seen_new = False
        append_only_new = True
        for key in keys:
            if key not in previous:
                seen_new = True
            elif seen_new:
                append_only_new = False
                break
        if preserves_order and append_only_new:
            next_order = max(previous.values(), default=-1) + 1
            orders: dict[str, int] = {}
            for key in keys:
                if key in previous:
                    orders[key] = previous[key]
                else:
                    orders[key] = next_order
                    next_order += 1
        else:
            orders = {key: index for index, key in enumerate(keys)}
        entries = [
            {"key": key, "order": orders[key], "value": value}
            for key, value in items
        ]
        return entries, orders

    def _write_hash_shards(
        self, field: str, entries: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        if not entries:
            return []
        kind = _SHARD_KIND[field]
        root_payload = {
            "format_version": 2,
            "kind": kind,
            "source_id": self.source_id,
            "key": "root",
            "entries": entries,
        }
        root_raw = _json_object_bytes(root_payload, f"campaign {field} shard")
        if len(root_raw) <= MAX_STATE_SHARD_BYTES:
            return [
                self._put_state_object(
                    kind, "root", root_raw, item_count=len(entries)
                )
            ]
        grouped: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            prefix = sha256(entry["key"].encode("utf-8")).hexdigest()[:1]
            grouped.setdefault(prefix, []).append(entry)
        descriptors: list[dict[str, Any]] = []
        for prefix, group in sorted(grouped.items()):
            descriptors.extend(self._write_hash_bucket(field, prefix, group))
        if len(descriptors) > MAX_STATE_SHARDS:
            raise CampaignCapacityError("campaign state exceeds the state-shard descriptor limit")
        return descriptors

    def _write_hash_bucket(
        self, field: str, prefix: str, entries: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        kind = _SHARD_KIND[field]
        payload = {
            "format_version": 2,
            "kind": kind,
            "source_id": self.source_id,
            "key": prefix,
            "entries": entries,
        }
        raw = _json_object_bytes(payload, f"campaign {field} shard")
        if len(raw) <= MAX_STATE_SHARD_BYTES:
            return [self._put_state_object(kind, prefix, raw, item_count=len(entries))]
        if len(prefix) >= 64:
            raise CampaignCapacityError(f"one campaign {field} entry exceeds the shard limit")
        children: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            digest = sha256(entry["key"].encode("utf-8")).hexdigest()
            child = digest[: len(prefix) + 1]
            children.setdefault(child, []).append(entry)
        if len(children) == 1:
            return self._write_hash_bucket(field, next(iter(children)), entries)
        result: list[dict[str, Any]] = []
        for child, group in sorted(children.items()):
            result.extend(self._write_hash_bucket(field, child, group))
        return result

    def _write_state_segments(
        self, field: str, values: list[Any]
    ) -> list[dict[str, Any]]:
        kind = _SHARD_KIND[field]
        previous: dict[str, Any] | None = None
        start = 0
        while start < len(values):
            stop = min(start + RECEIPT_SEGMENT_ITEMS, len(values))
            while True:
                entries = [
                    {"key": str(index), "order": index, "value": values[index]}
                    for index in range(start, stop)
                ]
                key = f"{start:012d}"
                payload = {
                    "format_version": 2,
                    "kind": kind,
                    "source_id": self.source_id,
                    "key": key,
                    "entries": entries,
                    "previous": previous,
                    "total_items": stop,
                }
                raw = _json_object_bytes(payload, f"campaign {field} segment")
                if len(raw) <= MAX_STATE_SHARD_BYTES:
                    break
                if stop - start == 1:
                    raise CampaignCapacityError(
                        f"one campaign {field} entry exceeds the segment limit"
                    )
                stop = start + max(1, (stop - start) // 2)
            previous = self._put_state_object(
                kind, key, raw, item_count=len(entries)
            )
            start = stop
        # The root manifest retains only the linked head.  Old full segments
        # remain immutable and a growing receipt history cannot grow the root.
        return [] if previous is None else [previous]

    def _put_state_object(
        self, kind: str, key: str, raw: bytes, *, item_count: int = 0
    ) -> dict[str, Any]:
        digest = sha256(raw).hexdigest()
        name = f"state-{kind}-{key}-{digest}.json"
        cached = self._verified_state_objects.get(name)
        if cached is not None:
            return dict(cached)
        root = self._states_root()
        found = self._find(name, root)
        if len(found) > 1:
            raise CampaignStoreError("ambiguous content-addressed campaign state object")
        if found:
            object_id = found[0]
            if self._read(object_id, "campaign state object") != raw:
                raise CampaignStoreError(
                    "existing content-addressed campaign state object has different bytes"
                )
        else:
            object_id = self._create_verified(
                name, raw, root, "campaign state object"
            )
        descriptor = {
            "id": object_id,
            "name": name,
            "sha256": digest,
            "size_bytes": len(raw),
            "item_count": item_count,
            "key": key,
        }
        self._verified_state_objects[name] = descriptor
        return dict(descriptor)

    def _load_sharded_state(self, pointer: dict[str, Any]) -> dict[str, Any]:
        descriptor = {
            "id": pointer.get("manifest_file_id"),
            "name": pointer.get("manifest_file_name"),
            "sha256": pointer.get("manifest_sha256"),
            "size_bytes": pointer.get("manifest_size_bytes"),
            "item_count": 0,
            "key": "root",
        }
        raw = self._read_state_object(
            descriptor, "manifest", maximum=MAX_STATE_MANIFEST_BYTES
        )
        manifest = _decode_object(raw, "campaign state manifest")
        required = {
            "format_version",
            "kind",
            "source_id",
            "runtime",
            "present_fields",
            "collections",
            "materialized_state_sha256",
            "materialized_state_size_bytes",
        }
        if set(manifest) != required or (
            manifest.get("format_version") != 2
            or manifest.get("kind") != "campaign_state_manifest"
            or manifest.get("source_id") != self.source_id
        ):
            raise CampaignStoreError("campaign state manifest identity/fields are invalid")
        runtime = manifest.get("runtime")
        if not isinstance(runtime, dict) or any(key in runtime for key in _SHARDED_FIELDS):
            raise CampaignStoreError("campaign state manifest runtime is invalid")
        if "source_id" in runtime and runtime["source_id"] != self.source_id:
            raise CampaignStoreError("campaign state runtime source identity is invalid")
        present = manifest.get("present_fields")
        if (
            not isinstance(present, list)
            or any(field not in _SHARDED_FIELDS for field in present)
            or len(present) != len(set(present))
            or present != [field for field in _SHARDED_FIELDS if field in present]
        ):
            raise CampaignStoreError("campaign state manifest present_fields are invalid")
        collections = manifest.get("collections")
        if not isinstance(collections, dict) or set(collections) != set(_SHARDED_FIELDS):
            raise CampaignStoreError("campaign state manifest collections are invalid")

        state = dict(runtime)
        seen_object_ids: set[str] = set()
        seen_object_names: set[str] = set()
        for field in _SHARDED_FIELDS:
            descriptors = collections[field]
            if not isinstance(descriptors, list) or len(descriptors) > MAX_STATE_SHARDS:
                raise CampaignStoreError(f"campaign state {field} descriptors are invalid")
            if field in {"receipts", "rejected_receipts"}:
                if len(descriptors) > 1:
                    raise CampaignStoreError(
                        f"campaign state {field} must contain at most one segment head"
                    )
                entries = self._load_state_segment_chain(
                    field,
                    descriptors[0] if descriptors else None,
                    seen_object_ids,
                    seen_object_names,
                )
                values, orders = self._materialize_state_entries(field, entries)
                self._entry_orders[field] = orders
                if field in present:
                    state[field] = values
                elif descriptors:
                    raise CampaignStoreError(
                        f"campaign state absent field {field} has unexpected descriptors"
                    )
                continue
            entries: list[dict[str, Any]] = []
            shard_keys: set[str] = set()
            for item in descriptors:
                normalized = self._validate_state_descriptor(item, field)
                if normalized["key"] in shard_keys:
                    raise CampaignStoreError(f"campaign state {field} has duplicate shard keys")
                if normalized["key"] == "root" and descriptors != [item]:
                    raise CampaignStoreError(
                        f"campaign state {field} root shard cannot have siblings"
                    )
                if field not in {"receipts", "rejected_receipts"} and any(
                    normalized["key"].startswith(existing)
                    or existing.startswith(normalized["key"])
                    for existing in shard_keys
                ):
                    raise CampaignStoreError(
                        f"campaign state {field} has overlapping shard prefixes"
                    )
                if normalized["id"] in seen_object_ids or normalized["name"] in seen_object_names:
                    raise CampaignStoreError("campaign state manifest reuses a state object")
                shard_keys.add(normalized["key"])
                seen_object_ids.add(normalized["id"])
                seen_object_names.add(normalized["name"])
                shard_raw = self._read_state_object(
                    normalized, _SHARD_KIND[field], maximum=MAX_STATE_SHARD_BYTES
                )
                shard = _decode_object(shard_raw, f"campaign {field} shard")
                if set(shard) != {"format_version", "kind", "source_id", "key", "entries"} or (
                    shard.get("format_version") != 2
                    or shard.get("kind") != _SHARD_KIND[field]
                    or shard.get("source_id") != self.source_id
                    or shard.get("key") != normalized["key"]
                    or not isinstance(shard.get("entries"), list)
                    or len(shard["entries"]) != normalized["item_count"]
                ):
                    raise CampaignStoreError(f"campaign {field} shard identity/fields are invalid")
                for entry in shard["entries"]:
                    entry_key = entry.get("key") if isinstance(entry, dict) else None
                    if (
                        not isinstance(entry_key, str)
                        or (
                            normalized["key"] != "root"
                            and not sha256(entry_key.encode("utf-8"))
                            .hexdigest()
                            .startswith(normalized["key"])
                        )
                    ):
                        raise CampaignStoreError(
                            f"campaign state {field} entry is in the wrong shard"
                        )
                entries.extend(shard["entries"])

            values, orders = self._materialize_state_entries(field, entries)
            self._entry_orders[field] = orders
            if field in present:
                state[field] = values
            elif descriptors:
                raise CampaignStoreError(
                    f"campaign state absent field {field} has unexpected descriptors"
                )

        expected_digest = _require_sha256(
            manifest.get("materialized_state_sha256"), "materialized campaign state SHA-256"
        )
        expected_size = _nonnegative_size(
            manifest.get("materialized_state_size_bytes"), "materialized campaign state"
        )
        try:
            state_digest, state_size = _json_object_identity(
                state, "materialized campaign state"
            )
        except MemoryError as exc:
            raise CampaignCapacityError(
                "available memory could not materialize the sharded campaign state"
            ) from exc
        if self._max_materialized_bytes is not None and state_size > self._max_materialized_bytes:
            raise CampaignCapacityError(
                "campaign state exceeds the configured materialized-runner resource budget"
            )
        if state_size != expected_size or state_digest != expected_digest:
            raise CampaignStoreError("materialized campaign state does not match its manifest")
        return state

    def _load_state_segment_chain(
        self,
        field: str,
        head: Any,
        seen_object_ids: set[str],
        seen_object_names: set[str],
    ) -> list[dict[str, Any]]:
        if head is None:
            return []
        entries: list[dict[str, Any]] = []
        descriptor = head
        expected_total: int | None = None
        segments = 0
        while descriptor is not None:
            segments += 1
            if segments > MAX_RECEIPT_CHAIN_SEGMENTS:
                raise CampaignCapacityError("campaign receipt segment chain is excessive")
            normalized = self._validate_state_descriptor(descriptor, field)
            if normalized["id"] in seen_object_ids or normalized["name"] in seen_object_names:
                raise CampaignStoreError("campaign state receipt segment chain contains a cycle")
            seen_object_ids.add(normalized["id"])
            seen_object_names.add(normalized["name"])
            raw = self._read_state_object(
                normalized, _SHARD_KIND[field], maximum=MAX_STATE_SHARD_BYTES
            )
            segment = _decode_object(raw, f"campaign {field} segment")
            required = {
                "format_version",
                "kind",
                "source_id",
                "key",
                "entries",
                "previous",
                "total_items",
            }
            segment_entries = segment.get("entries")
            total = segment.get("total_items")
            if set(segment) != required or (
                segment.get("format_version") != 2
                or segment.get("kind") != _SHARD_KIND[field]
                or segment.get("source_id") != self.source_id
                or segment.get("key") != normalized["key"]
                or not isinstance(segment_entries, list)
                or len(segment_entries) != normalized["item_count"]
                or isinstance(total, bool)
                or not isinstance(total, int)
                or total < 1
            ):
                raise CampaignStoreError(f"campaign {field} segment identity/fields are invalid")
            try:
                start = int(normalized["key"])
            except ValueError as exc:
                raise CampaignStoreError(
                    f"campaign state {field} segment key is invalid"
                ) from exc
            if total != start + len(segment_entries) or (
                expected_total is not None and total != expected_total
            ):
                raise CampaignStoreError(f"campaign state {field} segment totals are invalid")
            segment_orders = [
                entry.get("order") if isinstance(entry, dict) else None
                for entry in segment_entries
            ]
            if segment_orders != list(range(start, total)):
                raise CampaignStoreError(f"campaign state {field} segment range is invalid")
            entries.extend(segment_entries)
            descriptor = segment.get("previous")
            if descriptor is not None and not isinstance(descriptor, dict):
                raise CampaignStoreError(f"campaign state {field} previous segment is invalid")
            expected_total = start if descriptor is not None else None
            if descriptor is None and start != 0:
                raise CampaignStoreError(f"campaign state {field} segment chain is incomplete")
        return entries

    def _validate_state_descriptor(
        self, value: Any, field: str
    ) -> dict[str, Any]:
        required = {"id", "name", "sha256", "size_bytes", "item_count", "key"}
        if not isinstance(value, dict) or set(value) != required:
            raise CampaignStoreError(f"campaign state {field} descriptor fields are invalid")
        kind = _SHARD_KIND[field]
        key = value.get("key")
        if not isinstance(key, str) or not re.fullmatch(r"[a-z0-9]{1,64}", key):
            raise CampaignStoreError(f"campaign state {field} descriptor key is invalid")
        digest = _require_sha256(value.get("sha256"), f"campaign {field} shard SHA-256")
        expected_name = f"state-{kind}-{key}-{digest}.json"
        if value.get("name") != expected_name or not _STATE_OBJECT_RE.fullmatch(expected_name):
            raise CampaignStoreError(f"campaign state {field} descriptor name is unsafe")
        object_id = _require_object_id(value.get("id"), f"campaign {field} shard id")
        size = _bounded_size(
            value.get("size_bytes"), MAX_STATE_SHARD_BYTES, f"campaign {field} shard"
        )
        count = value.get("item_count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise CampaignStoreError(f"campaign state {field} item count is invalid")
        return {
            "id": object_id,
            "name": expected_name,
            "sha256": digest,
            "size_bytes": size,
            "item_count": count,
            "key": key,
        }

    def _read_state_object(
        self, descriptor: dict[str, Any], kind: str, *, maximum: int
    ) -> bytes:
        name = descriptor.get("name")
        digest = _require_sha256(descriptor.get("sha256"), "campaign state object SHA-256")
        key = descriptor.get("key")
        expected_name = f"state-{kind}-{key}-{digest}.json"
        if name != expected_name or not _STATE_OBJECT_RE.fullmatch(expected_name):
            raise CampaignStoreError("campaign state object name is unsafe")
        object_id = _require_object_id(descriptor.get("id"), "campaign state object id")
        expected_size = _bounded_size(
            descriptor.get("size_bytes"), maximum, "campaign state object"
        )
        matches = self._find(expected_name, self._states_root())
        if len(matches) != 1 or matches[0] != object_id:
            raise CampaignStoreError(
                "campaign state object is missing, ambiguous, or outside its source namespace"
            )
        raw = self._read(object_id, "campaign state object")
        if len(raw) != expected_size or sha256(raw).hexdigest() != digest:
            raise CampaignStoreError("campaign state object does not match its descriptor")
        self._verified_state_objects[expected_name] = dict(descriptor)
        return raw

    def _materialize_state_entries(
        self, field: str, entries: list[Any]
    ) -> tuple[Any, dict[str, int]]:
        normalized: list[tuple[int, str, Any]] = []
        keys: set[str] = set()
        orders: set[int] = set()
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {"key", "order", "value"}:
                raise CampaignStoreError(f"campaign state {field} entry fields are invalid")
            key = entry.get("key")
            order = entry.get("order")
            if not isinstance(key, str) or not key:
                raise CampaignStoreError(f"campaign state {field} entry key is invalid")
            if isinstance(order, bool) or not isinstance(order, int) or order < 0:
                raise CampaignStoreError(f"campaign state {field} entry order is invalid")
            if key in keys or order in orders:
                raise CampaignStoreError(f"campaign state {field} contains duplicate entries")
            if field == "pending" and (
                not isinstance(entry["value"], dict) or entry["value"].get("id") != key
            ):
                raise CampaignStoreError("campaign pending shard task identity is inconsistent")
            keys.add(key)
            orders.add(order)
            normalized.append((order, key, entry["value"]))
        normalized.sort()
        order_map = {key: order for order, key, _ in normalized}
        if field in _MAP_FIELDS:
            return {key: value for _, key, value in normalized}, order_map
        if field in {"receipts", "rejected_receipts"}:
            if [order for order, _, _ in normalized] != list(range(len(normalized))):
                raise CampaignStoreError(f"campaign state {field} segment order is not contiguous")
        return [value for _, _, value in normalized], order_map

    def put_raw(self, body: bytes, metadata: dict[str, Any]) -> dict[str, Any]:
        """Persist exact response bytes and an immutable metadata receipt."""
        if not isinstance(body, bytes):
            raise CampaignStoreError("raw response body must be bytes")
        if not body:
            raise CampaignStoreError("raw response body must be nonempty")
        if len(body) > MAX_RAW_BYTES:
            raise CampaignCapacityError(
                f"raw response exceeds the {MAX_RAW_BYTES}-byte (8 MiB) limit"
            )
        if not isinstance(metadata, dict):
            raise CampaignStoreError("raw response metadata must be a JSON object")

        digest = sha256(body).hexdigest()
        descriptor = {"id": "pending", "sha256": digest, "size_bytes": len(body)}
        # Validate the receipt before writing the raw object, so invalid or
        # excessive metadata cannot leave a new unreferenced response behind.
        receipt_payload = {
            "format_version": 1,
            "source_id": self.source_id,
            "kind": "raw_response",
            "raw": {"sha256": digest, "size_bytes": len(body)},
            "metadata": metadata,
        }
        _bounded_json(receipt_payload, "raw response receipt", MAX_RECEIPT_BYTES)

        name = _raw_name(digest)
        found = self._find(name, self._responses_root_id)
        if len(found) > 1:
            raise CampaignStoreError("ambiguous content-addressed raw response")
        if found:
            raw_id = _require_object_id(found[0], "raw response id")
            existing = self._read(raw_id, "raw response")
            if existing != body:
                raise CampaignStoreError(
                    "existing content-addressed raw response has different bytes"
                )
        else:
            raw_id = self._create_verified(
                name, body, self._responses_root_id, "raw response"
            )
        descriptor["id"] = raw_id

        receipt_payload["raw"] = dict(descriptor)
        self.put_receipt(receipt_payload)
        return descriptor

    def read_raw(self, descriptor: dict[str, Any]) -> bytes:
        """Read response bytes only after descriptor, namespace, size, and hash checks."""
        if not isinstance(descriptor, dict):
            raise CampaignStoreError("raw response descriptor must be an object")
        raw_id = _require_object_id(descriptor.get("id"), "raw response id")
        digest = _require_sha256(descriptor.get("sha256"), "raw response SHA-256")
        expected_size = _bounded_size(
            descriptor.get("size_bytes"), MAX_RAW_BYTES, "raw response"
        )
        found = self._find(_raw_name(digest), self._responses_root_id)
        if len(found) != 1 or found[0] != raw_id:
            raise CampaignStoreError(
                "raw response is missing, ambiguous, or outside this source namespace"
            )
        body = self._read(raw_id, "raw response")
        if len(body) > MAX_RAW_BYTES:
            raise CampaignCapacityError("raw response exceeds the 8 MiB limit")
        if len(body) != expected_size:
            raise CampaignStoreError("raw response size does not match its descriptor")
        if sha256(body).hexdigest() != digest:
            raise CampaignStoreError("raw response checksum does not match its descriptor")
        return body

    def put_receipt(self, metadata: dict[str, Any]) -> dict[str, Any]:
        """Persist canonical metadata as a content-addressed immutable receipt."""
        raw = _bounded_json(metadata, "campaign receipt", MAX_RECEIPT_BYTES)
        digest = sha256(raw).hexdigest()
        root = self._receipts_root()
        name = f"receipt-{digest}.json"
        found = self._find(name, root)
        if len(found) > 1:
            raise CampaignStoreError("ambiguous content-addressed campaign receipt")
        if found:
            receipt_id = _require_object_id(found[0], "campaign receipt id")
            if self._read(receipt_id, "campaign receipt") != raw:
                raise CampaignStoreError(
                    "existing content-addressed campaign receipt has different bytes"
                )
        else:
            receipt_id = self._create_verified(name, raw, root, "campaign receipt")
        return {"id": receipt_id, "sha256": digest, "size_bytes": len(raw)}

    def read_receipt(self, descriptor: dict[str, Any]) -> dict[str, Any]:
        """Read a source-scoped receipt after exact identity and content checks."""
        if not isinstance(descriptor, dict):
            raise CampaignStoreError("campaign receipt descriptor must be an object")
        receipt_id = _require_object_id(descriptor.get("id"), "campaign receipt id")
        digest = _require_sha256(
            descriptor.get("sha256"), "campaign receipt SHA-256"
        )
        expected_size = _bounded_size(
            descriptor.get("size_bytes"), MAX_RECEIPT_BYTES, "campaign receipt"
        )
        found = self._find(f"receipt-{digest}.json", self._receipts_root())
        if len(found) != 1 or found[0] != receipt_id:
            raise CampaignStoreError(
                "campaign receipt is missing, ambiguous, or outside this source namespace"
            )
        raw = self._read(receipt_id, "campaign receipt")
        if len(raw) > MAX_RECEIPT_BYTES:
            raise CampaignCapacityError("campaign receipt exceeds the 4 MiB limit")
        if len(raw) != expected_size:
            raise CampaignStoreError("campaign receipt size does not match its descriptor")
        if sha256(raw).hexdigest() != digest:
            raise CampaignStoreError("campaign receipt checksum does not match its descriptor")
        return _decode_object(raw, "campaign receipt")

    def load_landing_pointer(self) -> dict[str, Any] | None:
        """Observe the source-scoped Landing pointer for a later safe promotion."""
        observed = self._read_landing_pointer()
        self._expected_landing_pointer = observed
        self._landing_pointer_observed = True
        return None if observed is None else dict(observed.value)

    def put_landing_object(
        self, name: str, data: bytes, *, maximum_bytes: int = MAX_LANDING_FILE_BYTES
    ) -> dict[str, Any]:
        """Create and verify one immutable publication object in the source namespace."""
        _require_landing_object_name(name)
        if not isinstance(data, bytes) or not data:
            raise CampaignStoreError("Landing publication object must contain bytes")
        if (
            not isinstance(maximum_bytes, int)
            or isinstance(maximum_bytes, bool)
            or maximum_bytes < 1
            or maximum_bytes > MAX_LANDING_FILE_BYTES
        ):
            raise CampaignStoreError("invalid Landing publication object size limit")
        if len(data) > maximum_bytes:
            raise CampaignCapacityError(
                f"Landing publication object exceeds the {maximum_bytes}-byte safety limit"
            )
        object_id = self._create_verified(
            name, data, self._landing_root(), "Landing publication object"
        )
        return {
            "id": object_id,
            "name": name,
            "size": len(data),
            "sha256": sha256(data).hexdigest(),
        }

    def read_landing_object(
        self, descriptor: dict[str, Any], *, maximum_bytes: int = MAX_LANDING_FILE_BYTES
    ) -> bytes:
        """Read a publication object after namespace, size, and digest verification."""
        if not isinstance(descriptor, dict):
            raise CampaignStoreError("Landing publication descriptor must be an object")
        name = _require_landing_object_name(descriptor.get("name"))
        object_id = _require_object_id(descriptor.get("id"), "Landing publication object id")
        if (
            not isinstance(maximum_bytes, int)
            or isinstance(maximum_bytes, bool)
            or maximum_bytes < 1
            or maximum_bytes > MAX_LANDING_FILE_BYTES
        ):
            raise CampaignStoreError("invalid Landing publication object size limit")
        expected_size = _bounded_size(
            descriptor.get("size"), maximum_bytes, "Landing publication object"
        )
        digest = _require_sha256(
            descriptor.get("sha256"), "Landing publication object SHA-256"
        )
        found = self._find(name, self._landing_root())
        if len(found) != 1 or found[0] != object_id:
            raise CampaignStoreError(
                "Landing publication object is missing, ambiguous, or outside this source namespace"
            )
        raw = self._read(object_id, "Landing publication object")
        if len(raw) != expected_size or sha256(raw).hexdigest() != digest:
            raise CampaignStoreError("Landing publication object does not match its descriptor")
        return raw

    def promote_landing_pointer(self, pointer: dict[str, Any]) -> None:
        """Promote a fully written immutable snapshot with drift and readback checks."""
        pointer_raw = _json_object_bytes(pointer, "Landing snapshot pointer")
        if len(pointer_raw) > MAX_POINTER_BYTES:
            raise CampaignCapacityError("Landing snapshot pointer exceeds its safety limit")
        _validate_landing_pointer(pointer, self.source_id)
        if not self._landing_pointer_observed:
            self.load_landing_pointer()
        expected = self._expected_landing_pointer
        if not _same_pointer(self._read_landing_pointer(), expected):
            raise CampaignStoreError("current Landing pointer changed since it was loaded")
        if not _same_pointer(self._read_landing_pointer(), expected):
            raise CampaignStoreError("current Landing pointer changed during snapshot upload")
        promoted = self._promote_landing_pointer(expected, pointer_raw)
        self._expected_landing_pointer = promoted
        self._landing_pointer_observed = True

    def _states_root(self) -> str:
        return self._one_folder("states", self._control_root_id)

    def _receipts_root(self) -> str:
        return self._one_folder("receipts", self._control_root_id)

    def _landing_root(self) -> str:
        return self._one_folder("landing_publications", self._control_root_id)

    def _one_folder(self, name: str, parent_id: str) -> str:
        cache_key = (parent_id, name)
        cached = self._folder_ids.get(cache_key)
        if cached is not None:
            return cached
        found = self._find(name, parent_id)
        if len(found) > 1:
            raise CampaignStoreError(f"ambiguous {name} folders in source campaign")
        if found:
            folder_id = _require_object_id(found[0], f"{name} folder id")
            self._folder_ids[cache_key] = folder_id
            return folder_id
        folder_id = self._store.mkdir(name, parent_id)
        folder_id = _require_object_id(folder_id, f"{name} folder id")
        after = self._find(name, parent_id)
        if len(after) != 1 or after[0] != folder_id:
            raise CampaignStoreError(f"new {name} folder did not resolve unambiguously")
        self._folder_ids[cache_key] = folder_id
        return folder_id

    def _create_verified(self, name: str, data: bytes, parent_id: str, label: str) -> str:
        if self._find(name, parent_id):
            raise CampaignStoreError(f"{label} name already exists")
        try:
            file_id = _require_object_id(
                self._store.create(name, data, parent_id), f"{label} id"
            )
        except Exception as exc:
            try:
                found = self._find(name, parent_id)
                if len(found) == 1 and self._read(found[0], label) == data:
                    return found[0]
            except Exception as verify_error:
                raise UncertainCampaignWriteError(
                    f"{label} write failed and could not be verified"
                ) from verify_error
            raise UncertainCampaignWriteError(
                f"{label} write outcome is uncertain"
            ) from exc
        try:
            found = self._find(name, parent_id)
        except Exception as exc:
            raise UncertainCampaignWriteError(
                f"new {label} was created but its namespace could not be verified"
            ) from exc
        if len(found) != 1 or found[0] != file_id:
            raise UncertainCampaignWriteError(
                f"new {label} did not resolve unambiguously"
            )
        try:
            readback = self._read(file_id, label)
        except Exception as exc:
            raise UncertainCampaignWriteError(
                f"new {label} was created but its bytes could not be verified"
            ) from exc
        if readback != data or sha256(readback).digest() != sha256(data).digest():
            raise CampaignStoreError(f"{label} did not read back exactly")
        return file_id

    def _find(self, name: str, parent_id: str) -> list[str]:
        found = self._store.find(name, parent_id)
        if not isinstance(found, list):
            raise CampaignStoreError("campaign object lookup did not return a list")
        return [_require_object_id(item, "campaign object id") for item in found]

    def _read(self, file_id: str, label: str) -> bytes:
        try:
            raw = self._store.read(file_id)
        except CampaignStoreError:
            raise
        except Exception as exc:
            raise CampaignStoreError(f"unable to read {label}") from exc
        if not isinstance(raw, bytes):
            raise CampaignStoreError(f"{label} read did not return bytes")
        return raw

    def _read_pointer(self) -> _PointerObservation | None:
        found = self._find(_POINTER_NAME, self._control_root_id)
        if len(found) > 1:
            raise CampaignStoreError("ambiguous current campaign state pointers")
        if not found:
            return None
        file_id = found[0]
        raw = self._read(file_id, "current campaign state pointer")
        if len(raw) > MAX_POINTER_BYTES:
            raise CampaignCapacityError("current campaign state pointer exceeds its safety limit")
        value = _decode_object(raw, "current campaign state pointer")
        version = value.get("format_version")
        if version not in {1, 2} or value.get("source_id") != self.source_id:
            raise CampaignStoreError("current campaign state pointer has invalid identity")
        if version == 1:
            snapshot_name = value.get("state_file_name")
            if not isinstance(snapshot_name, str) or not _STATE_FILE_RE.fullmatch(snapshot_name):
                raise CampaignStoreError("current campaign pointer has an unsafe state file name")
            _require_object_id(value.get("state_file_id"), "state snapshot id")
            _require_sha256(value.get("state_sha256"), "state snapshot SHA-256")
            _bounded_size(value.get("state_size_bytes"), MAX_STATE_BYTES, "state snapshot")
        else:
            required = {
                "format_version",
                "source_id",
                "manifest_file_id",
                "manifest_file_name",
                "manifest_sha256",
                "manifest_size_bytes",
                "state_file_id",
                "state_file_name",
                "state_sha256",
                "state_size_bytes",
            }
            if set(value) != required:
                raise CampaignStoreError("current campaign v2 pointer fields are invalid")
            digest = _require_sha256(
                value.get("manifest_sha256"), "state manifest SHA-256"
            )
            expected_name = f"state-manifest-root-{digest}.json"
            if value.get("manifest_file_name") != expected_name:
                raise CampaignStoreError("current campaign manifest name is unsafe")
            manifest_id = _require_object_id(value.get("manifest_file_id"), "state manifest id")
            manifest_size = _bounded_size(
                value.get("manifest_size_bytes"),
                MAX_STATE_MANIFEST_BYTES,
                "state manifest",
            )
            if (
                value.get("state_file_id") != manifest_id
                or value.get("state_file_name") != expected_name
                or value.get("state_sha256") != digest
                or value.get("state_size_bytes") != manifest_size
            ):
                raise CampaignStoreError("current campaign v2 pointer aliases are inconsistent")
        return _PointerObservation(file_id, raw, value)

    def _promote_pointer(
        self, expected: _PointerObservation | None, pointer_raw: bytes
    ) -> _PointerObservation:
        if expected is None:
            try:
                pointer_id = _require_object_id(
                    self._store.create(_POINTER_NAME, pointer_raw, self._control_root_id),
                    "campaign state pointer id",
                )
            except Exception as exc:
                return self._resolve_promotion_after_error(None, pointer_raw, exc)
        else:
            pointer_id = expected.file_id
            try:
                self._store.replace(pointer_id, pointer_raw)
            except Exception as exc:
                return self._resolve_promotion_after_error(expected, pointer_raw, exc)

        try:
            observed = self._read_pointer()
        except Exception as exc:
            raise UncertainCampaignPointerError(
                "campaign state pointer promotion completed but its outcome is ambiguous"
            ) from exc
        if observed is None or observed.file_id != pointer_id or observed.raw != pointer_raw:
            raise UncertainCampaignPointerError(
                "campaign state pointer readback differs after promotion"
            )
        return observed

    def _resolve_promotion_after_error(
        self,
        expected: _PointerObservation | None,
        pointer_raw: bytes,
        error: Exception,
    ) -> _PointerObservation:
        try:
            observed = self._read_pointer()
        except Exception as read_error:
            raise UncertainCampaignPointerError(
                "campaign state pointer update failed and could not be verified"
            ) from read_error
        if observed is not None and observed.raw == pointer_raw:
            return observed
        if _same_pointer(observed, expected):
            raise CampaignStoreError(
                "campaign state pointer update failed; previous trusted state was retained"
            ) from error
        raise UncertainCampaignPointerError(
            "campaign state pointer update outcome is uncertain; no success was recorded"
        ) from error

    def _read_landing_pointer(self) -> _PointerObservation | None:
        found = self._find(_LANDING_POINTER_NAME, self._control_root_id)
        if len(found) > 1:
            raise CampaignStoreError("ambiguous current Landing pointers")
        if not found:
            return None
        file_id = found[0]
        raw = self._read(file_id, "current Landing pointer")
        if len(raw) > MAX_POINTER_BYTES:
            raise CampaignCapacityError("current Landing pointer exceeds its safety limit")
        value = _decode_object(raw, "current Landing pointer")
        _validate_landing_pointer(value, self.source_id)
        return _PointerObservation(file_id, raw, value)

    def _promote_landing_pointer(
        self, expected: _PointerObservation | None, pointer_raw: bytes
    ) -> _PointerObservation:
        if expected is None:
            try:
                pointer_id = _require_object_id(
                    self._store.create(
                        _LANDING_POINTER_NAME, pointer_raw, self._control_root_id
                    ),
                    "Landing pointer id",
                )
            except Exception as exc:
                return self._resolve_landing_promotion_after_error(None, pointer_raw, exc)
        else:
            pointer_id = expected.file_id
            try:
                self._store.replace(pointer_id, pointer_raw)
            except Exception as exc:
                return self._resolve_landing_promotion_after_error(expected, pointer_raw, exc)
        try:
            observed = self._read_landing_pointer()
        except Exception as exc:
            raise UncertainCampaignPointerError(
                "Landing pointer promotion completed but its outcome is ambiguous"
            ) from exc
        if observed is None or observed.file_id != pointer_id or observed.raw != pointer_raw:
            raise UncertainCampaignPointerError(
                "Landing pointer readback differs after promotion"
            )
        return observed

    def _resolve_landing_promotion_after_error(
        self,
        expected: _PointerObservation | None,
        pointer_raw: bytes,
        error: Exception,
    ) -> _PointerObservation:
        try:
            observed = self._read_landing_pointer()
        except Exception as read_error:
            raise UncertainCampaignPointerError(
                "Landing pointer update failed and could not be verified"
            ) from read_error
        if observed is not None and observed.raw == pointer_raw:
            return observed
        if _same_pointer(observed, expected):
            raise CampaignStoreError(
                "Landing pointer update failed; previous trusted snapshot was retained"
            ) from error
        raise UncertainCampaignPointerError(
            "Landing pointer update outcome is uncertain; no success was recorded"
        ) from error


class DriveCampaignStore(_CampaignStore):
    """Campaign transport rooted inside the selected configured Drive tree."""

    def __init__(
        self, storage: Any, source_id: str, *, max_materialized_bytes: int | None = None
    ) -> None:
        # LocalCampaignStore remains usable when Google dependencies are not
        # installed; only constructing the Drive backend imports them.
        from ingestion.drive_state_store import DriveStateStore

        source_id = _require_source_id(source_id)
        selected_root = storage.resolve_root(create=True)
        control_root = storage.get_or_create_nested_folder(
            ["06_control", "source_campaigns", source_id], root_id=selected_root
        )
        landing_root = storage.resolve_zone("landing", create=True)
        responses_root = storage.get_or_create_nested_folder(
            [source_id, "responses"], root_id=landing_root
        )
        transport = DriveStateStore(
            storage, selected_root, control_root, allow_landing_pointer=True
        )
        super().__init__(
            transport,
            source_id,
            control_root,
            responses_root,
            max_materialized_bytes=max_materialized_bytes,
        )


class LocalCampaignStore(_CampaignStore):
    """Filesystem campaign transport; it never initializes or calls Drive."""

    def __init__(
        self, root: Path, source_id: str, *, max_materialized_bytes: int | None = None
    ) -> None:
        source_id = _require_source_id(source_id)
        if not isinstance(root, Path):
            raise CampaignStoreError("local campaign root must be a pathlib.Path")
        transport = _LocalObjectStore(root)
        control_root = transport.ensure_path(["06_control", "source_campaigns", source_id])
        responses_root = transport.ensure_path(["01_landing", source_id, "responses"])
        super().__init__(
            transport,
            source_id,
            control_root,
            responses_root,
            max_materialized_bytes=max_materialized_bytes,
        )


class _LocalObjectStore:
    """Small persistent filesystem implementation of the object-store protocol."""

    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.is_symlink() or not self.root.is_dir():
            raise CampaignStoreError("local campaign root must be a real directory")

    def ensure_path(self, segments: list[str]) -> str:
        current = self.root
        for segment in segments:
            _require_segment(segment)
            current = current / segment
            existed = current.exists()
            current.mkdir(exist_ok=True)
            if current.is_symlink() or not current.is_dir():
                raise CampaignStoreError("local campaign path contains a non-directory")
            if not existed:
                _fsync_directory(current.parent)
        return self._id(current)

    def find(self, name: str, parent_id: str) -> list[str]:
        _require_segment(name)
        parent = self._path(parent_id)
        if not parent.is_dir() or parent.is_symlink():
            raise CampaignStoreError("local campaign parent is not a real directory")
        candidate = parent / name
        return [self._id(candidate)] if candidate.exists() or candidate.is_symlink() else []

    def create(self, name: str, data: bytes, parent_id: str) -> str:
        _require_segment(name)
        if not isinstance(data, bytes) or not data:
            raise CampaignStoreError("campaign objects must contain bytes")
        parent = self._path(parent_id)
        path = parent / name
        try:
            with path.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            _fsync_directory(parent)
        except FileExistsError as exc:
            raise CampaignStoreError("campaign object already exists") from exc
        return self._id(path)

    def read(self, file_id: str) -> bytes:
        path = self._path(file_id)
        if path.is_symlink() or not path.is_file():
            raise CampaignStoreError("local campaign object is not a real file")
        if path.stat().st_size > MAX_RAW_BYTES:
            raise CampaignCapacityError("local campaign object exceeds the 8 MiB read limit")
        return path.read_bytes()

    def replace(self, file_id: str, data: bytes) -> None:
        path = self._path(file_id)
        pointer_names = {_POINTER_NAME, _LANDING_POINTER_NAME}
        expected_parent = self.root / "06_control" / "source_campaigns" / path.parent.name
        if (
            path.parent != expected_parent
            or path.name not in pointer_names
            or path.is_symlink()
            or not path.is_file()
        ):
            raise CampaignStoreError("only a source campaign pointer may be replaced")
        temporary = path.with_name(f".{path.name}.{uuid4()}.tmp")
        try:
            with temporary.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            _fsync_directory(path.parent)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def mkdir(self, name: str, parent_id: str) -> str:
        _require_segment(name)
        parent = self._path(parent_id)
        path = parent / name
        try:
            path.mkdir()
        except FileExistsError as exc:
            raise CampaignStoreError("campaign folder already exists") from exc
        _fsync_directory(parent)
        return self._id(path)

    def _path(self, object_id: str) -> Path:
        if not isinstance(object_id, str) or not object_id:
            raise CampaignStoreError("unsafe local campaign object id")
        relative = Path(object_id)
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise CampaignStoreError("unsafe local campaign object id")
        candidate = self.root
        for part in relative.parts:
            candidate = candidate / part
            if candidate.is_symlink():
                raise CampaignStoreError("local campaign object path contains a symbolic link")
        return candidate

    def _id(self, path: Path) -> str:
        try:
            return path.relative_to(self.root).as_posix()
        except ValueError as exc:  # pragma: no cover - all callers build below root
            raise CampaignStoreError("local campaign object escapes its root") from exc


def _same_pointer(
    left: _PointerObservation | None, right: _PointerObservation | None
) -> bool:
    if left is None or right is None:
        return left is right
    return left.file_id == right.file_id and left.raw == right.raw


def _raw_name(digest: str) -> str:
    return f"response-{digest}.bin"


def _require_source_id(value: Any) -> str:
    if not isinstance(value, str) or not _SOURCE_ID_RE.fullmatch(value):
        raise CampaignStoreError(
            "unsafe source_id; use lowercase letters, digits, and underscores"
        )
    return value


def _require_object_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 1024:
        raise CampaignStoreError(f"unsafe {label}")
    if any(ord(char) < 32 for char in value):
        raise CampaignStoreError(f"unsafe {label}")
    return value


def _require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise CampaignStoreError(f"invalid {label}")
    return value


def _require_landing_object_name(value: Any) -> str:
    if not isinstance(value, str) or not _LANDING_OBJECT_RE.fullmatch(value):
        raise CampaignStoreError("unsafe Landing publication object name")
    return value


def _validate_landing_pointer(value: dict[str, Any], source_id: str) -> None:
    if value.get("format_version") != 1 or value.get("source_id") != source_id:
        raise CampaignStoreError("current Landing pointer has invalid identity")
    snapshot_id = value.get("snapshot_id")
    if (
        not isinstance(snapshot_id, str)
        or not re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            snapshot_id,
        )
    ):
        raise CampaignStoreError("current Landing pointer has invalid snapshot identity")
    name = _require_landing_object_name(value.get("manifest_file_name"))
    if name != f"manifest-{snapshot_id}.json":
        raise CampaignStoreError("current Landing pointer manifest name does not match snapshot")
    _require_object_id(value.get("manifest_file_id"), "Landing manifest id")
    _require_sha256(value.get("manifest_sha256"), "Landing manifest SHA-256")
    _bounded_size(
        value.get("manifest_size_bytes"),
        MAX_LANDING_MANIFEST_BYTES,
        "Landing manifest",
    )


def _bounded_size(value: Any, maximum: int, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise CampaignStoreError(f"invalid {label} size")
    if value > maximum:
        raise CampaignCapacityError(f"{label} exceeds the {maximum}-byte safety limit")
    return value


def _json_object_bytes(value: Any, label: str) -> bytes:
    if not isinstance(value, dict):
        raise CampaignStoreError(f"{label} must be a JSON object")
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise CampaignStoreError(f"{label} is not safely JSON serializable") from exc


def _json_object_identity(value: Any, label: str) -> tuple[str, int]:
    """Return canonical JSON digest/size without allocating the complete encoding."""
    if not isinstance(value, dict):
        raise CampaignStoreError(f"{label} must be a JSON object")
    encoder = json.JSONEncoder(
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    digest = sha256()
    size = 0
    try:
        for text_part in encoder.iterencode(value):
            part = text_part.encode("utf-8")
            digest.update(part)
            size += len(part)
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise CampaignStoreError(f"{label} is not safely JSON serializable") from exc
    return digest.hexdigest(), size


def _bounded_json(value: Any, label: str, maximum: int) -> bytes:
    raw = _json_object_bytes(value, label)
    if len(raw) > maximum:
        raise CampaignCapacityError(f"{label} exceeds the {maximum}-byte safety limit")
    return raw


def _nonnegative_size(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CampaignStoreError(f"invalid {label} size")
    return value


def _decode_object(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CampaignStoreError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise CampaignStoreError(f"{label} must be a JSON object")
    return value


def _require_segment(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
        or any(ord(char) < 32 for char in value)
    ):
        raise CampaignStoreError("unsafe local campaign path segment")
    return value


def _fsync_directory(path: Path) -> None:
    """Make a local create/rename directory entry durable before returning."""
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "CampaignCapacityError",
    "CampaignStoreError",
    "DriveCampaignStore",
    "LocalCampaignStore",
    "MAX_RAW_BYTES",
    "MAX_LANDING_FILE_BYTES",
    "MAX_LANDING_MANIFEST_BYTES",
    "MAX_STATE_BYTES",
    "UncertainCampaignPointerError",
    "UncertainCampaignWriteError",
]
