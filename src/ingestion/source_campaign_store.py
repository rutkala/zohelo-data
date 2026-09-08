"""Bounded durable transport for one non-NBP source ingestion campaign.

The public stores expose source-scoped state and exact raw response persistence.
State publication follows the repository's immutable-snapshot-plus-pointer model;
the only mutable object is ``current-ingestion-state.json`` inside the source's
own control directory.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from typing import Any, Protocol
from uuid import uuid4

MAX_STATE_BYTES = 4 * 1024 * 1024
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
_POINTER_NAME = "current-ingestion-state.json"
_LANDING_POINTER_NAME = "current-landing.json"
_LANDING_OBJECT_RE = re.compile(
    r"^(?:fragment|manifest)-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\.(?:parquet|json)$"
)


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
    ) -> None:
        self._store = store
        self.source_id = _require_source_id(source_id)
        self._control_root_id = _require_object_id(control_root_id, "control root id")
        self._responses_root_id = _require_object_id(responses_root_id, "responses root id")
        self._expected_pointer: _PointerObservation | None = None
        self._pointer_observed = False
        self._expected_landing_pointer: _PointerObservation | None = None
        self._landing_pointer_observed = False

    def load(self) -> dict[str, Any] | None:
        """Load and verify the current immutable state snapshot, if it exists."""
        observed = self._read_pointer()
        if observed is None:
            self._expected_pointer = None
            self._pointer_observed = True
            return None

        pointer = observed.value
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

        self._expected_pointer = observed
        self._pointer_observed = True
        return state

    def save(self, state: dict[str, Any]) -> None:
        """Persist an immutable state snapshot, then safely promote its pointer."""
        state_raw = _json_object_bytes(state, "campaign state")
        if len(state_raw) > MAX_STATE_BYTES:
            raise CampaignCapacityError(
                f"campaign state exceeds the {MAX_STATE_BYTES}-byte (4 MiB) limit"
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

        states_root = self._states_root()
        snapshot_name = f"state-{uuid4()}.json"
        snapshot_id = self._create_verified(
            snapshot_name, state_raw, states_root, "state snapshot"
        )
        pointer = {
            "format_version": 1,
            "source_id": self.source_id,
            "state_file_id": snapshot_id,
            "state_file_name": snapshot_name,
            "state_sha256": sha256(state_raw).hexdigest(),
            "state_size_bytes": len(state_raw),
        }
        pointer_raw = _json_object_bytes(pointer, "campaign state pointer")
        if len(pointer_raw) > MAX_POINTER_BYTES:  # defensive; normal pointers are tiny
            raise CampaignCapacityError("campaign state pointer exceeds its safety limit")

        # This is deliberately fresh and immediately precedes the only mutable
        # operation. Drive has no compare-and-swap, so the root workflow must
        # still serialize writers for each provider.
        if not _same_pointer(self._read_pointer(), expected):
            raise CampaignStoreError("current campaign pointer changed during state upload")

        promoted = self._promote_pointer(expected, pointer_raw)
        self._expected_pointer = promoted
        self._pointer_observed = True

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
        found = self._find(name, parent_id)
        if len(found) > 1:
            raise CampaignStoreError(f"ambiguous {name} folders in source campaign")
        if found:
            return _require_object_id(found[0], f"{name} folder id")
        folder_id = self._store.mkdir(name, parent_id)
        folder_id = _require_object_id(folder_id, f"{name} folder id")
        after = self._find(name, parent_id)
        if len(after) != 1 or after[0] != folder_id:
            raise CampaignStoreError(f"new {name} folder did not resolve unambiguously")
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
        found = self._find(name, parent_id)
        if len(found) != 1 or found[0] != file_id:
            raise UncertainCampaignWriteError(
                f"new {label} did not resolve unambiguously"
            )
        readback = self._read(file_id, label)
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
        if value.get("format_version") != 1 or value.get("source_id") != self.source_id:
            raise CampaignStoreError("current campaign state pointer has invalid identity")
        snapshot_name = value.get("state_file_name")
        if not isinstance(snapshot_name, str) or not _STATE_FILE_RE.fullmatch(snapshot_name):
            raise CampaignStoreError("current campaign pointer has an unsafe state file name")
        _require_object_id(value.get("state_file_id"), "state snapshot id")
        _require_sha256(value.get("state_sha256"), "state snapshot SHA-256")
        _bounded_size(value.get("state_size_bytes"), MAX_STATE_BYTES, "state snapshot")
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

    def __init__(self, storage: Any, source_id: str) -> None:
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
        super().__init__(transport, source_id, control_root, responses_root)


class LocalCampaignStore(_CampaignStore):
    """Filesystem campaign transport; it never initializes or calls Drive."""

    def __init__(self, root: Path, source_id: str) -> None:
        source_id = _require_source_id(source_id)
        if not isinstance(root, Path):
            raise CampaignStoreError("local campaign root must be a pathlib.Path")
        transport = _LocalObjectStore(root)
        control_root = transport.ensure_path(["06_control", "source_campaigns", source_id])
        responses_root = transport.ensure_path(["01_landing", source_id, "responses"])
        super().__init__(transport, source_id, control_root, responses_root)


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


def _bounded_json(value: Any, label: str, maximum: int) -> bytes:
    raw = _json_object_bytes(value, label)
    if len(raw) > maximum:
        raise CampaignCapacityError(f"{label} exceeds the {maximum}-byte safety limit")
    return raw


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
