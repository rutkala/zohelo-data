"""Landing-to-Bronze Parquet transformation loader for GUS DBW.

Memory-bounded, vectorized streaming architecture:
- Reads original native files from Google Drive (01_landing/gus_dbw/native/)
- Streams CSVs directly into DuckDB with strict 300MB memory ceiling
- Generates partitioned ZSTD Apache Parquet files per indicator
- Fully idempotent: skips already processed indicators in Google Drive
- Tracks progress in 06_control/source_campaigns/gus_dbw_bronze/checkpoint.json
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
from typing import Any
import zipfile

import duckdb
from googleapiclient.http import MediaFileUpload
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage_manager import StorageManager

logger = logging.getLogger("dbw_bronze_loader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

DRIVE_LOCK = threading.Lock()
LANDING_COMPLETION_PREFIX = "landing-complete-v2"


class DBWLandingIncompleteError(RuntimeError):
    """The DBW bulk Landing contract has not been completed and verified."""


def _require_production_context(allow_codespace: bool = False):
    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    on_main = os.environ.get("GITHUB_REF") == "refs/heads/main"
    codespace_opt_in = (
        allow_codespace
        or os.environ.get("ZOHELO_ALLOW_CODESPACE_EXECUTION", "").lower() == "true"
    )
    if in_actions and on_main:
        return
    if codespace_opt_in:
        return
    raise PermissionError(
        "DBW Bronze writes require serialized main-branch GitHub Actions or explicit "
        "Codespace production authorization."
    )


def _escape_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _hash_file(path: Path) -> tuple[str, str]:
    d_sha = hashlib.sha256()
    d_md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            d_sha.update(chunk)
            d_md5.update(chunk)
    return d_sha.hexdigest(), d_md5.hexdigest()


def _has_integrity_metadata(item: dict[str, Any] | None, *, min_size: int = 0) -> bool:
    """Return whether a Drive object has the checksums required for safe resume."""
    if not item:
        return False
    try:
        size = int(item.get("size", -1))
    except (TypeError, ValueError):
        return False
    props = item.get("appProperties") or {}
    return (
        size > min_size
        and re.fullmatch(r"[0-9a-f]{32}", item.get("md5Checksum", "")) is not None
        and re.fullmatch(r"[0-9a-f]{64}", props.get("sha256", "")) is not None
    )


def _verify_native_bytes(content: bytes, descriptor: dict[str, Any]) -> None:
    """Verify downloaded Landing bytes against the identity-bound receipt."""
    if (
        len(content) != descriptor["size"]
        or hashlib.sha256(content).hexdigest() != descriptor["sha256"]
        or hashlib.md5(content).hexdigest() != descriptor["md5"]
    ):
        raise DBWLandingIncompleteError(
            f"DBW Landing object failed byte verification: {descriptor['name']}"
        )


def _membership_sha256(objects: list[dict[str, Any]]) -> str:
    canonical = sorted(
        (
            item["role"], item["source_name"], item["id"], item["name"],
            item["size"], item["sha256"], item["md5"],
        )
        for item in objects
    )
    return hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _snapshot_sha256(native_snapshot_id: str, memberships: dict[int, str]) -> str:
    canonical = {
        "native_snapshot_id": native_snapshot_id,
        "memberships": sorted(
            (indicator_id, digest) for indicator_id, digest in memberships.items()
        ),
    }
    return hashlib.sha256(
        json.dumps(canonical, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _upload_file_to_drive(
    storage: StorageManager,
    local_path: Path,
    name: str,
    parent_id: str,
    mime_type: str = "application/octet-stream",
) -> dict[str, Any]:
    sha256_hex, md5_hex = _hash_file(local_path)
    query = f"name='{_escape_query(name)}' and '{_escape_query(parent_id)}' in parents and trashed=false"
    with DRIVE_LOCK:
        existing = storage.drive_service.files().list(
            q=query, spaces="drive", fields="files(id,name,size,md5Checksum,appProperties)"
        ).execute().get("files", [])

    if len(existing) > 1:
        raise RuntimeError(f"Ambiguous existing DBW Bronze object: {name}")
    if existing:
        item = existing[0]
        props = item.get("appProperties") or {}
        if (
            props.get("sha256") == sha256_hex
            and item.get("md5Checksum") == md5_hex
            and int(item.get("size", -1)) == local_path.stat().st_size
        ):
            return {"id": item["id"], "name": name, "size": local_path.stat().st_size, "reused": True}
        raise RuntimeError(
            f"Existing DBW Bronze object differs from the candidate: {name}. "
            "The prior object was preserved; publish a versioned release instead of replacing it."
        )

    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True)
    body = {"name": name, "parents": [parent_id], "appProperties": {"sha256": sha256_hex}}
    with DRIVE_LOCK:
        created = storage.drive_service.files().create(
            body=body, media_body=media, fields="id,name,size,md5Checksum,appProperties"
        ).execute(num_retries=4)
    if (
        not created
        or created.get("name") != name
        or int(created.get("size", -1)) != local_path.stat().st_size
        or created.get("md5Checksum") != md5_hex
        or (created.get("appProperties") or {}).get("sha256") != sha256_hex
    ):
        raise RuntimeError(f"DBW Bronze upload did not verify: {name}")
    return {"id": created["id"], "name": name, "size": int(created["size"]), "reused": False}


def _restore_verified_drive_file(
    storage: StorageManager,
    *,
    name: str,
    parent_id: str,
    local_path: Path,
) -> bool:
    """Restore an immutable output only after Drive metadata and bytes agree."""
    query = f"name='{_escape_query(name)}' and '{_escape_query(parent_id)}' in parents and trashed=false"
    with DRIVE_LOCK:
        files = storage.drive_service.files().list(
            q=query,
            spaces="drive",
            fields="files(id,name,size,md5Checksum,appProperties,trashed)",
        ).execute(num_retries=4).get("files", [])
    matches = [item for item in files if item.get("name") == name and item.get("trashed") is not True]
    if not matches:
        return False
    if len(matches) != 1:
        raise RuntimeError(f"Ambiguous existing DBW Bronze object: {name}")
    item = matches[0]
    expected_sha = (item.get("appProperties") or {}).get("sha256")
    if not isinstance(expected_sha, str) or len(expected_sha) != 64:
        raise RuntimeError(f"Existing DBW Bronze object has no verified SHA-256: {name}")
    with DRIVE_LOCK:
        raw = storage.drive_service.files().get_media(fileId=item["id"]).execute(num_retries=4)
    sha = hashlib.sha256(raw).hexdigest()
    md5 = hashlib.md5(raw).hexdigest()
    if (
        sha != expected_sha
        or md5 != item.get("md5Checksum")
        or len(raw) != int(item.get("size", -1))
    ):
        raise RuntimeError(f"Existing DBW Bronze object failed byte verification: {name}")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(raw)
    return True


def validate_landing_completion(document: dict[str, Any]) -> dict[str, Any]:
    """Validate the only evidence that authorizes DBW Landing-to-Bronze work."""
    required = {
        "schema_version": 2,
        "record_type": "gus_dbw_landing_completion",
        "source_id": "gus_dbw",
        "status": "complete_current_catalogue",
        "landing_scope": "native_bytes_only",
        "bulk_complete": True,
        "metadata_complete": True,
        "pending_indicators": 0,
        "failed_indicators": 0,
    }
    for key, expected in required.items():
        if document.get(key) != expected:
            raise DBWLandingIncompleteError(
                f"DBW Landing completion field {key!r} must be {expected!r}; "
                f"received {document.get(key)!r}."
            )
    catalogue = document.get("catalogue_indicators")
    completed = document.get("completed_indicators")
    if not isinstance(catalogue, int) or catalogue <= 0 or completed != catalogue:
        raise DBWLandingIncompleteError(
            "DBW Landing completion must reconcile every catalogue indicator."
        )
    tree_sha = document.get("catalogue_sha256")
    if not isinstance(tree_sha, str) or len(tree_sha) != 64:
        raise DBWLandingIncompleteError("DBW Landing completion has no valid catalogue SHA-256.")
    snapshot_id = document.get("native_snapshot_id")
    if not isinstance(snapshot_id, str) or re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
        snapshot_id,
    ) is None:
        raise DBWLandingIncompleteError("DBW Landing completion has no valid native snapshot ID.")
    snapshot_sha = document.get("native_snapshot_sha256")
    if not isinstance(snapshot_sha, str) or re.fullmatch(r"[0-9a-f]{64}", snapshot_sha) is None:
        raise DBWLandingIncompleteError("DBW Landing completion has no valid native snapshot SHA-256.")
    return document


def validate_full_release_selection(
    sample_indicators: int | None,
    indicator_ids: list[int] | None,
) -> None:
    """Reject ad-hoc subsets that could masquerade as an immutable production release."""
    if sample_indicators is not None or indicator_ids:
        raise ValueError(
            "Partial DBW Bronze selections are not permitted in the complete production release. "
            "Use the resumable full-catalogue run instead."
        )


def _resolve_existing_folder(storage: StorageManager, name: str, parent_id: str) -> str:
    query = (
        f"name='{_escape_query(name)}' and "
        "mimeType='application/vnd.google-apps.folder' and "
        f"'{_escape_query(parent_id)}' in parents and trashed=false"
    )
    with DRIVE_LOCK:
        items = storage.drive_service.files().list(
            q=query, spaces="drive", fields="files(id,name,mimeType,trashed)"
        ).execute(num_retries=4).get("files", [])
    matches = [item for item in items if item.get("name") == name and item.get("trashed") is not True]
    if len(matches) != 1:
        raise DBWLandingIncompleteError(
            f"Expected exactly one existing DBW Landing folder {name!r}; found {len(matches)}."
        )
    return matches[0]["id"]


class DBWBronzeLoader:
    def __init__(
        self,
        workspace: Path,
        storage: StorageManager,
        allow_codespace: bool = False,
    ):
        self.base_workspace = workspace
        self.workspace = workspace
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.storage = storage
        self.allow_codespace = allow_codespace
        _require_production_context(allow_codespace)

        self.session = storage.begin_write_session()
        self.landing_root = storage.resolve_zone("landing", create=False)
        self.bronze_root = storage.resolve_zone("bronze", create=True)
        self.control_root = storage.resolve_zone("control", create=True)

        # Resolve Landing strictly read-only. Bronze must not create or repair its input layer.
        self.dbw_landing = _resolve_existing_folder(storage, "gus_dbw", self.landing_root)
        self.landing_native = _resolve_existing_folder(storage, "native", self.dbw_landing)
        self.landing_taxonomy = _resolve_existing_folder(storage, "taxonomy", self.landing_native)
        self.landing_metadata = _resolve_existing_folder(storage, "metadata", self.landing_native)
        self.landing_bulk = _resolve_existing_folder(storage, "bulk", self.landing_native)
        self.landing_control = _resolve_existing_folder(storage, "_control", self.dbw_landing)
        self.landing_checkpoints = _resolve_existing_folder(
            storage, "checkpoints", self.landing_control
        )

        # Resolve Bronze directories
        self.dbw_bronze = storage.get_or_create_nested_folder(["gus_dbw"], root_id=self.bronze_root, write_session=self.session)
        self.releases_root = storage.get_or_create_nested_folder(["releases"], root_id=self.dbw_bronze, write_session=self.session)

        # Campaign control directory
        self.campaign_control = storage.get_or_create_nested_folder(
            ["source_campaigns", "gus_dbw_bronze"], root_id=self.control_root, write_session=self.session
        )

    def require_complete_landing(self) -> dict[str, Any]:
        """Fail closed unless the Web bulk writer exhausted and reconciled its catalogue."""
        query = (
            f"name contains '{LANDING_COMPLETION_PREFIX}' and "
            f"'{_escape_query(self.landing_control)}' in parents and trashed=false"
        )
        files: list[dict[str, Any]] = []
        token = None
        while True:
            args: dict[str, Any] = {
                "q": query,
                "spaces": "drive",
                "pageSize": 1000,
                "fields": "nextPageToken,files(id,name,size,md5Checksum,appProperties,createdTime,trashed)",
            }
            if token:
                args["pageToken"] = token
            with DRIVE_LOCK:
                response = self.storage.drive_service.files().list(**args).execute(num_retries=4)
            files.extend(response.get("files", []))
            token = response.get("nextPageToken")
            if not token:
                break
        matches = [
            item for item in files
            if re.fullmatch(r"landing-complete-v2-[0-9a-f]{64}\.json", item.get("name", ""))
        ]
        if not matches:
            raise DBWLandingIncompleteError(
                "DBW Bronze requires a content-addressed full-catalogue Landing completion record."
            )
        matches.sort(key=lambda item: (item.get("createdTime", ""), item.get("name", "")), reverse=True)
        item = matches[0]
        with DRIVE_LOCK:
            raw = self.storage.drive_service.files().get_media(fileId=item["id"]).execute(num_retries=4)
        digest = hashlib.sha256(raw).hexdigest()
        if (item.get("appProperties") or {}).get("sha256") != digest:
            raise DBWLandingIncompleteError("DBW Landing completion checksum does not match Drive metadata.")
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DBWLandingIncompleteError("DBW Landing completion is not valid UTF-8 JSON.") from exc
        document = validate_landing_completion(document)
        expected_name = (
            f"{LANDING_COMPLETION_PREFIX}-{document['native_snapshot_sha256']}.json"
        )
        if item.get("name") != expected_name:
            raise DBWLandingIncompleteError(
                "DBW Landing completion name is not bound to its native snapshot SHA-256."
            )
        document["_completion_created_at_utc"] = item.get("createdTime") or "1970-01-01T00:00:00Z"
        return document

    def bind_release(self, completion: dict[str, Any]) -> None:
        """Isolate Bronze output and local resume state by immutable catalogue identity."""
        release_id = completion["native_snapshot_sha256"]
        self.release_id = release_id
        self.catalogue_sha256 = completion["catalogue_sha256"]
        self.native_snapshot_id = completion["native_snapshot_id"]
        self.processed_at_utc = completion["_completion_created_at_utc"]
        self.workspace = self.base_workspace / release_id
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.release_root = self.storage.get_or_create_nested_folder(
            [release_id], root_id=self.releases_root, write_session=self.session
        )
        self.bronze_obs = self.storage.get_or_create_nested_folder(
            ["observations"], root_id=self.release_root, write_session=self.session
        )
        self.bronze_dict = self.storage.get_or_create_nested_folder(
            ["dictionaries"], root_id=self.release_root, write_session=self.session
        )
        self.bronze_tax = self.storage.get_or_create_nested_folder(
            ["taxonomy"], root_id=self.release_root, write_session=self.session
        )
        self.bronze_met = self.storage.get_or_create_nested_folder(
            ["metadata"], root_id=self.release_root, write_session=self.session
        )
        self.bronze_control = self.storage.get_or_create_nested_folder(
            ["_control"], root_id=self.release_root, write_session=self.session
        )

    def load_release_receipts(self, completion: dict[str, Any]) -> dict[str, Any]:
        """Verify every per-indicator receipt and return its exact native membership."""
        def list_folder(parent_id: str) -> dict[str, dict[str, Any]]:
            folder_query = f"'{_escape_query(parent_id)}' in parents and trashed=false"
            objects: dict[str, dict[str, Any]] = {}
            page_token = None
            while True:
                list_args: dict[str, Any] = {
                    "q": folder_query,
                    "spaces": "drive",
                    "pageSize": 1000,
                    "fields": (
                        "nextPageToken,files("
                        "id,name,size,md5Checksum,appProperties,trashed)"
                    ),
                }
                if page_token:
                    list_args["pageToken"] = page_token
                with DRIVE_LOCK:
                    page = self.storage.drive_service.files().list(
                        **list_args
                    ).execute(num_retries=4)
                for obj in page.get("files", []):
                    object_id = obj.get("id")
                    if not isinstance(object_id, str) or not object_id or object_id in objects:
                        raise DBWLandingIncompleteError(
                            "DBW Landing contains an invalid or duplicated Drive object identity."
                        )
                    objects[object_id] = obj
                page_token = page.get("nextPageToken")
                if not page_token:
                    return objects

        metadata_objects = list_folder(self.landing_metadata)
        bulk_objects = list_folder(self.landing_bulk)
        query = f"'{_escape_query(self.landing_checkpoints)}' in parents and trashed=false"
        token = None
        files: list[dict[str, Any]] = []
        while True:
            args: dict[str, Any] = {
                "q": query,
                "spaces": "drive",
                "pageSize": 1000,
                "fields": "nextPageToken,files(id,name,size,md5Checksum,appProperties,trashed)",
            }
            if token:
                args["pageToken"] = token
            with DRIVE_LOCK:
                response = self.storage.drive_service.files().list(**args).execute(num_retries=4)
            files.extend(response.get("files", []))
            token = response.get("nextPageToken")
            if not token:
                break

        catalogue_sha = completion["catalogue_sha256"]
        indicator_ids: set[int] = set()
        metadata_members: dict[str, dict[str, Any]] = {}
        bulk_members: dict[str, dict[str, Any]] = {}
        bound_object_ids: set[str] = set()
        memberships: dict[int, str] = {}
        for item in files:
            props = item.get("appProperties") or {}
            if (
                props.get("catalogue_sha256") != catalogue_sha
                or props.get("native_snapshot_id") != completion["native_snapshot_id"]
                or props.get("checkpoint_schema") != "3"
                or props.get("checkpoint_status") != "completed"
                or props.get("bulk_complete") != "true"
                or props.get("metadata_complete") != "true"
            ):
                continue
            with DRIVE_LOCK:
                raw = self.storage.drive_service.files().get_media(
                    fileId=item["id"]
                ).execute(num_retries=4)
            if (
                hashlib.sha256(raw).hexdigest() != props.get("sha256")
                or hashlib.md5(raw).hexdigest() != item.get("md5Checksum")
                or len(raw) != int(item.get("size", -1))
            ):
                raise DBWLandingIncompleteError(
                    f"DBW indicator receipt failed verification: {item.get('name')}"
                )
            try:
                receipt = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DBWLandingIncompleteError("DBW indicator receipt is not valid JSON.") from exc
            indicator_id = receipt.get("indicator_id")
            if (
                receipt.get("schema_version") != 3
                or receipt.get("record_type") != "gus_dbw_indicator_completion"
                or receipt.get("catalogue_sha256") != catalogue_sha
                or receipt.get("native_snapshot_id") != completion["native_snapshot_id"]
                or receipt.get("status") != "completed"
                or receipt.get("bulk_complete") is not True
                or receipt.get("metadata_complete") is not True
                or not isinstance(indicator_id, int)
                or indicator_id in indicator_ids
            ):
                raise DBWLandingIncompleteError("DBW indicator receipts do not form a unique complete catalogue.")
            indicator_ids.add(indicator_id)
            landed_names = receipt.get("files_landed", [])
            landed_objects = receipt.get("landed_objects")
            if not isinstance(landed_names, list) or not isinstance(landed_objects, list):
                raise DBWLandingIncompleteError(
                    "DBW indicator receipt has no verifiable native object membership."
                )
            roles: list[str] = []
            object_names: list[str] = []
            bulk_source_names: list[str] = []
            for descriptor in landed_objects:
                if not isinstance(descriptor, dict):
                    raise DBWLandingIncompleteError(
                        "DBW indicator receipt contains an invalid native object descriptor."
                    )
                object_id = descriptor.get("id")
                name = descriptor.get("name")
                size = descriptor.get("size")
                sha256_hex = descriptor.get("sha256")
                md5_hex = descriptor.get("md5")
                role = descriptor.get("role")
                source_name = descriptor.get("source_name")
                if (
                    not isinstance(object_id, str)
                    or not object_id
                    or not isinstance(name, str)
                    or not name
                    or not isinstance(size, int)
                    or size < 0
                    or not isinstance(sha256_hex, str)
                    or re.fullmatch(r"[0-9a-f]{64}", sha256_hex) is None
                    or not isinstance(md5_hex, str)
                    or re.fullmatch(r"[0-9a-f]{32}", md5_hex) is None
                    or role not in {"aggregates", "metryka", "bulk_zip"}
                    or not isinstance(source_name, str)
                    or not source_name
                    or Path(source_name).name != source_name
                    or "\\" in source_name
                    or object_id in bound_object_ids
                ):
                    raise DBWLandingIncompleteError(
                        "DBW indicator receipt contains an invalid or duplicated native identity."
                    )
                source_objects = bulk_objects if role == "bulk_zip" else metadata_objects
                actual = source_objects.get(object_id)
                if (
                    actual is None
                    or actual.get("name") != name
                    or int(actual.get("size", -1)) != size
                    or actual.get("md5Checksum") != md5_hex
                    or (actual.get("appProperties") or {}).get("sha256") != sha256_hex
                ):
                    raise DBWLandingIncompleteError(
                        f"DBW receipt/native object mismatch for indicator {indicator_id}: {name}"
                    )
                roles.append(role)
                object_names.append(name)
                bound_object_ids.add(object_id)
                if role == "metryka":
                    metadata_members[object_id] = descriptor
                elif role == "bulk_zip":
                    bulk_members[object_id] = descriptor
                    bulk_source_names.append(source_name)
            if sorted(landed_names) != sorted(object_names):
                raise DBWLandingIncompleteError(
                    "DBW indicator receipt file names do not match its native object descriptors."
                )
            if roles.count("metryka") != 1 or roles.count("aggregates") != 1:
                raise DBWLandingIncompleteError(
                    "Each completed DBW indicator receipt must bind one aggregate and one metryka file."
                )
            expected_bulk_files = receipt.get("expected_bulk_files")
            if (
                not isinstance(expected_bulk_files, list)
                or not expected_bulk_files
                or any(not isinstance(name, str) for name in expected_bulk_files)
                or len(expected_bulk_files) != len(set(expected_bulk_files))
                or sorted(expected_bulk_files) != sorted(bulk_source_names)
            ):
                raise DBWLandingIncompleteError(
                    "DBW indicator receipt does not reconcile discovered and landed bulk files."
                )
            membership = _membership_sha256(landed_objects)
            if (
                receipt.get("native_membership_sha256") != membership
                or props.get("native_membership_sha256") != membership
            ):
                raise DBWLandingIncompleteError(
                    "DBW indicator receipt native membership checksum does not reconcile."
                )
            memberships[indicator_id] = membership

        if len(indicator_ids) != completion["catalogue_indicators"]:
            raise DBWLandingIncompleteError(
                "Verified DBW indicator receipts do not reconcile the completion marker."
            )
        if _snapshot_sha256(
            completion["native_snapshot_id"], memberships
        ) != completion["native_snapshot_sha256"]:
            raise DBWLandingIncompleteError(
                "Verified DBW native memberships do not match the completion snapshot."
            )
        return {
            "indicator_ids": indicator_ids,
            "metadata_members": metadata_members,
            "bulk_members": bulk_members,
        }

    def load_taxonomy_tree(self) -> list[dict[str, Any]]:
        """Load only the taxonomy bytes bound to this Bronze release."""
        local_tree = self.workspace / "indicators_tree.json"
        if local_tree.exists():
            raw = local_tree.read_bytes()
            if hashlib.sha256(raw).hexdigest() == self.catalogue_sha256:
                return json.loads(raw.decode("utf-8"))
            raise DBWLandingIncompleteError("Local DBW taxonomy does not match the bound catalogue release.")

        query = f"'{_escape_query(self.landing_taxonomy)}' in parents and trashed=false"
        files: list[dict[str, Any]] = []
        token = None
        while True:
            args: dict[str, Any] = {
                "q": query,
                "spaces": "drive",
                "pageSize": 1000,
                "fields": "nextPageToken,files(id,name,size,md5Checksum,appProperties,trashed)",
            }
            if token:
                args["pageToken"] = token
            with DRIVE_LOCK:
                response = self.storage.drive_service.files().list(**args).execute(num_retries=4)
            files.extend(response.get("files", []))
            token = response.get("nextPageToken")
            if not token:
                break
        matches = [
            item for item in files
            if (item.get("appProperties") or {}).get("sha256") == self.catalogue_sha256
            and (item.get("appProperties") or {}).get("kind") == "taxonomy"
        ]
        if len(matches) != 1:
            raise DBWLandingIncompleteError(
                f"Expected one taxonomy object for catalogue {self.release_id}; found {len(matches)}."
            )
        item = matches[0]
        with DRIVE_LOCK:
            content = self.storage.drive_service.files().get_media(fileId=item["id"]).execute(num_retries=4)
        if (
            hashlib.sha256(content).hexdigest() != self.catalogue_sha256
            or hashlib.md5(content).hexdigest() != item.get("md5Checksum")
            or len(content) != int(item.get("size", -1))
        ):
            raise DBWLandingIncompleteError("Bound DBW taxonomy bytes failed verification.")
        local_tree.write_bytes(content)
        return json.loads(content.decode("utf-8"))

    def build_taxonomy_table(self) -> Path:
        """Parse indicators tree into br_dbw_indicators Parquet table."""
        tree = self.load_taxonomy_tree()
        rows: list[dict[str, Any]] = []

        def walk(nodes: list[dict[str, Any]], area: str = "", domain: str = "", path: str = ""):
            for n in nodes:
                name = n.get("name", "")
                ntype = n.get("type", "")
                cur_area = area
                cur_domain = domain
                if ntype == "AREA":
                    cur_area = name
                elif cur_area and not cur_domain and ntype == "GROUP":
                    cur_domain = name

                cur_path = f"{path} > {name}" if path else name
                if ntype == "INDICATOR" and "indicator_id" in n:
                    rows.append({
                        "indicator_id": int(n["indicator_id"]),
                        "indicator_name": name,
                        "indicator_name_en": n.get("name_en") or "",
                        "thematic_area": cur_area,
                        "domain": cur_domain,
                        "taxonomy_path": cur_path,
                        "node_id": str(n["id"]) if n.get("id") else None,
                        "parent_id": str(n["parrent_id"]) if n.get("parrent_id") else None,
                        "processed_at_utc": self.processed_at_utc,
                    })
                if "children" in n and n["children"]:
                    walk(n["children"], cur_area, cur_domain, cur_path)

        walk(tree)
        logger.info(f"Parsed {len(rows)} indicators for taxonomy table across areas & domains.")

        out_path = self.workspace / "br_dbw_indicators.parquet"
        con = duckdb.connect(":memory:")
        df = pd.DataFrame(rows)
        con.register("df_tax", df)
        con.execute(f"COPY df_tax TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.close()
        return out_path

    def build_metadata_table(
        self,
        allowed_objects: dict[str, dict[str, Any]] | None = None,
    ) -> Path:
        """Parse metryka CSVs into br_dbw_metadata Parquet table."""
        query = f"'{_escape_query(self.landing_metadata)}' in parents and name contains 'metryka' and trashed=false"
        token = None
        met_files = []
        while True:
            with DRIVE_LOCK:
                res = self.storage.drive_service.files().list(
                    q=query, pageSize=1000, pageToken=token, fields="nextPageToken, files(id, name)"
                ).execute()
            met_files.extend(res.get("files", []))
            token = res.get("nextPageToken")
            if not token:
                break

        logger.info(f"Found {len(met_files)} metryka CSV files in Landing metadata.")
        if allowed_objects is not None:
            met_files = [item for item in met_files if item.get("id") in allowed_objects]
            if len(met_files) != len(allowed_objects):
                raise DBWLandingIncompleteError(
                    "DBW Landing metadata objects do not match the bound completion receipts."
                )

        def download_and_parse(mf: dict[str, Any]) -> dict[str, Any]:
            with DRIVE_LOCK:
                content = self.storage.drive_service.files().get_media(
                    fileId=mf["id"]
                ).execute(num_retries=4)
            if allowed_objects is not None:
                _verify_native_bytes(content, allowed_objects[mf["id"]])
            text = content.decode("utf-8-sig")
            lines = text.splitlines()
            if len(lines) < 2:
                raise DBWLandingIncompleteError(
                    f"DBW metryka object has no data row: {mf['name']}"
                )
            reader = csv.reader(io.StringIO(text), delimiter=";", quotechar='"')
            headers = next(reader)
            vals = next(reader)
            entry = {headers[i]: vals[i] if i < len(vals) else "" for i in range(len(headers))}
            iid_str = entry.get("id_zmienna", "").strip()
            if not iid_str.isdigit():
                raise DBWLandingIncompleteError(
                    f"DBW metryka object has no valid indicator identity: {mf['name']}"
                )
            return {
                "indicator_id": int(iid_str),
                "metric_name": entry.get("nazwa", "").strip(),
                "metric_name_en": entry.get("nazwa_ang", "").strip(),
                "description": entry.get("definicja_pojecie", "").strip(),
                "frequency": entry.get("nazwa_czestotliwosc", "").strip(),
                "measure_unit": entry.get("nazwa_jednostki", "").strip(),
                "data_source": entry.get("temat_badanie", "").strip(),
                "legal_basis": entry.get("tytul_akt", "").strip(),
                "last_update": entry.get("aktualizacja_ostatnia", "").strip(),
                "processed_at_utc": self.processed_at_utc,
            }

        rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(download_and_parse, mf) for mf in met_files]
            for f in as_completed(futures):
                rows.append(f.result())

        out_path = self.workspace / "br_dbw_metadata.parquet"
        con = duckdb.connect(":memory:")
        df = pd.DataFrame(rows)
        con.register("df_met", df)
        con.execute(f"COPY df_met TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.close()
        logger.info(f"Generated br_dbw_metadata.parquet with {len(rows)} records.")
        return out_path


def main():
    parser = argparse.ArgumentParser(description="Transform GUS DBW Landing files into Bronze Parquet tables.")
    parser.add_argument("--workspace", type=Path, default=Path("scratch/dbw_bronze"), help="Working directory")
    parser.add_argument("--allow-codespace", action="store_true", help="Allow production write in Codespaces")
    parser.add_argument("--sample-indicators", type=int, default=None, help="Limit to N indicators for pilot run")
    parser.add_argument("--indicator-ids", type=int, nargs="+", default=None, help="Specific indicator IDs to process")
    parser.add_argument("--skip-bulk", action="store_true", help="Only build taxonomy and metadata tables")
    args = parser.parse_args()

    try:
        validate_full_release_selection(args.sample_indicators, args.indicator_ids)
    except ValueError as exc:
        parser.error(str(exc))

    sm = StorageManager()
    loader = DBWBronzeLoader(workspace=args.workspace, storage=sm, allow_codespace=args.allow_codespace)
    completion = loader.require_complete_landing()
    loader.bind_release(completion)
    args.workspace = loader.workspace
    release_receipts = loader.load_release_receipts(completion)
    logger.info(
        "Verified complete DBW Landing catalogue: %s/%s indicators; Bronze release %s.",
        completion["completed_indicators"],
        completion["catalogue_indicators"],
        loader.release_id,
    )

    # 1. Discover Bulk Archives in Landing
    logger.info("=== Discovering Bulk Archives in Landing ===")
    query = f"'{_escape_query(loader.landing_bulk)}' in parents and trashed=false"
    token = None
    all_zips = []
    while True:
        with DRIVE_LOCK:
            res = sm.drive_service.files().list(
                q=query, pageSize=1000, pageToken=token, fields="nextPageToken, files(id, name, size)"
            ).execute()
        all_zips.extend(res.get("files", []))
        token = res.get("nextPageToken")
        if not token:
            break
    logger.info(f"Discovered {len(all_zips)} bulk zip files in Landing.")
    all_zips = [
        item for item in all_zips
        if item.get("id") in release_receipts["bulk_members"]
    ]
    if len(all_zips) != len(release_receipts["bulk_members"]):
        raise DBWLandingIncompleteError(
            "DBW Landing bulk objects do not match the bound completion receipts."
        )
    logger.info(f"Bound {len(all_zips)} bulk zip files to Bronze release {loader.release_id}.")

    from collections import defaultdict
    zips_by_indicator: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for z in all_zips:
        name = z["name"]
        prefix = name.split("_")[0]
        if prefix.isdigit():
            zips_by_indicator[int(prefix)].append(z)

    known_indicators = sorted(zips_by_indicator.keys())
    logger.info(f"Grouped into {len(known_indicators)} distinct indicators.")

    targets = known_indicators
    logger.info(f"Targeting {len(targets)} indicators for Bronze processing.")

    # 2. Build or restore release-bound Taxonomy Table
    tax_parquet = args.workspace / "br_dbw_indicators.parquet"
    if _restore_verified_drive_file(
        sm, name=tax_parquet.name, parent_id=loader.bronze_tax, local_path=tax_parquet
    ):
        logger.info(f"Phase A: Restored verified {tax_parquet.name} from release {loader.release_id}.")
    else:
        logger.info("=== Phase A: Building br_dbw_indicators (Taxonomy) ===")
        tax_parquet = loader.build_taxonomy_table()
        tax_res = _upload_file_to_drive(
            sm, tax_parquet, name="br_dbw_indicators.parquet", parent_id=loader.bronze_tax, mime_type="application/octet-stream"
        )
        logger.info(f"Uploaded br_dbw_indicators.parquet to Drive ({tax_res['size']} bytes, reused={tax_res['reused']}).")

    # 3. Build or restore release-bound Metadata Table
    met_parquet = args.workspace / "br_dbw_metadata.parquet"
    if _restore_verified_drive_file(
        sm, name=met_parquet.name, parent_id=loader.bronze_met, local_path=met_parquet
    ):
        logger.info(f"Phase B: Restored verified {met_parquet.name} from release {loader.release_id}.")
    else:
        logger.info("=== Phase B: Building br_dbw_metadata (Metryka) ===")
        met_parquet = loader.build_metadata_table(
            allowed_objects=release_receipts["metadata_members"],
        )
        met_res = _upload_file_to_drive(
            sm, met_parquet, name="br_dbw_metadata.parquet", parent_id=loader.bronze_met, mime_type="application/octet-stream"
        )
        logger.info(f"Uploaded br_dbw_metadata.parquet to Drive ({met_res['size']} bytes, reused={met_res['reused']}).")

    if args.skip_bulk:
        logger.info("Skipping bulk observations per --skip-bulk.")
        return

    # 4. Process Bulk Archives with Vectorized DuckDB
    logger.info("=== Phase C: Vectorized Bulk Extraction into Bronze ===")
    dict_dir = args.workspace / "dicts"
    dict_dir.mkdir(parents=True, exist_ok=True)

    # Initialize DuckDB with robust memory bounds and disk spillage
    duckdb_tmp = args.workspace / "duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA memory_limit = '2GB'")
    con.execute(f"PRAGMA temp_directory = '{duckdb_tmp}'")
    con.execute("PRAGMA preserve_insertion_order = false")
    con.execute("PRAGMA threads = 4")

    # Scan already completed indicator partitions on Drive with pagination
    query_obs = f"'{_escape_query(loader.bronze_obs)}' in parents and trashed=false"
    existing_obs_files = {}
    page_token = None
    with DRIVE_LOCK:
        while True:
            resp = sm.drive_service.files().list(
                q=query_obs,
                fields="nextPageToken, files(id, name, size,md5Checksum,appProperties)",
                pageSize=1000,
                pageToken=page_token
            ).execute()
            for f in resp.get("files", []):
                existing_obs_files[f["name"]] = f
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    logger.info(f"Discovered {len(existing_obs_files)} existing observation partitions on Drive.")

    query_dict = f"'{_escape_query(loader.bronze_dict)}' in parents and trashed=false"
    existing_dict_files: dict[str, dict[str, Any]] = {}
    page_token = None
    while True:
        with DRIVE_LOCK:
            response = sm.drive_service.files().list(
                q=query_dict,
                fields="nextPageToken,files(id,name,size,md5Checksum,appProperties)",
                pageSize=1000,
                pageToken=page_token,
            ).execute(num_retries=4)
        for item in response.get("files", []):
            name = item.get("name", "")
            if not re.fullmatch(r"dict_\d+\.parquet", name):
                continue
            if name in existing_dict_files:
                raise RuntimeError(
                    "Ambiguous DBW dictionary partitions exist in the bound release."
                )
            existing_dict_files[name] = item
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    logger.info(
        "Discovered %s existing dictionary partitions on Drive.",
        len(existing_dict_files),
    )

    # Historical pilot objects are retained. Cleanup is a separate, explicitly
    # authorized lifecycle operation and never belongs in a Bronze build.

    total_obs_rows = 0
    cp_path = args.workspace / "checkpoint.json"
    if cp_path.is_file():
        try:
            prior_cp = json.loads(cp_path.read_text(encoding="utf-8"))
            total_obs_rows = prior_cp.get("total_observations_rows", 0)
            logger.info(f"Resuming with prior total_observations_rows = {total_obs_rows:,}")
        except Exception:
            total_obs_rows = 0

    completed_indicators = 0
    total_targets = len(targets)

    for idx, ind_id in enumerate(targets, 1):
        part_name = f"part_{ind_id}.parquet"
        dict_part_name = f"dict_{ind_id}.parquet"

        existing_part = existing_obs_files.get(part_name)
        existing_dict_part = existing_dict_files.get(dict_part_name)
        if existing_part and not _has_integrity_metadata(existing_part, min_size=1000):
            raise RuntimeError(
                f"Existing DBW observation partition is not verifiable: {part_name}"
            )
        if existing_dict_part and not _has_integrity_metadata(existing_dict_part):
            raise RuntimeError(
                f"Existing DBW dictionary partition is not verifiable: {dict_part_name}"
            )
        if existing_part and existing_dict_part:
            logger.info(
                f"[{idx}/{total_targets}] Indicator {ind_id} has verified observation "
                "and dictionary partitions on Drive. Skipping."
            )
            completed_indicators += 1
            continue

        zfiles = zips_by_indicator.get(ind_id, [])
        if not zfiles:
            continue

        t_start = time.time()
        (args.workspace / part_name).unlink(missing_ok=True)
        # Initialize fresh temporary DuckDB tables
        con.execute("DROP TABLE IF EXISTS current_obs")
        con.execute("DROP TABLE IF EXISTS current_dict")
        con.execute("""
            CREATE TEMP TABLE current_obs (
                indicator_id BIGINT,
                przekroj_id BIGINT,
                wymiar_1 BIGINT,
                pozycja_1 BIGINT,
                wymiar_2 BIGINT,
                pozycja_2 BIGINT,
                wymiar_3 BIGINT,
                pozycja_3 BIGINT,
                wymiar_4 BIGINT,
                pozycja_4 BIGINT,
                wymiar_5 BIGINT,
                pozycja_5 BIGINT,
                wymiar_6 BIGINT,
                pozycja_6 BIGINT,
                wymiar_7 BIGINT,
                pozycja_7 BIGINT,
                wymiar_8 BIGINT,
                pozycja_8 BIGINT,
                wymiar_9 BIGINT,
                pozycja_9 BIGINT,
                okres_id INTEGER,
                sposob_prezentacji_miara_id INTEGER,
                period_year INTEGER,
                wartosc_raw VARCHAR,
                wartosc_numeric DOUBLE,
                precyzja INTEGER,
                brak_wartosci_id INTEGER,
                tajnosci_id INTEGER,
                flaga_id INTEGER,
                raw_archive_file VARCHAR,
                source_row_number BIGINT,
                processed_at_utc VARCHAR
            )
        """)
        con.execute("""
            CREATE TEMP TABLE current_dict (
                indicator_id BIGINT,
                column_name VARCHAR,
                dictionary_name VARCHAR,
                element_id BIGINT,
                element_name VARCHAR,
                processed_at_utc VARCHAR
            )
        """)

        # Download zip archives in parallel
        def fetch_zip(zf_meta):
            with DRIVE_LOCK:
                content = sm.drive_service.files().get_media(
                    fileId=zf_meta["id"]
                ).execute(num_retries=4)
            _verify_native_bytes(
                content, release_receipts["bulk_members"][zf_meta["id"]]
            )
            return zf_meta["name"], content

        with ThreadPoolExecutor(max_workers=6) as executor:
            download_futures = [executor.submit(fetch_zip, zf) for zf in zfiles]
            
            with tempfile.TemporaryDirectory() as tmpdir:
                for future in as_completed(download_futures):
                    fname, content = future.result()
                    try:
                        zf = zipfile.ZipFile(io.BytesIO(content))
                        csv_names = [n for n in zf.namelist() if not "Slowniki" in n and n.endswith(".csv")]
                        dict_names = [n for n in zf.namelist() if "Slowniki" in n and n.endswith(".csv")]

                        # 1. Observations
                        if csv_names:
                            csv_path = os.path.join(tmpdir, f"obs_{fname}.csv")
                            with open(csv_path, "wb") as f_out:
                                f_out.write(zf.read(csv_names[0]))

                            con.execute(f"""
                                INSERT INTO current_obs
                                SELECT 
                                    {ind_id}::BIGINT,
                                    TRY_CAST(id_przekroj AS BIGINT),
                                    TRY_CAST(id_wymiar_1 AS BIGINT),
                                    TRY_CAST(id_pozycja_1 AS BIGINT),
                                    TRY_CAST(id_wymiar_2 AS BIGINT),
                                    TRY_CAST(id_pozycja_2 AS BIGINT),
                                    TRY_CAST(id_wymiar_3 AS BIGINT),
                                    TRY_CAST(id_pozycja_3 AS BIGINT),
                                    TRY_CAST(id_wymiar_4 AS BIGINT),
                                    TRY_CAST(id_pozycja_4 AS BIGINT),
                                    TRY_CAST(id_wymiar_5 AS BIGINT),
                                    TRY_CAST(id_pozycja_5 AS BIGINT),
                                    TRY_CAST(id_wymiar_6 AS BIGINT),
                                    TRY_CAST(id_pozycja_6 AS BIGINT),
                                    TRY_CAST(id_wymiar_7 AS BIGINT),
                                    TRY_CAST(id_pozycja_7 AS BIGINT),
                                    TRY_CAST(id_wymiar_8 AS BIGINT),
                                    TRY_CAST(id_pozycja_8 AS BIGINT),
                                    TRY_CAST(id_wymiar_9 AS BIGINT),
                                    TRY_CAST(id_pozycja_9 AS BIGINT),
                                    TRY_CAST(id_okres AS INTEGER),
                                    TRY_CAST(id_sposob_prezentacji_miara AS INTEGER),
                                    TRY_CAST(id_daty AS INTEGER),
                                    wartosc as wartosc_raw,
                                    TRY_CAST(REPLACE(wartosc, ',', '.') AS DOUBLE),
                                    TRY_CAST(precyzja AS INTEGER),
                                    TRY_CAST(id_brak_wartosci AS INTEGER),
                                    TRY_CAST(id_tajnosci AS INTEGER),
                                    TRY_CAST(id_flaga AS INTEGER),
                                    '{fname}' as raw_archive_file,
                                    TRY_CAST(rowNumber AS BIGINT),
                                    '{loader.processed_at_utc}'
                                FROM read_csv('{csv_path}', delim=';', header=true, all_varchar=true, ignore_errors=true)
                            """)
                            os.remove(csv_path)

                        # 2. Dictionaries
                        if dict_names:
                            dict_path = os.path.join(tmpdir, f"dict_{fname}.csv")
                            with open(dict_path, "wb") as f_out:
                                f_out.write(zf.read(dict_names[0]))

                            con.execute(f"""
                                INSERT INTO current_dict
                                SELECT DISTINCT
                                    {ind_id}::BIGINT,
                                    TRIM(nazwa_kolumny),
                                    TRIM(nazwa_slownika),
                                    TRY_CAST(id_elementu AS BIGINT),
                                    TRIM(opis),
                                    '{loader.processed_at_utc}'
                                FROM read_csv('{dict_path}', delim=';', header=true, all_varchar=true, ignore_errors=true)
                                WHERE id_elementu IS NOT NULL
                            """)
                            os.remove(dict_path)
                    except Exception as exc:
                        raise DBWLandingIncompleteError(
                            f"DBW bulk archive could not be parsed completely: {fname}"
                        ) from exc

        obs_count = con.execute("SELECT count(*) FROM current_obs").fetchone()[0]
        dict_count = con.execute("SELECT count(*) FROM current_dict").fetchone()[0]

        # Write Parquet and upload to Google Drive
        if obs_count <= 0:
            raise RuntimeError(
                f"DBW indicator {ind_id} produced no observation rows."
            )
        if not existing_part:
            part_path = args.workspace / part_name
            con.execute(f"COPY current_obs TO '{part_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            res = _upload_file_to_drive(
                sm, part_path, name=part_name, parent_id=loader.bronze_obs, mime_type="application/octet-stream"
            )
            part_path.unlink(missing_ok=True)
            total_obs_rows += obs_count

        if not existing_dict_part:
            dict_part_path = dict_dir / dict_part_name
            con.execute(f"COPY (SELECT DISTINCT * FROM current_dict) TO '{dict_part_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            _upload_file_to_drive(
                sm,
                dict_part_path,
                name=dict_part_name,
                parent_id=loader.bronze_dict,
                mime_type="application/octet-stream",
            )
            dict_part_path.unlink(missing_ok=True)

        con.execute("DROP TABLE current_obs")
        con.execute("DROP TABLE current_dict")

        completed_indicators += 1
        elapsed = time.time() - t_start
        logger.info(
            f"[{idx}/{total_targets}] Indicator {ind_id}: {obs_count:,} observations, {dict_count} dicts from {len(zfiles)} archives ({elapsed:.1f}s)."
        )

        # Update checkpoint every 5 indicators
        if completed_indicators % 5 == 0 or idx == total_targets:
            cp_data = {
                "source_id": "gus_dbw_bronze",
                "release_id": loader.release_id,
                "total_indicators": total_targets,
                "completed_indicators": completed_indicators,
                "total_observations_rows": total_obs_rows,
                "last_indicator_id": ind_id,
                "status": "completed" if completed_indicators >= total_targets else "running",
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            cp_bytes = json.dumps(cp_data, indent=2).encode("utf-8")
            cp_path = args.workspace / "checkpoint.json"
            cp_path.write_bytes(cp_bytes)
            checkpoint_sha = hashlib.sha256(cp_bytes).hexdigest()
            checkpoint_name = (
                f"checkpoint-{completed_indicators:06d}-{checkpoint_sha[:16]}.json"
            )
            _upload_file_to_drive(
                sm,
                cp_path,
                name=checkpoint_name,
                parent_id=loader.bronze_control,
                mime_type="application/json",
            )
            _upload_file_to_drive(
                sm,
                cp_path,
                name=checkpoint_name,
                parent_id=loader.campaign_control,
                mime_type="application/json",
            )

    # Restore every persisted dictionary partition before deterministic consolidation.
    token = None
    remote_dict_parts: list[dict[str, Any]] = []
    while True:
        with DRIVE_LOCK:
            response = sm.drive_service.files().list(
                q=query_dict,
                fields="nextPageToken,files(id,name,size,md5Checksum,appProperties)",
                pageSize=1000,
                pageToken=token,
            ).execute(num_retries=4)
        remote_dict_parts.extend(
            item for item in response.get("files", [])
            if re.fullmatch(r"dict_\d+\.parquet", item.get("name", ""))
        )
        token = response.get("nextPageToken")
        if not token:
            break
    if len({item["name"] for item in remote_dict_parts}) != len(remote_dict_parts):
        raise RuntimeError("Ambiguous DBW dictionary partitions exist in the bound release.")
    expected_dict_names = {f"dict_{indicator_id}.parquet" for indicator_id in targets}
    verified_dict_names = {
        item["name"] for item in remote_dict_parts if _has_integrity_metadata(item)
    }
    if verified_dict_names != expected_dict_names:
        raise RuntimeError(
            "DBW Bronze dictionary partitions do not reconcile the complete native snapshot."
        )
    for item in remote_dict_parts:
        local_part = dict_dir / item["name"]
        if not _restore_verified_drive_file(
            sm, name=item["name"], parent_id=loader.bronze_dict, local_path=local_part
        ):
            raise RuntimeError(f"DBW dictionary partition disappeared: {item['name']}")

    # Final dictionary consolidation
    dict_parts = list(dict_dir.glob("dict_*.parquet"))
    dict_cons_path = args.workspace / "br_dbw_dictionaries.parquet"
    if dict_parts:
        logger.info(f"Consolidating {len(dict_parts)} dictionary partition files...")
        con.execute(f"""
            COPY (
                SELECT DISTINCT indicator_id, column_name, dictionary_name, element_id, element_name, processed_at_utc
                FROM read_parquet('{dict_dir}/dict_*.parquet')
            ) TO '{dict_cons_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    else:
        con.execute(f"""
            COPY (
                SELECT
                    NULL::BIGINT AS indicator_id,
                    NULL::VARCHAR AS column_name,
                    NULL::VARCHAR AS dictionary_name,
                    NULL::BIGINT AS element_id,
                    NULL::VARCHAR AS element_name,
                    NULL::VARCHAR AS processed_at_utc
                WHERE false
            ) TO '{dict_cons_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    dict_res = _upload_file_to_drive(
        sm, dict_cons_path, name="br_dbw_dictionaries.parquet",
        parent_id=loader.bronze_dict, mime_type="application/octet-stream"
    )
    logger.info(f"Uploaded consolidated br_dbw_dictionaries.parquet ({dict_res['size']} bytes).")

    if completed_indicators != total_targets:
        raise RuntimeError("DBW Bronze cannot complete before every indicator is processed.")
    final_obs: list[dict[str, Any]] = []
    token = None
    while True:
        with DRIVE_LOCK:
            response = sm.drive_service.files().list(
                q=query_obs,
                fields="nextPageToken,files(id,name,size,md5Checksum,appProperties)",
                pageSize=1000,
                pageToken=token,
            ).execute(num_retries=4)
        final_obs.extend(response.get("files", []))
        token = response.get("nextPageToken")
        if not token:
            break
    part_items = [
        item for item in final_obs
        if re.fullmatch(r"part_\d+\.parquet", item.get("name", ""))
    ]
    if len({item["name"] for item in part_items}) != len(part_items):
        raise RuntimeError("Ambiguous DBW observation partitions exist in the bound release.")
    verified_part_names = {
        item["name"] for item in part_items
        if re.fullmatch(r"part_\d+\.parquet", item.get("name", ""))
        and int(item.get("size", 0)) > 1000
        and re.fullmatch(r"[0-9a-f]{32}", item.get("md5Checksum", ""))
        and re.fullmatch(r"[0-9a-f]{64}", (item.get("appProperties") or {}).get("sha256", ""))
    }
    expected_part_names = {f"part_{indicator_id}.parquet" for indicator_id in targets}
    if verified_part_names != expected_part_names:
        raise RuntimeError(
            "DBW Bronze observation partitions do not reconcile the complete native snapshot."
        )

    completion_document = {
        "schema_version": 1,
        "record_type": "gus_dbw_bronze_completion",
        "source_id": "gus_dbw",
        "status": "complete_native_snapshot",
        "release_id": loader.release_id,
        "native_snapshot_id": loader.native_snapshot_id,
        "catalogue_sha256": loader.catalogue_sha256,
        "completed_indicators": completed_indicators,
        "observation_partitions": len(verified_part_names),
        "dictionary_partitions": len(verified_dict_names),
        "processed_at_utc": loader.processed_at_utc,
    }
    completion_path = args.workspace / f"bronze-complete-v1-{loader.release_id}.json"
    completion_path.write_text(
        json.dumps(completion_document, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    _upload_file_to_drive(
        sm,
        completion_path,
        name=completion_path.name,
        parent_id=loader.bronze_control,
        mime_type="application/json",
    )

    con.close()
    logger.info(f"GUS DBW Bronze transformation completed successfully: {total_obs_rows:,} total observations across {completed_indicators} indicators.")


if __name__ == "__main__":
    main()
