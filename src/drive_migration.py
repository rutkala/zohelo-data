"""Google Drive layout consolidation engine for rutkala/zohelo-data.

Consolidates Google Drive layout into canonical medallion and releases hierarchy:
- releases/{nbp,bdl,wdi}/{current-release.json,<uuid>}
- 06_control/nbp and 06_control/source_campaigns
- 02_bronze, 03_silver, 04_gold navigation shortcuts & indexes
- 05_archive/ containing archived legacy wrappers (bdl-platform, wdi-platform)

Guarantees:
- Strictly bounded read-only plan (--dry-run default).
- Explicit --confirm / --apply with safety pin --expected-root-id.
- Durable ID-based journal stored in Drive under 06_control/migration-journal.json.
- Atomic Drive API moves (addParents/removeParents) preserving exact Parquet bytes,
  file IDs, checksums, and permissions without downloading or re-uploading.
- Idempotent resume and reversible rollback.
- Drift detection: aborts if current release pointers, manifests, or NBP state change.
- Ambiguity detection: fails closed if conflicting pointers exist.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import copy
import logging
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from drive_release_store import DriveReleaseStore
from layout_resolution import (
    CANONICAL_CONTROL_FOLDER,
    CANONICAL_NBP_CONTROL_FOLDER,
    CANONICAL_RELEASES_FOLDER,
    CANONICAL_SOURCE_CAMPAIGNS_FOLDER,
    CANONICAL_SOURCES,
    LEGACY_BDL_WRAPPER,
    LEGACY_NBP_CONTROL_FOLDER,
    LEGACY_WDI_WRAPPER,
    ARCHIVE_FOLDER,
    FOLDER_MIME_TYPE,
    SHORTCUT_MIME_TYPE,
    detect_layout_mode,
    resolve_nbp_control_root,
    resolve_source_release_root,
    AmbiguousLayoutError,
    LayoutResolutionError,
)
from medallion_navigation import (
    verify_medallion_navigation,
    ALL_MEDALLION_LAYERS,
)
from release_protocol import (
    read_current_release_manifest,
)
from storage_manager import StorageManager

logger = logging.getLogger(__name__)

JOURNAL_FILE_NAME = "migration-journal.json"
DRIVE_REPEATABLE_RETRIES = 4
UUID_REGEX = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


class MigrationError(RuntimeError):
    """Base exception for migration failures."""


class DriftError(MigrationError):
    """Raised when release or state identity changes during migration."""


class SafetyPinError(MigrationError):
    """Raised when expected root ID does not match resolved root ID."""


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")


class DriveMigrationEngine:
    """Manages the planning, execution, resumption, and rollback of Drive layout consolidation."""

    def __init__(
        self,
        storage: StorageManager,
        expected_root_id: str,
        *,
        journal_local_path: Optional[Path] = None,
    ):
        self.storage = storage
        self.expected_root_id = expected_root_id.strip()
        self.journal_local_path = journal_local_path
        self.drive_service = getattr(storage, "drive_service", None) or getattr(
            getattr(storage, "storage", None), "drive_service", None
        )

        # Safety verification: resolve root and verify against expected_root_id
        actual_root_id = self.storage.resolve_root(create=False)
        if actual_root_id != self.expected_root_id:
            raise SafetyPinError(
                f"Drive root ID '{actual_root_id}' does not match expected safety pin '{self.expected_root_id}'"
            )
        self.root_id = actual_root_id

    # -------------------------------------------------------------------------
    # Drive helpers
    # -------------------------------------------------------------------------

    def _paged_list(self, *, q: str, fields: str, page_size: int) -> List[Dict[str, Any]]:
        """List a bounded result set and reject malformed or looping pagination."""
        result: List[Dict[str, Any]] = []
        token: Optional[str] = None
        seen_tokens: Set[str] = set()
        for _page in range(1000):
            response = self.drive_service.files().list(
                q=q,
                fields=fields,
                pageSize=page_size,
                pageToken=token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            if not isinstance(response, dict):
                raise MigrationError("Drive list response is not an object")
            items = response.get("files", [])
            if not isinstance(items, list):
                raise MigrationError("Drive list response has a malformed files page")
            for item in items:
                if not isinstance(item, dict):
                    raise MigrationError("Drive list response contains a malformed item")
                if not item.get("id") or not isinstance(item.get("name"), str) or not item.get("mimeType"):
                    raise MigrationError("Drive list response contains an incomplete item")
                parents = item.get("parents", [])
                if not isinstance(parents, list):
                    raise MigrationError("Drive list response contains malformed parents")
                result.append(item)
            next_token = response.get("nextPageToken")
            if next_token in (None, ""):
                return result
            if not isinstance(next_token, str):
                raise MigrationError("Drive list response has a malformed nextPageToken")
            if next_token in seen_tokens or next_token == token:
                raise MigrationError("Drive list pagination repeated a page token")
            seen_tokens.add(next_token)
            token = next_token
        raise MigrationError("Drive list pagination exceeded the safety bound")

    def _list_children(self, parent_id: str, *, mime_type: Optional[str] = None) -> List[Dict[str, Any]]:
        q = f"'{parent_id}' in parents and trashed=false"
        if mime_type:
            q += f" and mimeType='{mime_type}'"
        return self._paged_list(
            q=q,
            fields="nextPageToken,files(id,name,mimeType,parents,size,md5Checksum,shortcutDetails,trashed)",
            page_size=100,
        )

    def _find_child_by_name(
        self, parent_id: str, name: str, *, mime_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        escaped_name = name.replace("\\", "\\\\").replace("'", "\\'")
        q = f"name='{escaped_name}' and '{parent_id}' in parents and trashed=false"
        if mime_type:
            q += f" and mimeType='{mime_type}'"
        return self._paged_list(
            q=q,
            fields="nextPageToken,files(id,name,mimeType,parents,size,md5Checksum,shortcutDetails,trashed)",
            page_size=50,
        )

    def _read_file_bytes(self, file_id: str) -> bytes:
        data = self.drive_service.files().get_media(
            fileId=file_id, supportsAllDrives=True
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        if not isinstance(data, bytes):
            raise MigrationError(f"Drive did not return bytes for file {file_id}")
        return data

    def _get_metadata(self, file_id: str) -> Optional[Dict[str, Any]]:
        try:
            return self.drive_service.files().get(
                fileId=file_id,
                fields="id,name,mimeType,parents,trashed,size,md5Checksum,shortcutDetails",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        except Exception as exc:
            status = getattr(getattr(exc, "resp", None), "status", None)
            if str(status) == "404":
                return None
            raise

    def _generate_ids(self, count: int, object_type: str) -> List[str]:
        if count == 0:
            return []
        response = self.drive_service.files().generateIds(
            count=count, space="drive", type=object_type
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        ids = response.get("ids") if isinstance(response, dict) else None
        if not isinstance(ids, list) or len(ids) != count or any(not isinstance(value, str) or not value for value in ids):
            raise MigrationError(f"Drive did not allocate {count} durable {object_type} IDs")
        if len(set(ids)) != len(ids):
            raise MigrationError("Drive allocated duplicate object IDs")
        return ids

    def _move_and_rename_item(
        self,
        item_id: str,
        *,
        expected_name: str,
        from_parent_id: str,
        to_parent_id: str,
        to_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Move one pinned item only when its complete current state is known."""
        meta = self._get_metadata(item_id)
        if not meta or meta.get("trashed"):
            raise DriftError(f"Item {item_id} is missing or trashed")
        parents = list(meta.get("parents") or [])
        target_name = to_name or expected_name
        if parents == [to_parent_id] and meta.get("name") == target_name:
            return meta
        if parents != [from_parent_id] or meta.get("name") != expected_name:
            raise DriftError(
                f"Item {item_id} drifted: expected name={expected_name!r}, "
                f"parents={[from_parent_id]!r}; found name={meta.get('name')!r}, parents={parents!r}"
            )
        body = {"name": target_name} if target_name != expected_name else None
        self.drive_service.files().update(
            fileId=item_id,
            body=body,
            addParents=to_parent_id,
            removeParents=from_parent_id,
            fields="id,name,parents,trashed,mimeType",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        verified = self._get_metadata(item_id)
        if (not verified or verified.get("trashed") or verified.get("name") != target_name
                or list(verified.get("parents") or []) != [to_parent_id]):
            raise DriftError(f"Move of {item_id} did not reach its exact pinned target")
        return verified

    def _save_local_receipt(self, journal: Dict[str, Any]) -> None:
        if not self.journal_local_path:
            return
        self.journal_local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.journal_local_path.with_name(self.journal_local_path.name + ".tmp")
        tmp.write_bytes(_json_bytes(journal))
        tmp.replace(self.journal_local_path)

    def _mark_local_failure(self, journal: Dict[str, Any], exc: BaseException, *, rollback: bool = False) -> None:
        journal["status"] = "rollback_interrupted" if rollback else "interrupted"
        journal["failure"] = {
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "failed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        self._save_local_receipt(journal)

    def _verify_owned_object(self, step: Dict[str, Any], *, allow_missing: bool = False) -> Optional[Dict[str, Any]]:
        meta = self._get_metadata(step["item_id"])
        if not meta or meta.get("trashed"):
            if allow_missing:
                return None
            raise DriftError(f"Planned object {step['item_id']} is missing or trashed")
        expected = (step["name"], step["mime_type"], [step["parent_id"]])
        actual = (meta.get("name"), meta.get("mimeType"), list(meta.get("parents") or []))
        if actual != expected:
            raise DriftError(
                f"Planned object {step['item_id']} drifted: expected {expected!r}, found {actual!r}"
            )
        if step["object_kind"] == "shortcut":
            if (meta.get("shortcutDetails") or {}).get("targetId") != step["target_id"]:
                raise DriftError(f"Shortcut {step['item_id']} target drifted")
        elif step["object_kind"] == "json":
            if sha256(self._read_file_bytes(step["item_id"])).hexdigest() != step["content_sha256"]:
                raise DriftError(f"JSON object {step['item_id']} content drifted")
        return meta

    def _execute_owned_object(self, step: Dict[str, Any]) -> None:
        existing = self._get_metadata(step["item_id"])
        if existing and not existing.get("trashed"):
            self._verify_owned_object(step)
            if not step["preexisting"]:
                step["was_created"] = True
            return
        if step["preexisting"]:
            raise DriftError(f"Pre-existing planned object {step['item_id']} disappeared")
        if existing and existing.get("trashed"):
            raise DriftError(f"Owned object {step['item_id']} was trashed and cannot be recreated")
        collisions = self._find_child_by_name(step["parent_id"], step["name"])
        if collisions:
            raise DriftError(
                f"Planned name {step['name']!r} under {step['parent_id']} is occupied by a foreign object"
            )
        import io
        from googleapiclient.http import MediaIoBaseUpload
        body = {
            "id": step["item_id"],
            "name": step["name"],
            "mimeType": step["mime_type"],
            "parents": [step["parent_id"]],
        }
        media = None
        if step["object_kind"] == "shortcut":
            body["shortcutDetails"] = {"targetId": step["target_id"]}
        elif step["object_kind"] == "json":
            data = _json_bytes(step["document"])
            media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/json", resumable=False)
        created = self.drive_service.files().create(
            body=body, media_body=media,
            fields="id,name,mimeType,parents,shortcutDetails",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        if created.get("id") != step["item_id"]:
            raise MigrationError("Drive created a planned object with an unexpected ID")
        self._verify_owned_object(step)
        step["was_created"] = True

    def _rollback_owned_object(self, step: Dict[str, Any], allowed_child_ids: Set[str]) -> None:
        if step["preexisting"]:
            self._verify_owned_object(step)
            return
        meta = self._verify_owned_object(step, allow_missing=True)
        if meta is None:
            return
        if step["object_kind"] == "folder":
            active_children = self._list_children(step["item_id"])
            foreign = [child for child in active_children if child["id"] not in allowed_child_ids]
            if foreign:
                raise DriftError(
                    f"Rollback refuses non-owned children in folder {step['item_id']}: "
                    + ", ".join(child["id"] for child in foreign)
                )
            if active_children:
                raise DriftError(f"Rollback order left owned children in folder {step['item_id']}")
        self.drive_service.files().delete(
            fileId=step["item_id"], supportsAllDrives=True
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

    def _plan_digest(self, plan: Dict[str, Any]) -> str:
        immutable = copy.deepcopy(plan)
        immutable.pop("plan_sha256", None)
        return sha256(json.dumps(
            immutable, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")).hexdigest()

    def _journal_digest(self, journal: Dict[str, Any]) -> str:
        plan = journal.get("plan")
        if not isinstance(plan, dict):
            raise MigrationError("Journal has no immutable plan snapshot")
        return self._plan_digest(plan)

    def _validate_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(plan, dict) or plan.get("status") != "planned":
            raise MigrationError("Reviewed migration plan is malformed")
        required = (
            "plan_id", "expected_root_id", "root_id", "control_folder_id",
            "releases_root_id", "pins", "steps", "step_count", "plan_sha256",
        )
        missing = [key for key in required if not plan.get(key)]
        if missing:
            raise MigrationError("Reviewed migration plan is missing: " + ", ".join(missing))
        if plan["expected_root_id"] != self.root_id or plan["root_id"] != self.root_id:
            raise SafetyPinError("Reviewed migration plan root pins do not match this Drive root")
        if plan["plan_sha256"] != self._plan_digest(plan):
            raise MigrationError("Reviewed migration plan digest does not match its contents")
        steps = plan["steps"]
        if not isinstance(steps, list) or plan["step_count"] != len(steps) or not steps:
            raise MigrationError("Reviewed migration plan steps are malformed")
        if set(("nbp", "bdl", "wdi", "control", "source_campaigns")).difference(plan["pins"]):
            raise MigrationError("Reviewed migration plan does not pin every established source")
        seen_steps: Set[str] = set()
        seen_objects: Set[str] = set()
        for step in steps:
            if not isinstance(step, dict) or not isinstance(step.get("step_id"), str):
                raise MigrationError("Reviewed migration plan contains a malformed step")
            if step["step_id"] in seen_steps:
                raise MigrationError(f"Reviewed migration plan repeats step {step['step_id']}")
            seen_steps.add(step["step_id"])
            if step.get("status") != "pending":
                raise MigrationError("Reviewed migration plan contains mutable progress")
            action = step.get("action")
            if action == "move":
                needed = ("item_id", "from_parent_id", "to_parent_id", "from_name", "to_name", "mime_type")
                if any(not step.get(key) for key in needed):
                    raise MigrationError(f"Move step {step['step_id']} is incomplete")
                if step["from_parent_id"] == step["to_parent_id"] and step["from_name"] == step["to_name"]:
                    raise MigrationError(f"Move step {step['step_id']} has no effect")
                snapshot = step.get("initial_children")
                if step["mime_type"] == FOLDER_MIME_TYPE:
                    if not isinstance(snapshot, list):
                        raise MigrationError(f"Folder move step {step['step_id']} has no child snapshot")
                    child_ids: Set[str] = set()
                    for child in snapshot:
                        if (not isinstance(child, dict)
                                or set(child) != {"id", "name", "mime_type"}
                                or not all(isinstance(child.get(key), str) and child[key]
                                           for key in ("id", "name", "mime_type"))
                                or child["id"] in child_ids):
                            raise MigrationError(
                                f"Folder move step {step['step_id']} has a malformed child snapshot"
                            )
                        child_ids.add(child["id"])
                elif snapshot is not None:
                    raise MigrationError(f"File move step {step['step_id']} has a child snapshot")
            elif action == "ensure_object":
                needed = ("item_id", "name", "mime_type", "parent_id", "object_kind")
                if any(not step.get(key) for key in needed) or not isinstance(step.get("preexisting"), bool):
                    raise MigrationError(f"Object step {step['step_id']} is incomplete")
                if step["item_id"] in seen_objects:
                    raise MigrationError(f"Reviewed migration plan repeats object ID {step['item_id']}")
                seen_objects.add(step["item_id"])
                if step["object_kind"] not in ("folder", "shortcut", "json"):
                    raise MigrationError(f"Object step {step['step_id']} has an invalid kind")
                if step["object_kind"] == "folder" and step["mime_type"] != FOLDER_MIME_TYPE:
                    raise MigrationError(f"Folder step {step['step_id']} has the wrong MIME type")
                if step["object_kind"] == "shortcut":
                    if step["mime_type"] != SHORTCUT_MIME_TYPE or not step.get("target_id"):
                        raise MigrationError(f"Shortcut step {step['step_id']} is incomplete")
                if step["object_kind"] == "json":
                    if step["mime_type"] != "application/json" or not isinstance(step.get("document"), dict):
                        raise MigrationError(f"JSON step {step['step_id']} is incomplete")
                    if sha256(_json_bytes(step["document"])).hexdigest() != step.get("content_sha256"):
                        raise MigrationError(f"JSON step {step['step_id']} content hash is invalid")
            else:
                raise MigrationError(f"Reviewed migration plan contains unsupported action {action!r}")
        return plan

    def _validate_journal(self, journal: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(journal, dict):
            raise MigrationError("Migration journal must be a JSON object")
        required = (
            "schema_version", "plan_id", "expected_root_id", "root_id",
            "control_folder_id", "plan", "plan_sha256", "steps", "status",
        )
        missing = [key for key in required if not journal.get(key)]
        if missing:
            raise MigrationError("Migration journal is missing required fields: " + ", ".join(missing))
        if journal["schema_version"] != 1:
            raise MigrationError(f"Unsupported migration journal schema {journal['schema_version']!r}")
        if journal["expected_root_id"] != self.root_id or journal["root_id"] != self.root_id:
            raise SafetyPinError("Migration journal root pin does not match this Drive root")
        plan = self._validate_plan(journal["plan"])
        if journal["plan_id"] != plan["plan_id"] or journal["plan_sha256"] != plan["plan_sha256"]:
            raise MigrationError("Migration journal immutable identity does not match its plan")
        if journal["control_folder_id"] != plan["control_folder_id"]:
            raise MigrationError("Migration journal control folder pin changed")
        if journal["plan_sha256"] != self._journal_digest(journal):
            raise MigrationError("Migration journal plan digest does not match its immutable plan")
        if journal["status"] not in (
            "in_progress", "interrupted", "completed",
            "rolling_back", "rollback_interrupted", "rolled_back",
        ):
            raise MigrationError(f"Migration journal has invalid status {journal['status']!r}")
        if not isinstance(journal["steps"], list) or len(journal["steps"]) != len(plan["steps"]):
            raise MigrationError("Migration journal progress does not match reviewed plan steps")
        progress_fields = {
            "status", "started_at_utc", "completed_at_utc", "rollback_started_at_utc",
            "rolled_back_at_utc", "was_created",
        }
        statuses = []
        for index, (original, progress) in enumerate(zip(plan["steps"], journal["steps"])):
            if not isinstance(progress, dict):
                raise MigrationError("Migration journal step is malformed")
            for key, value in original.items():
                if key not in progress_fields and progress.get(key) != value:
                    raise MigrationError(
                        f"Migration journal step {index} changed reviewed field {key!r}"
                    )
            unexpected = set(progress).difference(original).difference(progress_fields)
            if unexpected:
                raise MigrationError(
                    f"Migration journal step {index} contains unsupported progress fields: {sorted(unexpected)}"
                )
            if progress.get("status") not in (
                "pending", "started", "completed", "rollback_started", "rolled_back"
            ):
                raise MigrationError(f"Migration journal step {index} has invalid status")
            statuses.append(progress["status"])
        if journal["status"] in ("in_progress", "interrupted", "completed"):
            ranks = {"pending": 0, "started": 1, "completed": 2}
            if any(status not in ranks for status in statuses):
                raise MigrationError("Apply journal contains rollback progress")
            values = [ranks[status] for status in statuses]
            if any(left < right for left, right in zip(values, values[1:])):
                raise MigrationError("Apply journal progress is not an ordered prefix")
            if statuses.count("started") > 1:
                raise MigrationError("Apply journal contains multiple started actions")
            if journal["status"] == "completed" and any(status != "completed" for status in statuses):
                raise MigrationError("Completed journal has incomplete actions")
        else:
            seen_rollback = False
            rollback_started = 0
            for status in statuses:
                if status in ("rollback_started", "rolled_back"):
                    seen_rollback = True
                    rollback_started += status == "rollback_started"
                elif seen_rollback:
                    raise MigrationError("Rollback journal progress is not an ordered suffix")
            if rollback_started > 1:
                raise MigrationError("Rollback journal contains multiple active reverse actions")
            if journal["status"] == "rolled_back" and any(status != "rolled_back" for status in statuses):
                raise MigrationError("Rolled-back journal has unreversed actions")
        journal_id = journal.get("journal_file_id")
        if journal_id is not None and (not isinstance(journal_id, str) or not journal_id):
            raise MigrationError("Migration journal has an invalid pinned file ID")
        return journal

    def _journal_progress_score(self, journal: Dict[str, Any]) -> int:
        rank = {
            "pending": 0, "started": 1, "completed": 2,
            "rollback_started": 3, "rolled_back": 4,
        }
        return sum(rank[step["status"]] for step in journal["steps"])

    def _load_journal(self, control_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Load one validated pinned journal; corruption and duplicates are stop conditions."""
        local = None
        if self.journal_local_path and self.journal_local_path.exists():
            try:
                local = json.loads(self.journal_local_path.read_text("utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MigrationError(f"Local migration journal is unreadable: {exc}") from exc
        if control_id is None:
            controls = self._find_child_by_name(self.root_id, CANONICAL_CONTROL_FOLDER)
            if len(controls) > 1:
                raise AmbiguousLayoutError("Duplicate top-level 06_control items")
            if controls and controls[0].get("mimeType") != FOLDER_MIME_TYPE:
                raise MigrationError("Top-level 06_control name has a MIME collision")
            control_id = controls[0]["id"] if controls else None
        remote = None
        if control_id:
            same_name = self._find_child_by_name(control_id, JOURNAL_FILE_NAME)
            if len(same_name) > 1:
                raise AmbiguousLayoutError("Duplicate migration journal files under 06_control")
            if same_name:
                item = same_name[0]
                if item.get("mimeType") != "application/json":
                    raise MigrationError("Migration journal name is occupied by a non-JSON item")
                try:
                    remote = json.loads(self._read_file_bytes(item["id"]).decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError, OSError) as exc:
                    raise MigrationError(f"Drive migration journal is unreadable: {exc}") from exc
                if remote.get("journal_file_id") != item["id"]:
                    raise MigrationError("Drive migration journal does not pin its own file ID")
        if local is None and remote is None:
            return None
        if local is not None:
            self._validate_journal(local)
        if remote is not None:
            self._validate_journal(remote)
        if local is not None and remote is not None:
            immutable_keys = ("plan_id", "expected_root_id", "root_id", "plan_sha256", "control_folder_id", "journal_file_id")
            if any(local.get(key) != remote.get(key) for key in immutable_keys):
                raise MigrationError("Local and Drive journals have different immutable identities")
            return max((local, remote), key=self._journal_progress_score)
        selected = local or remote
        if selected.get("journal_file_id") and remote is None:
            raise MigrationError("Pinned Drive migration journal is missing")
        return selected

    def _save_journal(self, journal: Dict[str, Any], control_id: str) -> str:
        """Persist locally first, then create or update only the pinned Drive journal."""
        self._validate_journal(journal)
        if control_id != journal["control_folder_id"]:
            raise DriftError("Migration journal control parent differs from its immutable pin")
        self._save_local_receipt(journal)
        import io
        from googleapiclient.http import MediaIoBaseUpload
        journal_file_id = journal.get("journal_file_id")
        if journal_file_id:
            meta = self._get_metadata(journal_file_id)
            if (not meta or meta.get("trashed") or meta.get("name") != JOURNAL_FILE_NAME
                    or meta.get("mimeType") != "application/json"
                    or list(meta.get("parents") or []) != [control_id]):
                raise DriftError("Pinned migration journal identity has drifted")
            try:
                remote_before = json.loads(self._read_file_bytes(journal_file_id).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MigrationError("Pinned Drive migration journal is corrupt") from exc
            self._validate_journal(remote_before)
            for key in ("plan_id", "expected_root_id", "root_id", "plan_sha256", "control_folder_id", "journal_file_id"):
                if remote_before.get(key) != journal.get(key):
                    raise MigrationError("Refusing to overwrite a different migration journal")
            media = MediaIoBaseUpload(
                io.BytesIO(_json_bytes(journal)), mimetype="application/json", resumable=False
            )
            self.drive_service.files().update(
                fileId=journal_file_id, media_body=media, fields="id",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            return journal_file_id
        if self._find_child_by_name(control_id, JOURNAL_FILE_NAME):
            raise MigrationError("A migration journal exists without the reviewed pinned ID")
        ids = self._generate_ids(1, "files")
        journal["journal_file_id"] = ids[0]
        self._save_local_receipt(journal)
        media = MediaIoBaseUpload(
            io.BytesIO(_json_bytes(journal)), mimetype="application/json", resumable=False
        )
        created = self.drive_service.files().create(
            body={
                "id": ids[0], "name": JOURNAL_FILE_NAME,
                "parents": [control_id], "mimeType": "application/json",
            },
            media_body=media, fields="id", supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        if created.get("id") != ids[0]:
            raise MigrationError("Drive created migration journal with an unexpected ID")
        return ids[0]

    def plan(self) -> Dict[str, Any]:
        """Inspect every pinned source and return a read-only, immutable action plan."""
        root_meta = self._get_metadata(self.root_id)
        if (not root_meta or root_meta.get("trashed")
                or root_meta.get("mimeType") != FOLDER_MIME_TYPE):
            raise SafetyPinError("Pinned Drive root is missing, trashed, or not a folder")
        root_children = self._list_children(self.root_id)

        control_candidates = [
            item for item in root_children
            if item.get("name") == CANONICAL_CONTROL_FOLDER
        ]
        if len(control_candidates) == 1 and control_candidates[0].get("mimeType") == FOLDER_MIME_TYPE:
            journal = self._load_journal(control_candidates[0]["id"])
            if journal and journal.get("status") in (
                "in_progress", "interrupted", "rolling_back", "rollback_interrupted"
            ):
                return {
                    "plan_id": journal["plan_id"],
                    "status": "interrupted_migration_found",
                    "mode": "interrupted",
                    "expected_root_id": self.expected_root_id,
                    "journal": journal,
                    "summary": "Interrupted migration detected. Run with --resume or --rollback.",
                    "read_only": True,
                }

        legacy_names = {
            "current-release.json",
            LEGACY_BDL_WRAPPER,
            LEGACY_WDI_WRAPPER,
            LEGACY_NBP_CONTROL_FOLDER,
        }
        has_legacy = any(item.get("name") in legacy_names for item in root_children)
        releases_candidates = [
            item for item in root_children
            if item.get("name") == "releases" and item.get("mimeType") == FOLDER_MIME_TYPE
        ]
        has_canonical = False
        if len(releases_candidates) == 1:
            release_children = self._list_children(releases_candidates[0]["id"])
            has_canonical = any(
                item.get("name") in CANONICAL_SOURCES
                and item.get("mimeType") == FOLDER_MIME_TYPE
                for item in release_children
            )
        if len(control_candidates) == 1 and control_candidates[0].get("mimeType") == FOLDER_MIME_TYPE:
            control_children = self._list_children(control_candidates[0]["id"])
            has_canonical = has_canonical or any(
                item.get("name") == CANONICAL_NBP_CONTROL_FOLDER
                and item.get("mimeType") == FOLDER_MIME_TYPE
                for item in control_children
            )
        if has_legacy and has_canonical:
            raise AmbiguousLayoutError(
                f"Conflicting legacy and canonical layout structures found under root {self.root_id}"
            )
        if has_canonical:
            return {
                "plan_id": f"plan-verify-{uuid4().hex[:8]}",
                "status": "already_canonical",
                "mode": "canonical",
                "expected_root_id": self.expected_root_id,
                "root_id": self.root_id,
                "summary": "Google Drive layout is already established in canonical structure.",
                "read_only": True,
            }
        if not has_legacy:
            raise MigrationError("Migration requires the established legacy layout, found 'fresh'")

        def one(children: List[Dict[str, Any]], name: str, mime_type: str, location: str) -> Dict[str, Any]:
            matches = [item for item in children if item.get("name") == name]
            if len(matches) > 1:
                raise AmbiguousLayoutError(
                    f"Expected exactly one {name!r} in {location}; found {len(matches)}"
                )
            if not matches:
                raise MigrationError(f"Expected exactly one {name!r} in {location}; found 0")
            item = matches[0]
            if item.get("mimeType") != mime_type:
                raise MigrationError(
                    f"{location}/{name} has MIME type {item.get('mimeType')!r}, expected {mime_type!r}"
                )
            return item

        top_folders = {}
        for name in (
            "releases", LEGACY_BDL_WRAPPER, LEGACY_WDI_WRAPPER,
            LEGACY_NBP_CONTROL_FOLDER, CANONICAL_CONTROL_FOLDER,
            "01_landing", "02_bronze", "03_silver", "04_gold", ARCHIVE_FOLDER,
        ):
            top_folders[name] = one(root_children, name, FOLDER_MIME_TYPE, "root")
        nbp_pointer = one(root_children, "current-release.json", "application/json", "root")
        releases = top_folders["releases"]
        control = top_folders[CANONICAL_CONTROL_FOLDER]
        archive = top_folders[ARCHIVE_FOLDER]

        source_manifests: Dict[str, Dict[str, Any]] = {}
        source_release_dirs: Dict[str, str] = {}
        pins: Dict[str, Any] = {}

        def pin_source(
            source: str,
            pointer: Dict[str, Any],
            release_root: Dict[str, Any],
            *,
            wrapper_id: Optional[str],
        ) -> None:
            if list(pointer.get("parents") or []) != [self.root_id if source == "nbp" else wrapper_id]:
                raise MigrationError(f"{source} current pointer has the wrong parent")
            pointer_bytes = self._read_file_bytes(pointer["id"])
            try:
                pointer_doc = json.loads(pointer_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MigrationError(f"{source} current pointer is invalid JSON") from exc
            if not isinstance(pointer_doc, dict):
                raise MigrationError(f"{source} current pointer is not an object")
            release_id = pointer_doc.get("release_id")
            manifest_id = pointer_doc.get("manifest_file_id")
            manifest_hash = pointer_doc.get("manifest_sha256")
            if not isinstance(release_id, str) or not UUID_REGEX.fullmatch(release_id):
                raise MigrationError(f"{source} current pointer has an invalid release_id")
            if not isinstance(manifest_id, str) or not manifest_id or not isinstance(manifest_hash, str):
                raise MigrationError(f"{source} current pointer is incomplete")

            release_children = self._list_children(release_root["id"])
            release_names: Set[str] = set()
            for candidate in release_children:
                candidate_name = candidate.get("name")
                if (candidate.get("mimeType") != FOLDER_MIME_TYPE
                        or not isinstance(candidate_name, str)
                        or not UUID_REGEX.fullmatch(candidate_name)
                        or list(candidate.get("parents") or []) != [release_root["id"]]):
                    raise MigrationError(
                        f"{source} releases contains a non-UUID directory or wrong-parent item"
                    )
                if candidate_name in release_names:
                    raise MigrationError(f"{source} releases repeats UUID directory {candidate_name}")
                release_names.add(candidate_name)
            release_dir = one(release_children, release_id, FOLDER_MIME_TYPE, f"{source} releases")
            if list(release_dir.get("parents") or []) != [release_root["id"]]:
                raise MigrationError(f"{source} current release directory has the wrong parent")
            manifest_meta = self._get_metadata(manifest_id)
            if (not manifest_meta or manifest_meta.get("trashed")
                    or manifest_meta.get("name") != "release.json"
                    or manifest_meta.get("mimeType") != "application/json"
                    or list(manifest_meta.get("parents") or []) != [release_dir["id"]]):
                raise MigrationError(f"{source} manifest ID is not release.json in the current release")
            manifest_bytes = self._read_file_bytes(manifest_id)
            actual_manifest_hash = sha256(manifest_bytes).hexdigest()
            if actual_manifest_hash != manifest_hash:
                raise MigrationError(f"{source} pointer manifest hash does not match the pinned manifest bytes")
            try:
                manifest = json.loads(manifest_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MigrationError(f"{source} manifest is invalid JSON") from exc
            if not isinstance(manifest, dict) or manifest.get("release_id") != release_id:
                raise MigrationError(f"{source} manifest release identity is inconsistent")

            target_pins = []
            seen_targets: Set[str] = set()
            seen_tables: Set[Tuple[str, str]] = set()
            datasets = manifest.get("datasets", [])
            if not isinstance(datasets, list):
                raise MigrationError(f"{source} manifest datasets are malformed")
            for dataset in datasets:
                if not isinstance(dataset, dict):
                    raise MigrationError(f"{source} manifest contains a malformed dataset")
                layer = dataset.get("layer")
                table_name = dataset.get("table_name")
                if layer not in ALL_MEDALLION_LAYERS or not isinstance(table_name, str) or not table_name:
                    raise MigrationError(f"{source} manifest contains an invalid layer/table")
                table_key = (layer, table_name)
                if table_key in seen_tables:
                    raise MigrationError(f"{source} manifest repeats table {layer}/{table_name}")
                seen_tables.add(table_key)
                files = dataset.get("files")
                if not isinstance(files, list) or not files:
                    raise MigrationError(f"{source} manifest table {table_name} has no file targets")
                for entry in files:
                    if not isinstance(entry, dict) or not isinstance(entry.get("id"), str) or not entry["id"]:
                        raise MigrationError(f"{source} manifest table {table_name} has an invalid target")
                    target_id = entry["id"]
                    if target_id in seen_targets:
                        raise MigrationError(f"{source} manifest repeats target ID {target_id}")
                    seen_targets.add(target_id)
                    target = self._get_metadata(target_id)
                    if (not target or target.get("trashed")
                            or list(target.get("parents") or []) != [release_dir["id"]]):
                        raise MigrationError(f"{source} manifest target {target_id} is missing or outside the current release")
                    target_pins.append({
                        "id": target_id,
                        "name": target.get("name"),
                        "mime_type": target.get("mimeType"),
                        "parent_id": release_dir["id"],
                    })

            pins[source] = {
                "release_id": release_id,
                "code_sha": manifest.get("code_sha", ""),
                "pointer_file_id": pointer["id"],
                "pointer_parent_id": self.root_id if source == "nbp" else wrapper_id,
                "pointer_sha256": sha256(pointer_bytes).hexdigest(),
                "manifest_file_id": manifest_id,
                "manifest_sha256": actual_manifest_hash,
                "releases_folder_id": release_root["id"],
                "release_dir_id": release_dir["id"],
                "wrapper_folder_id": wrapper_id,
                "targets": target_pins,
            }
            source_manifests[source] = manifest
            source_release_dirs[source] = release_dir["id"]

        pin_source("nbp", nbp_pointer, releases, wrapper_id=None)
        wrappers: Dict[str, Dict[str, Any]] = {}
        wrapper_releases: Dict[str, Dict[str, Any]] = {}
        wrapper_pointers: Dict[str, Dict[str, Any]] = {}
        for source, wrapper_name in (("bdl", LEGACY_BDL_WRAPPER), ("wdi", LEGACY_WDI_WRAPPER)):
            wrapper = top_folders[wrapper_name]
            wrappers[source] = wrapper
            children = self._list_children(wrapper["id"])
            pointer = one(children, "current-release.json", "application/json", wrapper_name)
            release_root = one(children, "releases", FOLDER_MIME_TYPE, wrapper_name)
            if list(release_root.get("parents") or []) != [wrapper["id"]]:
                raise MigrationError(f"{source} releases folder has the wrong parent")
            wrapper_releases[source] = release_root
            wrapper_pointers[source] = pointer
            pin_source(source, pointer, release_root, wrapper_id=wrapper["id"])

        ingestion = top_folders[LEGACY_NBP_CONTROL_FOLDER]
        ingestion_children = self._list_children(ingestion["id"])
        state_pointer = one(
            ingestion_children, "current-ingestion-state.json", "application/json",
            LEGACY_NBP_CONTROL_FOLDER,
        )
        state_bytes = self._read_file_bytes(state_pointer["id"])
        try:
            state_doc = json.loads(state_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MigrationError("current-ingestion-state.json is invalid JSON") from exc
        state_id = state_doc.get("state_file_id") if isinstance(state_doc, dict) else None
        state_hash = state_doc.get("state_sha256") if isinstance(state_doc, dict) else None
        if not isinstance(state_id, str) or not state_id or not isinstance(state_hash, str):
            raise MigrationError("current-ingestion-state.json is incomplete")
        state_meta = self._get_metadata(state_id)
        if not state_meta or state_meta.get("trashed") or state_meta.get("mimeType") != "application/json":
            raise MigrationError("Referenced NBP state snapshot is missing or invalid")
        state_snapshot = self._read_file_bytes(state_id)
        if sha256(state_snapshot).hexdigest() != state_hash:
            raise MigrationError("Referenced NBP state snapshot hash does not match")
        control_children = self._list_children(control["id"])
        campaigns = one(
            control_children, CANONICAL_SOURCE_CAMPAIGNS_FOLDER, FOLDER_MIME_TYPE,
            CANONICAL_CONTROL_FOLDER,
        )
        if list(campaigns.get("parents") or []) != [control["id"]]:
            raise MigrationError("source_campaigns must remain directly under 06_control")
        if [item for item in ingestion_children if item.get("name") == CANONICAL_SOURCE_CAMPAIGNS_FOLDER]:
            raise MigrationError("source_campaigns also exists under legacy ingestion-control")
        pins["control"] = {
            "ingestion_control_id": ingestion["id"],
            "state_pointer_id": state_pointer["id"],
            "state_pointer_name": "current-ingestion-state.json",
            "state_pointer_parent_id": ingestion["id"],
            "state_pointer_sha256": sha256(state_bytes).hexdigest(),
            "state_snapshot_id": state_id,
            "state_snapshot_name": state_meta.get("name"),
            "state_snapshot_parent_ids": list(state_meta.get("parents") or []),
            "state_snapshot_sha256": state_hash,
        }
        pins["source_campaigns"] = {
            "id": campaigns["id"],
            "parent_id": control["id"],
            "name": CANONICAL_SOURCE_CAMPAIGNS_FOLDER,
        }

        created_at = datetime.now(timezone.utc).isoformat()
        steps: List[Dict[str, Any]] = []
        def move_step(step_id: str, item: Dict[str, Any], destination: str, to_name: Optional[str] = None) -> None:
            step = {
                "step_id": step_id,
                "action": "move",
                "item_id": item["id"],
                "from_parent_id": item["parents"][0],
                "to_parent_id": destination,
                "from_name": item["name"],
                "to_name": to_name or item["name"],
                "mime_type": item["mimeType"],
                "status": "pending",
            }
            if item["mimeType"] == FOLDER_MIME_TYPE:
                children = self._list_children(item["id"])
                step["initial_children"] = [
                    {
                        "id": child["id"],
                        "name": child.get("name"),
                        "mime_type": child.get("mimeType"),
                    }
                    for child in sorted(children, key=lambda child: child["id"])
                ]
            steps.append(step)
        def object_step(
            step_id: str, item_id: str, name: str, mime_type: str, parent_id: str,
            object_kind: str, *, preexisting: bool = False,
            target_id: Optional[str] = None, document: Optional[Dict[str, Any]] = None,
        ) -> Dict[str, Any]:
            step = {
                "step_id": step_id,
                "action": "ensure_object",
                "item_id": item_id,
                "name": name,
                "mime_type": mime_type,
                "parent_id": parent_id,
                "object_kind": object_kind,
                "preexisting": preexisting,
                "status": "pending",
            }
            if target_id is not None:
                step["target_id"] = target_id
            if document is not None:
                step["document"] = document
                step["content_sha256"] = sha256(_json_bytes(document)).hexdigest()
            steps.append(step)
            return step

        releases_nbp_id = self._generate_ids(1, "files")[0]
        object_step(
            "create_releases_nbp", releases_nbp_id, "nbp", FOLDER_MIME_TYPE,
            releases["id"], "folder",
        )
        move_step("move_nbp_control", ingestion, control["id"], CANONICAL_NBP_CONTROL_FOLDER)
        for item in self._list_children(releases["id"]):
            if item.get("mimeType") == FOLDER_MIME_TYPE and UUID_REGEX.fullmatch(item.get("name", "")):
                move_step(f"move_nbp_release_{item['id']}", item, releases_nbp_id)
        move_step("move_nbp_pointer", nbp_pointer, releases_nbp_id)
        for source in ("bdl", "wdi"):
            release_root = wrapper_releases[source]
            move_step(
                f"move_{source}_releases", release_root, releases["id"], source
            )
            move_step(
                f"move_{source}_pointer", wrapper_pointers[source], release_root["id"]
            )
        move_step("archive_bdl_wrapper", wrappers["bdl"], archive["id"])
        move_step("archive_wdi_wrapper", wrappers["wdi"], archive["id"])

        def folder_spec(parent_id: str, name: str, step_id: str, *, require_empty: bool) -> str:
            matches = self._find_child_by_name(parent_id, name)
            if len(matches) > 1:
                raise MigrationError(f"Navigation path {parent_id}/{name} is duplicated")
            if matches:
                item = matches[0]
                if item.get("mimeType") != FOLDER_MIME_TYPE:
                    raise MigrationError(f"Navigation path {parent_id}/{name} has a MIME collision")
                if list(item.get("parents") or []) != [parent_id]:
                    raise MigrationError(f"Navigation folder {item['id']} has the wrong parent")
                if require_empty and self._list_children(item["id"]):
                    raise MigrationError(
                        f"Existing navigation folder {parent_id}/{name} is non-empty; refusing to replace content"
                    )
                item_id = item["id"]
                preexisting = True
            else:
                item_id = self._generate_ids(1, "files")[0]
                preexisting = False
            object_step(
                step_id, item_id, name, FOLDER_MIME_TYPE, parent_id, "folder",
                preexisting=preexisting,
            )
            return item_id

        layer_current: Dict[str, str] = {}
        for layer in ALL_MEDALLION_LAYERS:
            layer_id = top_folders[layer]["id"]
            layer_current[layer] = folder_spec(
                layer_id, "current", f"nav_{layer}_current", require_empty=False
            )

        for source in CANONICAL_SOURCES:
            manifest = source_manifests[source]
            datasets_by_layer = {layer: [] for layer in ALL_MEDALLION_LAYERS}
            for dataset in manifest["datasets"]:
                datasets_by_layer[dataset["layer"]].append(dataset)
            for layer in ALL_MEDALLION_LAYERS:
                source_nav_id = folder_spec(
                    layer_current[layer], source,
                    f"nav_{layer}_{source}", require_empty=True,
                )
                receipts: Dict[str, Any] = {}
                for dataset in datasets_by_layer[layer]:
                    table_name = dataset["table_name"]
                    files = dataset["files"]
                    shortcut_receipts = []
                    if len(files) == 1:
                        container_id = source_nav_id
                        shortcut_names = [f"{table_name}.parquet"]
                    else:
                        container_id = folder_spec(
                            source_nav_id, table_name,
                            f"nav_{layer}_{source}_{table_name}_parts",
                            require_empty=True,
                        )
                        shortcut_names = [
                            entry.get("name", f"{table_name}--part-{index}.parquet")
                            for index, entry in enumerate(files)
                        ]
                        if len(set(shortcut_names)) != len(shortcut_names):
                            raise MigrationError(f"{source} manifest repeats part names for {table_name}")
                    shortcut_ids = self._generate_ids(len(files), "shortcuts")
                    for index, (entry, shortcut_name, shortcut_id) in enumerate(
                        zip(files, shortcut_names, shortcut_ids)
                    ):
                        object_step(
                            f"nav_{layer}_{source}_{table_name}_shortcut_{index}",
                            shortcut_id, shortcut_name, SHORTCUT_MIME_TYPE,
                            container_id, "shortcut", target_id=entry["id"],
                        )
                        shortcut_receipts.append({
                            "name": shortcut_name,
                            "shortcut_id": shortcut_id,
                            "target_id": entry["id"],
                            "size": entry.get("size", 0),
                            "sha256": entry.get("sha256", ""),
                        })
                    receipt = {
                        "is_multi_part": len(files) > 1,
                        "file_count": len(files),
                        "shortcuts": shortcut_receipts,
                    }
                    if len(files) > 1:
                        receipt["container_id"] = container_id
                    receipts[table_name] = receipt
                index_doc = {
                    "format_version": 1,
                    "source_id": source,
                    "layer": layer,
                    "release_id": manifest["release_id"],
                    "updated_at_utc": created_at,
                    "status": "current_verified",
                    "tables": receipts,
                }
                index_id = self._generate_ids(1, "files")[0]
                object_step(
                    f"nav_{layer}_{source}_index", index_id,
                    "navigation-index.json", "application/json",
                    source_nav_id, "json", document=index_doc,
                )

        plan = {
            "plan_id": f"plan-migrate-{uuid4().hex[:8]}",
            "status": "planned",
            "mode": "legacy",
            "expected_root_id": self.expected_root_id,
            "root_id": self.root_id,
            "control_folder_id": control["id"],
            "releases_root_id": releases["id"],
            "pins": pins,
            "steps": steps,
            "step_count": len(steps),
            "read_only": True,
            "created_at_utc": created_at,
        }
        canonical = json.dumps(plan, sort_keys=True, separators=(",", ":")).encode("utf-8")
        plan["plan_sha256"] = sha256(canonical).hexdigest()
        return plan

    # -------------------------------------------------------------------------
    # Apply
    # -------------------------------------------------------------------------

    def apply(
        self,
        plan: Optional[Dict[str, Any]] = None,
        *,
        resume: bool = False,
        confirmed: bool = False,
        stop_after_step: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Apply exactly one reviewed plan, reconciling every started action by pinned ID."""
        if not confirmed:
            raise MigrationError("Mutating operation 'apply' requires explicit confirmed=True.")
        if resume:
            journal = self._load_journal()
            if not journal:
                raise MigrationError("Cannot resume: no migration journal found")
            self._validate_journal(journal)
            if journal["status"] in ("rolling_back", "rollback_interrupted", "rolled_back"):
                raise MigrationError("Cannot resume apply after rollback has started")
            if journal["status"] == "completed":
                validation = self._validate_canonical_layout(journal["pins"])
                return {
                    "status": "migration_completed",
                    "plan_id": journal["plan_id"],
                    "journal_file_id": journal.get("journal_file_id"),
                    "validation": validation,
                    "journal": journal,
                }
        else:
            if plan is None:
                raise MigrationError(
                    "Apply requires an exact reviewed plan; run the read-only plan operation first"
                )
            if plan.get("status") == "already_canonical":
                if plan.get("expected_root_id") != self.root_id or plan.get("root_id") != self.root_id:
                    raise SafetyPinError("Canonical verification plan root pins do not match")
                return self.verify()
            self._validate_plan(plan)
            immutable_plan = copy.deepcopy(plan)
            journal = copy.deepcopy(plan)
            journal["schema_version"] = 1
            journal["plan"] = immutable_plan
            journal["plan_sha256"] = immutable_plan["plan_sha256"]
            journal["status"] = "in_progress"
            journal["started_at_utc"] = datetime.now(timezone.utc).isoformat()

        control_id = journal["control_folder_id"]
        controls = self._find_child_by_name(self.root_id, CANONICAL_CONTROL_FOLDER)
        if (len(controls) != 1 or controls[0].get("mimeType") != FOLDER_MIME_TYPE
                or controls[0]["id"] != control_id
                or list(controls[0].get("parents") or []) != [self.root_id]):
            raise DriftError("Pinned top-level 06_control folder is missing or changed")
        self.storage.authorize_writes()
        journal["status"] = "in_progress"
        journal.pop("failure", None)
        self._assert_no_drift(journal["pins"], journal["steps"])
        journal_file_id = self._save_journal(journal, control_id)

        executed = 0
        try:
            for step in journal["steps"]:
                if step["status"] == "completed":
                    continue
                if step["status"] not in ("pending", "started"):
                    raise MigrationError(
                        f"Apply refuses step {step['step_id']} in status {step['status']!r}"
                    )
                if stop_after_step is not None and executed >= stop_after_step:
                    journal["status"] = "interrupted"
                    self._save_journal(journal, control_id)
                    return {
                        "status": "interrupted",
                        "plan_id": journal["plan_id"],
                        "steps_executed": executed,
                        "journal_file_id": journal_file_id,
                        "journal": journal,
                    }
                step["status"] = "started"
                step["started_at_utc"] = datetime.now(timezone.utc).isoformat()
                self._save_journal(journal, control_id)
                if step["action"] == "ensure_object":
                    self._execute_owned_object(step)
                elif step["action"] == "move":
                    self._move_and_rename_item(
                        step["item_id"],
                        expected_name=step["from_name"],
                        from_parent_id=step["from_parent_id"],
                        to_parent_id=step["to_parent_id"],
                        to_name=step["to_name"],
                    )
                else:
                    raise MigrationError(f"Unsupported migration action {step['action']!r}")
                step["status"] = "completed"
                step["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
                self._save_journal(journal, control_id)
                executed += 1

            validation = self._validate_canonical_layout(journal["pins"])
            journal["status"] = "completed"
            journal["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
            journal["validation"] = validation
            self._save_journal(journal, control_id)
            return {
                "status": "migration_completed",
                "plan_id": journal["plan_id"],
                "journal_file_id": journal_file_id,
                "validation": validation,
                "journal": journal,
            }
        except BaseException as exc:
            self._mark_local_failure(journal, exc)
            raise

    def _allowed_owned_children(self, steps: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
        allowed: Dict[str, Set[str]] = {}
        for step in steps:
            if step["action"] == "ensure_object":
                allowed.setdefault(step["parent_id"], set()).add(step["item_id"])
            elif step["action"] == "move":
                allowed.setdefault(step["to_parent_id"], set()).add(step["item_id"])
        return allowed

    def _assert_transition_states(self, steps: List[Dict[str, Any]]) -> None:
        allowed = self._allowed_owned_children(steps)
        action_by_item = {step["item_id"]: step for step in steps}
        for step in steps:
            status = step["status"]
            if step["action"] == "move":
                meta = self._get_metadata(step["item_id"])
                if not meta or meta.get("trashed") or meta.get("mimeType") != step["mime_type"]:
                    raise DriftError(f"Migration item {step['item_id']} is missing or changed")
                state = (meta.get("name"), tuple(meta.get("parents") or []))
                original = (step["from_name"], (step["from_parent_id"],))
                applied = (step["to_name"], (step["to_parent_id"],))
                valid = {original}
                if status in ("started", "completed", "rollback_started"):
                    valid.add(applied)
                if status == "completed":
                    valid = {applied}
                if status == "rolled_back":
                    valid = {original}
                if state not in valid:
                    raise DriftError(
                        f"Migration item {step['item_id']} is outside its recorded transition"
                    )
            else:
                if step["preexisting"]:
                    self._verify_owned_object(step)
                    continue
                allow_missing = status in ("pending", "started", "rollback_started", "rolled_back")
                meta = self._verify_owned_object(step, allow_missing=allow_missing)
                if status == "completed" and meta is None:
                    raise DriftError(f"Completed object {step['item_id']} is missing")
                if status == "rolled_back" and meta is not None:
                    raise DriftError(f"Rolled-back object {step['item_id']} still exists")
                if meta is not None and step["object_kind"] == "folder":
                    foreign = [
                        child for child in self._list_children(step["item_id"])
                        if child["id"] not in allowed.get(step["item_id"], set())
                    ]
                    if foreign:
                        raise DriftError(
                            f"Owned folder {step['item_id']} contains foreign objects: "
                            + ", ".join(child["id"] for child in foreign)
                        )

        for container_step in steps:
            if (container_step["action"] != "move"
                    or container_step["mime_type"] != FOLDER_MIME_TYPE):
                continue
            container_id = container_step["item_id"]
            expected_ids: Set[str] = set()
            pinned_ids: Set[str] = set()
            for child in container_step["initial_children"]:
                child_id = child["id"]
                pinned_ids.add(child_id)
                meta = self._get_metadata(child_id)
                child_action = action_by_item.get(child_id)
                if (not meta or meta.get("trashed")
                        or meta.get("mimeType") != child["mime_type"]
                        or (child_action is None and meta.get("name") != child["name"])):
                    raise DriftError(
                        f"Pinned child {child_id} of moved folder {container_id} changed"
                    )
                parents = list(meta.get("parents") or [])
                if child_action is None and parents != [container_id]:
                    raise DriftError(
                        f"Pinned child {child_id} left moved folder {container_id}"
                    )
                if parents == [container_id]:
                    expected_ids.add(child_id)
            for candidate in steps:
                if candidate["item_id"] in pinned_ids:
                    continue
                if candidate["action"] == "move":
                    relevant = container_id in (
                        candidate["from_parent_id"], candidate["to_parent_id"]
                    )
                else:
                    relevant = candidate["parent_id"] == container_id
                if not relevant:
                    continue
                meta = self._get_metadata(candidate["item_id"])
                if (meta and not meta.get("trashed")
                        and list(meta.get("parents") or []) == [container_id]):
                    expected_ids.add(candidate["item_id"])
            actual_ids = {
                child["id"] for child in self._list_children(container_id)
            }
            if actual_ids != expected_ids:
                raise DriftError(
                    f"Moved folder {container_id} child set changed; "
                    f"expected {sorted(expected_ids)}, found {sorted(actual_ids)}"
                )

    def rollback(
        self,
        journal: Optional[Dict[str, Any]] = None,
        *,
        confirmed: bool = False,
    ) -> Dict[str, Any]:
        """Reverse recorded transitions in reverse order and delete only pinned owned IDs."""
        if not confirmed:
            raise MigrationError("Mutating operation 'rollback' requires explicit confirmed=True.")
        if journal is None:
            journal = self._load_journal()
            if not journal:
                raise MigrationError("Cannot rollback: no migration journal found")
        self._validate_journal(journal)
        if journal["status"] == "rolled_back":
            return {
                "status": "migration_rolled_back",
                "plan_id": journal["plan_id"],
                "post_rollback_mode": detect_layout_mode(self.storage, self.root_id),
                "journal": journal,
            }
        control_id = journal["control_folder_id"]
        control_meta = self._get_metadata(control_id)
        if (not control_meta or control_meta.get("trashed")
                or control_meta.get("name") != CANONICAL_CONTROL_FOLDER
                or control_meta.get("mimeType") != FOLDER_MIME_TYPE
                or list(control_meta.get("parents") or []) != [self.root_id]):
            raise DriftError("Pinned rollback journal parent is missing or changed")

        self.storage.authorize_writes()
        try:
            self._assert_no_drift(journal["pins"], journal["steps"])
            self._assert_transition_states(journal["steps"])
            journal["status"] = "rolling_back"
            journal["rollback_started_at_utc"] = journal.get(
                "rollback_started_at_utc", datetime.now(timezone.utc).isoformat()
            )
            journal.pop("failure", None)
            self._save_journal(journal, control_id)
            allowed = self._allowed_owned_children(journal["steps"])

            for step in reversed(journal["steps"]):
                if step["status"] == "rolled_back":
                    continue
                step["status"] = "rollback_started"
                step["rollback_started_at_utc"] = datetime.now(timezone.utc).isoformat()
                self._save_journal(journal, control_id)
                if step["action"] == "move":
                    meta = self._get_metadata(step["item_id"])
                    if not meta or meta.get("trashed"):
                        raise DriftError(f"Rollback item {step['item_id']} is missing or trashed")
                    current = (meta.get("name"), tuple(meta.get("parents") or []))
                    original = (step["from_name"], (step["from_parent_id"],))
                    applied = (step["to_name"], (step["to_parent_id"],))
                    if current == applied:
                        self._move_and_rename_item(
                            step["item_id"],
                            expected_name=step["to_name"],
                            from_parent_id=step["to_parent_id"],
                            to_parent_id=step["from_parent_id"],
                            to_name=step["from_name"],
                        )
                    elif current != original:
                        raise DriftError(
                            f"Rollback refuses drifted item {step['item_id']}"
                        )
                else:
                    self._rollback_owned_object(
                        step, allowed.get(step["item_id"], set())
                    )
                step["status"] = "rolled_back"
                step["rolled_back_at_utc"] = datetime.now(timezone.utc).isoformat()
                self._save_journal(journal, control_id)

            post_mode = detect_layout_mode(self.storage, self.root_id)
            if post_mode not in ("legacy", "fresh"):
                raise MigrationError(
                    f"Rollback did not restore a legacy layout; found {post_mode!r}"
                )
            journal["status"] = "rolled_back"
            journal["rollback_completed_at_utc"] = datetime.now(timezone.utc).isoformat()
            self._save_journal(journal, control_id)
            return {
                "status": "migration_rolled_back",
                "plan_id": journal["plan_id"],
                "post_rollback_mode": post_mode,
                "journal": journal,
            }
        except BaseException as exc:
            self._mark_local_failure(journal, exc, rollback=True)
            raise

    # -------------------------------------------------------------------------
    # Verification & Drift checks
    # -------------------------------------------------------------------------

    def _assert_no_drift(
        self,
        pins: Dict[str, Any],
        steps: Optional[List[Dict[str, Any]]] = None,
        context: Optional[Dict[str, str]] = None,
    ) -> None:
        """Verify all pinned publisher bytes and every known object transition."""
        if not pins:
            raise MigrationError("Migration recovery has no immutable source pins")
        steps = steps or []
        step_by_item = {
            step["item_id"]: step for step in steps if step.get("action") == "move"
        }

        def exact_transition(
            item_id: str, original_name: str, original_parent: str,
            *, mime_type: Optional[str] = None,
        ) -> Dict[str, Any]:
            meta = self._get_metadata(item_id)
            if not meta or meta.get("trashed"):
                raise DriftError(f"Pinned item {item_id} is missing or trashed")
            if mime_type and meta.get("mimeType") != mime_type:
                raise DriftError(f"Pinned item {item_id} MIME type changed")
            original = (original_name, (original_parent,))
            action = step_by_item.get(item_id)
            valid = {original}
            if action:
                applied = (action["to_name"], (action["to_parent_id"],))
                status = action["status"]
                if status == "completed":
                    valid = {applied}
                elif status in ("started", "rollback_started"):
                    valid.add(applied)
                elif status == "rolled_back":
                    valid = {original}
            current = (meta.get("name"), tuple(meta.get("parents") or []))
            if current not in valid:
                raise DriftError(
                    f"Pinned item {item_id} is outside its exact recorded location"
                )
            return meta

        campaigns = pins.get("source_campaigns")
        if not campaigns:
            raise MigrationError("Migration recovery is missing source_campaigns pin")
        exact_transition(
            campaigns["id"], campaigns["name"], campaigns["parent_id"],
            mime_type=FOLDER_MIME_TYPE,
        )

        control = pins.get("control")
        if not control:
            raise MigrationError("Migration recovery is missing NBP control pins")
        ingestion_id = control["ingestion_control_id"]
        ingestion_meta = exact_transition(
            ingestion_id, LEGACY_NBP_CONTROL_FOLDER, self.root_id,
            mime_type=FOLDER_MIME_TYPE,
        )
        state_pointer_id = control["state_pointer_id"]
        state_meta = self._get_metadata(state_pointer_id)
        if (not state_meta or state_meta.get("trashed")
                or state_meta.get("name") != control["state_pointer_name"]
                or state_meta.get("mimeType") != "application/json"
                or list(state_meta.get("parents") or []) != [ingestion_id]):
            raise DriftError("NBP state pointer identity or parent changed")
        state_bytes = self._read_file_bytes(state_pointer_id)
        if sha256(state_bytes).hexdigest() != control["state_pointer_sha256"]:
            raise DriftError("NBP state pointer bytes changed")
        try:
            state_doc = json.loads(state_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DriftError("NBP state pointer is no longer valid JSON") from exc
        if (state_doc.get("state_file_id") != control["state_snapshot_id"]
                or state_doc.get("state_sha256") != control["state_snapshot_sha256"]):
            raise DriftError("NBP state pointer reference changed")
        snapshot_meta = self._get_metadata(control["state_snapshot_id"])
        if (not snapshot_meta or snapshot_meta.get("trashed")
                or snapshot_meta.get("name") != control["state_snapshot_name"]
                or snapshot_meta.get("mimeType") != "application/json"
                or list(snapshot_meta.get("parents") or []) != control["state_snapshot_parent_ids"]):
            raise DriftError("NBP state snapshot identity or parent changed")
        if sha256(self._read_file_bytes(control["state_snapshot_id"])).hexdigest() != control["state_snapshot_sha256"]:
            raise DriftError("NBP state snapshot bytes changed")

        for source in CANONICAL_SOURCES:
            pin = pins.get(source)
            if not pin:
                raise MigrationError(f"Migration recovery is missing {source} pins")
            pointer_meta = exact_transition(
                pin["pointer_file_id"], "current-release.json",
                pin["pointer_parent_id"], mime_type="application/json",
            )
            pointer_bytes = self._read_file_bytes(pin["pointer_file_id"])
            if sha256(pointer_bytes).hexdigest() != pin["pointer_sha256"]:
                raise DriftError(f"{source} current pointer bytes changed")
            try:
                pointer_doc = json.loads(pointer_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DriftError(f"{source} current pointer is invalid JSON") from exc
            if (pointer_doc.get("release_id") != pin["release_id"]
                    or pointer_doc.get("manifest_file_id") != pin["manifest_file_id"]
                    or pointer_doc.get("manifest_sha256") != pin["manifest_sha256"]):
                raise DriftError(f"{source} current pointer identity changed")

            release_dir_parent = pin["releases_folder_id"]
            release_dir = exact_transition(
                pin["release_dir_id"], pin["release_id"], release_dir_parent,
                mime_type=FOLDER_MIME_TYPE,
            )
            manifest_meta = self._get_metadata(pin["manifest_file_id"])
            if (not manifest_meta or manifest_meta.get("trashed")
                    or manifest_meta.get("name") != "release.json"
                    or manifest_meta.get("mimeType") != "application/json"
                    or list(manifest_meta.get("parents") or []) != [pin["release_dir_id"]]):
                raise DriftError(f"{source} manifest identity or parent changed")
            manifest_bytes = self._read_file_bytes(pin["manifest_file_id"])
            if sha256(manifest_bytes).hexdigest() != pin["manifest_sha256"]:
                raise DriftError(f"{source} manifest bytes changed")
            try:
                manifest_doc = json.loads(manifest_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DriftError(f"{source} manifest is invalid JSON") from exc
            if manifest_doc.get("release_id") != pin["release_id"]:
                raise DriftError(f"{source} manifest release identity changed")
            for target in pin.get("targets", []):
                target_meta = self._get_metadata(target["id"])
                if (not target_meta or target_meta.get("trashed")
                        or target_meta.get("name") != target["name"]
                        or target_meta.get("mimeType") != target["mime_type"]
                        or list(target_meta.get("parents") or []) != [target["parent_id"]]):
                    raise DriftError(f"{source} manifest target {target['id']} changed")

        self._assert_transition_states(steps)

    def _validate_canonical_layout(self, pins: Dict[str, Any]) -> Dict[str, Any]:
        """Verify canonical layout mode, source release roots, and medallion navigation."""
        mode = detect_layout_mode(self.storage, self.root_id)
        if mode != "canonical":
            raise MigrationError(f"Post-migration validation failed: layout mode is '{mode}', expected 'canonical'")

        # Verify NBP control root
        nbp_ctrl = resolve_nbp_control_root(self.storage, self.root_id, is_writer=False)

        source_reports = {}
        for source in CANONICAL_SOURCES:
            rel_root, direct = resolve_source_release_root(self.storage, self.root_id, source, is_writer=False)
            if not direct:
                raise MigrationError(f"Post-migration validation failed: source '{source}' does not use direct_releases")
            rel_store = DriveReleaseStore(self.storage, rel_root)
            manifest = read_current_release_manifest(rel_store, rel_root)

            # Check manifest against pin if available
            if source in pins:
                expected_id = pins[source]["release_id"]
                if manifest["release_id"] != expected_id:
                    raise MigrationError(
                        f"Post-migration validation failed: source '{source}' release {manifest['release_id']} != pinned {expected_id}"
                    )

            # Verify navigation
            nav_result = verify_medallion_navigation(self.storage, self.root_id, source, manifest)
            source_reports[source] = {
                "release_root_id": rel_root,
                "current_release_id": manifest["release_id"],
                "navigation": nav_result,
            }

        return {
            "status": "canonical_layout_verified",
            "nbp_control_id": nbp_ctrl,
            "sources": source_reports,
        }

    def verify(self) -> Dict[str, Any]:
        """Perform full validation of current Drive layout without mutations."""
        mode = detect_layout_mode(self.storage, self.root_id)
        report = {
            "root_id": self.root_id,
            "mode": mode,
            "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        if mode == "canonical":
            report["canonical_validation"] = self._validate_canonical_layout({})
            report["status"] = "canonical_verified"
        elif mode == "legacy":
            report["status"] = "legacy_established"
        elif mode == "ambiguous":
            report["status"] = "ambiguous_layout"
        else:
            report["status"] = "fresh_uninitialized"
        return report
