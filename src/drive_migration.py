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
import logging
import os
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
    sync_source_medallion_navigation,
    verify_medallion_navigation,
    ALL_MEDALLION_LAYERS,
)
from release_protocol import (
    read_current_release_manifest,
    read_release_manifest,
    ReleaseProtocolError,
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

    def _list_children(self, parent_id: str, *, mime_type: Optional[str] = None) -> List[Dict[str, Any]]:
        q = f"'{parent_id}' in parents and trashed=false"
        if mime_type:
            q += f" and mimeType='{mime_type}'"
        result = []
        token = None
        while True:
            resp = self.drive_service.files().list(
                q=q,
                fields="nextPageToken,files(id,name,mimeType,parents,size,md5Checksum,shortcutDetails)",
                pageSize=100,
                pageToken=token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            result.extend(resp.get("files", []))
            token = resp.get("nextPageToken")
            if not token:
                break
        return result

    def _find_child_by_name(self, parent_id: str, name: str, *, mime_type: Optional[str] = None) -> List[Dict[str, Any]]:
        escaped_name = name.replace("\\", "\\\\").replace("'", "\\'")
        q = f"name='{escaped_name}' and '{parent_id}' in parents and trashed=false"
        if mime_type:
            q += f" and mimeType='{mime_type}'"
        resp = self.drive_service.files().list(
            q=q,
            fields="files(id,name,mimeType,parents)",
            pageSize=50,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        return resp.get("files", [])

    def _read_file_bytes(self, file_id: str) -> bytes:
        data = self.drive_service.files().get_media(
            fileId=file_id, supportsAllDrives=True
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        if not isinstance(data, bytes):
            raise MigrationError(f"Drive did not return bytes for file {file_id}")
        return data

    def _get_or_create_folder(self, parent_id: str, name: str) -> Tuple[str, bool]:
        """Get or create folder under parent_id. Returns (folder_id, was_created: bool)."""
        matches = self._find_child_by_name(parent_id, name, mime_type=FOLDER_MIME_TYPE)
        if len(matches) > 1:
            raise AmbiguousLayoutError(f"Duplicate folder '{name}' found under parent '{parent_id}'")
        if matches:
            return matches[0]["id"], False
        body = {
            "name": name,
            "mimeType": FOLDER_MIME_TYPE,
            "parents": [parent_id],
        }
        res = self.drive_service.files().create(
            body=body,
            fields="id",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        return res["id"], True

    def _move_and_rename_item(
        self,
        item_id: str,
        *,
        expected_name: str,
        from_parent_id: str,
        to_parent_id: str,
        to_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Atomically move item from from_parent_id to to_parent_id and optionally rename.

        Guarantees:
        - Fails closed if expected name or from_parent_id does not match current state.
        - Idempotently recognizes already-completed state.
        - Removes ONLY from_parent_id (never touches other parents).
        - Reads back and verifies immediately after mutation.
        """
        meta = self.drive_service.files().get(
            fileId=item_id,
            fields="id,name,parents,trashed",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

        if meta.get("trashed") is True:
            raise MigrationError(f"Item {item_id} ('{meta.get('name')}') is trashed in Drive")

        current_parents = meta.get("parents") or []
        current_name = meta.get("name")
        target_name = to_name or expected_name

        # Check already-completed state
        already_in_dest = (to_parent_id in current_parents and from_parent_id not in current_parents)
        already_has_name = (current_name == target_name)
        if already_in_dest and already_has_name:
            return meta

        # Fail closed on foreign parent or name drift
        if from_parent_id not in current_parents:
            raise MigrationError(
                f"Cannot move item {item_id} ('{current_name}'): expected source parent '{from_parent_id}' "
                f"not in actual parents {current_parents}"
            )
        if current_name != expected_name:
            raise MigrationError(
                f"Cannot move item {item_id}: expected name '{expected_name}', but found '{current_name}' (external rename)"
            )

        body = {}
        if to_name and to_name != current_name:
            body["name"] = to_name

        # Remove ONLY the recorded source parent
        updated = self.drive_service.files().update(
            fileId=item_id,
            body=body if body else None,
            addParents=to_parent_id,
            removeParents=from_parent_id,
            fields="id,name,parents,trashed",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

        # Read back and verify
        verify_meta = self.drive_service.files().get(
            fileId=item_id,
            fields="id,name,parents,trashed",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

        ver_parents = verify_meta.get("parents") or []
        ver_name = verify_meta.get("name")

        if to_parent_id not in ver_parents or from_parent_id in ver_parents or ver_name != target_name:
            raise MigrationError(
                f"Readback verification failed after moving item {item_id}: expected parent '{to_parent_id}', "
                f"name '{target_name}'; got parents {ver_parents}, name '{ver_name}'"
            )

        return verify_meta

    # -------------------------------------------------------------------------
    # Journal management
    # -------------------------------------------------------------------------

    def _load_journal(self, control_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        journal = None
        if self.journal_local_path and self.journal_local_path.is_file():
            try:
                journal = json.loads(self.journal_local_path.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning(f"Could not read local journal {self.journal_local_path}: {exc}")

        if not control_id:
            ctrl_folders = self._find_child_by_name(self.root_id, CANONICAL_CONTROL_FOLDER, mime_type=FOLDER_MIME_TYPE)
            if len(ctrl_folders) == 1:
                control_id = ctrl_folders[0]["id"]

        if control_id:
            journal_files = self._find_child_by_name(control_id, JOURNAL_FILE_NAME)
            if len(journal_files) == 1:
                try:
                    data = self._read_file_bytes(journal_files[0]["id"])
                    drive_journal = json.loads(data.decode("utf-8"))
                    # Drive journal takes precedence if both exist
                    journal = drive_journal
                except Exception as exc:
                    logger.warning(f"Could not read Drive journal: {exc}")

        if journal:
            # Validate root ID pin in journal
            j_root = journal.get("root_id") or journal.get("expected_root_id")
            if j_root and j_root != self.root_id:
                raise SafetyPinError(
                    f"Journal root ID '{j_root}' does not match active engine root ID '{self.root_id}'"
                )

        return journal

    def _save_journal(self, journal: Dict[str, Any], control_id: str) -> str:
        """Persist journal to local path and Drive top-level 06_control/ folder."""
        data = _json_bytes(journal)
        if self.journal_local_path:
            self.journal_local_path.parent.mkdir(parents=True, exist_ok=True)
            self.journal_local_path.write_bytes(data)

        import io
        from googleapiclient.http import MediaIoBaseUpload

        journal_file_id = journal.get("journal_file_id")
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/json", resumable=False)

        if journal_file_id:
            try:
                self.drive_service.files().update(
                    fileId=journal_file_id,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
                return journal_file_id
            except Exception:
                pass

        existing = self._find_child_by_name(control_id, JOURNAL_FILE_NAME)
        if existing:
            file_id = existing[0]["id"]
            self.drive_service.files().update(
                fileId=file_id,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            journal["journal_file_id"] = file_id
            return file_id
        else:
            created = self.drive_service.files().create(
                body={"name": JOURNAL_FILE_NAME, "parents": [control_id], "mimeType": "application/json"},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            journal["journal_file_id"] = created["id"]
            return created["id"]

    # -------------------------------------------------------------------------
    # Planning
    # -------------------------------------------------------------------------

    def plan(self) -> Dict[str, Any]:
        """Inspect Drive and generate an ordered, bounded read-only migration plan."""
        mode = detect_layout_mode(self.storage, self.root_id)
        if mode == "ambiguous":
            journal = self._load_journal()
            if journal and journal.get("status") in ("in_progress", "interrupted", "rolling_back"):
                return {
                    "plan_id": journal.get("plan_id"),
                    "status": "interrupted_migration_found",
                    "mode": "interrupted",
                    "expected_root_id": self.expected_root_id,
                    "journal": journal,
                    "summary": "Interrupted migration detected. Run with --resume or --rollback.",
                    "read_only": True,
                }
            raise AmbiguousLayoutError(
                f"Conflicting legacy and canonical layout structures found under root {self.root_id}"
            )

        if mode == "canonical":
            return {
                "plan_id": f"plan-verify-{uuid4().hex[:8]}",
                "status": "already_canonical",
                "mode": "canonical",
                "expected_root_id": self.expected_root_id,
                "summary": "Google Drive layout is already established in canonical structure.",
                "read_only": True,
            }

        # Legacy layout: construct migration plan
        root_children = self._list_children(self.root_id)
        by_name: Dict[str, List[Dict[str, Any]]] = {}
        for item in root_children:
            by_name.setdefault(item["name"], []).append(item)

        # Pre-flight duplicate check: reject duplicate containers or wrappers
        critical_entities = {
            "releases", "ingestion-control", "bdl-platform", "wdi-platform",
            "01_landing", "02_bronze", "03_silver", "04_gold", "05_archive", "06_control",
            "current-release.json",
        }
        duplicates = [name for name, items in by_name.items() if len(items) > 1 and name in critical_entities]
        if duplicates:
            raise AmbiguousLayoutError(f"Duplicate top-level entities found: {duplicates}")

        pins: Dict[str, Any] = {}

        # 1. NBP current release and releases folder
        nbp_root_pointer = by_name.get("current-release.json", [None])[0]
        releases_folder = by_name.get("releases", [None])[0]
        if not releases_folder:
            raise LayoutResolutionError("Legacy releases/ folder not found under root")

        releases_id = releases_folder["id"]
        releases_children = self._list_children(releases_id)

        # Find legacy NBP UUID release dirs
        nbp_uuid_dirs = [
            c for c in releases_children
            if c.get("mimeType") == FOLDER_MIME_TYPE and UUID_REGEX.match(c.get("name", ""))
        ]

        if nbp_root_pointer:
            pointer_bytes = self._read_file_bytes(nbp_root_pointer["id"])
            pointer_data = json.loads(pointer_bytes.decode("utf-8"))
            rel_id = pointer_data["release_id"]
            manifest_id = pointer_data["manifest_file_id"]
            manifest_bytes = self._read_file_bytes(manifest_id)
            manifest_data = json.loads(manifest_bytes.decode("utf-8"))

            pins["nbp"] = {
                "pointer_file_id": nbp_root_pointer["id"],
                "pointer_parent_id": self.root_id,
                "pointer_sha256": sha256(pointer_bytes).hexdigest(),
                "manifest_file_id": manifest_id,
                "manifest_sha256": sha256(manifest_bytes).hexdigest(),
                "release_id": rel_id,
                "code_sha": manifest_data.get("code_sha", ""),
                "releases_folder_id": releases_id,
                "uuid_count": len(nbp_uuid_dirs),
            }

        # 2. BDL wrapper, pointer & releases
        bdl_wrapper = by_name.get(LEGACY_BDL_WRAPPER, [None])[0]
        bdl_releases_folder = None
        bdl_pointer_file = None
        if bdl_wrapper:
            bdl_children = self._list_children(bdl_wrapper["id"])
            bdl_by_name = {c["name"]: c for c in bdl_children}
            bdl_pointer_file = bdl_by_name.get("current-release.json")
            bdl_releases_folder = bdl_by_name.get("releases")
            if bdl_pointer_file and bdl_releases_folder:
                bdl_ptr_bytes = self._read_file_bytes(bdl_pointer_file["id"])
                bdl_ptr_data = json.loads(bdl_ptr_bytes.decode("utf-8"))
                bdl_store = DriveReleaseStore(self.storage, bdl_releases_folder["id"])
                bdl_manifest = read_current_release_manifest(bdl_store, bdl_wrapper["id"])
                bdl_manifest_id = bdl_ptr_data["manifest_file_id"]
                bdl_manifest_bytes = self._read_file_bytes(bdl_manifest_id)

                pins["bdl"] = {
                    "release_id": bdl_manifest["release_id"],
                    "code_sha": bdl_manifest.get("code_sha", ""),
                    "pointer_file_id": bdl_pointer_file["id"],
                    "pointer_parent_id": bdl_wrapper["id"],
                    "pointer_sha256": sha256(bdl_ptr_bytes).hexdigest(),
                    "manifest_file_id": bdl_manifest_id,
                    "manifest_sha256": sha256(bdl_manifest_bytes).hexdigest(),
                    "releases_folder_id": bdl_releases_folder["id"],
                    "wrapper_folder_id": bdl_wrapper["id"],
                }

        # 3. WDI wrapper, pointer & releases
        wdi_wrapper = by_name.get(LEGACY_WDI_WRAPPER, [None])[0]
        wdi_releases_folder = None
        wdi_pointer_file = None
        if wdi_wrapper:
            wdi_children = self._list_children(wdi_wrapper["id"])
            wdi_by_name = {c["name"]: c for c in wdi_children}
            wdi_pointer_file = wdi_by_name.get("current-release.json")
            wdi_releases_folder = wdi_by_name.get("releases")
            if wdi_pointer_file and wdi_releases_folder:
                wdi_ptr_bytes = self._read_file_bytes(wdi_pointer_file["id"])
                wdi_ptr_data = json.loads(wdi_ptr_bytes.decode("utf-8"))
                wdi_store = DriveReleaseStore(self.storage, wdi_releases_folder["id"])
                wdi_manifest = read_current_release_manifest(wdi_store, wdi_wrapper["id"])
                wdi_manifest_id = wdi_ptr_data["manifest_file_id"]
                wdi_manifest_bytes = self._read_file_bytes(wdi_manifest_id)

                pins["wdi"] = {
                    "release_id": wdi_manifest["release_id"],
                    "code_sha": wdi_manifest.get("code_sha", ""),
                    "pointer_file_id": wdi_pointer_file["id"],
                    "pointer_parent_id": wdi_wrapper["id"],
                    "pointer_sha256": sha256(wdi_ptr_bytes).hexdigest(),
                    "manifest_file_id": wdi_manifest_id,
                    "manifest_sha256": sha256(wdi_manifest_bytes).hexdigest(),
                    "releases_folder_id": wdi_releases_folder["id"],
                    "wrapper_folder_id": wdi_wrapper["id"],
                }

        # 4. Ingestion control & source campaigns & NBP state
        ingestion_control = by_name.get(LEGACY_NBP_CONTROL_FOLDER, [None])[0]
        control_folder = by_name.get(CANONICAL_CONTROL_FOLDER, [None])[0]

        source_campaigns_folder = None
        state_pointer_file = None
        state_snapshot_id = None
        state_snapshot_sha = None

        if ingestion_control:
            ctrl_children = self._list_children(ingestion_control["id"])
            ctrl_by_name = {c["name"]: c for c in ctrl_children}
            source_campaigns_folder = ctrl_by_name.get(CANONICAL_SOURCE_CAMPAIGNS_FOLDER)

            # Check NBP ingestion state: current-ingestion-state.json or fallback state.json
            state_pointer_file = ctrl_by_name.get("current-ingestion-state.json") or ctrl_by_name.get("state.json")
            if state_pointer_file:
                st_ptr_bytes = self._read_file_bytes(state_pointer_file["id"])
                st_ptr_data = json.loads(st_ptr_bytes.decode("utf-8"))
                st_ptr_sha = sha256(st_ptr_bytes).hexdigest()

                if "state_file_id" in st_ptr_data:
                    state_snapshot_id = st_ptr_data["state_file_id"]
                    sn_bytes = self._read_file_bytes(state_snapshot_id)
                    state_snapshot_sha = sha256(sn_bytes).hexdigest()

                pins["control"] = {
                    "ingestion_control_id": ingestion_control["id"],
                    "source_campaigns_id": source_campaigns_folder["id"] if source_campaigns_folder else None,
                    "state_pointer_id": state_pointer_file["id"],
                    "state_pointer_parent_id": ingestion_control["id"],
                    "state_pointer_sha256": st_ptr_sha,
                    "state_snapshot_id": state_snapshot_id,
                    "state_snapshot_sha256": state_snapshot_sha,
                }

        # Check if source_campaigns is already under 06_control (production baseline)
        if not source_campaigns_folder and control_folder:
            c_children = self._list_children(control_folder["id"])
            for c in c_children:
                if c.get("name") == CANONICAL_SOURCE_CAMPAIGNS_FOLDER:
                    source_campaigns_folder = c
                    break

        archive_folder = by_name.get(ARCHIVE_FOLDER, [None])[0]

        # ---------------------------------------------------------------------
        # Construct migration steps
        # ---------------------------------------------------------------------
        steps: List[Dict[str, Any]] = []

        # Step 1: Ensure 06_control exists under root
        steps.append({
            "step_id": "ensure_06_control",
            "action": "ensure_folder",
            "name": CANONICAL_CONTROL_FOLDER,
            "parent_id": self.root_id,
            "existing_id": control_folder["id"] if control_folder else None,
            "status": "pending",
        })

        # Step 2: Move source_campaigns to 06_control (ONLY IF it's currently inside ingestion-control)
        if source_campaigns_folder and ingestion_control and source_campaigns_folder.get("parents") == [ingestion_control["id"]]:
            steps.append({
                "step_id": "move_source_campaigns",
                "action": "move",
                "item_id": source_campaigns_folder["id"],
                "item_name": CANONICAL_SOURCE_CAMPAIGNS_FOLDER,
                "from_parent_id": ingestion_control["id"],
                "to_parent_name": CANONICAL_CONTROL_FOLDER,
                "from_name": CANONICAL_SOURCE_CAMPAIGNS_FOLDER,
                "to_name": CANONICAL_SOURCE_CAMPAIGNS_FOLDER,
                "status": "pending",
            })

        # Step 3: Move ingestion-control to 06_control and rename to nbp
        if ingestion_control:
            steps.append({
                "step_id": "move_and_rename_nbp_control",
                "action": "move_and_rename",
                "item_id": ingestion_control["id"],
                "item_name": LEGACY_NBP_CONTROL_FOLDER,
                "from_parent_id": self.root_id,
                "to_parent_name": CANONICAL_CONTROL_FOLDER,
                "from_name": LEGACY_NBP_CONTROL_FOLDER,
                "to_name": CANONICAL_NBP_CONTROL_FOLDER,
                "status": "pending",
            })

        # Step 4: Ensure releases/nbp exists
        steps.append({
            "step_id": "ensure_releases_nbp",
            "action": "ensure_folder",
            "name": "nbp",
            "parent_id": releases_id,
            "status": "pending",
        })

        # Step 5: Move NBP release UUID folders into releases/nbp
        for uuid_dir in nbp_uuid_dirs:
            steps.append({
                "step_id": f"move_nbp_release_{uuid_dir['name'][:8]}",
                "action": "move",
                "item_id": uuid_dir["id"],
                "item_name": uuid_dir["name"],
                "from_parent_id": releases_id,
                "to_parent_name": "releases/nbp",
                "from_name": uuid_dir["name"],
                "to_name": uuid_dir["name"],
                "status": "pending",
            })

        # Step 6: Move NBP current-release.json to releases/nbp
        if nbp_root_pointer:
            steps.append({
                "step_id": "move_nbp_root_pointer",
                "action": "move",
                "item_id": nbp_root_pointer["id"],
                "item_name": "current-release.json",
                "from_parent_id": self.root_id,
                "to_parent_name": "releases/nbp",
                "from_name": "current-release.json",
                "to_name": "current-release.json",
                "status": "pending",
            })

        # Step 7: Move bdl-platform/releases to releases/ and rename to bdl
        if bdl_releases_folder and bdl_wrapper:
            steps.append({
                "step_id": "move_and_rename_bdl_releases",
                "action": "move_and_rename",
                "item_id": bdl_releases_folder["id"],
                "item_name": "releases",
                "from_parent_id": bdl_wrapper["id"],
                "to_parent_id": releases_id,
                "from_name": "releases",
                "to_name": "bdl",
                "status": "pending",
            })

        # Step 8: Move BDL current-release.json to releases/bdl
        if bdl_pointer_file and bdl_releases_folder and bdl_wrapper:
            steps.append({
                "step_id": "move_bdl_pointer",
                "action": "move",
                "item_id": bdl_pointer_file["id"],
                "item_name": "current-release.json",
                "from_parent_id": bdl_wrapper["id"],
                "to_parent_item_id": bdl_releases_folder["id"],
                "from_name": "current-release.json",
                "to_name": "current-release.json",
                "status": "pending",
            })

        # Step 9: Move wdi-platform/releases to releases/ and rename to wdi
        if wdi_releases_folder and wdi_wrapper:
            steps.append({
                "step_id": "move_and_rename_wdi_releases",
                "action": "move_and_rename",
                "item_id": wdi_releases_folder["id"],
                "item_name": "releases",
                "from_parent_id": wdi_wrapper["id"],
                "to_parent_id": releases_id,
                "from_name": "releases",
                "to_name": "wdi",
                "status": "pending",
            })

        # Step 10: Move WDI current-release.json to releases/wdi
        if wdi_pointer_file and wdi_releases_folder and wdi_wrapper:
            steps.append({
                "step_id": "move_wdi_pointer",
                "action": "move",
                "item_id": wdi_pointer_file["id"],
                "item_name": "current-release.json",
                "from_parent_id": wdi_wrapper["id"],
                "to_parent_item_id": wdi_releases_folder["id"],
                "from_name": "current-release.json",
                "to_name": "current-release.json",
                "status": "pending",
            })

        # Step 11: Ensure 05_archive exists under root
        steps.append({
            "step_id": "ensure_05_archive",
            "action": "ensure_folder",
            "name": ARCHIVE_FOLDER,
            "parent_id": self.root_id,
            "existing_id": archive_folder["id"] if archive_folder else None,
            "status": "pending",
        })

        # Step 12: Move empty bdl-platform to 05_archive
        if bdl_wrapper:
            steps.append({
                "step_id": "archive_bdl_wrapper",
                "action": "move",
                "item_id": bdl_wrapper["id"],
                "item_name": LEGACY_BDL_WRAPPER,
                "from_parent_id": self.root_id,
                "to_parent_name": ARCHIVE_FOLDER,
                "from_name": LEGACY_BDL_WRAPPER,
                "to_name": LEGACY_BDL_WRAPPER,
                "status": "pending",
            })

        # Step 13: Move empty wdi-platform to 05_archive
        if wdi_wrapper:
            steps.append({
                "step_id": "archive_wdi_wrapper",
                "action": "move",
                "item_id": wdi_wrapper["id"],
                "item_name": LEGACY_WDI_WRAPPER,
                "from_parent_id": self.root_id,
                "to_parent_name": ARCHIVE_FOLDER,
                "from_name": LEGACY_WDI_WRAPPER,
                "to_name": LEGACY_WDI_WRAPPER,
                "status": "pending",
            })

        # Step 14: Sync medallion navigation shortcuts for nbp, bdl, wdi
        steps.append({
            "step_id": "sync_medallion_navigation",
            "action": "sync_navigation",
            "sources": ["nbp", "bdl", "wdi"],
            "status": "pending",
        })

        plan = {
            "plan_id": f"plan-migrate-{uuid4().hex[:8]}",
            "status": "planned",
            "mode": "legacy",
            "expected_root_id": self.expected_root_id,
            "root_id": self.root_id,
            "releases_root_id": releases_id,
            "pins": pins,
            "steps": steps,
            "step_count": len(steps),
            "read_only": True,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        }
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
        """Apply the migration plan with durable journal, idempotency, and post-validation."""
        if not confirmed:
            raise MigrationError("Mutating operation 'apply' requires explicit confirmed=True.")

        self.storage.authorize_writes()

        if resume:
            journal = self._load_journal()
            if not journal:
                raise MigrationError("Cannot resume: no migration journal found in Drive or local path")
            plan = journal
        elif plan is None:
            plan = self.plan()

        if plan.get("status") == "already_canonical":
            return self.verify()

        pins = plan.get("pins", {})
        steps = plan.get("steps", [])

        # Ensure top-level 06_control folder exists for journal storage
        ctrl_step = next((s for s in steps if s["step_id"] == "ensure_06_control"), None)
        control_id = ctrl_step.get("resolved_id") or ctrl_step.get("existing_id") if ctrl_step else None
        if not control_id:
            control_id, _ = self._get_or_create_folder(self.root_id, CANONICAL_CONTROL_FOLDER)
            if ctrl_step:
                ctrl_step["resolved_id"] = control_id
                ctrl_step["status"] = "completed"

        # Rehydrate all context mapping from journal and completed steps
        context: Dict[str, str] = {
            "root_id": self.root_id,
            CANONICAL_CONTROL_FOLDER: control_id,
            "releases_id": plan.get("releases_root_id"),
        }
        for step in steps:
            resolved = step.get("resolved_id")
            if resolved:
                name = step.get("name")
                if name:
                    context[name] = resolved
                if step["step_id"] == "ensure_releases_nbp":
                    context["releases/nbp"] = resolved
                    context["nbp"] = resolved
                elif step["step_id"] == "ensure_06_control":
                    context[CANONICAL_CONTROL_FOLDER] = resolved
                elif step["step_id"] == "ensure_05_archive":
                    context[ARCHIVE_FOLDER] = resolved

        # Pre-apply drift check: verify current release pointers, manifests, and NBP state
        self._assert_no_drift(pins, steps, context)

        journal = dict(plan)
        journal["status"] = "in_progress"
        journal["control_folder_id"] = control_id
        if "started_at_utc" not in journal:
            journal["started_at_utc"] = datetime.now(timezone.utc).isoformat()
        journal_file_id = self._save_journal(journal, control_id)

        steps_executed = 0
        # Execute each step sequentially
        for step in steps:
            step_id = step["step_id"]
            if step.get("status") == "completed":
                continue

            if stop_after_step is not None and steps_executed >= stop_after_step:
                return {
                    "status": "interrupted",
                    "plan_id": plan["plan_id"],
                    "steps_executed": steps_executed,
                    "journal_file_id": journal_file_id,
                    "journal": journal,
                }

            action = step["action"]
            if action == "ensure_folder":
                parent_id = context.get(step.get("parent_name"), step.get("parent_id"))
                folder_id, was_created = self._get_or_create_folder(parent_id, step["name"])
                step["resolved_id"] = folder_id
                step["was_created"] = was_created
                context[step["name"]] = folder_id
                if step_id == "ensure_releases_nbp":
                    context["releases/nbp"] = folder_id
                    context["nbp"] = folder_id
                elif step_id == "ensure_05_archive":
                    context[ARCHIVE_FOLDER] = folder_id
                step["status"] = "completed"

            elif action in ("move", "move_and_rename"):
                item_id = step["item_id"]
                to_parent_id = step.get("to_parent_id")
                if not to_parent_id:
                    if step.get("to_parent_name"):
                        to_parent_id = context.get(step["to_parent_name"])
                    elif step.get("to_parent_item_id"):
                        to_parent_id = step["to_parent_item_id"]

                if not to_parent_id:
                    raise MigrationError(f"Step {step_id}: cannot resolve destination parent ID")

                from_parent_id = step["from_parent_id"]
                expected_name = step.get("from_name", step.get("item_name"))
                to_name = step.get("to_name")

                self._move_and_rename_item(
                    item_id,
                    expected_name=expected_name,
                    from_parent_id=from_parent_id,
                    to_parent_id=to_parent_id,
                    to_name=to_name,
                )
                step["status"] = "completed"

            elif action == "sync_navigation":
                for source in step.get("sources", []):
                    rel_root, direct = resolve_source_release_root(self.storage, self.root_id, source, is_writer=True)
                    rel_store = DriveReleaseStore(self.storage, rel_root)
                    manifest = read_current_release_manifest(rel_store, rel_root)
                    sync_source_medallion_navigation(self.storage, self.root_id, source, manifest)
                step["status"] = "completed"

            step["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
            self._save_journal(journal, control_id)
            steps_executed += 1

        # Post-migration validation
        validation_report = self._validate_canonical_layout(pins)

        journal["status"] = "completed"
        journal["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        journal["validation"] = validation_report
        self._save_journal(journal, control_id)

        return {
            "status": "migration_completed",
            "plan_id": plan["plan_id"],
            "journal_file_id": journal_file_id,
            "validation": validation_report,
        }

    # -------------------------------------------------------------------------
    # Rollback
    # -------------------------------------------------------------------------

    def rollback(
        self,
        journal: Optional[Dict[str, Any]] = None,
        *,
        confirmed: bool = False,
    ) -> Dict[str, Any]:
        """Reverse all completed migration steps in reverse order using the durable journal."""
        if not confirmed:
            raise MigrationError("Mutating operation 'rollback' requires explicit confirmed=True.")

        self.storage.authorize_writes()

        if journal is None:
            journal = self._load_journal()
            if not journal:
                raise MigrationError("Cannot rollback: no migration journal found")

        steps = journal.get("steps", [])

        # Find the canonical top-level 06_control folder
        control_id = journal.get("control_folder_id")
        if not control_id:
            ctrl_folders = self._find_child_by_name(self.root_id, CANONICAL_CONTROL_FOLDER, mime_type=FOLDER_MIME_TYPE)
            if ctrl_folders:
                control_id = ctrl_folders[0]["id"]

        journal["status"] = "rolling_back"
        journal["rollback_started_at_utc"] = datetime.now(timezone.utc).isoformat()
        if control_id:
            self._save_journal(journal, control_id)

        # Rehydrate context from steps
        context: Dict[str, str] = {
            "root_id": self.root_id,
            CANONICAL_CONTROL_FOLDER: control_id,
            "releases_id": journal.get("releases_root_id"),
        }
        for step in steps:
            resolved = step.get("resolved_id")
            if resolved:
                name = step.get("name")
                if name:
                    context[name] = resolved
                if step["step_id"] == "ensure_releases_nbp":
                    context["releases/nbp"] = resolved
                    context["nbp"] = resolved
                elif step["step_id"] == "ensure_06_control":
                    context[CANONICAL_CONTROL_FOLDER] = resolved
                elif step["step_id"] == "ensure_05_archive":
                    context[ARCHIVE_FOLDER] = resolved

        # Reverse in reverse order
        for step in reversed(steps):
            if step.get("status") not in ("completed", "pending"):
                continue

            action = step["action"]
            if action in ("move", "move_and_rename"):
                item_id = step["item_id"]
                original_parent_id = step["from_parent_id"]
                original_name = step.get("from_name", step.get("item_name"))

                applied_parent_id = step.get("to_parent_id")
                if not applied_parent_id:
                    if step.get("to_parent_name"):
                        applied_parent_id = context.get(step["to_parent_name"])
                    elif step.get("to_parent_item_id"):
                        applied_parent_id = step["to_parent_item_id"]

                applied_name = step.get("to_name", original_name)

                # Check current metadata to see if it needs moving back
                item_meta = self.drive_service.files().get(
                    fileId=item_id,
                    fields="id,name,parents,trashed",
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

                current_parents = item_meta.get("parents") or []
                if original_parent_id not in current_parents and applied_parent_id in current_parents:
                    self._move_and_rename_item(
                        item_id,
                        expected_name=applied_name,
                        from_parent_id=applied_parent_id,
                        to_parent_id=original_parent_id,
                        to_name=original_name,
                    )
                step["status"] = "rolled_back"
                step["rolled_back_at_utc"] = datetime.now(timezone.utc).isoformat()
                if control_id:
                    self._save_journal(journal, control_id)

            elif action == "sync_navigation":
                # Remove shortcuts created during migration from medallion layers
                for layer in ALL_MEDALLION_LAYERS:
                    layer_folders = self._find_child_by_name(self.root_id, layer, mime_type=FOLDER_MIME_TYPE)
                    if not layer_folders:
                        continue
                    current_folders = self._find_child_by_name(layer_folders[0]["id"], "current", mime_type=FOLDER_MIME_TYPE)
                    if not current_folders:
                        continue
                    current_id = current_folders[0]["id"]
                    for src in CANONICAL_SOURCES:
                        src_folders = self._find_child_by_name(current_id, src, mime_type=FOLDER_MIME_TYPE)
                        for sf in src_folders:
                            # Delete shortcuts and index inside source nav dir
                            children = self._list_children(sf["id"])
                            for c in children:
                                if c.get("mimeType") == SHORTCUT_MIME_TYPE or c.get("name") == "navigation-index.json":
                                    self.drive_service.files().delete(fileId=c["id"], supportsAllDrives=True).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
                                elif c.get("mimeType") == FOLDER_MIME_TYPE:
                                    sub_children = self._list_children(c["id"])
                                    for sc in sub_children:
                                        if sc.get("mimeType") == SHORTCUT_MIME_TYPE:
                                            self.drive_service.files().delete(fileId=sc["id"], supportsAllDrives=True).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
                                    # Delete empty folder
                                    self.drive_service.files().delete(fileId=c["id"], supportsAllDrives=True).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
                            # Delete source nav folder
                            self.drive_service.files().delete(fileId=sf["id"], supportsAllDrives=True).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
                step["status"] = "rolled_back"

            elif action == "ensure_folder":
                # Only clean up newly created folders if they are empty
                if step.get("was_created") is True:
                    folder_id = step.get("resolved_id")
                    if folder_id:
                        children = self._list_children(folder_id)
                        # Do not delete 06_control if it contains the journal!
                        if not children:
                            try:
                                self.drive_service.files().delete(fileId=folder_id, supportsAllDrives=True).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
                            except Exception:
                                pass
                step["status"] = "rolled_back"

        # Verify legacy mode is restored
        post_rollback_mode = detect_layout_mode(self.storage, self.root_id)
        if post_rollback_mode not in ("legacy", "fresh"):
            logger.warning(f"Post-rollback layout mode is '{post_rollback_mode}', expected 'legacy'")

        journal["status"] = "rolled_back"
        journal["rollback_completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        if control_id:
            self._save_journal(journal, control_id)

        return {
            "status": "migration_rolled_back",
            "plan_id": journal.get("plan_id"),
            "post_rollback_mode": post_rollback_mode,
        }

    # -------------------------------------------------------------------------
    # Verification & Drift checks
    # -------------------------------------------------------------------------

    def _assert_no_drift(
        self,
        pins: Dict[str, Any],
        steps: Optional[List[Dict[str, Any]]] = None,
        context: Optional[Dict[str, str]] = None,
    ) -> None:
        """Verify that current release IDs, manifests, and NBP state match pinned identities.

        Respects already-completed steps during resume so completed moves are not treated as foreign drift.
        """
        if not pins:
            return

        # Determine step completion status for pointer locations
        completed_step_ids = {
            s["step_id"] for s in (steps or []) if s.get("status") == "completed"
        }

        # 1. NBP pointer & manifest
        if "nbp" in pins:
            nbp_pin = pins["nbp"]
            ptr_meta = self.drive_service.files().get(
                fileId=nbp_pin["pointer_file_id"],
                fields="id,name,parents,trashed",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

            if ptr_meta.get("trashed") is True:
                raise DriftError(f"Drift detected: NBP current-release pointer {nbp_pin['pointer_file_id']} is trashed")

            # Check parent: original root OR releases/nbp (if move_nbp_root_pointer is completed)
            allowed_parents = {nbp_pin["pointer_parent_id"]}
            if "move_nbp_root_pointer" in completed_step_ids and context and context.get("releases/nbp"):
                allowed_parents.add(context["releases/nbp"])

            if not any(p in allowed_parents for p in ptr_meta.get("parents", [])):
                raise DriftError(
                    f"Drift detected: NBP pointer parent {ptr_meta.get('parents')} not in allowed {allowed_parents}"
                )

            ptr_bytes = self._read_file_bytes(nbp_pin["pointer_file_id"])
            if sha256(ptr_bytes).hexdigest() != nbp_pin["pointer_sha256"]:
                raise DriftError("Drift detected: NBP current-release pointer content has changed")

            ptr_data = json.loads(ptr_bytes.decode("utf-8"))
            if ptr_data.get("release_id") != nbp_pin["release_id"]:
                raise DriftError(
                    f"Drift detected: NBP release ID changed from {nbp_pin['release_id']} to {ptr_data.get('release_id')}"
                )

            # Check manifest
            manifest_bytes = self._read_file_bytes(nbp_pin["manifest_file_id"])
            if sha256(manifest_bytes).hexdigest() != nbp_pin["manifest_sha256"]:
                raise DriftError("Drift detected: NBP release manifest content has changed")

        # 2. BDL pointer & manifest
        if "bdl" in pins:
            bdl_pin = pins["bdl"]
            ptr_meta = self.drive_service.files().get(
                fileId=bdl_pin["pointer_file_id"],
                fields="id,name,parents,trashed",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

            if ptr_meta.get("trashed") is True:
                raise DriftError(f"Drift detected: BDL pointer {bdl_pin['pointer_file_id']} is trashed")

            allowed_parents = {bdl_pin["pointer_parent_id"], bdl_pin["releases_folder_id"]}
            if not any(p in allowed_parents for p in ptr_meta.get("parents", [])):
                raise DriftError(
                    f"Drift detected: BDL pointer parent {ptr_meta.get('parents')} not in allowed {allowed_parents}"
                )

            ptr_bytes = self._read_file_bytes(bdl_pin["pointer_file_id"])
            if sha256(ptr_bytes).hexdigest() != bdl_pin["pointer_sha256"]:
                raise DriftError("Drift detected: BDL current-release pointer content has changed")

            ptr_data = json.loads(ptr_bytes.decode("utf-8"))
            if ptr_data.get("release_id") != bdl_pin["release_id"]:
                raise DriftError(
                    f"Drift detected: BDL release ID changed from {bdl_pin['release_id']} to {ptr_data.get('release_id')}"
                )

            manifest_bytes = self._read_file_bytes(bdl_pin["manifest_file_id"])
            if sha256(manifest_bytes).hexdigest() != ndl_sha if (ndl_sha := bdl_pin.get("manifest_sha256")) else True:
                if ndl_sha and sha256(manifest_bytes).hexdigest() != ndl_sha:
                    raise DriftError("Drift detected: BDL release manifest content has changed")

        # 3. WDI pointer & manifest
        if "wdi" in pins:
            wdi_pin = pins["wdi"]
            ptr_meta = self.drive_service.files().get(
                fileId=wdi_pin["pointer_file_id"],
                fields="id,name,parents,trashed",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

            if ptr_meta.get("trashed") is True:
                raise DriftError(f"Drift detected: WDI pointer {wdi_pin['pointer_file_id']} is trashed")

            allowed_parents = {wdi_pin["pointer_parent_id"], wdi_pin["releases_folder_id"]}
            if not any(p in allowed_parents for p in ptr_meta.get("parents", [])):
                raise DriftError(
                    f"Drift detected: WDI pointer parent {ptr_meta.get('parents')} not in allowed {allowed_parents}"
                )

            ptr_bytes = self._read_file_bytes(wdi_pin["pointer_file_id"])
            if sha256(ptr_bytes).hexdigest() != wdi_pin["pointer_sha256"]:
                raise DriftError("Drift detected: WDI current-release pointer content has changed")

            ptr_data = json.loads(ptr_bytes.decode("utf-8"))
            if ptr_data.get("release_id") != wdi_pin["release_id"]:
                raise DriftError(
                    f"Drift detected: WDI release ID changed from {wdi_pin['release_id']} to {ptr_data.get('release_id')}"
                )

            manifest_bytes = self._read_file_bytes(wdi_pin["manifest_file_id"])
            if wdi_sha := wdi_pin.get("manifest_sha256"):
                if sha256(manifest_bytes).hexdigest() != wdi_sha:
                    raise DriftError("Drift detected: WDI release manifest content has changed")

        # 4. Ingestion control & state pointer
        if "control" in pins:
            ctrl_pin = pins["control"]
            st_ptr_id = ctrl_pin.get("state_pointer_id")
            if st_ptr_id:
                st_meta = self.drive_service.files().get(
                    fileId=st_ptr_id,
                    fields="id,name,parents,trashed",
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

                if st_meta.get("trashed") is True:
                    raise DriftError("Drift detected: NBP ingestion state pointer is trashed")

                st_bytes = self._read_file_bytes(st_ptr_id)
                if sha256(st_bytes).hexdigest() != ctrl_pin["state_pointer_sha256"]:
                    raise DriftError("Drift detected: NBP ingestion state pointer content has changed")

            sn_id = ctrl_pin.get("state_snapshot_id")
            if sn_id:
                sn_meta = self.drive_service.files().get(
                    fileId=sn_id,
                    fields="id,trashed",
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

                if sn_meta.get("trashed") is True:
                    raise DriftError("Drift detected: NBP ingestion state snapshot is trashed")

                sn_bytes = self._read_file_bytes(sn_id)
                if ctrl_pin.get("state_snapshot_sha256") and sha256(sn_bytes).hexdigest() != ctrl_pin["state_snapshot_sha256"]:
                    raise DriftError("Drift detected: NBP ingestion state snapshot content has changed")

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
