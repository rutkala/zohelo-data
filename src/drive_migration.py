"""Google Drive layout consolidation engine for rutkala/zohelo-data.

Consolidates Google Drive layout into canonical medallion and releases hierarchy:
- releases/{nbp,bdl,wdi}/{current-release.json,<uuid>}
- 06_control/nbp and 06_control/source_campaigns
- 02_bronze, 03_silver, 04_gold navigation shortcuts & indexes
- 05_archive/ containing archived legacy wrappers (bdl-platform, wdi-platform)

Guarantees:
- Strictly bounded read-only plan (--dry-run default).
- Explicit --apply with safety pin --expected-root-id.
- Durable ID-based journal stored in Drive under 06_control/migration-journal.json.
- Atomic Drive API moves (addParents/removeParents) preserving exact Parquet bytes,
  file IDs, checksums, and permissions without downloading or re-uploading.
- Idempotent resume and reversible rollback.
- Drift detection: aborts if current release pointers or state change during migration.
- Ambiguity detection: fails closed if conflicting pointers exist.
"""
from __future__ import annotations

from datetime import datetime, timezone
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
    detect_layout_mode,
    resolve_nbp_control_root,
    resolve_source_release_root,
    AmbiguousLayoutError,
    LayoutResolutionError,
)
from medallion_navigation import sync_source_medallion_navigation, verify_medallion_navigation
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
        self.drive_service = storage.drive_service

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
                fields="nextPageToken,files(id,name,mimeType,parents,size,md5Checksum)",
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

    def _get_or_create_folder(self, parent_id: str, name: str) -> str:
        matches = self._find_child_by_name(parent_id, name, mime_type=FOLDER_MIME_TYPE)
        if len(matches) > 1:
            raise AmbiguousLayoutError(f"Duplicate folder '{name}' found under parent '{parent_id}'")
        if matches:
            return matches[0]["id"]
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
        return res["id"]

    def _move_and_rename_item(
        self,
        item_id: str,
        *,
        from_parent_id: str,
        to_parent_id: str,
        to_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Atomically move item from from_parent_id to to_parent_id and optionally rename."""
        meta = self.drive_service.files().get(
            fileId=item_id,
            fields="id,name,parents",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

        current_parents = meta.get("parents") or []
        body = {}
        if to_name and to_name != meta.get("name"):
            body["name"] = to_name

        remove_parents = ",".join([p for p in current_parents if p != to_parent_id])
        add_parents = to_parent_id if to_parent_id not in current_parents else None

        updated = self.drive_service.files().update(
            fileId=item_id,
            body=body if body else None,
            addParents=add_parents,
            removeParents=remove_parents if remove_parents else None,
            fields="id,name,parents",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        return updated

    # -------------------------------------------------------------------------
    # Journal management
    # -------------------------------------------------------------------------

    def _load_journal(self, control_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if self.journal_local_path and self.journal_local_path.is_file():
            try:
                return json.loads(self.journal_local_path.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning(f"Could not read local journal {self.journal_local_path}: {exc}")

        if not control_id:
            ctrl_folders = self._find_child_by_name(self.root_id, CANONICAL_CONTROL_FOLDER, mime_type=FOLDER_MIME_TYPE)
            if ctrl_folders:
                control_id = ctrl_folders[0]["id"]

        if control_id:
            journal_files = self._find_child_by_name(control_id, JOURNAL_FILE_NAME)
            if journal_files:
                data = self.drive_service.files().get_media(
                    fileId=journal_files[0]["id"], supportsAllDrives=True
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
                try:
                    return json.loads(data.decode("utf-8"))
                except Exception as exc:
                    logger.warning(f"Could not parse Drive journal: {exc}")
        return None

    def _save_journal(self, journal: Dict[str, Any], control_id: str) -> str:
        data = _json_bytes(journal)
        if self.journal_local_path:
            self.journal_local_path.parent.mkdir(parents=True, exist_ok=True)
            self.journal_local_path.write_bytes(data)

        # Save to Drive 06_control/
        import io
        from googleapiclient.http import MediaIoBaseUpload

        existing = self._find_child_by_name(control_id, JOURNAL_FILE_NAME)
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/json", resumable=False)
        if existing:
            updated = self.drive_service.files().update(
                fileId=existing[0]["id"],
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            return updated["id"]
        else:
            created = self.drive_service.files().create(
                body={"name": JOURNAL_FILE_NAME, "parents": [control_id], "mimeType": "application/json"},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            return created["id"]

    # -------------------------------------------------------------------------
    # Planning
    # -------------------------------------------------------------------------

    def plan(self) -> Dict[str, Any]:
        """Inspect Drive and generate an ordered, bounded read-only migration plan."""
        mode = detect_layout_mode(self.storage, self.root_id)
        if mode == "ambiguous":
            # Check if there is an in-progress journal from an interrupted run
            journal = self._load_journal()
            if journal and journal.get("status") in ("in_progress", "interrupted"):
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
            # Verify canonical structure
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
        by_name = {}
        for item in root_children:
            by_name.setdefault(item["name"], []).append(item)

        # Pre-flight duplicate check
        duplicates = [name for name, items in by_name.items() if len(items) > 1 and name in {
            "releases", "ingestion-control", "bdl-platform", "wdi-platform", "01_landing", "02_bronze", "03_silver", "04_gold", "05_archive", "06_control"
        }]
        if duplicates:
            raise AmbiguousLayoutError(f"Duplicate top-level entities found: {duplicates}")

        # Identify key legacy components
        releases_folder = by_name.get("releases", [None])[0]
        if not releases_folder:
            raise LayoutResolutionError("Legacy 'releases' folder missing from root")
        releases_id = releases_folder["id"]

        bdl_wrapper = by_name.get(LEGACY_BDL_WRAPPER, [None])[0]
        wdi_wrapper = by_name.get(LEGACY_WDI_WRAPPER, [None])[0]
        ingestion_control = by_name.get(LEGACY_NBP_CONTROL_FOLDER, [None])[0]

        # Pins: collect current releases and state identifiers
        pins: Dict[str, Any] = {"expected_root_id": self.expected_root_id, "captured_at_utc": datetime.now(timezone.utc).isoformat()}

        # 1. NBP pointer & releases
        root_pointers = by_name.get("current-release.json", [])
        rel_pointers = self._find_child_by_name(releases_id, "current-release.json")
        nbp_pointer_file = None
        nbp_pointer_parent = None
        if root_pointers:
            nbp_pointer_file = root_pointers[0]
            nbp_pointer_parent = self.root_id
        elif rel_pointers:
            nbp_pointer_file = rel_pointers[0]
            nbp_pointer_parent = releases_id
        else:
            raise LayoutResolutionError("NBP current-release.json pointer not found at root or releases/")

        nbp_store = DriveReleaseStore(self.storage, releases_id)
        nbp_manifest = read_current_release_manifest(nbp_store, nbp_pointer_parent)
        pins["nbp"] = {
            "release_id": nbp_manifest["release_id"],
            "code_sha": nbp_manifest["code_sha"],
            "pointer_file_id": nbp_pointer_file["id"],
            "pointer_parent_id": nbp_pointer_parent,
        }

        # NBP UUID folders in releases/
        releases_children = self._list_children(releases_id, mime_type=FOLDER_MIME_TYPE)
        nbp_release_folders = [
            f for f in releases_children if UUID_REGEX.match(f["name"])
        ]

        # 2. BDL wrapper, pointer & releases
        bdl_releases_folder = None
        bdl_pointer_file = None
        if bdl_wrapper:
            bdl_children = self._list_children(bdl_wrapper["id"])
            bdl_by_name = {c["name"]: c for c in bdl_children}
            bdl_pointer_file = bdl_by_name.get("current-release.json")
            bdl_releases_folder = bdl_by_name.get("releases")
            if bdl_pointer_file and bdl_releases_folder:
                bdl_store = DriveReleaseStore(self.storage, bdl_releases_folder["id"])
                bdl_manifest = read_current_release_manifest(bdl_store, bdl_wrapper["id"])
                pins["bdl"] = {
                    "release_id": bdl_manifest["release_id"],
                    "code_sha": bdl_manifest["code_sha"],
                    "pointer_file_id": bdl_pointer_file["id"],
                    "releases_folder_id": bdl_releases_folder["id"],
                    "wrapper_folder_id": bdl_wrapper["id"],
                }

        # 3. WDI wrapper, pointer & releases
        wdi_releases_folder = None
        wdi_pointer_file = None
        if wdi_wrapper:
            wdi_children = self._list_children(wdi_wrapper["id"])
            wdi_by_name = {c["name"]: c for c in wdi_children}
            wdi_pointer_file = wdi_by_name.get("current-release.json")
            wdi_releases_folder = wdi_by_name.get("releases")
            if wdi_pointer_file and wdi_releases_folder:
                wdi_store = DriveReleaseStore(self.storage, wdi_releases_folder["id"])
                wdi_manifest = read_current_release_manifest(wdi_store, wdi_wrapper["id"])
                pins["wdi"] = {
                    "release_id": wdi_manifest["release_id"],
                    "code_sha": wdi_manifest["code_sha"],
                    "pointer_file_id": wdi_pointer_file["id"],
                    "releases_folder_id": wdi_releases_folder["id"],
                    "wrapper_folder_id": wdi_wrapper["id"],
                }

        # 4. Ingestion control & source campaigns
        source_campaigns_folder = None
        if ingestion_control:
            ctrl_children = self._list_children(ingestion_control["id"])
            ctrl_by_name = {c["name"]: c for c in ctrl_children}
            source_campaigns_folder = ctrl_by_name.get(CANONICAL_SOURCE_CAMPAIGNS_FOLDER)
            state_file = ctrl_by_name.get("state.json")
            pins["control"] = {
                "ingestion_control_id": ingestion_control["id"],
                "source_campaigns_id": source_campaigns_folder["id"] if source_campaigns_folder else None,
                "state_file_id": state_file["id"] if state_file else None,
            }

        # 5. Archive folder
        archive_folder = by_name.get(ARCHIVE_FOLDER, [None])[0]

        # 6. Control folder (if existing)
        control_folder = by_name.get(CANONICAL_CONTROL_FOLDER, [None])[0]

        # Construct migration steps
        steps: List[Dict[str, Any]] = []

        # Step 1: Ensure 06_control exists
        steps.append({
            "step_id": "ensure_06_control",
            "action": "ensure_folder",
            "name": CANONICAL_CONTROL_FOLDER,
            "parent_id": self.root_id,
            "existing_id": control_folder["id"] if control_folder else None,
            "status": "pending",
        })

        # Step 2: Move source_campaigns from ingestion-control to 06_control (if inside ingestion-control)
        if source_campaigns_folder and ingestion_control:
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

        # Step 4: Ensure releases/nbp folder exists
        steps.append({
            "step_id": "ensure_releases_nbp",
            "action": "ensure_folder",
            "name": "nbp",
            "parent_id": releases_id,
            "existing_id": None,
            "status": "pending",
        })

        # Step 5: Move NBP release UUID directories into releases/nbp
        for rel_dir in nbp_release_folders:
            steps.append({
                "step_id": f"move_nbp_release_{rel_dir['name']}",
                "action": "move",
                "item_id": rel_dir["id"],
                "item_name": rel_dir["name"],
                "from_parent_id": releases_id,
                "to_parent_name": "releases/nbp",
                "from_name": rel_dir["name"],
                "to_name": rel_dir["name"],
                "status": "pending",
            })

        # Step 6: Move NBP current-release.json into releases/nbp
        if nbp_pointer_file and nbp_pointer_parent:
            steps.append({
                "step_id": "move_nbp_pointer",
                "action": "move",
                "item_id": nbp_pointer_file["id"],
                "item_name": "current-release.json",
                "from_parent_id": nbp_pointer_parent,
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

        # Step 8: Move bdl-platform/current-release.json to releases/bdl
        if bdl_pointer_file and bdl_wrapper and bdl_releases_folder:
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

        # Step 10: Move wdi-platform/current-release.json to releases/wdi
        if wdi_pointer_file and wdi_wrapper and wdi_releases_folder:
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

    def apply(self, plan: Optional[Dict[str, Any]] = None, *, resume: bool = False) -> Dict[str, Any]:
        """Apply the migration plan with durable journal, idempotency, and post-validation."""
        self.storage.authorize_writes()

        if resume:
            journal = self._load_journal()
            if not journal:
                raise MigrationError("Cannot resume: no migration journal found in Drive or local path")
            plan = journal
        elif plan is None:
            plan = self.plan()

        if plan.get("status") == "already_canonical":
            # Verify and exit cleanly
            return self.verify()

        pins = plan.get("pins", {})
        steps = plan.get("steps", [])

        # Pre-apply drift check: verify current release IDs and pointers still match pins
        self._assert_no_drift(pins)

        # Ensure control folder is resolved first to write the journal
        ctrl_step = next((s for s in steps if s["step_id"] == "ensure_06_control"), None)
        control_id = ctrl_step.get("existing_id") if ctrl_step else None
        if not control_id:
            control_id = self._get_or_create_folder(self.root_id, CANONICAL_CONTROL_FOLDER)
            if ctrl_step:
                ctrl_step["existing_id"] = control_id
                ctrl_step["status"] = "completed"

        journal = dict(plan)
        journal["status"] = "in_progress"
        journal["started_at_utc"] = datetime.now(timezone.utc).isoformat()
        journal_file_id = self._save_journal(journal, control_id)

        # Context dictionary for dynamic parent resolution
        context = {
            CANONICAL_CONTROL_FOLDER: control_id,
            "root_id": self.root_id,
            "releases_id": plan.get("releases_root_id"),
        }

        # Execute each step sequentially
        for step in steps:
            step_id = step["step_id"]
            if step.get("status") == "completed":
                continue

            action = step["action"]
            if action == "ensure_folder":
                parent_id = context.get(step.get("parent_name"), step.get("parent_id"))
                folder_id = self._get_or_create_folder(parent_id, step["name"])
                step["resolved_id"] = folder_id
                context[step["name"]] = folder_id
                if step_id == "ensure_releases_nbp":
                    context["releases/nbp"] = folder_id
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
                    raise MigrationError(f"Step {step_id}: cannot resolve to_parent_id")

                from_parent_id = step["from_parent_id"]
                to_name = step.get("to_name")

                # Check current metadata to see if already moved (idempotent resume)
                item_meta = self.drive_service.files().get(
                    fileId=item_id,
                    fields="id,name,parents",
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

                item_parents = item_meta.get("parents") or []
                item_name = item_meta.get("name")

                already_moved = (to_parent_id in item_parents and from_parent_id not in item_parents)
                already_renamed = (to_name is None or item_name == to_name)

                if already_moved and already_renamed:
                    step["status"] = "completed"
                else:
                    self._move_and_rename_item(
                        item_id,
                        from_parent_id=from_parent_id,
                        to_parent_id=to_parent_id,
                        to_name=to_name,
                    )
                    step["status"] = "completed"

            elif action == "sync_navigation":
                # Execute medallion navigation sync
                for source in step.get("sources", []):
                    rel_root, direct = resolve_source_release_root(self.storage, self.root_id, source, is_writer=True)
                    rel_store = DriveReleaseStore(self.storage, rel_root)
                    manifest = read_current_release_manifest(rel_store, rel_root)
                    sync_source_medallion_navigation(self.storage, self.root_id, source, manifest)
                step["status"] = "completed"

            step["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
            self._save_journal(journal, control_id)

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

    def rollback(self, journal: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Reverse all completed migration steps in reverse order using the durable journal."""
        self.storage.authorize_writes()

        if journal is None:
            journal = self._load_journal()
            if not journal:
                raise MigrationError("Cannot rollback: no migration journal found")

        steps = journal.get("steps", [])
        control_id = journal.get("pins", {}).get("control", {}).get("ingestion_control_id")
        ctrl_step = next((s for s in steps if s["step_id"] == "ensure_06_control"), None)
        if not control_id and ctrl_step:
            control_id = ctrl_step.get("resolved_id") or ctrl_step.get("existing_id")

        journal["status"] = "rolling_back"
        journal["rollback_started_at_utc"] = datetime.now(timezone.utc).isoformat()
        if control_id:
            self._save_journal(journal, control_id)

        # Reverse in reverse order
        for step in reversed(steps):
            if step.get("status") != "completed":
                continue

            action = step["action"]
            if action in ("move", "move_and_rename"):
                item_id = step["item_id"]
                original_parent_id = step["from_parent_id"]
                original_name = step.get("from_name")

                # The destination parent during apply was:
                applied_parent_id = step.get("to_parent_id")
                if not applied_parent_id:
                    if step.get("to_parent_item_id"):
                        applied_parent_id = step["to_parent_item_id"]

                # Reverse move
                item_meta = self.drive_service.files().get(
                    fileId=item_id,
                    fields="id,name,parents",
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

                current_parents = item_meta.get("parents") or []
                remove_parents = [p for p in current_parents if p != original_parent_id]

                body = {}
                if original_name and original_name != item_meta.get("name"):
                    body["name"] = original_name

                self.drive_service.files().update(
                    fileId=item_id,
                    body=body if body else None,
                    addParents=original_parent_id if original_parent_id not in current_parents else None,
                    removeParents=",".join(remove_parents) if remove_parents else None,
                    fields="id,name,parents",
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

                step["status"] = "rolled_back"
                step["rolled_back_at_utc"] = datetime.now(timezone.utc).isoformat()
                if control_id:
                    self._save_journal(journal, control_id)

        journal["status"] = "rolled_back"
        journal["rollback_completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        if control_id:
            self._save_journal(journal, control_id)

        return {
            "status": "migration_rolled_back",
            "plan_id": journal.get("plan_id"),
        }

    # -------------------------------------------------------------------------
    # Verification & Drift checks
    # -------------------------------------------------------------------------

    def _assert_no_drift(self, pins: Dict[str, Any]) -> None:
        """Verify that current release IDs and NBP state match pinned identities."""
        if not pins:
            return

        if "nbp" in pins:
            nbp_pin = pins["nbp"]
            store = DriveReleaseStore(self.storage, self.root_id)
            manifest = read_current_release_manifest(store, nbp_pin.get("pointer_parent_id", self.root_id))
            if manifest["release_id"] != nbp_pin["release_id"]:
                raise DriftError(
                    f"Drift detected: NBP current release changed from {nbp_pin['release_id']} to {manifest['release_id']}"
                )

        if "bdl" in pins:
            bdl_pin = pins["bdl"]
            store = DriveReleaseStore(self.storage, bdl_pin["releases_folder_id"])
            manifest = read_current_release_manifest(store, bdl_pin["wrapper_folder_id"])
            if manifest["release_id"] != bdl_pin["release_id"]:
                raise DriftError(
                    f"Drift detected: BDL current release changed from {bdl_pin['release_id']} to {manifest['release_id']}"
                )

        if "wdi" in pins:
            wdi_pin = pins["wdi"]
            store = DriveReleaseStore(self.storage, wdi_pin["releases_folder_id"])
            manifest = read_current_release_manifest(store, wdi_pin["wrapper_folder_id"])
            if manifest["release_id"] != wdi_pin["release_id"]:
                raise DriftError(
                    f"Drift detected: WDI current release changed from {wdi_pin['release_id']} to {manifest['release_id']}"
                )

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
