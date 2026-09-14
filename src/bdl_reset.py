"""Safe administrative engine for GUS BDL-only reset on Google Drive.

Key Architecture & Safety Invariants:
1. Root-Only Mutation: Mutates ONLY the exact non-overlapping BDL root folders
   (7 known roots in canonical layout), never 36k individual descendants.
   Google Drive v3 inherits trashed=true across the whole subtree; explicitlyTrashed
   distinguishes the root from descendants.
2. Descendant Verification: Retains every descendant in reviewed inventory, verifies
   exact subtree ID/metadata before mutation, and verifies inherited trashed=true afterward.
3. Fail-Closed Ambiguity & Mixed Assets: Strict exact-path discovery; duplicate folders,
   cycles, foreign source ownership, or broad substring matching are rejected.
4. Production Runtime Guards: Unconditional enforcement of main GitHub Actions runtime,
   ZOHELO_ALLOW_PRODUCTION_WRITES=true, zohelo-production-data concurrency, and
   fail-closed verification that source-gus-bdl.yml is disabled with zero active runs.
5. Durable Quota Retention: Uses hash-verified reconstruction via read-only adapter
   to extract registered-profile quota attempts (15m, 12h, 7d windows) and cooldowns
   from 06_control/source_campaigns/gus_bdl, saving a clean fresh ledger with
   coverage_status='awaiting_web_bulk' and NO prior work.
6. Write-Ahead Remote Journaling: Persists plan, fresh quota ledger, and journal into
   06_control/bdl_resets/<plan_id>/ BEFORE first mutation. Each root uses durable
   write-ahead intent + verified completion; remote write errors stop immediately.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import io
import json
import logging
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

logger = logging.getLogger(__name__)

DRIVE_REPEATABLE_RETRIES = 4
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut"
JSON_MIME_TYPE = "application/json"

ALL_MEDALLION_LAYERS = ("02_bronze", "03_silver", "04_gold")
KNOWN_NON_BDL_SOURCES = frozenset({"nbp", "world_bank_wdi", "wdi", "eurostat", "opendata_org", "opendata_org_bronze"})
BDL_RESETS_DIR = "bdl_resets"

REGISTERED_BDL_QUOTA_WINDOWS = (
    {"seconds": 900, "requests": 400},
    {"seconds": 43_200, "requests": 4_000},
    {"seconds": 604_800, "requests": 40_000},
)
MAX_QUOTA_HISTORY_SECONDS = 604_800  # 7 days


class BdlResetError(RuntimeError):
    """Base exception for BDL reset errors."""


class AmbiguityError(BdlResetError):
    """Raised when duplicate folders, conflicting paths, cycles, or mixed assets are detected."""


class DriftError(BdlResetError):
    """Raised when target items or non-BDL baseline items have drifted."""


class SafetyPinError(BdlResetError):
    """Raised when expected root ID does not match resolved root ID."""


class ActiveProducerError(BdlResetError):
    """Raised when an active or pending BDL writer is detected or workflow is not disabled."""


class RuntimeGuardError(BdlResetError):
    """Raised when production mutation prerequisites (Actions main runtime, opt-in) fail."""


def _canonical_digest(obj: dict[str, Any]) -> str:
    """Compute canonical SHA-256 digest of a dictionary without embedded plan_sha256."""
    copy_obj = dict(obj)
    copy_obj.pop("plan_sha256", None)
    return sha256(json.dumps(copy_obj, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


class ReadOnlyDriveObjectStore:
    """Read-only object-store protocol adapter for Drive without folder/root creation."""

    def __init__(self, drive_service: Any):
        self.drive_service = drive_service

    def find(self, name: str, parent_id: str) -> list[str]:
        escaped_name = name.replace("\\", "\\\\").replace("'", "\\'")
        q = f"name='{escaped_name}' and '{parent_id}' in parents and trashed=false"
        items = []
        token = None
        for _ in range(100):
            resp = self.drive_service.files().list(
                q=q,
                fields="nextPageToken,incompleteSearch,files(id)",
                pageSize=100,
                pageToken=token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            if not isinstance(resp, dict) or resp.get("incompleteSearch") not in (None, False):
                raise BdlResetError("Drive list query failed in read-only adapter")
            for f in resp.get("files", []):
                items.append(f["id"])
            token = resp.get("nextPageToken")
            if not token:
                break
        return items

    def read(self, file_id: str) -> bytes:
        data = self.drive_service.files().get_media(
            fileId=file_id, supportsAllDrives=True
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        if not isinstance(data, bytes):
            raise BdlResetError(f"Drive did not return bytes for file {file_id}")
        return data

    def create(self, name: str, data: bytes, parent_id: str) -> str:
        raise RuntimeError("ReadOnlyDriveObjectStore cannot create Drive objects")

    def replace(self, file_id: str, data: bytes) -> None:
        raise RuntimeError("ReadOnlyDriveObjectStore cannot replace Drive objects")

    def mkdir(self, name: str, parent_id: str) -> str:
        raise RuntimeError("ReadOnlyDriveObjectStore cannot create folders")


class BdlResetEngine:
    """Engine for planning, executing, and resuming a GUS BDL-only reset."""

    def __init__(
        self,
        storage: Any,
        expected_root_id: str,
        *,
        journal_local_path: Optional[Path] = None,
    ):
        self.storage = storage
        self.expected_root_id = expected_root_id.strip()
        self.journal_local_path = journal_local_path or Path("bdl-reset-journal.json")
        self.drive_service = getattr(storage, "drive_service", None) or getattr(
            getattr(storage, "storage", None), "drive_service", None
        )
        if self.drive_service is None:
            raise BdlResetError("Drive service is not available on storage manager")

        # Safety verification: resolve root strictly without creating folders
        actual_root_id = self.storage.resolve_root(create=False)
        if actual_root_id != self.expected_root_id:
            raise SafetyPinError(
                f"Drive root ID '{actual_root_id}' does not match expected safety pin '{self.expected_root_id}'"
            )
        self.root_id = actual_root_id

    # -------------------------------------------------------------------------
    # Drive pagination helpers (pageSize=1000 for efficiency)
    # -------------------------------------------------------------------------

    def _paged_list(self, *, q: str, fields: str, page_size: int = 1000) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        token: Optional[str] = None
        seen_tokens: Set[str] = set()

        for _page in range(2000):
            response = self.drive_service.files().list(
                q=q,
                fields=fields,
                pageSize=page_size,
                pageToken=token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

            if not isinstance(response, dict):
                raise BdlResetError("Drive list response is not a JSON object")
            if response.get("incompleteSearch") not in (None, False):
                raise BdlResetError("Drive list reported an incomplete search")

            items = response.get("files")
            if not isinstance(items, list):
                raise BdlResetError("Drive list response omitted or corrupted files list")

            for item in items:
                if not isinstance(item, dict) or not item.get("id"):
                    raise BdlResetError("Drive list response contains a malformed item")
                result.append(item)

            next_token = response.get("nextPageToken")
            if next_token in (None, ""):
                return result
            if not isinstance(next_token, str) or next_token in seen_tokens or next_token == token:
                raise BdlResetError("Drive list pagination repeated a page token or is malformed")
            seen_tokens.add(next_token)
            token = next_token

        raise BdlResetError("Drive list pagination exceeded safety limit (2000 pages)")

    def _list_children(self, parent_id: str, *, trashed: bool = False) -> List[Dict[str, Any]]:
        trashed_q = "true" if trashed else "false"
        q = f"'{parent_id}' in parents and trashed={trashed_q}"
        fields = "nextPageToken,incompleteSearch,files(id,name,mimeType,parents,size,md5Checksum,appProperties,shortcutDetails,trashed,explicitlyTrashed)"
        return self._paged_list(q=q, fields=fields, page_size=1000)

    def _find_exact_children(self, parent_id: str, name: str, *, trashed: bool = False) -> List[Dict[str, Any]]:
        escaped_name = name.replace("\\", "\\\\").replace("'", "\\'")
        trashed_q = "true" if trashed else "false"
        q = f"name='{escaped_name}' and '{parent_id}' in parents and trashed={trashed_q}"
        fields = "nextPageToken,incompleteSearch,files(id,name,mimeType,parents,size,md5Checksum,appProperties,shortcutDetails,trashed,explicitlyTrashed)"
        return self._paged_list(q=q, fields=fields, page_size=100)

    def _read_file_bytes(self, file_id: str) -> bytes:
        data = self.drive_service.files().get_media(
            fileId=file_id, supportsAllDrives=True
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        if not isinstance(data, bytes):
            raise BdlResetError(f"Drive did not return bytes for file {file_id}")
        return data

    def _get_item_metadata(self, file_id: str) -> Optional[Dict[str, Any]]:
        try:
            return self.drive_service.files().get(
                fileId=file_id,
                fields="id,name,mimeType,parents,size,md5Checksum,appProperties,shortcutDetails,trashed,explicitlyTrashed",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        except Exception:
            return None

    # -------------------------------------------------------------------------
    # Exact Root Discovery & Ambiguity Validation
    # -------------------------------------------------------------------------

    def discover_bdl_roots(self) -> Dict[str, Dict[str, Any]]:
        """Discover exact known non-overlapping BDL roots under the storage root.

        Fails closed on ambiguity (multiple matches or duplicate folders).
        """
        roots: Dict[str, Dict[str, Any]] = {}

        def find_unique_folder(parent_id: str, name: str, context: str) -> Optional[Dict[str, Any]]:
            matches = [f for f in self._find_exact_children(parent_id, name) if f.get("mimeType") == FOLDER_MIME_TYPE]
            if len(matches) > 1:
                raise AmbiguityError(f"Multiple folders named '{name}' found under {context}")
            return matches[0] if matches else None

        # 1. 01_landing/gus_bdl
        landing = find_unique_folder(self.root_id, "01_landing", "root")
        if landing:
            landing_bdl = find_unique_folder(landing["id"], "gus_bdl", "01_landing")
            if landing_bdl:
                roots["01_landing/gus_bdl"] = landing_bdl

        # 2. 06_control/source_campaigns/gus_bdl
        control = find_unique_folder(self.root_id, "06_control", "root")
        if control:
            campaigns = find_unique_folder(control["id"], "source_campaigns", "06_control")
            if campaigns:
                ctrl_bdl = find_unique_folder(campaigns["id"], "gus_bdl", "06_control/source_campaigns")
                if ctrl_bdl:
                    roots["06_control/source_campaigns/gus_bdl"] = ctrl_bdl

        # 3. releases/bdl
        releases = find_unique_folder(self.root_id, "releases", "root")
        if releases:
            rel_bdl = find_unique_folder(releases["id"], "bdl", "releases")
            if rel_bdl:
                roots["releases/bdl"] = rel_bdl

        # 4. 05_archive/bdl-platform (if present)
        archive = find_unique_folder(self.root_id, "05_archive", "root")
        if archive:
            arch_bdl = find_unique_folder(archive["id"], "bdl-platform", "05_archive")
            if arch_bdl:
                roots["05_archive/bdl-platform"] = arch_bdl

        # 5. legacy root bdl-platform (if present)
        legacy_bdl = find_unique_folder(self.root_id, "bdl-platform", "root")
        if legacy_bdl:
            roots["legacy_root/bdl-platform"] = legacy_bdl

        # 6. Medallion navigation under 02_bronze, 03_silver, 04_gold
        for layer in ALL_MEDALLION_LAYERS:
            layer_folder = find_unique_folder(self.root_id, layer, "root")
            if not layer_folder:
                continue
            current_folder = find_unique_folder(layer_folder["id"], "current", layer)
            if current_folder:
                nav_bdl = find_unique_folder(current_folder["id"], "bdl", f"{layer}/current")
                if nav_bdl:
                    roots[f"{layer}/current/bdl"] = nav_bdl
                nav_gus_bdl = find_unique_folder(current_folder["id"], "gus_bdl", f"{layer}/current")
                if nav_gus_bdl:
                    roots[f"{layer}/current/gus_bdl"] = nav_gus_bdl

        return roots

    # -------------------------------------------------------------------------
    # Descendant Inventory Enumeration (No shortcut traversal)
    # -------------------------------------------------------------------------

    def enumerate_root_descendants(
        self, root_key: str, root_folder: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Enumerate all descendants of a BDL root folder without traversing shortcuts.

        Validates parent subtree ownership, cycles, duplicate IDs, foreign appProperties,
        and foreign shortcut targets.
        """
        targets: List[Dict[str, Any]] = []
        queue = [root_folder]
        seen_ids: Set[str] = {root_folder["id"]}
        parent_map: Dict[str, str] = {}

        while queue:
            current = queue.pop(0)
            children = self._list_children(current["id"])

            for child in children:
                child_id = child["id"]
                child_name = child.get("name", "")
                mime_type = child.get("mimeType", "")
                parents = sorted(child.get("parents") or [])

                # Duplicate ID or cycle detection
                if child_id in seen_ids:
                    raise AmbiguityError(f"Duplicate object ID or cycle detected in BDL scope: {child_id}")
                seen_ids.add(child_id)
                parent_map[child_id] = current["id"]

                # Reject if child is an unrelated source directory
                if mime_type == FOLDER_MIME_TYPE and child_name in KNOWN_NON_BDL_SOURCES:
                    raise AmbiguityError(
                        f"Mixed asset detected in BDL scope: found non-BDL folder '{child_name}' inside {root_key}"
                    )

                # Validate appProperties (no foreign source claim)
                app_props = child.get("appProperties") or {}
                if "source_id" in app_props and app_props["source_id"] not in ("gus_bdl", "bdl"):
                    raise AmbiguityError(
                        f"Foreign source_id in appProperties: {app_props['source_id']} on {child_id}"
                    )

                # Validate shortcutDetails: NEVER traverse target, but verify target is not foreign
                shortcut_details = child.get("shortcutDetails")
                if mime_type == SHORTCUT_MIME_TYPE and shortcut_details:
                    target_id = shortcut_details.get("targetId")
                    if not target_id:
                        raise AmbiguityError(f"Shortcut {child_id} has malformed or missing targetId")

                target_item = {
                    "id": child_id,
                    "name": child_name,
                    "mime_type": mime_type,
                    "parents": parents,
                    "parent_id": current["id"],
                    "root_key": root_key,
                    "size": int(child.get("size") or 0),
                    "md5_checksum": child.get("md5Checksum"),
                    "app_properties": app_props,
                    "shortcut_details": shortcut_details,
                    "explicitly_trashed": child.get("explicitlyTrashed", False),
                    "is_shortcut": mime_type == SHORTCUT_MIME_TYPE,
                    "is_root": False,
                }
                targets.append(target_item)

                # Only queue children for folders, NEVER for shortcuts
                if mime_type == FOLDER_MIME_TYPE:
                    queue.append(child)

        return targets

    # -------------------------------------------------------------------------
    # Baseline Capture for Non-BDL Preservation & Key Pointer Hashes
    # -------------------------------------------------------------------------

    def capture_non_bdl_baseline(self) -> Dict[str, Any]:
        """Capture metadata of non-BDL roots and exact bytes/hashes of current pointers."""
        baseline_items: List[Dict[str, Any]] = []
        key_pointers: Dict[str, Dict[str, Any]] = {}
        top_children = self._list_children(self.root_id)

        for child in top_children:
            name = child.get("name", "")
            if name in ("bdl-platform",):
                continue
            baseline_items.append({
                "id": child["id"],
                "name": name,
                "parents": sorted(child.get("parents") or []),
                "mime_type": child.get("mimeType"),
            })

            # Subfolders for key managed roots
            if name in ("01_landing", "06_control", "releases", "02_bronze", "03_silver", "04_gold", "05_archive"):
                sub_children = self._list_children(child["id"])
                for sub in sub_children:
                    sub_name = sub.get("name", "")
                    if name == "01_landing" and sub_name == "gus_bdl":
                        continue
                    if name == "releases" and sub_name == "bdl":
                        continue
                    if name == "05_archive" and sub_name == "bdl-platform":
                        continue
                    if name in ALL_MEDALLION_LAYERS and sub_name == "current":
                        curr_children = self._list_children(sub["id"])
                        for curr in curr_children:
                            if curr.get("name") in ("bdl", "gus_bdl"):
                                continue
                            baseline_items.append({
                                "id": curr["id"],
                                "name": curr.get("name"),
                                "parents": sorted(curr.get("parents") or []),
                                "path": f"{name}/current/{curr.get('name')}",
                                "mime_type": curr.get("mimeType"),
                            })
                            # Capture navigation index for other sources
                            idx_matches = self._find_exact_children(curr["id"], "navigation-index.json")
                            if idx_matches:
                                raw = self._read_file_bytes(idx_matches[0]["id"])
                                key_pointers[f"{name}/current/{curr.get('name')}/navigation-index.json"] = {
                                    "id": idx_matches[0]["id"],
                                    "sha256": sha256(raw).hexdigest(),
                                    "size_bytes": len(raw),
                                }
                    if (name in ALL_MEDALLION_LAYERS) and (sub_name in ("bdl", "gus_bdl")):
                        continue

                    baseline_items.append({
                        "id": sub["id"],
                        "name": sub_name,
                        "parents": sorted(sub.get("parents") or []),
                        "path": f"{name}/{sub_name}",
                        "mime_type": sub.get("mimeType"),
                    })

                    # Capture current release pointers for nbp and wdi
                    if name == "releases" and sub_name in ("nbp", "wdi"):
                        ptr_matches = self._find_exact_children(sub["id"], "current-release.json")
                        if ptr_matches:
                            raw = self._read_file_bytes(ptr_matches[0]["id"])
                            key_pointers[f"releases/{sub_name}/current-release.json"] = {
                                "id": ptr_matches[0]["id"],
                                "sha256": sha256(raw).hexdigest(),
                                "size_bytes": len(raw),
                            }

                    # Capture NBP ingestion state pointer
                    if name == "06_control" and sub_name == "nbp":
                        ptr_matches = self._find_exact_children(sub["id"], "current-ingestion-state.json")
                        if ptr_matches:
                            raw = self._read_file_bytes(ptr_matches[0]["id"])
                            key_pointers["06_control/nbp/current-ingestion-state.json"] = {
                                "id": ptr_matches[0]["id"],
                                "sha256": sha256(raw).hexdigest(),
                                "size_bytes": len(raw),
                            }

        return {
            "items": baseline_items,
            "key_pointers": key_pointers,
        }

    # -------------------------------------------------------------------------
    # Verified Campaign Quota Extraction & Clean Fresh Ledger
    # -------------------------------------------------------------------------

    def extract_retained_quota_evidence(
        self, bdl_control_root: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Reconstruct BDL state using read-only object-store adapter and extract durable quota evidence.

        Fails closed on missing or corrupt state when BDL control root exists.
        Filters quota attempts to the active 7-day window and generates a fresh clean ledger.
        """
        if not bdl_control_root:
            now_ts = datetime.now(timezone.utc).timestamp()
            return {
                "source_id": "gus_bdl",
                "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
                "quota_attempts": [],
                "provider_retry_at": None,
                "last_attempt_utc": None,
                "quota_windows": [dict(w) for w in REGISTERED_BDL_QUOTA_WINDOWS],
                "clean_fresh_ledger": self._build_clean_fresh_ledger([], None, None),
                "state_found": False,
            }

        ptr_matches = self._find_exact_children(bdl_control_root["id"], "current-ingestion-state.json")
        if not ptr_matches:
            raise BdlResetError("BDL campaign control folder exists but current-ingestion-state.json is missing")

        # Use read-only adapter with source_campaign_store._CampaignStore.load()
        from ingestion.source_campaign_store import _CampaignStore
        ro_adapter = ReadOnlyDriveObjectStore(self.drive_service)
        store = _CampaignStore(
            ro_adapter,
            "gus_bdl",
            bdl_control_root["id"],
            bdl_control_root["id"],  # dummy responses id for read-only state loading
        )
        try:
            state = store.load()
        except Exception as exc:
            raise BdlResetError(f"Failed hash-verified reconstruction of BDL campaign state: {exc}") from exc

        if not isinstance(state, dict):
            raise BdlResetError("Reconstructed BDL campaign state is malformed or empty")

        raw_attempts = state.get("quota_attempts") or []
        provider_retry_at = state.get("provider_retry_at")
        last_attempt_utc = state.get("last_attempt_utc")

        # Bounded filter: retain attempts within the 7-day provider quota window
        now_ts = datetime.now(timezone.utc).timestamp()
        cutoff_ts = now_ts - MAX_QUOTA_HISTORY_SECONDS
        active_attempts = sorted([float(ts) for ts in raw_attempts if float(ts) > cutoff_ts])

        clean_ledger = self._build_clean_fresh_ledger(active_attempts, provider_retry_at, last_attempt_utc)

        return {
            "source_id": "gus_bdl",
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
            "quota_attempts": active_attempts,
            "provider_retry_at": provider_retry_at,
            "last_attempt_utc": last_attempt_utc,
            "quota_windows": [dict(w) for w in REGISTERED_BDL_QUOTA_WINDOWS],
            "original_pointer_id": ptr_matches[0]["id"],
            "state_found": True,
            "clean_fresh_ledger": clean_ledger,
        }

    def _build_clean_fresh_ledger(
        self,
        quota_attempts: List[float],
        provider_retry_at: Optional[float],
        last_attempt_utc: Optional[str],
    ) -> Dict[str, Any]:
        """Construct a clean, valid v2 campaign state with preserved quota and NO prior work."""
        today = datetime.now(timezone.utc).date().isoformat()
        return {
            "schema_version": 2,
            "source_id": "gus_bdl",
            "onboarding_date": today,
            "pending": [],
            "completed": {},
            "recent_roots": {},
            "receipts": [],
            "rejected_receipts": [],
            "raw_bytes": 0,
            "accepted_responses": 0,
            "record_count": 0,
            "lane_position": 0,
            "coverage_status": "awaiting_web_bulk",
            "gate": "awaiting_web_bulk",
            "catalogue_totals": {},
            "last_error": None,
            "quota_attempts": list(quota_attempts),
            "provider_retry_at": provider_retry_at,
            "last_attempt_utc": last_attempt_utc,
            "quota_windows": [dict(w) for w in REGISTERED_BDL_QUOTA_WINDOWS],
        }

    # -------------------------------------------------------------------------
    # Plan Generation (Strictly Read-Only)
    # -------------------------------------------------------------------------

    def plan(self) -> Dict[str, Any]:
        """Generate a strict, read-only reset plan for GUS BDL data.

        Discovers exact root folders, enumerates all descendants without shortcut traversal,
        captures non-BDL baseline & key pointer hashes, extracts durable quota evidence,
        and computes immutable canonical SHA-256 digest. Zero mutations.
        """
        created_at = datetime.now(timezone.utc).isoformat()
        bdl_roots = self.discover_bdl_roots()

        all_descendants: List[Dict[str, Any]] = []
        counts_by_root: Dict[str, int] = {}
        bytes_by_root: Dict[str, int] = {}
        roots_inventory: Dict[str, Dict[str, Any]] = {}

        for root_key, root_item in bdl_roots.items():
            descendants = self.enumerate_root_descendants(root_key, root_item)
            all_descendants.extend(descendants)
            # Count includes descendants + root folder itself
            counts_by_root[root_key] = len(descendants) + 1
            total_b = sum(d["size"] for d in descendants)
            bytes_by_root[root_key] = total_b
            roots_inventory[root_key] = {
                "id": root_item["id"],
                "name": root_item["name"],
                "parents": sorted(root_item.get("parents") or []),
                "mime_type": FOLDER_MIME_TYPE,
                "descendant_count": len(descendants),
                "total_bytes": total_b,
                "explicitly_trashed": root_item.get("explicitlyTrashed", False),
            }

        # Quota extraction from 06_control/source_campaigns/gus_bdl
        ctrl_root = bdl_roots.get("06_control/source_campaigns/gus_bdl")
        quota_evidence = self.extract_retained_quota_evidence(ctrl_root)

        # Baseline capture
        baseline = self.capture_non_bdl_baseline()

        plan_id = f"plan-bdl-reset-{uuid4().hex[:8]}"
        plan_doc = {
            "plan_id": plan_id,
            "status": "planned",
            "read_only": True,
            "created_at_utc": created_at,
            "expected_root_id": self.expected_root_id,
            "root_id": self.root_id,
            "roots": roots_inventory,
            "root_count": len(roots_inventory),
            "targets": all_descendants,
            "total_descendant_count": len(all_descendants),
            "total_object_count": len(all_descendants) + len(roots_inventory),
            "total_bytes": sum(bytes_by_root.values()),
            "counts_by_root": counts_by_root,
            "bytes_by_root": bytes_by_root,
            "non_bdl_baseline": baseline["items"],
            "non_bdl_baseline_count": len(baseline["items"]),
            "key_pointers": baseline["key_pointers"],
            "retained_quota_evidence": quota_evidence,
            "mutation_strategy": "root_only_trash_with_inherited_descendants",
        }
        plan_doc["plan_sha256"] = _canonical_digest(plan_doc)
        return plan_doc

    # -------------------------------------------------------------------------
    # Production Runtime Guards & Active Producer Verification
    # -------------------------------------------------------------------------

    def _verify_runtime_guards(self) -> None:
        """Unconditionally enforce production opt-in, main Actions runtime, and concurrency."""
        if os.environ.get("ZOHELO_ALLOW_PRODUCTION_WRITES", "").lower() != "true":
            raise RuntimeGuardError(
                "Mutating operations require explicit ZOHELO_ALLOW_PRODUCTION_WRITES=true"
            )
        if os.environ.get("GITHUB_ACTIONS", "").lower() != "true":
            raise RuntimeGuardError(
                "Mutating operations must execute within genuine GitHub Actions runtime (GITHUB_ACTIONS=true)"
            )
        ref = os.environ.get("GITHUB_REF", "")
        if ref != "refs/heads/main":
            raise RuntimeGuardError(f"Mutating operations must run on 'refs/heads/main'; got '{ref}'")

    def verify_active_producers(
        self,
        *,
        token: Optional[str] = None,
        repo: str = "rutkala/zohelo-data",
        workflow_filename: str = "source-gus-bdl.yml",
    ) -> None:
        """Fail closed unless source-gus-bdl.yml is disabled and has zero active/queued runs."""
        import urllib.error
        import urllib.request

        headers = {
            "User-Agent": "zohelo-bdl-reset-guard",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        token = token or os.environ.get("GITHUB_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"

        # 1. Check workflow state
        wf_url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_filename}"
        req = urllib.request.Request(wf_url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                wf_data = json.loads(resp.read().decode("utf-8"))
                wf_state = wf_data.get("state")
                if wf_state not in ("disabled_manually", "disabled_inactivity"):
                    raise ActiveProducerError(
                        f"BDL workflow '{workflow_filename}' is not disabled (state: '{wf_state}'). "
                        "It must be disabled before reset mutation."
                    )
        except urllib.error.HTTPError as exc:
            raise ActiveProducerError(f"GitHub API HTTP error {exc.code} checking BDL workflow: {exc}") from exc
        except Exception as exc:
            raise ActiveProducerError(f"GitHub API error checking BDL workflow: {exc}") from exc

        # 2. Check for active/queued/pending/waiting runs
        for status in ("queued", "in_progress", "pending", "waiting", "requested"):
            runs_url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_filename}/runs?status={status}"
            runs_req = urllib.request.Request(runs_url, headers=headers)
            try:
                with urllib.request.urlopen(runs_req, timeout=15) as resp:
                    runs_data = json.loads(resp.read().decode("utf-8"))
                    runs = runs_data.get("workflow_runs")
                    if not isinstance(runs, list):
                        raise ActiveProducerError(f"Malformed runs response from GitHub API for status {status}")
                    if runs:
                        run_ids = [str(r.get("id")) for r in runs]
                        raise ActiveProducerError(
                            f"Found {len(runs)} active/pending runs of '{workflow_filename}' ({status}): "
                            f"{', '.join(run_ids)}. Must be drained/cancelled before reset."
                        )
            except urllib.error.HTTPError as exc:
                raise ActiveProducerError(f"GitHub API HTTP error {exc.code} checking active runs: {exc}") from exc
            except Exception as exc:
                raise ActiveProducerError(f"GitHub API error checking active runs: {exc}") from exc

    # -------------------------------------------------------------------------
    # Re-enumeration & Drift Checking
    # -------------------------------------------------------------------------

    def verify_plan_and_drift(
        self, plan: Dict[str, Any], *, expected_sha256: Optional[str] = None
    ) -> None:
        """Verify plan integrity and check target/baseline drift against Drive."""
        if plan.get("status") != "planned":
            raise BdlResetError(f"Plan status is '{plan.get('status')}'; expected 'planned'")
        if plan.get("root_id") != self.root_id or plan.get("expected_root_id") != self.expected_root_id:
            raise SafetyPinError("Plan root ID pins do not match current storage root ID")

        digest = _canonical_digest(plan)
        if expected_sha256 and expected_sha256 != digest:
            raise DriftError(f"Supplied plan_sha256 '{expected_sha256}' does not match computed '{digest}'")
        if plan.get("plan_sha256") and plan["plan_sha256"] != digest:
            raise DriftError("Embedded plan_sha256 does not match canonical plan digest")

        # Verify key release & control pointer hashes
        for ptr_path, ptr_meta in plan.get("key_pointers", {}).items():
            meta = self._get_item_metadata(ptr_meta["id"])
            if not meta or meta.get("trashed"):
                raise DriftError(f"Key non-BDL pointer {ptr_path} ({ptr_meta['id']}) is missing or trashed")
            raw = self._read_file_bytes(ptr_meta["id"])
            actual_sha = sha256(raw).hexdigest()
            if actual_sha != ptr_meta["sha256"]:
                raise DriftError(f"Key non-BDL pointer {ptr_path} drifted in content hash")

        # Verify non-BDL baseline items remain untrashed
        for item in plan.get("non_bdl_baseline", []):
            meta = self._get_item_metadata(item["id"])
            if not meta or meta.get("trashed"):
                raise DriftError(f"Non-BDL baseline item '{item['name']}' ({item['id']}) is missing or trashed")

    def _verify_root_subtree_drift(
        self, root_key: str, root_id: str, planned_descendants: List[Dict[str, Any]]
    ) -> None:
        """Re-enumerate a root's subtree immediately before trashing to guarantee zero drift."""
        current_meta = self._get_item_metadata(root_id)
        if not current_meta or current_meta.get("trashed"):
            raise DriftError(f"Root folder {root_key} ({root_id}) is missing or already trashed before mutation")

        current_descendants = self.enumerate_root_descendants(root_key, current_meta)
        planned_map = {d["id"]: d for d in planned_descendants}
        current_map = {d["id"]: d for d in current_descendants}

        if set(planned_map) != set(current_map):
            added = set(current_map) - set(planned_map)
            removed = set(planned_map) - set(current_map)
            raise DriftError(
                f"Descendant drift detected in {root_key}: {len(added)} added, {len(removed)} removed"
            )

        for item_id, planned_item in planned_map.items():
            current_item = current_map[item_id]
            if planned_item["name"] != current_item["name"]:
                raise DriftError(f"Item {item_id} in {root_key} was renamed")
            if planned_item["parents"] != current_item["parents"]:
                raise DriftError(f"Item {item_id} in {root_key} was moved")
            if planned_item["size"] != current_item["size"]:
                raise DriftError(f"Item {item_id} in {root_key} changed size")
            if planned_item.get("md5_checksum") != current_item.get("md5_checksum"):
                raise DriftError(f"Item {item_id} in {root_key} changed md5Checksum")

    # -------------------------------------------------------------------------
    # Remote Journal & Artifact Management (06_control/bdl_resets/<plan_id>/)
    # -------------------------------------------------------------------------

    def _ensure_remote_plan_folder(self, plan_id: str) -> str:
        ctrl_matches = [
            f for f in self._find_exact_children(self.root_id, "06_control")
            if f.get("mimeType") == FOLDER_MIME_TYPE
        ]
        if not ctrl_matches:
            raise BdlResetError("06_control root folder not found on Drive")
        ctrl_id = ctrl_matches[0]["id"]

        # Find or create bdl_resets under 06_control
        resets_folders = [
            f for f in self._find_exact_children(ctrl_id, BDL_RESETS_DIR)
            if f.get("mimeType") == FOLDER_MIME_TYPE
        ]
        if resets_folders:
            resets_id = resets_folders[0]["id"]
        else:
            created = self.drive_service.files().create(
                body={"name": BDL_RESETS_DIR, "parents": [ctrl_id], "mimeType": FOLDER_MIME_TYPE},
                fields="id",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            resets_id = created["id"]

        # Find or create <plan_id> folder
        plan_folders = [
            f for f in self._find_exact_children(resets_id, plan_id)
            if f.get("mimeType") == FOLDER_MIME_TYPE
        ]
        if plan_folders:
            return plan_folders[0]["id"]
        created = self.drive_service.files().create(
            body={"name": plan_id, "parents": [resets_id], "mimeType": FOLDER_MIME_TYPE},
            fields="id",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        return created["id"]

    def _upload_json_to_remote_plan_folder(self, plan_folder_id: str, name: str, data: dict[str, Any]) -> str:
        from googleapiclient.http import MediaIoBaseUpload
        raw = json.dumps(data, indent=2, sort_keys=True).encode("utf-8")
        media = MediaIoBaseUpload(io.BytesIO(raw), mimetype="application/json", resumable=False)

        existing = self._find_exact_children(plan_folder_id, name)
        if existing:
            updated = self.drive_service.files().update(
                fileId=existing[0]["id"],
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            return updated["id"]
        created = self.drive_service.files().create(
            body={"name": name, "parents": [plan_folder_id], "mimeType": "application/json"},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        return created["id"]

    # -------------------------------------------------------------------------
    # Apply and Resume Execution (Mutating Root Folders Only)
    # -------------------------------------------------------------------------

    def apply(
        self,
        *,
        plan: Dict[str, Any],
        confirmed: bool = False,
        resume: bool = False,
        expected_sha256: Optional[str] = None,
        github_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Apply or resume BDL reset by trashing only the exact BDL root folders.

        All descendants inherit trashed=true.
        Zero permanent deletions. Maintains write-ahead remote journal.
        """
        if not confirmed:
            raise BdlResetError("Explicit confirmed=True is required to apply or resume BDL reset")

        # 1. Enforce production runtime guards
        self._verify_runtime_guards()

        # 2. Verify plan integrity and baseline drift
        self.verify_plan_and_drift(plan, expected_sha256=expected_sha256)

        # 3. Verify active producer exclusion
        self.verify_active_producers(token=github_token)

        plan_id = plan["plan_id"]
        plan_sha256 = plan["plan_sha256"]

        # 4. Initialize or load remote plan folder and write-ahead journal
        plan_folder_id = self._ensure_remote_plan_folder(plan_id)

        # Persist plan, identity, and fresh quota ledger BEFORE first mutation
        self._upload_json_to_remote_plan_folder(plan_folder_id, "plan.json", plan)
        quota_ledger = plan.get("retained_quota_evidence", {}).get("clean_fresh_ledger", {})
        self._upload_json_to_remote_plan_folder(plan_folder_id, "retained-quota-ledger.json", quota_ledger)

        journal = self._load_or_init_journal(plan, plan_folder_id, resume=resume)

        roots_to_trash = plan.get("roots", {})
        planned_targets = plan.get("targets", [])
        targets_by_root: Dict[str, List[Dict[str, Any]]] = {}
        for t in planned_targets:
            targets_by_root.setdefault(t["root_key"], []).append(t)

        trashed_roots = {entry["root_key"]: entry for entry in journal.get("root_checkpoints", [])}

        # 5. Mutate only the exact BDL root folders with write-ahead intent
        for root_key, root_info in roots_to_trash.items():
            root_id = root_info["id"]

            if root_key in trashed_roots and trashed_roots[root_key].get("status") == "trashed_verified":
                # Verify that it is indeed still trashed
                meta = self._get_item_metadata(root_id)
                if not meta or not meta.get("trashed"):
                    raise DriftError(f"Root {root_key} marked verified in journal is not trashed on Drive")
                continue

            # Check if previous execution had intent_to_trash and succeeded before unrecorded checkpoint
            if root_key in trashed_roots and trashed_roots[root_key].get("status") == "intent_to_trash":
                meta = self._get_item_metadata(root_id)
                if meta and meta.get("trashed"):
                    # Success was achieved before unrecorded completion
                    trashed_roots[root_key]["status"] = "trashed_verified"
                    trashed_roots[root_key]["verified_at_utc"] = datetime.now(timezone.utc).isoformat()
                    self._save_journal(journal, plan_folder_id)
                    continue

            # Recheck active producer immediately before this root mutation
            self.verify_active_producers(token=github_token)

            # Re-enumerate subtree and verify zero drift before mutation
            self._verify_root_subtree_drift(root_key, root_id, targets_by_root.get(root_key, []))

            # 5a. Record Write-Ahead Intent
            intent_checkpoint = {
                "root_key": root_key,
                "root_id": root_id,
                "status": "intent_to_trash",
                "intent_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            # Update journal in-memory and write remote immediately (fail on write error)
            journal.setdefault("root_checkpoints", []).append(intent_checkpoint)
            trashed_roots[root_key] = intent_checkpoint
            self._save_journal(journal, plan_folder_id)

            # 5b. Execute RECOVERABLE TRASH on root folder ONLY
            self.drive_service.files().update(
                fileId=root_id,
                body={"trashed": True},
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

            # 5c. Verify root is trashed
            updated_root_meta = self._get_item_metadata(root_id)
            if not updated_root_meta or not updated_root_meta.get("trashed"):
                raise BdlResetError(f"Drive root update failed to trash {root_key} ({root_id})")

            # 5d. Record Verified Completion
            intent_checkpoint["status"] = "trashed_verified"
            intent_checkpoint["verified_at_utc"] = datetime.now(timezone.utc).isoformat()
            self._save_journal(journal, plan_folder_id)

        # 6. Post-reset verification across all 7 roots, 36k descendants, and baseline
        verification = self.verify_post_reset(plan)

        journal["status"] = "completed"
        journal["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        self._save_journal(journal, plan_folder_id)

        # Final producer recheck after all work
        self.verify_active_producers(token=github_token)

        receipt = {
            "status": "bdl_reset_applied" if not resume else "bdl_reset_resumed",
            "plan_id": plan_id,
            "plan_sha256": plan_sha256,
            "roots_trashed": len(roots_to_trash),
            "total_descendants_inherited_trash": plan.get("total_descendant_count", 0),
            "remote_plan_folder": f"06_control/bdl_resets/{plan_id}",
            "journal_path": str(self.journal_local_path),
            "verification": verification,
            "retained_quota_evidence": plan.get("retained_quota_evidence"),
            "restoration_runbook": {
                "summary": "All 7 BDL root folders were moved to recoverable Google Drive trash. "
                           "Descendants inherit trashed state. Nothing was permanently deleted.",
                "restoration_method": "Call Drive API files().update(fileId=root_id, body={'trashed': False}) "
                                      "for the 7 root IDs in the journal to restore whole subtrees.",
            },
        }
        self._upload_json_to_remote_plan_folder(plan_folder_id, "recovery-receipt.json", receipt)
        return receipt

    def verify_post_reset(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Verify that all root folders have explicitlyTrashed=True, descendants have inherited trashed=True,

        and non-BDL baseline items and key pointers remain untouched.
        """
        roots = plan.get("roots", {})
        targets = plan.get("targets", [])
        baseline = plan.get("non_bdl_baseline", [])
        key_pointers = plan.get("key_pointers", {})

        # Verify roots have trashed=True and explicitlyTrashed=True
        for root_key, root_info in roots.items():
            meta = self._get_item_metadata(root_info["id"])
            if not meta or not meta.get("trashed"):
                raise BdlResetError(f"Post-reset verification failed: root {root_key} is not trashed")
            if not meta.get("explicitlyTrashed", True):
                raise BdlResetError(f"Post-reset verification failed: root {root_key} lacks explicitlyTrashed status")

        # Verify descendants have inherited trashed=True
        untrashed_targets = []
        for t in targets:
            meta = self._get_item_metadata(t["id"])
            if not meta or not meta.get("trashed"):
                untrashed_targets.append(t["id"])
        if untrashed_targets:
            raise BdlResetError(
                f"Post-reset verification failed: {len(untrashed_targets)} descendants did not inherit trashed state"
            )

        # Verify non-BDL baseline items remain untrashed
        for b in baseline:
            meta = self._get_item_metadata(b["id"])
            if not meta or meta.get("trashed"):
                raise BdlResetError(f"Non-BDL baseline item {b['name']} ({b['id']}) was corrupted or trashed")

        # Verify key pointers unchanged
        for ptr_path, ptr_meta in key_pointers.items():
            raw = self._read_file_bytes(ptr_meta["id"])
            if sha256(raw).hexdigest() != ptr_meta["sha256"]:
                raise BdlResetError(f"Key non-BDL pointer {ptr_path} was corrupted during reset")

        return {
            "status": "verified_clean",
            "roots_trashed": len(roots),
            "descendants_inherited_trashed": len(targets),
            "baseline_preserved": len(baseline),
            "key_pointers_verified": len(key_pointers),
        }

    # -------------------------------------------------------------------------
    # Journal Persistence Helpers
    # -------------------------------------------------------------------------

    def _load_or_init_journal(self, plan: Dict[str, Any], plan_folder_id: str, *, resume: bool) -> Dict[str, Any]:
        if resume:
            # Try loading remote journal first
            existing = self._find_exact_children(plan_folder_id, "journal.json")
            if existing:
                raw = self._read_file_bytes(existing[0]["id"])
                journal = json.loads(raw.decode("utf-8"))
                if journal.get("plan_sha256") != plan.get("plan_sha256"):
                    raise DriftError("Remote journal plan_sha256 does not match reviewed plan")
                return journal
            elif self.journal_local_path.is_file():
                journal = json.loads(self.journal_local_path.read_text(encoding="utf-8"))
                if journal.get("plan_sha256") != plan.get("plan_sha256"):
                    raise DriftError("Local journal plan_sha256 does not match reviewed plan")
                return journal
            else:
                raise BdlResetError("Resume failed: no remote or local journal exists for this plan")

        initial_journal = {
            "format_version": 2,
            "operation": "apply",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan["plan_sha256"],
            "root_id": self.root_id,
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress",
            "root_checkpoints": [],
        }
        self._save_journal(initial_journal, plan_folder_id)
        return initial_journal

    def _save_journal(self, journal: Dict[str, Any], plan_folder_id: str) -> None:
        # Save locally
        self.journal_local_path.parent.mkdir(parents=True, exist_ok=True)
        self.journal_local_path.write_text(
            json.dumps(journal, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        # Save remotely - fail closed on error
        try:
            self._upload_json_to_remote_plan_folder(plan_folder_id, "journal.json", journal)
        except Exception as exc:
            raise BdlResetError(f"Failed to persist remote write-ahead journal to Drive: {exc}") from exc
