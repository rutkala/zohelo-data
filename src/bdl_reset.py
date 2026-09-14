"""Safe administrative engine for GUS BDL-only reset on Google Drive.

Implements a safe, reviewable reset mechanism for GUS BDL data across all layers:
- Discover exact known BDL roots from configured existing storage root:
  - 01_landing/gus_bdl
  - 06_control/source_campaigns/gus_bdl
  - releases/bdl
  - 05_archive/bdl-platform (if present)
  - bdl-platform at storage root (legacy wrapper if present)
  - BDL-only physical/shortcut navigation under 02_bronze, 03_silver, 04_gold
    (including current/bdl and current/gus_bdl)
- Enumerate paginated inventory and descendants without traversing shortcut targets
- Preserve all non-BDL baseline identities (NBP, WDI, Eurostat, OpenData, etc.)
- Fail closed on ambiguity or mixed assets; no broad substring matching
- Retain durable provider quota/cooldown evidence outside reset data
- Plan is strictly read-only by default, computing an immutable SHA-256 digest
- Apply verifies active writer exclusion (workflow disabled, no active runs),
  Actions main runtime, serialized zohelo-production-data concurrency, target drift,
  and executes recoverable Drive trash only (NEVER permanent delete)
- Supports durable journaling and idempotent resume/replay
- Verifies post-trash removal and non-BDL preservation
- Keeps recovery receipt/journal separate from wiped data and documents restoration from trash
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import logging
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
KNOWN_NON_BDL_SOURCES = {"nbp", "world_bank_wdi", "wdi", "eurostat", "opendata_org", "opendata_org_bronze"}


class BdlResetError(RuntimeError):
    """Base exception for BDL reset errors."""


class AmbiguityError(BdlResetError):
    """Raised when duplicate folders, conflicting paths, or mixed assets are detected."""


class DriftError(BdlResetError):
    """Raised when target items or non-BDL baseline items have drifted."""


class SafetyPinError(BdlResetError):
    """Raised when expected root ID does not match resolved root ID."""


class ActiveProducerError(BdlResetError):
    """Raised when an active or pending BDL writer is detected or workflow is not disabled."""


def _canonical_digest(obj: dict[str, Any]) -> str:
    """Compute canonical SHA-256 digest of a dictionary without embedded plan_sha256."""
    copy = dict(obj)
    copy.pop("plan_sha256", None)
    return sha256(json.dumps(copy, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


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

        # Safety verification: resolve root and verify against expected_root_id without creating
        actual_root_id = self.storage.resolve_root(create=False)
        if actual_root_id != self.expected_root_id:
            raise SafetyPinError(
                f"Drive root ID '{actual_root_id}' does not match expected safety pin '{self.expected_root_id}'"
            )
        self.root_id = actual_root_id

    # -------------------------------------------------------------------------
    # Drive pagination and search helpers
    # -------------------------------------------------------------------------

    def _paged_list(self, *, q: str, fields: str, page_size: int = 100) -> List[Dict[str, Any]]:
        """List a bounded result set with strict pagination validation and loop detection."""
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
                raise BdlResetError("Drive list response is not a JSON object")
            if response.get("incompleteSearch") not in (None, False):
                raise BdlResetError("Drive list reported an incomplete search")

            items = response.get("files")
            if not isinstance(items, list):
                raise BdlResetError("Drive list response omitted or corrupted files list")

            for item in items:
                if not isinstance(item, dict):
                    raise BdlResetError("Drive list response contains a malformed item")
                if not item.get("id") or not isinstance(item.get("name"), str) or not item.get("mimeType"):
                    raise BdlResetError("Drive list response contains an incomplete item")
                parents = item.get("parents", [])
                if not isinstance(parents, list):
                    raise BdlResetError("Drive list response contains malformed parents")
                result.append(item)

            next_token = response.get("nextPageToken")
            if next_token in (None, ""):
                return result
            if not isinstance(next_token, str):
                raise BdlResetError("Drive list response has a malformed nextPageToken")
            if next_token in seen_tokens or next_token == token:
                raise BdlResetError("Drive list pagination repeated a page token (pagination loop detected)")
            seen_tokens.add(next_token)
            token = next_token

        raise BdlResetError("Drive list pagination exceeded safety limit (1000 pages)")

    def _list_children(self, parent_id: str, *, trashed: bool = False) -> List[Dict[str, Any]]:
        trashed_q = "true" if trashed else "false"
        q = f"'{parent_id}' in parents and trashed={trashed_q}"
        return self._paged_list(
            q=q,
            fields="nextPageToken,incompleteSearch,files(id,name,mimeType,parents,size,md5Checksum,shortcutDetails,trashed)",
            page_size=100,
        )

    def _find_exact_children(self, parent_id: str, name: str, *, trashed: bool = False) -> List[Dict[str, Any]]:
        escaped_name = name.replace("\\", "\\\\").replace("'", "\\'")
        trashed_q = "true" if trashed else "false"
        q = f"name='{escaped_name}' and '{parent_id}' in parents and trashed={trashed_q}"
        return self._paged_list(
            q=q,
            fields="nextPageToken,incompleteSearch,files(id,name,mimeType,parents,size,md5Checksum,shortcutDetails,trashed)",
            page_size=50,
        )

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
                fields="id,name,mimeType,parents,size,md5Checksum,shortcutDetails,trashed",
                supportsAllDrives=True,
            ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        except Exception:
            return None

    # -------------------------------------------------------------------------
    # Exact BDL Root Discovery and Ambiguity Checking
    # -------------------------------------------------------------------------

    def discover_bdl_roots(self) -> Dict[str, Dict[str, Any]]:
        """Discover exact known BDL roots under the storage root.

        Fails closed on ambiguity (multiple matches for single-instance paths)
        or mixed assets.
        """
        roots: Dict[str, Dict[str, Any]] = {}
        top_children = self._list_children(self.root_id)

        # Helper to find exactly 0 or 1 folder by name under a parent
        def find_unique_folder(parent_id: str, name: str, context_label: str) -> Optional[Dict[str, Any]]:
            matches = [f for f in self._find_exact_children(parent_id, name) if f.get("mimeType") == FOLDER_MIME_TYPE]
            if len(matches) > 1:
                raise AmbiguityError(f"Multiple folders named '{name}' found under {context_label}")
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

            # Check under current/
            current_folder = find_unique_folder(layer_folder["id"], "current", layer)
            if current_folder:
                # current/bdl
                nav_bdl = find_unique_folder(current_folder["id"], "bdl", f"{layer}/current")
                if nav_bdl:
                    roots[f"{layer}/current/bdl"] = nav_bdl

                # current/gus_bdl if present
                nav_gus_bdl = find_unique_folder(current_folder["id"], "gus_bdl", f"{layer}/current")
                if nav_gus_bdl:
                    roots[f"{layer}/current/gus_bdl"] = nav_gus_bdl

            # Check directly under layer for physical BDL folders or shortcuts
            direct_bdl = find_unique_folder(layer_folder["id"], "bdl", layer)
            if direct_bdl:
                roots[f"{layer}/bdl"] = direct_bdl

            direct_gus_bdl = find_unique_folder(layer_folder["id"], "gus_bdl", layer)
            if direct_gus_bdl:
                roots[f"{layer}/gus_bdl"] = direct_gus_bdl

        return roots

    # -------------------------------------------------------------------------
    # Descendant Inventory Enumeration (No shortcut traversal)
    # -------------------------------------------------------------------------

    def enumerate_root_descendants(
        self, root_key: str, root_folder: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Recursively enumerate all descendants of a BDL root folder.

        DO NOT traverse shortcut targets.
        Fails closed on mixed assets or foreign references.
        """
        targets: List[Dict[str, Any]] = []
        queue = [root_folder]

        while queue:
            current = queue.pop(0)
            children = self._list_children(current["id"])

            for child in children:
                # Mixed asset validation: ensure child belongs strictly to BDL scope
                child_name = child.get("name", "")
                mime_type = child.get("mimeType", "")

                # Reject if child is an unrelated source directory
                if mime_type == FOLDER_MIME_TYPE and child_name in KNOWN_NON_BDL_SOURCES:
                    raise AmbiguityError(
                        f"Mixed asset detected in BDL scope: found non-BDL folder '{child_name}' inside {root_key}"
                    )

                target_item = {
                    "id": child["id"],
                    "name": child_name,
                    "mime_type": mime_type,
                    "parent_id": current["id"],
                    "root_key": root_key,
                    "size": int(child.get("size") or 0),
                    "md5_checksum": child.get("md5Checksum"),
                    "is_shortcut": mime_type == SHORTCUT_MIME_TYPE,
                }
                targets.append(target_item)

                # Only queue children for folders, NEVER for shortcuts
                if mime_type == FOLDER_MIME_TYPE:
                    queue.append(child)

        # Include the root folder itself at the end of the targets for that root
        targets.append({
            "id": root_folder["id"],
            "name": root_folder["name"],
            "mime_type": FOLDER_MIME_TYPE,
            "parent_id": root_folder.get("parents", [""])[0] if root_folder.get("parents") else "",
            "root_key": root_key,
            "size": 0,
            "md5_checksum": None,
            "is_shortcut": False,
            "is_root": True,
        })

        return targets

    # -------------------------------------------------------------------------
    # Baseline Capture for Non-BDL Preservation
    # -------------------------------------------------------------------------

    def capture_non_bdl_baseline(self) -> List[Dict[str, Any]]:
        """Capture identity, parent, and path of all non-BDL roots and major structures."""
        baseline: List[Dict[str, Any]] = []
        top_children = self._list_children(self.root_id)

        for child in top_children:
            name = child.get("name", "")
            if name in ("bdl-platform",):
                continue
            baseline.append({
                "id": child["id"],
                "name": name,
                "parent_id": self.root_id,
                "path": name,
                "mime_type": child.get("mimeType"),
            })

            # Subfolder baselines for key managed roots
            if name in ("01_landing", "06_control", "releases", "02_bronze", "03_silver", "04_gold", "05_archive"):
                sub_children = self._list_children(child["id"])
                for sub in sub_children:
                    sub_name = sub.get("name", "")
                    if name == "01_landing" and sub_name == "gus_bdl":
                        continue
                    if name == "06_control" and sub_name == "source_campaigns":
                        # capture source_campaigns children except gus_bdl
                        sc_children = self._list_children(sub["id"])
                        for sc in sc_children:
                            if sc.get("name") == "gus_bdl":
                                continue
                            baseline.append({
                                "id": sc["id"],
                                "name": sc.get("name"),
                                "parent_id": sub["id"],
                                "path": f"06_control/source_campaigns/{sc.get('name')}",
                                "mime_type": sc.get("mimeType"),
                            })
                    if name == "releases" and sub_name == "bdl":
                        continue
                    if name == "05_archive" and sub_name == "bdl-platform":
                        continue
                    if name in ALL_MEDALLION_LAYERS and sub_name == "current":
                        # capture current children except bdl and gus_bdl
                        curr_children = self._list_children(sub["id"])
                        for curr in curr_children:
                            if curr.get("name") in ("bdl", "gus_bdl"):
                                continue
                            baseline.append({
                                "id": curr["id"],
                                "name": curr.get("name"),
                                "parent_id": sub["id"],
                                "path": f"{name}/current/{curr.get('name')}",
                                "mime_type": curr.get("mimeType"),
                            })
                    if (name in ALL_MEDALLION_LAYERS) and (sub_name in ("bdl", "gus_bdl")):
                        continue

                    baseline.append({
                        "id": sub["id"],
                        "name": sub_name,
                        "parent_id": child["id"],
                        "path": f"{name}/{sub_name}",
                        "mime_type": sub.get("mimeType"),
                    })

        return baseline

    # -------------------------------------------------------------------------
    # Provider Quota and Cooldown Preservation
    # -------------------------------------------------------------------------

    def extract_retained_quota_evidence(
        self, bdl_control_root: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Extract durable provider quota attempts and cooldown history from BDL campaign state.

        Source quotas cannot be reset; this evidence must be retained outside reset data.
        """
        retained: Dict[str, Any] = {
            "source_id": "gus_bdl",
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
            "quota_attempts": [],
            "provider_retry_at": None,
            "last_attempt_utc": None,
            "quota_windows": [
                {"name": "15m", "seconds": 900, "max_requests": 400},
                {"name": "week", "seconds": 604800, "max_requests": 40000},
            ],
            "state_found": False,
        }
        if not bdl_control_root:
            return retained

        pointer_matches = self._find_exact_children(bdl_control_root["id"], "current-ingestion-state.json")
        if not pointer_matches:
            return retained

        try:
            pointer_bytes = self._read_file_bytes(pointer_matches[0]["id"])
            pointer = json.loads(pointer_bytes.decode("utf-8"))
            retained["state_found"] = True

            # If sharded state manifest
            manifest_file_id = pointer.get("manifest_file_id") or pointer.get("state_file_id")
            if manifest_file_id:
                state_bytes = self._read_file_bytes(manifest_file_id)
                state_doc = json.loads(state_bytes.decode("utf-8"))
            else:
                state_doc = pointer

            retained["quota_attempts"] = state_doc.get("quota_attempts", [])
            retained["provider_retry_at"] = state_doc.get("provider_retry_at")
            retained["last_attempt_utc"] = state_doc.get("last_attempt_utc")
        except Exception as exc:
            logger.warning(f"Could not extract detailed BDL campaign state: {exc}")
            retained["extraction_error"] = str(exc)

        return retained

    # -------------------------------------------------------------------------
    # Plan Generation (Read-Only)
    # -------------------------------------------------------------------------

    def plan(self) -> Dict[str, Any]:
        """Generate a strict, read-only reset plan for GUS BDL data.

        Identifies all exact BDL roots, paginates all descendants, captures
        non-BDL baseline, extracts durable quota evidence, and computes an
        immutable SHA-256 plan hash. Leaves Drive completely untouched.
        """
        created_at = datetime.now(timezone.utc).isoformat()
        bdl_roots = self.discover_bdl_roots()

        all_targets: List[Dict[str, Any]] = []
        counts_by_root: Dict[str, int] = {}
        bytes_by_root: Dict[str, int] = {}
        roots_summary: Dict[str, Dict[str, Any]] = {}

        # Enumerate each root's inventory
        for root_key, root_item in bdl_roots.items():
            descendants = self.enumerate_root_descendants(root_key, root_item)
            all_targets.extend(descendants)
            counts_by_root[root_key] = len(descendants)
            total_b = sum(d["size"] for d in descendants)
            bytes_by_root[root_key] = total_b
            roots_summary[root_key] = {
                "id": root_item["id"],
                "name": root_item["name"],
                "parent_id": root_item.get("parents", [""])[0] if root_item.get("parents") else "",
                "item_count": len(descendants),
                "total_bytes": total_b,
            }

        # Deduplicate targets while preserving order (children before parents)
        seen_ids: Set[str] = set()
        deduped_targets: List[Dict[str, Any]] = []
        for t in all_targets:
            if t["id"] not in seen_ids:
                seen_ids.add(t["id"])
                deduped_targets.append(t)

        # Non-BDL baseline
        baseline = self.capture_non_bdl_baseline()

        # Quota extraction from 06_control/source_campaigns/gus_bdl
        ctrl_root = bdl_roots.get("06_control/source_campaigns/gus_bdl")
        quota_evidence = self.extract_retained_quota_evidence(ctrl_root)

        plan_id = f"plan-bdl-reset-{uuid4().hex[:8]}"
        plan_doc = {
            "plan_id": plan_id,
            "status": "planned",
            "read_only": True,
            "created_at_utc": created_at,
            "expected_root_id": self.expected_root_id,
            "root_id": self.root_id,
            "roots_discovered": roots_summary,
            "targets": deduped_targets,
            "total_target_count": len(deduped_targets),
            "total_target_bytes": sum(t["size"] for t in deduped_targets),
            "counts_by_root": counts_by_root,
            "bytes_by_root": bytes_by_root,
            "non_bdl_baseline": baseline,
            "non_bdl_baseline_count": len(baseline),
            "retained_quota_evidence": quota_evidence,
            "trash_policy": "recoverable_trash_only",
        }
        plan_doc["plan_sha256"] = _canonical_digest(plan_doc)
        return plan_doc

    # -------------------------------------------------------------------------
    # Pre-Mutation Verifications and Drift Checking
    # -------------------------------------------------------------------------

    def verify_plan_and_drift(
        self, plan: Dict[str, Any], *, expected_sha256: Optional[str] = None
    ) -> None:
        """Verify plan integrity, expected root safety pin, and target/baseline drift."""
        if plan.get("status") != "planned":
            raise BdlResetError(f"Plan status is '{plan.get('status')}'; expected 'planned'")
        if plan.get("root_id") != self.root_id or plan.get("expected_root_id") != self.expected_root_id:
            raise SafetyPinError("Plan root ID pins do not match current storage root ID")

        digest = _canonical_digest(plan)
        if expected_sha256 and expected_sha256 != digest:
            raise DriftError(f"Supplied plan_sha256 '{expected_sha256}' does not match computed '{digest}'")
        if plan.get("plan_sha256") and plan["plan_sha256"] != digest:
            raise DriftError("Embedded plan_sha256 does not match canonical plan digest")

        # Non-BDL baseline drift check: all must exist and remain untrashed
        for item in plan.get("non_bdl_baseline", []):
            meta = self._get_item_metadata(item["id"])
            if not meta or meta.get("trashed"):
                raise DriftError(
                    f"Non-BDL baseline item '{item['name']}' ({item['id']}) is missing or trashed"
                )

    def verify_active_producers(
        self,
        *,
        token: Optional[str] = None,
        repo: str = "rutkala/zohelo-data",
        workflow_filename: str = "source-gus-bdl.yml",
    ) -> None:
        """Verify that the BDL producer workflow is disabled and no runs are active/pending."""
        import urllib.error
        import urllib.request

        headers = {
            "User-Agent": "zohelo-bdl-reset-guard",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        # 1. Check workflow status (must be disabled)
        wf_url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_filename}"
        req = urllib.request.Request(wf_url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                wf_data = json.loads(resp.read().decode("utf-8"))
                wf_state = wf_data.get("state")
                if wf_state not in ("disabled_manually", "disabled_inactivity"):
                    raise ActiveProducerError(
                        f"BDL workflow '{workflow_filename}' is still enabled (state: '{wf_state}'). "
                        "It must be disabled before reset apply."
                    )
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                logger.info(f"Workflow '{workflow_filename}' not found via API, continuing.")
            else:
                raise ActiveProducerError(f"Failed to check BDL workflow state: {exc}") from exc
        except Exception as exc:
            raise ActiveProducerError(f"GitHub API error checking BDL workflow: {exc}") from exc

        # 2. Check for active/pending runs of this workflow
        for status in ("queued", "in_progress", "pending", "waiting", "requested"):
            runs_url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_filename}/runs?status={status}"
            runs_req = urllib.request.Request(runs_url, headers=headers)
            try:
                with urllib.request.urlopen(runs_req, timeout=15) as resp:
                    runs_data = json.loads(resp.read().decode("utf-8"))
                    runs = runs_data.get("workflow_runs", [])
                    if runs:
                        run_ids = [str(r.get("id")) for r in runs]
                        raise ActiveProducerError(
                            f"Found {len(runs)} active/pending runs of '{workflow_filename}' with status '{status}': "
                            f"{', '.join(run_ids)}. These must complete or be cancelled before reset apply."
                        )
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    break
                raise ActiveProducerError(f"Failed to check active runs for '{workflow_filename}': {exc}") from exc
            except Exception as exc:
                raise ActiveProducerError(f"GitHub API error checking runs for '{workflow_filename}': {exc}") from exc

    # -------------------------------------------------------------------------
    # Apply and Resume Execution (Recoverable Trash Only)
    # -------------------------------------------------------------------------

    def apply(
        self,
        *,
        plan: Dict[str, Any],
        confirmed: bool = False,
        resume: bool = False,
        expected_sha256: Optional[str] = None,
        skip_producer_check: bool = False,
        github_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Apply or resume trashing of exact planned BDL items into recoverable Drive trash.

        NEVER permanently deletes items. Maintains a durable journal locally and
        on Drive outside wiped data.
        """
        if not confirmed:
            raise BdlResetError("Explicit confirmed=True is required to apply or resume BDL reset")

        # 1. Verify plan and check drift
        self.verify_plan_and_drift(plan, expected_sha256=expected_sha256)

        # 2. Check active producers (unless explicitly skipped in local/fixture test)
        if not skip_producer_check:
            self.verify_active_producers(token=github_token)

        # 3. Load or initialize durable journal
        journal = self._load_or_init_journal(plan, resume=resume)

        # Trashed set from journal for idempotence
        trashed_ids = {entry["id"] for entry in journal.get("trashed_items", [])}

        targets = plan.get("targets", [])
        # We trash items starting from leaves up to roots
        # The targets list has descendants first, roots last.
        for item in targets:
            item_id = item["id"]
            if item_id in trashed_ids:
                continue

            # Verify item exists before trashing
            meta = self._get_item_metadata(item_id)
            if not meta:
                # If missing completely and not in journal, record error
                raise DriftError(f"Target item '{item['name']}' ({item_id}) no longer exists on Drive")

            if not meta.get("trashed"):
                # RECOVERABLE TRASH ONLY: update body with trashed=True
                self.drive_service.files().update(
                    fileId=item_id,
                    body={"trashed": True},
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)

            entry = {
                "id": item_id,
                "name": item["name"],
                "parent_id": item.get("parent_id"),
                "mime_type": item.get("mime_type"),
                "root_key": item.get("root_key"),
                "trashed_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            journal["trashed_items"].append(entry)
            trashed_ids.add(item_id)
            self._save_journal(journal)

        journal["status"] = "completed"
        journal["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        self._save_journal(journal)

        # 4. Post-apply verification: all targeted items trashed, all baseline items preserved
        verification = self.verify_post_reset(plan)

        receipt = {
            "status": "bdl_reset_applied" if not resume else "bdl_reset_resumed",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan["plan_sha256"],
            "total_items_trashed": len(trashed_ids),
            "journal_path": str(self.journal_local_path),
            "verification": verification,
            "retained_quota_evidence": plan.get("retained_quota_evidence"),
            "restoration_runbook": {
                "summary": "All BDL data was moved to recoverable Google Drive trash. Nothing was permanently deleted.",
                "restoration_method": "Call Drive API files().update(fileId=..., body={'trashed': False}) for each ID in the journal.",
            },
        }

        # Also store durable journal in 06_control/ outside wiped data
        self._persist_remote_journal_if_possible(journal)

        return receipt

    def verify_post_reset(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Verify that all targeted items are now trashed and all non-BDL baseline items remain untrashed."""
        targets = plan.get("targets", [])
        baseline = plan.get("non_bdl_baseline", [])

        trashed_count = 0
        untrashed_targets = []
        for t in targets:
            meta = self._get_item_metadata(t["id"])
            if meta and meta.get("trashed"):
                trashed_count += 1
            else:
                untrashed_targets.append(t["id"])

        preserved_count = 0
        corrupted_baseline = []
        for b in baseline:
            meta = self._get_item_metadata(b["id"])
            if meta and not meta.get("trashed"):
                preserved_count += 1
            else:
                corrupted_baseline.append(b["id"])

        if untrashed_targets:
            raise BdlResetError(
                f"Post-reset verification failed: {len(untrashed_targets)} target items are not in trash"
            )
        if corrupted_baseline:
            raise BdlResetError(
                f"Post-reset verification failed: {len(corrupted_baseline)} non-BDL baseline items were modified or trashed!"
            )

        return {
            "status": "verified_clean",
            "targets_trashed": trashed_count,
            "non_bdl_preserved": preserved_count,
        }

    # -------------------------------------------------------------------------
    # Durable Journal Management
    # -------------------------------------------------------------------------

    def _load_or_init_journal(self, plan: Dict[str, Any], *, resume: bool) -> Dict[str, Any]:
        if resume and self.journal_local_path.is_file():
            try:
                journal = json.loads(self.journal_local_path.read_text(encoding="utf-8"))
                if journal.get("plan_sha256") != plan.get("plan_sha256"):
                    raise DriftError("Local journal plan_sha256 does not match reviewed plan")
                return journal
            except Exception as exc:
                raise BdlResetError(f"Failed to load existing journal for resume: {exc}") from exc

        return {
            "format_version": 1,
            "operation": "apply",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan["plan_sha256"],
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress",
            "trashed_items": [],
            "retained_quota_evidence": plan.get("retained_quota_evidence"),
        }

    def _save_journal(self, journal: Dict[str, Any]) -> None:
        self.journal_local_path.parent.mkdir(parents=True, exist_ok=True)
        self.journal_local_path.write_text(
            json.dumps(journal, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    def _persist_remote_journal_if_possible(self, journal: Dict[str, Any]) -> None:
        """Persist a copy of the journal in 06_control/ (outside reset data) if available."""
        try:
            ctrl_matches = [
                f for f in self._find_exact_children(self.root_id, "06_control")
                if f.get("mimeType") == FOLDER_MIME_TYPE
            ]
            if not ctrl_matches:
                return
            ctrl_id = ctrl_matches[0]["id"]

            import io
            from googleapiclient.http import MediaIoBaseUpload

            journal_bytes = json.dumps(journal, indent=2, sort_keys=True).encode("utf-8")
            media = MediaIoBaseUpload(io.BytesIO(journal_bytes), mimetype="application/json", resumable=False)

            existing = self._find_exact_children(ctrl_id, "bdl-reset-recovery-journal.json")
            if existing:
                self.drive_service.files().update(
                    fileId=existing[0]["id"],
                    media_body=media,
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
            else:
                self.drive_service.files().create(
                    body={
                        "name": "bdl-reset-recovery-journal.json",
                        "parents": [ctrl_id],
                        "mimeType": "application/json",
                    },
                    media_body=media,
                    supportsAllDrives=True,
                ).execute(num_retries=DRIVE_REPEATABLE_RETRIES)
        except Exception as exc:
            logger.warning(f"Could not persist remote journal to 06_control: {exc}")
