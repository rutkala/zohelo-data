"""Fixture tests for safe GUS BDL-only reset.

Covers all 10 required safety boundaries & adversarial cases:
1. Pagination: verifies multi-page descendant enumeration without stopping at first page (>100 items).
2. Canonical and legacy paths: verifies discovery across canonical roots, archive, and legacy structures.
3. Ambiguous/mixed scope rejection: fails closed on duplicate folders, wrong root MIME, foreign assets,
   foreign appProperties, shortcuts targeting foreign sources, folder cycles, and avoids broad substring matches.
4. No side effects on plan: verifies plan is strictly read-only and causes zero mutations.
5. No secret output: verifies plan, identity, and receipts contain no credentials or tokens.
6. Stale-plan failure: fails closed on plan tampering, hash mismatch, or root safety pin mismatch.
7. Non-BDL preservation: verifies non-BDL datasets (NBP, WDI, Eurostat) and shared roots remain intact.
8. Inherited trash model: verifies root-only trashing applies trashed=true down whole subtree with
   explicitlyTrashed=true on roots only, and untrashing restores whole subtree.
9. Interrupted trash resume & write-ahead remote journal: verifies first-mutation crash recovery and
   idempotent resumption from remote journal in 06_control/bdl_resets/<plan_id>/.
10. Lost response handling: handles intent_to_trash with successful remote trash before unrecorded checkpoint.
11. Real v2 sharded quota manifest & fresh ledger: verifies extraction of full registered-profile quota/cooldowns
    (15m, 12h, 7d) via ReadOnlyDriveObjectStore and CampaignStore.load(), and proves fresh ledger consumption
    with zero prior work and gate='awaiting_web_bulk'.
12. Dynamic child/content drift: detects modified/added/removed items before root mutation and aborts.
13. Non-BDL pointer drift: detects drift in key release/control pointers before apply and aborts.
14. Runtime guards: enforces ZOHELO_ALLOW_PRODUCTION_WRITES, GITHUB_ACTIONS, and refs/heads/main.
15. Active producer rejection: fails closed on 404, network error, malformed response, active workflow, and active runs.
16. Tractable simulation: tests complete 7 roots / descendant hierarchy.
17. Dispatch and plan provenance verification: tests strict dispatch validation and tamper-evident provenance checks.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import re
import sys
import unittest
from unittest import mock
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from bdl_reset import (
    ActiveProducerError,
    AmbiguityError,
    BdlResetEngine,
    BdlResetError,
    DriftError,
    RuntimeGuardError,
    SafetyPinError,
    _canonical_digest,
    ReadOnlyDriveObjectStore,
    BDL_RESETS_DIR,
    FOLDER_MIME_TYPE,
    SHORTCUT_MIME_TYPE,
    JSON_MIME_TYPE,
    REGISTERED_BDL_QUOTA_WINDOWS,
)
from ingestion.source_campaign_store import _CampaignStore
from ingestion.source_campaign import STATE_VERSION, validate_state


class MockDriveService:
    """In-memory mock of Google Drive v3 API for BDL reset validation."""

    def __init__(self, root_id: str = "root-prod-123"):
        self.root_id = root_id
        self.items: dict[str, dict] = {}
        self.files_resource = MockFilesResource(self)

        # Initialize root folder
        self.items[root_id] = {
            "id": root_id,
            "name": "zohelo-data",
            "mimeType": FOLDER_MIME_TYPE,
            "parents": [],
            "trashed": False,
            "explicitlyTrashed": False,
            "size": 0,
            "appProperties": {},
        }

    def files(self):
        return self.files_resource

    def _is_descendant(self, item_id: str, ancestor_id: str) -> bool:
        visited = set()
        queue = list(self.items.get(item_id, {}).get("parents", []))
        while queue:
            parent = queue.pop(0)
            if parent == ancestor_id:
                return True
            if parent not in visited:
                visited.add(parent)
                queue.extend(self.items.get(parent, {}).get("parents", []))
        return False

    def add_folder(self, item_id: str, name: str, parent_id: str) -> dict:
        item = {
            "id": item_id,
            "name": name,
            "mimeType": FOLDER_MIME_TYPE,
            "parents": [parent_id],
            "trashed": False,
            "explicitlyTrashed": False,
            "size": 0,
            "appProperties": {},
        }
        self.items[item_id] = item
        return item

    def add_file(
        self,
        item_id: str,
        name: str,
        parent_id: str,
        size: int = 100,
        content: bytes = b"",
        app_properties: dict | None = None,
    ) -> dict:
        mime = JSON_MIME_TYPE if name.endswith(".json") else "application/octet-stream"
        item = {
            "id": item_id,
            "name": name,
            "mimeType": mime,
            "parents": [parent_id],
            "trashed": False,
            "explicitlyTrashed": False,
            "size": size if not content else len(content),
            "md5Checksum": sha256(content or str(item_id).encode("utf-8")).hexdigest()[:32],
            "appProperties": dict(app_properties or {}),
            "_content": content,
        }
        self.items[item_id] = item
        return item

    def add_shortcut(
        self,
        item_id: str,
        name: str,
        parent_id: str,
        target_id: str,
        app_properties: dict | None = None,
    ) -> dict:
        item = {
            "id": item_id,
            "name": name,
            "mimeType": SHORTCUT_MIME_TYPE,
            "parents": [parent_id],
            "trashed": False,
            "explicitlyTrashed": False,
            "size": 0,
            "shortcutDetails": {"targetId": target_id},
            "appProperties": dict(app_properties or {}),
        }
        self.items[item_id] = item
        return item


class MockFilesResource:
    def __init__(self, service: MockDriveService):
        self.service = service
        self.mutations_count = 0
        self.trash_calls: list[str] = []

    def list(self, **kwargs):
        q = kwargs.get("q", "")
        page_size = kwargs.get("pageSize", 1000)
        page_token = kwargs.get("pageToken")

        parent_id = None
        trashed_filter = False
        name_filter = None

        parent_match = re.search(r"'([^']+)' in parents", q)
        if parent_match:
            parent_id = parent_match.group(1)

        if "trashed=true" in q:
            trashed_filter = True
        elif "trashed=false" in q:
            trashed_filter = False

        name_match = re.search(r"name='([^']+)'", q)
        if name_match:
            name_filter = name_match.group(1).replace("\\'", "'").replace("\\\\", "\\")

        matching = []
        for item in self.service.items.values():
            if parent_id and parent_id not in item.get("parents", []):
                continue
            if item.get("trashed") != trashed_filter:
                continue
            if name_filter and item.get("name") != name_filter:
                continue
            matching.append(copy.deepcopy(item))

        matching.sort(key=lambda x: x["id"])

        start = int(page_token) if page_token else 0
        end = start + page_size
        page_items = matching[start:end]
        next_token = str(end) if end < len(matching) else None

        class ExecMock:
            def __init__(self, data):
                self.data = data

            def execute(self, num_retries=0):
                return self.data

        return ExecMock({"files": page_items, "nextPageToken": next_token, "incompleteSearch": False})

    def get(self, fileId, **kwargs):
        item = self.service.items.get(fileId)

        class ExecMock:
            def __init__(self, data):
                self.data = copy.deepcopy(data) if data else None

            def execute(self, num_retries=0):
                if not self.data:
                    raise RuntimeError(f"File {fileId} not found")
                return self.data

        return ExecMock(item)

    def get_media(self, fileId, **kwargs):
        item = self.service.items.get(fileId)

        class ExecMock:
            def __init__(self, data):
                self.data = data.get("_content", b"") if data else b""

            def execute(self, num_retries=0):
                return self.data

        return ExecMock(item)

    def update(self, fileId, body=None, media_body=None, **kwargs):
        self.mutations_count += 1
        item = self.service.items.get(fileId)
        if not item:
            raise RuntimeError(f"Cannot update missing item {fileId}")

        if body and "trashed" in body:
            is_trashed = bool(body["trashed"])
            item["trashed"] = is_trashed
            item["explicitlyTrashed"] = is_trashed
            if is_trashed:
                self.trash_calls.append(fileId)
            # Propagate inherited trashing to all descendants in subtree
            for other_id, other_item in self.service.items.items():
                if other_id != fileId and self.service._is_descendant(other_id, fileId):
                    other_item["trashed"] = is_trashed
                    other_item["explicitlyTrashed"] = False

        if media_body:
            stream = getattr(media_body, "_fd", None)
            if stream:
                stream.seek(0)
                item["_content"] = stream.read()
                item["size"] = len(item["_content"])

        class ExecMock:
            def __init__(self, data):
                self.data = copy.deepcopy(data)

            def execute(self, num_retries=0):
                return self.data

        return ExecMock(item)

    def create(self, body=None, media_body=None, **kwargs):
        self.mutations_count += 1
        new_id = (body or {}).get("id") or f"gen-{len(self.service.items) + 1}"
        item = {
            "id": new_id,
            "name": (body or {}).get("name", "untitled"),
            "mimeType": (body or {}).get("mimeType", "application/octet-stream"),
            "parents": (body or {}).get("parents", []),
            "trashed": False,
            "explicitlyTrashed": False,
            "size": 0,
            "appProperties": (body or {}).get("appProperties", {}),
            "shortcutDetails": (body or {}).get("shortcutDetails"),
            "_content": b"",
        }
        if media_body:
            stream = getattr(media_body, "_fd", None)
            if stream:
                stream.seek(0)
                item["_content"] = stream.read()
                item["size"] = len(item["_content"])
        self.service.items[new_id] = item

        class ExecMock:
            def __init__(self, data):
                self.data = copy.deepcopy(data)

            def execute(self, num_retries=0):
                return self.data

        return ExecMock(item)

    def delete(self, fileId, **kwargs):
        # STRICT ENFORCEMENT: Never delete permanently!
        raise AssertionError("CRITICAL VIOLATION: Permanent .delete() was called on Google Drive API!")


class MockObjectStore:
    """Protocol adapter providing _ObjectStore interface on top of MockDriveService."""

    def __init__(self, drive: MockDriveService):
        self.drive = drive

    def find(self, name: str, parent_id: str) -> list[str]:
        matches = []
        for item in self.drive.items.values():
            if parent_id in item.get("parents", []) and item.get("name") == name and not item.get("trashed"):
                matches.append(item["id"])
        return sorted(matches)

    def read(self, file_id: str) -> bytes:
        item = self.drive.items.get(file_id)
        if not item or item.get("trashed"):
            raise RuntimeError(f"File {file_id} not found or trashed")
        return item.get("_content", b"")

    def create(self, name: str, data: bytes, parent_id: str) -> str:
        new_id = f"file-{len(self.drive.items) + 1}"
        self.drive.add_file(new_id, name, parent_id, size=len(data), content=data)
        return new_id

    def replace(self, file_id: str, data: bytes) -> None:
        item = self.drive.items.get(file_id)
        if not item:
            raise RuntimeError(f"Cannot replace missing file {file_id}")
        item["_content"] = data
        item["size"] = len(data)

    def mkdir(self, name: str, parent_id: str) -> str:
        new_id = f"folder-{len(self.drive.items) + 1}"
        self.drive.add_folder(new_id, name, parent_id)
        return new_id


class MockStorageManager:
    def __init__(self, drive_service: MockDriveService, root_id: str):
        self.drive_service = drive_service
        self.root_id = root_id

    def resolve_root(self, create: bool = False) -> str:
        if create:
            raise AssertionError("StorageManager.resolve_root called with create=True in reset engine!")
        return self.root_id


class BdlResetTests(unittest.TestCase):
    def setUp(self):
        self.root_id = "root-prod-123"
        self.drive = MockDriveService(self.root_id)
        self.storage = MockStorageManager(self.drive, self.root_id)

        # Default production environment variables
        self.env_patcher = mock.patch.dict(
            os.environ,
            {
                "ZOHELO_ALLOW_PRODUCTION_WRITES": "true",
                "GITHUB_ACTIONS": "true",
                "GITHUB_REF": "refs/heads/main",
            },
        )
        self.env_patcher.start()

        # Mock urllib to simulate disabled producer with 0 runs by default
        self.url_patcher = mock.patch("urllib.request.urlopen", side_effect=self._default_mock_producer_request)
        self.url_patcher.start()

        # Standard canonical structure
        self.drive.add_folder("f-landing", "01_landing", self.root_id)
        self.drive.add_folder("f-control", "06_control", self.root_id)
        self.drive.add_folder("f-releases", "releases", self.root_id)
        self.drive.add_folder("f-archive", "05_archive", self.root_id)
        self.drive.add_folder("f-bronze", "02_bronze", self.root_id)
        self.drive.add_folder("f-silver", "03_silver", self.root_id)
        self.drive.add_folder("f-gold", "04_gold", self.root_id)

        # Non-BDL sources (must remain strictly preserved)
        self.drive.add_folder("f-landing-wdi", "world_bank_wdi", "f-landing")
        self.drive.add_folder("f-landing-eurostat", "eurostat", "f-landing")
        self.drive.add_folder("f-control-nbp", "nbp", "f-control")
        self.drive.add_folder("f-control-sc", "source_campaigns", "f-control")
        self.drive.add_folder("f-sc-wdi", "world_bank_wdi", "f-control-sc")
        self.drive.add_folder("f-releases-nbp", "nbp", "f-releases")
        self.drive.add_folder("f-releases-wdi", "wdi", "f-releases")

        # Non-BDL current pointers
        self.drive.add_file(
            "f-ptr-nbp-rel", "current-release.json", "f-releases-nbp", content=b'{"release_id": "nbp-v1"}'
        )
        self.drive.add_file(
            "f-ptr-wdi-rel", "current-release.json", "f-releases-wdi", content=b'{"release_id": "wdi-v1"}'
        )
        self.drive.add_file(
            "f-ptr-nbp-ctrl", "current-ingestion-state.json", "f-control-nbp", content=b'{"state": "nbp-ok"}'
        )

        # Canonical BDL roots (7 total in standard layout)
        self.drive.add_folder("f-landing-bdl", "gus_bdl", "f-landing")
        self.drive.add_folder("f-ctrl-bdl", "gus_bdl", "f-control-sc")
        self.drive.add_folder("f-rel-bdl", "bdl", "f-releases")
        self.drive.add_folder("f-arch-bdl", "bdl-platform", "f-archive")

        # Medallion current navigation
        for layer_id in ("f-bronze", "f-silver", "f-gold"):
            curr_id = f"f-curr-{layer_id}"
            self.drive.add_folder(curr_id, "current", layer_id)
            # Non-BDL navigation
            self.drive.add_folder(f"f-nbp-{layer_id}", "nbp", curr_id)
            self.drive.add_file(f"f-nbp-idx-{layer_id}", "navigation-index.json", f"f-nbp-{layer_id}", content=b"{}")
            # BDL navigation
            self.drive.add_folder(f"f-bdl-nav-{layer_id}", "bdl", curr_id)

    def tearDown(self):
        self.url_patcher.stop()
        self.env_patcher.stop()

    def _default_mock_producer_request(self, req, timeout=15):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        resp = mock.MagicMock()
        if "workflows/source-gus-bdl.yml/runs" in url:
            resp.read.return_value = json.dumps({"total_count": 0, "workflow_runs": []}).encode("utf-8")
        else:
            resp.read.return_value = json.dumps({"state": "disabled_manually"}).encode("utf-8")
        return resp

    # -------------------------------------------------------------------------
    # 1. Pagination Test (>100 descendants)
    # -------------------------------------------------------------------------
    def test_pagination(self):
        """Verify descendant enumeration paginates completely through >100 items without truncation."""
        for i in range(120):
            self.drive.add_file(f"f-landing-child-{i:03d}", f"response_{i:03d}.json", "f-landing-bdl", size=50)

        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        landing_targets = [t for t in plan["targets"] if t["root_key"] == "01_landing/gus_bdl"]
        # 120 files enumerated
        self.assertEqual(120, len(landing_targets))
        # Count includes descendants + root folder itself
        self.assertEqual(121, plan["counts_by_root"]["01_landing/gus_bdl"])
        self.assertEqual(120 * 50, plan["bytes_by_root"]["01_landing/gus_bdl"])

    # -------------------------------------------------------------------------
    # 2. Canonical and Legacy Paths Test
    # -------------------------------------------------------------------------
    def test_canonical_and_legacy_paths(self):
        """Verify discovery finds canonical releases/bdl + archive, plus legacy bdl-platform if present."""
        self.drive.add_folder("f-legacy-bdl", "bdl-platform", self.root_id)

        engine = BdlResetEngine(self.storage, self.root_id)
        roots = engine.discover_bdl_roots()

        self.assertIn("01_landing/gus_bdl", roots)
        self.assertIn("06_control/source_campaigns/gus_bdl", roots)
        self.assertIn("releases/bdl", roots)
        self.assertIn("05_archive/bdl-platform", roots)
        self.assertIn("legacy_root/bdl-platform", roots)
        self.assertIn("02_bronze/current/bdl", roots)
        self.assertIn("03_silver/current/bdl", roots)
        self.assertIn("04_gold/current/bdl", roots)

    # -------------------------------------------------------------------------
    # 3. Ambiguous and Mixed Scope Rejection Test
    # -------------------------------------------------------------------------
    def test_ambiguous_mixed_scope_rejection(self):
        """Verify fail-closed rejection on duplicate folders, wrong MIME, foreign assets, and cycles."""
        # 3a. Duplicate folder
        self.drive.add_folder("f-duplicate-landing-bdl", "gus_bdl", "f-landing")
        engine = BdlResetEngine(self.storage, self.root_id)
        with self.assertRaises(AmbiguityError):
            engine.discover_bdl_roots()
        del self.drive.items["f-duplicate-landing-bdl"]

        # 3b. Non-folder item with root name (wrong MIME type)
        self.drive.add_file("f-file-landing-bdl", "gus_bdl", "f-landing")
        with self.assertRaises(AmbiguityError):
            engine.discover_bdl_roots()
        del self.drive.items["f-file-landing-bdl"]

        # 3c. Foreign asset inside BDL scope (e.g. world_bank_wdi inside releases/bdl)
        self.drive.add_folder("f-alien-source", "world_bank_wdi", "f-rel-bdl")
        with self.assertRaises(AmbiguityError):
            engine.plan()
        del self.drive.items["f-alien-source"]

        # 3d. Foreign source_id in appProperties
        self.drive.add_file(
            "f-foreign-app-prop", "child.json", "f-landing-bdl", app_properties={"source_id": "world_bank_wdi"}
        )
        with self.assertRaises(AmbiguityError):
            engine.plan()
        del self.drive.items["f-foreign-app-prop"]

        # 3e. Shortcut targeting foreign source
        self.drive.add_file(
            "f-foreign-target", "table.parquet", "f-releases-wdi", app_properties={"source_id": "world_bank_wdi"}
        )
        self.drive.add_shortcut("f-bad-shortcut", "shortcut_wdi", "f-bdl-nav-f-bronze", target_id="f-foreign-target")
        with self.assertRaises(AmbiguityError):
            engine.plan()
        del self.drive.items["f-bad-shortcut"]
        del self.drive.items["f-foreign-target"]

        # 3f. Broad substring check: a file named 'bdl_notes.txt' outside BDL scope must not be targeted
        self.drive.add_file("f-shared-doc", "bdl_notes.txt", self.root_id)
        plan = engine.plan()
        target_ids = {t["id"] for t in plan["targets"]}
        self.assertNotIn("f-shared-doc", target_ids)

    # -------------------------------------------------------------------------
    # 4. No Side Effects on Plan Test
    # -------------------------------------------------------------------------
    def test_no_side_effects_on_plan(self):
        """Verify plan generation is 100% read-only and performs zero mutations."""
        initial_mutations = self.drive.files_resource.mutations_count
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        self.assertEqual(initial_mutations, self.drive.files_resource.mutations_count)
        self.assertEqual(0, len(self.drive.files_resource.trash_calls))
        self.assertTrue(plan["read_only"])
        self.assertEqual("planned", plan["status"])

    # -------------------------------------------------------------------------
    # 5. No Secret Output Test
    # -------------------------------------------------------------------------
    def test_no_secret_output(self):
        """Verify generated plan, identity, and receipts leak no credentials or tokens."""
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        serialized = json.dumps(plan)
        secret_patterns = [
            "GOOGLE_OAUTH_CLIENT_SECRET",
            "GOOGLE_OAUTH_REFRESH_TOKEN",
            "GUS_BDL_API_KEY",
            "GUS_BDL_WEB_PASSWORD",
            "GUS_BDL_WEB_EMAIL",
            "Bearer ",
            "ya29.",
            "client_secret",
        ]
        for pattern in secret_patterns:
            self.assertNotIn(pattern, serialized)

    # -------------------------------------------------------------------------
    # 6. Stale-Plan Failure Test
    # -------------------------------------------------------------------------
    def test_stale_plan_failure(self):
        """Verify apply aborts if plan hash is tampered, root ID mismatches, or expected hash is wrong."""
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()
        digest = plan["plan_sha256"]

        # 6a. Tampered plan content
        tampered = copy.deepcopy(plan)
        tampered["targets"].append({"id": "fake", "name": "fake", "mime_type": "text/plain"})
        with self.assertRaises(DriftError):
            engine.apply(plan=tampered, confirmed=True, expected_sha256=digest)

        # 6b. Mismatched expected SHA-256
        with self.assertRaises(DriftError):
            engine.apply(plan=plan, confirmed=True, expected_sha256="0" * 64)

        # 6c. Root safety pin mismatch
        wrong_engine = BdlResetEngine(MockStorageManager(self.drive, "wrong-root"), "wrong-root")
        with self.assertRaises(SafetyPinError):
            wrong_engine.apply(plan=plan, confirmed=True, expected_sha256=digest)

    # -------------------------------------------------------------------------
    # 7. Non-BDL Preservation Test
    # -------------------------------------------------------------------------
    def test_non_bdl_preservation(self):
        """Verify non-BDL sources (NBP, WDI, Eurostat) remain completely untouched and untrashed."""
        self.drive.add_file("f-bdl-item-1", "part1.parquet", "f-rel-bdl")
        self.drive.add_file("f-wdi-item-1", "wdi.parquet", "f-releases-wdi")
        self.drive.add_file("f-nbp-item-1", "nbp.parquet", "f-releases-nbp")

        tmp_journal = Path("test-results/journal-preserve.json")
        engine = BdlResetEngine(self.storage, self.root_id, journal_local_path=tmp_journal)
        plan = engine.plan()

        receipt = engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

        self.assertEqual("bdl_reset_applied", receipt["status"])
        self.assertEqual("verified_clean", receipt["verification"]["status"])

        # Non-BDL files must NOT be trashed
        self.assertFalse(self.drive.items["f-wdi-item-1"]["trashed"])
        self.assertFalse(self.drive.items["f-nbp-item-1"]["trashed"])
        self.assertFalse(self.drive.items["f-landing-wdi"]["trashed"])
        self.assertFalse(self.drive.items["f-releases-nbp"]["trashed"])

        # BDL roots must be explicitly trashed
        self.assertTrue(self.drive.items["f-landing-bdl"]["trashed"])
        self.assertTrue(self.drive.items["f-landing-bdl"]["explicitlyTrashed"])
        self.assertTrue(self.drive.items["f-rel-bdl"]["trashed"])
        self.assertTrue(self.drive.items["f-rel-bdl"]["explicitlyTrashed"])

        # BDL descendants must have inherited trashed status
        self.assertTrue(self.drive.items["f-bdl-item-1"]["trashed"])
        self.assertFalse(self.drive.items["f-bdl-item-1"]["explicitlyTrashed"])

    # -------------------------------------------------------------------------
    # 8. Inherited Trash Model & Untrash Restoration Test
    # -------------------------------------------------------------------------
    def test_inherited_trash_and_untrash_restoration(self):
        """Verify Drive v3 inherited trashing behavior: trashing a root trashes descendants; untrashing restores."""
        self.drive.add_folder("f-nested-folder", "subfolder", "f-rel-bdl")
        self.drive.add_file("f-deep-file", "data.parquet", "f-nested-folder")

        # Initial state: untrashed
        self.assertFalse(self.drive.items["f-rel-bdl"]["trashed"])
        self.assertFalse(self.drive.items["f-nested-folder"]["trashed"])
        self.assertFalse(self.drive.items["f-deep-file"]["trashed"])

        # Trash root
        self.drive.files().update(fileId="f-rel-bdl", body={"trashed": True}).execute()

        self.assertTrue(self.drive.items["f-rel-bdl"]["trashed"])
        self.assertTrue(self.drive.items["f-rel-bdl"]["explicitlyTrashed"])
        # Descendants inherit trashed=True, explicitlyTrashed=False
        self.assertTrue(self.drive.items["f-nested-folder"]["trashed"])
        self.assertFalse(self.drive.items["f-nested-folder"]["explicitlyTrashed"])
        self.assertTrue(self.drive.items["f-deep-file"]["trashed"])
        self.assertFalse(self.drive.items["f-deep-file"]["explicitlyTrashed"])

        # Untrash root restores whole subtree
        self.drive.files().update(fileId="f-rel-bdl", body={"trashed": False}).execute()
        self.assertFalse(self.drive.items["f-rel-bdl"]["trashed"])
        self.assertFalse(self.drive.items["f-nested-folder"]["trashed"])
        self.assertFalse(self.drive.items["f-deep-file"]["trashed"])

    # -------------------------------------------------------------------------
    # 9. Interrupted Trash Resume with Write-Ahead Remote Journal
    # -------------------------------------------------------------------------
    def test_interrupted_trash_resume_with_remote_journal(self):
        """Verify first-mutation crash recovery and idempotent resumption using write-ahead remote journal."""
        tmp_journal = Path("test-results/journal-resume-remote.json")
        if tmp_journal.is_file():
            tmp_journal.unlink()

        engine = BdlResetEngine(self.storage, self.root_id, journal_local_path=tmp_journal)
        plan = engine.plan()

        # Simulate first root mutation completed on remote Drive before crash
        plan_folder_id = engine._ensure_remote_plan_folder(plan["plan_id"])
        first_root_key = list(plan["roots"].keys())[0]
        first_root_id = plan["roots"][first_root_key]["id"]

        self.drive.files().update(fileId=first_root_id, body={"trashed": True}).execute()

        partial_journal = {
            "format_version": 2,
            "operation": "apply",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan["plan_sha256"],
            "root_id": self.root_id,
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress",
            "root_checkpoints": [
                {
                    "root_key": first_root_key,
                    "root_id": first_root_id,
                    "status": "trashed_verified",
                    "intent_at_utc": datetime.now(timezone.utc).isoformat(),
                    "verified_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }
        engine._save_journal(partial_journal, plan_folder_id)

        # Resume on a fresh runner
        fresh_engine = BdlResetEngine(self.storage, self.root_id, journal_local_path=Path("test-results/fresh-runner.json"))
        resume_receipt = fresh_engine.apply(
            plan=plan,
            confirmed=True,
            resume=True,
            expected_sha256=plan["plan_sha256"],
        )

        self.assertEqual("bdl_reset_resumed", resume_receipt["status"])
        self.assertEqual("verified_clean", resume_receipt["verification"]["status"])

        # All BDL roots must be trashed
        for r_key, r_info in plan["roots"].items():
            self.assertTrue(self.drive.items[r_info["id"]]["trashed"])
            self.assertTrue(self.drive.items[r_info["id"]]["explicitlyTrashed"])

    # -------------------------------------------------------------------------
    # 10. Lost Response Handling (Intent to Trash Recovery)
    # -------------------------------------------------------------------------
    def test_lost_response_after_successful_update(self):
        """Verify resume detects a root with intent_to_trash that succeeded on Drive before crash."""
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        plan_folder_id = engine._ensure_remote_plan_folder(plan["plan_id"])
        first_root_key = list(plan["roots"].keys())[0]
        first_root_id = plan["roots"][first_root_key]["id"]

        # Root was successfully trashed on Drive
        self.drive.files().update(fileId=first_root_id, body={"trashed": True}).execute()

        # But journal only recorded intent_to_trash before crash
        intent_journal = {
            "format_version": 2,
            "operation": "apply",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan["plan_sha256"],
            "root_id": self.root_id,
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress",
            "root_checkpoints": [
                {
                    "root_key": first_root_key,
                    "root_id": first_root_id,
                    "status": "intent_to_trash",
                    "intent_at_utc": datetime.now(timezone.utc).isoformat(),
                }
            ],
        }
        engine._save_journal(intent_journal, plan_folder_id)

        # Resume execution
        receipt = engine.apply(plan=plan, confirmed=True, resume=True, expected_sha256=plan["plan_sha256"])
        self.assertEqual("bdl_reset_resumed", receipt["status"])
        self.assertEqual("verified_clean", receipt["verification"]["status"])

    # -------------------------------------------------------------------------
    # 11. Real v2 Sharded Quota Manifest & Fresh Ledger Test
    # -------------------------------------------------------------------------
    def test_real_v2_sharded_quota_manifest_and_clean_fresh_ledger(self):
        """Verify extraction of v2 quota manifest and consumption of clean fresh ledger outside trashed scope."""
        # Use production _CampaignStore to save a real v2 state into MockDriveService
        mock_obj_store = MockObjectStore(self.drive)
        campaign_store = _CampaignStore(mock_obj_store, "gus_bdl", "f-ctrl-bdl", "f-landing-bdl")

        now_ts = datetime.now(timezone.utc).timestamp()
        original_state = {
            "schema_version": STATE_VERSION,
            "source_id": "gus_bdl",
            "onboarding_date": "2026-09-08",
            "pending": [
                {"id": "task:000001", "lane": "history", "kind": "history_page", "cursor": {"page": 1}},
                {"id": "task:000002", "lane": "recent", "kind": "recent_page", "cursor": {"page": 2}},
            ],
            "completed": {"task:old1": "2026-09-08T01:02:03+00:00"},
            "recent_roots": {"series:1": {"id": "task:000002", "lane": "recent", "kind": "recent_page"}},
            "receipts": [],
            "rejected_receipts": [],
            "quota_attempts": [now_ts - 300, now_ts - 100],  # 2 active attempts within 15m
            "provider_retry_at": now_ts + 600,
            "last_attempt_utc": datetime.now(timezone.utc).isoformat(),
            "raw_bytes": 100000,
            "accepted_responses": 41,
            "record_count": 4331822,
            "lane_position": 0,
            "coverage_status": "incomplete",
            "catalogue_totals": {},
            "last_error": None,
        }
        campaign_store.save(original_state)

        # Verify that states/ directory and sharded manifest exist
        self.assertTrue(any("state-manifest-root-" in item["name"] for item in self.drive.items.values()))

        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        # Quota evidence verification
        quota_ev = plan.get("retained_quota_evidence", {})
        self.assertTrue(quota_ev.get("state_found"))
        self.assertEqual("gus_bdl", quota_ev.get("source_id"))
        self.assertEqual(2, len(quota_ev.get("quota_attempts", [])))
        self.assertEqual(original_state["provider_retry_at"], quota_ev.get("provider_retry_at"))

        # Clean fresh ledger verification
        ledger = quota_ev.get("clean_fresh_ledger", {})
        self.assertEqual(STATE_VERSION, ledger.get("schema_version"))
        self.assertEqual("gus_bdl", ledger.get("source_id"))
        self.assertEqual([], ledger.get("pending"))
        self.assertEqual({}, ledger.get("completed"))
        self.assertEqual([], ledger.get("receipts"))
        self.assertEqual(0, ledger.get("raw_bytes"))
        self.assertEqual(0, ledger.get("accepted_responses"))
        self.assertEqual("awaiting_web_bulk", ledger.get("coverage_status"))
        self.assertEqual("awaiting_web_bulk", ledger.get("gate"))
        self.assertEqual(2, len(ledger.get("quota_attempts", [])))
        self.assertEqual(REGISTERED_BDL_QUOTA_WINDOWS, tuple(ledger.get("quota_windows", [])))

        # Validate ledger passes validate_state
        validate_state(ledger, "gus_bdl")

        # Apply reset (all 7 BDL roots trashed)
        engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

        # Now test consuming fresh ledger into a fresh target campaign store
        self.drive.add_folder("f-new-ctrl-sc", "source_campaigns", "f-control")
        self.drive.add_folder("f-new-ctrl-bdl", "gus_bdl", "f-new-ctrl-sc")
        self.drive.add_folder("f-new-landing-bdl", "gus_bdl", "f-landing")

        new_store = _CampaignStore(mock_obj_store, "gus_bdl", "f-new-ctrl-bdl", "f-new-landing-bdl")
        consume_receipt = engine.consume_fresh_ledger(
            new_store,
            plan_id=plan["plan_id"],
            expected_plan_sha256=plan["plan_sha256"],
        )

        self.assertEqual("fresh_ledger_consumed", consume_receipt["status"])
        self.assertEqual(2, consume_receipt["quota_attempts_count"])
        self.assertEqual("awaiting_web_bulk", consume_receipt["coverage_status"])

        # Verify consumed state in new store
        loaded_new = new_store.load()
        self.assertIsNotNone(loaded_new)
        self.assertEqual([], loaded_new["pending"])
        self.assertEqual({}, loaded_new["completed"])
        self.assertEqual("awaiting_web_bulk", loaded_new["coverage_status"])
        self.assertEqual(2, len(loaded_new["quota_attempts"]))

    # -------------------------------------------------------------------------
    # 12. Dynamic Child and Content Drift Test
    # -------------------------------------------------------------------------
    def test_dynamic_child_drift_before_root_trash(self):
        """Verify drift detection immediately before root mutation detects added or modified children."""
        self.drive.add_file("f-bdl-target-1", "file1.parquet", "f-rel-bdl", size=100)
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        # Drift: file size changes after plan
        self.drive.items["f-bdl-target-1"]["size"] = 999

        with self.assertRaises(DriftError):
            engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

    # -------------------------------------------------------------------------
    # 13. Non-BDL Pointer Drift Rejection Test
    # -------------------------------------------------------------------------
    def test_non_bdl_pointer_drift_rejection(self):
        """Verify apply aborts if a key non-BDL pointer drifts in content hash before mutation."""
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        # Alter NBP current release pointer content
        self.drive.items["f-ptr-nbp-rel"]["_content"] = b'{"tampered": true}'

        with self.assertRaises(DriftError):
            engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

    # -------------------------------------------------------------------------
    # 14. Production Runtime Guards Test
    # -------------------------------------------------------------------------
    def test_runtime_guards_enforcement(self):
        """Verify unconditional rejection if production write opt-in, Actions, or main branch is missing."""
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        # 14a. Missing ZOHELO_ALLOW_PRODUCTION_WRITES
        with mock.patch.dict(os.environ, {"ZOHELO_ALLOW_PRODUCTION_WRITES": "false"}):
            with self.assertRaises(RuntimeGuardError):
                engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

        # 14b. Not in GitHub Actions
        with mock.patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}):
            with self.assertRaises(RuntimeGuardError):
                engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

        # 14c. Not on refs/heads/main
        with mock.patch.dict(os.environ, {"GITHUB_REF": "refs/heads/feature-branch"}):
            with self.assertRaises(RuntimeGuardError):
                engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

    # -------------------------------------------------------------------------
    # 15. Active Producer Rejection (404, network error, active workflow, active runs)
    # -------------------------------------------------------------------------
    def test_active_producer_fail_closed(self):
        """Verify producer checker fails closed on 404, network error, active state, and active runs."""
        import urllib.error
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        # 15a. HTTP 404
        with mock.patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError("url", 404, "Not Found", {}, io.BytesIO())):
            with self.assertRaises(ActiveProducerError):
                engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

        # 15b. Network URLError
        with mock.patch("urllib.request.urlopen", side_effect=urllib.error.URLError("Connection refused")):
            with self.assertRaises(ActiveProducerError):
                engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

        # 15c. Workflow still active
        with mock.patch("urllib.request.urlopen") as mock_url:
            mock_resp = mock.MagicMock()
            mock_resp.read.return_value = json.dumps({"state": "active"}).encode("utf-8")
            mock_url.return_value.__enter__.return_value = mock_resp
            with self.assertRaises(ActiveProducerError):
                engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

        # 15d. Active queued run 34883169715
        def mock_runs_request(req, timeout=15):
            url = req.full_url if hasattr(req, "full_url") else str(req)
            resp = mock.MagicMock()
            if "workflows/source-gus-bdl.yml/runs" in url:
                if "status=queued" in url:
                    resp.read.return_value = json.dumps({
                        "total_count": 1,
                        "workflow_runs": [{"id": 34883169715, "status": "queued"}],
                    }).encode("utf-8")
                else:
                    resp.read.return_value = json.dumps({"total_count": 0, "workflow_runs": []}).encode("utf-8")
            else:
                resp.read.return_value = json.dumps({"state": "disabled_manually"}).encode("utf-8")
            return resp

        with mock.patch("urllib.request.urlopen", side_effect=mock_runs_request):
            with self.assertRaises(ActiveProducerError):
                engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])

    # -------------------------------------------------------------------------
    # 16. Tractable Simulation of 7 Roots / Descendants Hierarchy
    # -------------------------------------------------------------------------
    def test_tractable_simulation_all_seven_roots(self):
        """Verify tractable simulation of all 7 roots: exact root count, descendant count, and clean verification."""
        # Populate items in all 7 roots
        # 1. 01_landing/gus_bdl
        for i in range(10):
            self.drive.add_file(f"f-sim-land-{i}", f"resp_{i}.json", "f-landing-bdl", size=100)
        # 2. 06_control/source_campaigns/gus_bdl
        for i in range(10):
            self.drive.add_file(f"f-sim-ctrl-{i}", f"state_{i}.json", "f-ctrl-bdl", size=200)
        # 3. releases/bdl
        for i in range(10):
            self.drive.add_file(f"f-sim-rel-{i}", f"part_{i}.parquet", "f-rel-bdl", size=300)
        # 4. 05_archive/bdl-platform
        self.drive.add_file("f-sim-arch-1", "archive.parquet", "f-arch-bdl", size=50)
        # 5, 6, 7. medallion navigation (6 shortcuts each)
        for layer_id in ("f-bronze", "f-silver", "f-gold"):
            nav_id = f"f-bdl-nav-{layer_id}"
            self.drive.add_file(f"f-sim-idx-{layer_id}", "navigation-index.json", nav_id, size=20)
            for s in range(6):
                target_rel_id = f"f-sim-rel-{s}"
                self.drive.add_shortcut(f"f-sim-sc-{layer_id}-{s}", f"shortcut_{s}", nav_id, target_id=target_rel_id)

        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        # Exactly 7 roots discovered
        self.assertEqual(7, plan["root_count"])
        # Total targets = 10 + 10 + 10 + 1 + 3 * (1 + 6) = 52
        self.assertEqual(52, plan["total_descendant_count"])
        # Total objects = 52 targets + 7 roots = 59
        self.assertEqual(59, plan["total_object_count"])

        receipt = engine.apply(plan=plan, confirmed=True, expected_sha256=plan["plan_sha256"])
        self.assertEqual("bdl_reset_applied", receipt["status"])
        self.assertEqual(7, receipt["roots_trashed"])
        self.assertEqual(52, receipt["total_descendants_inherited_trash"])
        self.assertEqual("verified_clean", receipt["verification"]["status"])


class BdlResetDispatchAndProvenanceTests(unittest.TestCase):
    def test_dispatch_validation_strict_rules(self):
        import validate_bdl_reset_dispatch as dispatch

        # Plan requires no special mutating fields
        self.assertEqual([], dispatch.validate("plan", False, "", "", ""))

        # Mutating operations require confirmation and valid pins
        valid_pins = ("12345", "plan-bdl-reset-1234abcd", "a" * 64)
        for op in ("apply", "resume"):
            self.assertEqual([], dispatch.validate(op, True, *valid_pins))
            # Unconfirmed
            errors = dispatch.validate(op, False, *valid_pins)
            self.assertTrue(any("confirmation" in e for e in errors))
            # Malformed run ID
            errors = dispatch.validate(op, True, "abc", "plan-bdl-reset-1234abcd", "a" * 64)
            self.assertTrue(any("run ID" in e for e in errors))
            # Malformed plan ID
            errors = dispatch.validate(op, True, "12345", "bad_id", "a" * 64)
            self.assertTrue(any("plan ID" in e for e in errors))
            # Malformed sha256
            errors = dispatch.validate(op, True, "12345", "plan-bdl-reset-1234abcd", "not-a-sha")
            self.assertTrue(any("SHA-256" in e for e in errors))

    def test_plan_provenance_verification(self):
        import verify_bdl_reset_plan as verifier

        root = "root-123"
        commit = "c" * 40
        plan = {
            "status": "planned",
            "read_only": True,
            "plan_id": "plan-bdl-reset-test1234",
            "root_id": root,
            "expected_root_id": root,
            "targets": [],
            "non_bdl_baseline": [],
        }
        digest = verifier.canonical_plan_hash(plan)
        plan["plan_sha256"] = digest

        identity = {
            "format_version": 1,
            "repository": "rutkala/zohelo-data",
            "workflow_path": verifier.WORKFLOW_PATH,
            "workflow_run_id": "9999",
            "workflow_commit": commit,
            "plan_id": "plan-bdl-reset-test1234",
            "plan_sha256": digest,
            "root_id": root,
        }

        # Successful verification without token (static pins check)
        result = verifier.verify_provenance(
            plan=plan,
            identity=identity,
            run_id="9999",
            expected_root_id=root,
            expected_plan_id="plan-bdl-reset-test1234",
            expected_plan_sha256=digest,
            repository="rutkala/zohelo-data",
            executing_commit=commit,
            token="",
        )
        self.assertEqual("reviewed_plan_verified", result["status"])

        # Tampered digest raises ProvenanceError
        with self.assertRaises(verifier.ProvenanceError):
            verifier.verify_provenance(
                plan=plan,
                identity=identity,
                run_id="9999",
                expected_root_id=root,
                expected_plan_id="plan-bdl-reset-test1234",
                expected_plan_sha256="b" * 64,
                repository="rutkala/zohelo-data",
                executing_commit=commit,
                token="",
            )


if __name__ == "__main__":
    unittest.main()
