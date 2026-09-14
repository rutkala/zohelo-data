"""Tests for Google Drive layout migration engine, layout resolution, and medallion navigation."""
from __future__ import annotations

import copy
from hashlib import sha256
import io
import json
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4

import sys
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from drive_migration import (
    DriveMigrationEngine,
    DriftError,
    MigrationError,
    SafetyPinError,
)
from drive_release_store import DriveReleaseStore
from layout_resolution import (
    CANONICAL_CONTROL_FOLDER,
    CANONICAL_NBP_CONTROL_FOLDER,
    CANONICAL_RELEASES_FOLDER,
    CANONICAL_SOURCES,
    LEGACY_BDL_WRAPPER,
    LEGACY_NBP_CONTROL_FOLDER,
    LEGACY_WDI_WRAPPER,
    ARCHIVE_FOLDER,
    detect_layout_mode,
    resolve_nbp_control_root,
    resolve_source_release_root,
    AmbiguousLayoutError,
    LayoutResolutionError,
)
from medallion_navigation import (
    sync_source_medallion_navigation,
    verify_medallion_navigation,
    StaleNavigationError,
    SHORTCUT_MIME_TYPE,
)
from release_protocol import (
    publish_release,
    promote_retained_release,
    read_current_release_manifest,
)
from storage_manager import StorageManager


class MockDriveService:
    """In-memory mock Google Drive v3 files service faithful to real Drive behavior."""

    def __init__(self, initial_files: dict[str, dict]):
        self._files = copy.deepcopy(initial_files)
        self._next_id = 1000

    def files(self):
        return self

    def list(self, q="", fields=None, pageSize=100, pageToken=None, supportsAllDrives=True, includeItemsFromAllDrives=True, **kwargs):
        mock_req = MagicMock()

        def execute(num_retries=0):
            matched = []
            for file_id, meta in self._files.items():
                if meta.get("trashed", False):
                    continue
                match = True
                if "in parents" in q:
                    import re
                    m = re.search(r"'([^']+)' in parents", q)
                    if m:
                        parent_id = m.group(1)
                        if parent_id not in meta.get("parents", []):
                            match = False
                if match and "name=" in q:
                    import re
                    m = re.search(r"name='([^']+)'", q)
                    if m:
                        name = m.group(1).replace("\\'", "'").replace("\\\\", "\\")
                        if meta.get("name") != name:
                            match = False
                if match and "mimeType=" in q:
                    import re
                    m = re.search(r"mimeType='([^']+)'", q)
                    if m:
                        mime = m.group(1)
                        if meta.get("mimeType") != mime:
                            match = False
                if match:
                    item = {k: v for k, v in meta.items() if k != "content"}
                    matched.append(item)
            return {"files": matched, "nextPageToken": None}

        mock_req.execute = execute
        return mock_req

    def get(self, fileId, fields=None, supportsAllDrives=True):
        mock_req = MagicMock()

        def execute(num_retries=0):
            if fileId not in self._files or self._files[fileId].get("trashed", False):
                raise RuntimeError(f"File not found: {fileId}")
            item = {k: v for k, v in self._files[fileId].items() if k != "content"}
            item.setdefault("ownedByMe", True)
            item.setdefault("trashed", False)
            return item

        mock_req.execute = execute
        return mock_req

    def get_media(self, fileId, supportsAllDrives=True):
        mock_req = MagicMock()

        def execute(num_retries=0):
            if fileId not in self._files or self._files[fileId].get("trashed", False):
                raise RuntimeError(f"File not found: {fileId}")
            content = self._files[fileId].get("content")
            if content is None:
                content = b"{}"
            return content

        mock_req.execute = execute
        return mock_req

    def create(self, body=None, media_body=None, fields=None, supportsAllDrives=True):
        mock_req = MagicMock()

        def execute(num_retries=0):
            nonlocal body
            body = body or {}
            self._next_id += 1
            file_id = body.get("id") or f"file-{self._next_id}"
            content = b""
            if media_body and hasattr(media_body, "_fd"):
                media_body._fd.seek(0)
                content = media_body._fd.read()
            self._files[file_id] = {
                "id": file_id,
                "name": body.get("name", "untitled"),
                "mimeType": body.get("mimeType", "application/octet-stream"),
                "parents": list(body.get("parents", [])),
                "trashed": False,
                "content": content,
                "shortcutDetails": copy.deepcopy(body.get("shortcutDetails")),
            }
            return {"id": file_id, "name": body.get("name"), "parents": body.get("parents", [])}

        mock_req.execute = execute
        return mock_req

    def update(self, fileId, body=None, media_body=None, addParents=None, removeParents=None, fields=None, supportsAllDrives=True):
        mock_req = MagicMock()

        def execute(num_retries=0):
            if fileId not in self._files:
                raise RuntimeError(f"File not found: {fileId}")
            meta = self._files[fileId]
            if body and "name" in body:
                meta["name"] = body["name"]
            if body and "trashed" in body:
                meta["trashed"] = body["trashed"]
            if body and "shortcutDetails" in body:
                # Drive v3 rejects updating shortcutDetails on existing shortcuts
                raise RuntimeError("Drive v3 does not allow updating shortcutDetails on existing shortcuts")
            if media_body and hasattr(media_body, "_fd"):
                media_body._fd.seek(0)
                meta["content"] = media_body._fd.read()
            parents = set(meta.get("parents", []))
            if removeParents:
                for p in removeParents.split(","):
                    parents.discard(p.strip())
            if addParents:
                for p in addParents.split(","):
                    parents.add(p.strip())
            meta["parents"] = list(parents)
            item = {k: v for k, v in meta.items() if k != "content"}
            return item

        mock_req.execute = execute
        return mock_req

    def delete(self, fileId, supportsAllDrives=True):
        mock_req = MagicMock()

        def execute(num_retries=0):
            if fileId in self._files:
                del self._files[fileId]
            return {}

        mock_req.execute = execute
        return mock_req

    def generateIds(self, count=1, space="drive", type="files"):
        mock_req = MagicMock()

        def execute(num_retries=0):
            ids = []
            for _ in range(count):
                self._next_id += 1
                ids.append(f"gen-id-{self._next_id}")
            return {"ids": ids}

        mock_req.execute = execute
        return mock_req


def build_legacy_drive_state(root_id: str = "prod-root-123"):
    """Construct a full legacy Drive hierarchy for tests matching production facts."""
    nbp_release_id = "11111111-2222-3333-4444-555555555555"
    bdl_release_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    wdi_release_id = "99999999-8888-7777-6666-555555555555"

    nbp_manifest = {
        "format_version": 2,
        "release_id": nbp_release_id,
        "code_sha": "a" * 40,
        "release_scope": "nbp_platform",
        "created_at_utc": "2026-09-14T00:00:00Z",
        "status": "validated",
        "tests": {"passed": True},
        "datasets": [
            {
                "dataset_id": "dim_date",
                "table_name": "dim_date",
                "layer": "04_gold",
                "row_count": 100,
                "files": [{"id": "nbp-dim-date-pq", "size": 1024, "sha256": "b" * 64}],
            }
        ],
        "artifacts": [],
    }

    bdl_manifest = {
        "format_version": 2,
        "release_id": bdl_release_id,
        "code_sha": "b" * 40,
        "release_scope": "bdl_platform",
        "created_at_utc": "2026-09-14T00:00:00Z",
        "status": "validated",
        "tests": {"passed": True},
        "datasets": [
            {
                "dataset_id": "mart_bdl_coverage",
                "table_name": "mart_bdl_coverage",
                "layer": "04_gold",
                "row_count": 50,
                "files": [{"id": "bdl-mart-pq", "size": 2048, "sha256": "c" * 64}],
            }
        ],
        "artifacts": [],
    }

    wdi_manifest = {
        "format_version": 2,
        "release_id": wdi_release_id,
        "code_sha": "c" * 40,
        "release_scope": "wdi_platform",
        "created_at_utc": "2026-09-14T00:00:00Z",
        "status": "validated",
        "tests": {"passed": True},
        "datasets": [
            {
                "dataset_id": "wdi_observations",
                "table_name": "wdi_observations",
                "layer": "03_silver",
                "row_count": 1000,
                "files": [
                    {"id": "wdi-obs-part-0", "name": "wdi_observations--part-0.parquet", "size": 4096, "sha256": "d" * 64},
                    {"id": "wdi-obs-part-1", "name": "wdi_observations--part-1.parquet", "size": 4096, "sha256": "e" * 64},
                ],
            }
        ],
        "artifacts": [],
    }

    state_snapshot_bytes = json.dumps({"format_version": 1, "sources": {}}).encode()
    state_ptr_bytes = json.dumps({
        "format_version": 1,
        "state_file_id": "f-nbp-snapshot",
        "state_sha256": sha256(state_snapshot_bytes).hexdigest(),
    }).encode()

    files = {
        root_id: {"id": root_id, "name": "zohelo-data", "mimeType": "application/vnd.google-apps.folder", "parents": []},
        # Medallion folders
        "f-01-landing": {"id": "f-01-landing", "name": "01_landing", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-02-bronze": {"id": "f-02-bronze", "name": "02_bronze", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-03-silver": {"id": "f-03-silver", "name": "03_silver", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-04-gold": {"id": "f-04-gold", "name": "04_gold", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-05-archive": {"id": "f-05-archive", "name": "05_archive", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        # Legacy NBP: root current-release.json and releases/ folder
        "f-releases": {"id": "f-releases", "name": "releases", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-nbp-rel-uuid": {"id": "f-nbp-rel-uuid", "name": nbp_release_id, "mimeType": "application/vnd.google-apps.folder", "parents": ["f-releases"]},
        "f-nbp-manifest": {"id": "f-nbp-manifest", "name": "release.json", "mimeType": "application/json", "parents": ["f-nbp-rel-uuid"], "content": json.dumps(nbp_manifest).encode()},
        "nbp-dim-date-pq": {"id": "nbp-dim-date-pq", "name": "dim_date.parquet", "mimeType": "application/octet-stream", "parents": ["f-nbp-rel-uuid"], "content": b"PARQUET_NBP"},
        "f-root-pointer": {"id": "f-root-pointer", "name": "current-release.json", "mimeType": "application/json", "parents": [root_id], "content": json.dumps({"format_version": 1, "release_id": nbp_release_id, "manifest_file_id": "f-nbp-manifest", "manifest_sha256": sha256(json.dumps(nbp_manifest).encode()).hexdigest(), "updated_at_utc": nbp_manifest["created_at_utc"]}).encode()},
        # Legacy BDL: bdl-platform/ wrapper
        "f-bdl-wrapper": {"id": "f-bdl-wrapper", "name": "bdl-platform", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-bdl-releases": {"id": "f-bdl-releases", "name": "releases", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-bdl-wrapper"]},
        "f-bdl-rel-uuid": {"id": "f-bdl-rel-uuid", "name": bdl_release_id, "mimeType": "application/vnd.google-apps.folder", "parents": ["f-bdl-releases"]},
        "f-bdl-manifest": {"id": "f-bdl-manifest", "name": "release.json", "mimeType": "application/json", "parents": ["f-bdl-rel-uuid"], "content": json.dumps(bdl_manifest).encode()},
        "bdl-mart-pq": {"id": "bdl-mart-pq", "name": "mart_bdl_coverage.parquet", "mimeType": "application/octet-stream", "parents": ["f-bdl-rel-uuid"], "content": b"PARQUET_BDL"},
        "f-bdl-pointer": {"id": "f-bdl-pointer", "name": "current-release.json", "mimeType": "application/json", "parents": ["f-bdl-wrapper"], "content": json.dumps({"format_version": 1, "release_id": bdl_release_id, "manifest_file_id": "f-bdl-manifest", "manifest_sha256": sha256(json.dumps(bdl_manifest).encode()).hexdigest(), "updated_at_utc": bdl_manifest["created_at_utc"]}).encode()},
        # Legacy WDI: wdi-platform/ wrapper
        "f-wdi-wrapper": {"id": "f-wdi-wrapper", "name": "wdi-platform", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-wdi-releases": {"id": "f-wdi-releases", "name": "releases", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-wdi-wrapper"]},
        "f-wdi-rel-uuid": {"id": "f-wdi-rel-uuid", "name": wdi_release_id, "mimeType": "application/vnd.google-apps.folder", "parents": ["f-wdi-releases"]},
        "f-wdi-manifest": {"id": "f-wdi-manifest", "name": "release.json", "mimeType": "application/json", "parents": ["f-wdi-rel-uuid"], "content": json.dumps(wdi_manifest).encode()},
        "wdi-obs-part-0": {"id": "wdi-obs-part-0", "name": "wdi_observations--part-0.parquet", "mimeType": "application/octet-stream", "parents": ["f-wdi-rel-uuid"], "content": b"PARQUET_WDI_0"},
        "wdi-obs-part-1": {"id": "wdi-obs-part-1", "name": "wdi_observations--part-1.parquet", "mimeType": "application/octet-stream", "parents": ["f-wdi-rel-uuid"], "content": b"PARQUET_WDI_1"},
        "f-wdi-pointer": {"id": "f-wdi-pointer", "name": "current-release.json", "mimeType": "application/json", "parents": ["f-wdi-wrapper"], "content": json.dumps({"format_version": 1, "release_id": wdi_release_id, "manifest_file_id": "f-wdi-manifest", "manifest_sha256": sha256(json.dumps(wdi_manifest).encode()).hexdigest(), "updated_at_utc": wdi_manifest["created_at_utc"]}).encode()},
        # Ingestion control & states
        "f-ingestion-control": {"id": "f-ingestion-control", "name": "ingestion-control", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-source-campaigns": {"id": "f-source-campaigns", "name": "source_campaigns", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-ingestion-control"]},
        "f-states": {"id": "f-states", "name": "states", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-ingestion-control"]},
        "f-nbp-snapshot": {"id": "f-nbp-snapshot", "name": "snapshot-1.json", "mimeType": "application/json", "parents": ["f-states"], "content": state_snapshot_bytes},
        "f-state-pointer": {"id": "f-state-pointer", "name": "current-ingestion-state.json", "mimeType": "application/json", "parents": ["f-ingestion-control"], "content": state_ptr_bytes},
    }
    return files, nbp_release_id, bdl_release_id, wdi_release_id


def make_storage_manager_mock(files_dict, root_id="prod-root-123"):
    svc = MockDriveService(files_dict)
    storage = MagicMock(spec=StorageManager)
    storage.drive_service = svc
    storage.resolve_root.return_value = root_id
    storage.authorize_writes.return_value = True

    def _list_exact(name, parent_id=None):
        res = svc.files().list(
            q=f"name='{name}' and '{parent_id or root_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
        ).execute()
        return res.get("files", [])

    def _get_or_create(path_segments, root_id=root_id, write_session=None):
        current_id = root_id
        for seg in path_segments:
            found = _list_exact(seg, current_id)
            if found:
                current_id = found[0]["id"]
            else:
                created = svc.files().create(body={"name": seg, "parents": [current_id], "mimeType": "application/vnd.google-apps.folder"}).execute()
                current_id = created["id"]
        return current_id

    storage._list_exact_folders.side_effect = _list_exact
    storage.get_or_create_nested_folder.side_effect = _get_or_create
    return storage, svc


class DriveMigrationTests(unittest.TestCase):

    def test_wrong_root_id_fails_closed(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, _ = make_storage_manager_mock(files, "prod-root-123")
        with self.assertRaises(SafetyPinError):
            DriveMigrationEngine(storage, expected_root_id="wrong-root-456")

    def test_plan_dry_run_makes_no_mutations(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        initial_file_count = len(files)
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        plan = engine.plan()

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["mode"], "legacy")
        self.assertTrue(plan["read_only"])
        self.assertGreater(len(plan["steps"]), 5)
        self.assertEqual(len(svc._files), initial_file_count)

    def test_drift_detection_aborts_apply(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        plan = engine.plan()

        # Simulate drift: pointer changes release_id
        svc._files["f-root-pointer"]["content"] = json.dumps({
            "format_version": 1,
            "release_id": "drifted-rel-id-999",
            "manifest_file_id": "f-nbp-manifest",
        }).encode()

        with self.assertRaises(DriftError):
            engine.apply(plan, confirmed=True)

    def test_conflicting_pointers_fail_closed(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        # Add a conflicting canonical releases/nbp pointer while root pointer exists
        files["f-nbp-canon-dir"] = {"id": "f-nbp-canon-dir", "name": "nbp", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-releases"]}
        files["f-nbp-canon-ptr"] = {"id": "f-nbp-canon-ptr", "name": "current-release.json", "mimeType": "application/json", "parents": ["f-nbp-canon-dir"], "content": b'{"format_version":1,"release_id":"conflicting-id"}'}

        storage, _ = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        with self.assertRaises(AmbiguousLayoutError):
            engine.plan()

    def test_duplicate_containers_fail_closed(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        # Add a duplicate top-level 'releases' folder
        files["f-dup-releases"] = {"id": "f-dup-releases", "name": "releases", "mimeType": "application/vnd.google-apps.folder", "parents": ["prod-root-123"]}

        storage, _ = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        with self.assertRaises(AmbiguousLayoutError):
            engine.plan()

    def test_mutating_operations_require_explicit_confirmation(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, _ = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        plan = engine.plan()

        with self.assertRaises(MigrationError):
            engine.apply(plan, confirmed=False)

        with self.assertRaises(MigrationError):
            engine.rollback(confirmed=False)

    def test_full_apply_migration_and_unchanged_identities(self):
        files, nbp_id, bdl_id, wdi_id = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")

        plan = engine.plan()
        result = engine.apply(plan, confirmed=True)

        self.assertEqual(result["status"], "migration_completed")
        self.assertEqual(detect_layout_mode(storage, "prod-root-123"), "canonical")

        # Verify exact Parquet file bytes and IDs are preserved
        self.assertEqual(svc._files["nbp-dim-date-pq"]["content"], b"PARQUET_NBP")
        self.assertEqual(svc._files["bdl-mart-pq"]["content"], b"PARQUET_BDL")
        self.assertEqual(svc._files["wdi-obs-part-0"]["content"], b"PARQUET_WDI_0")
        self.assertEqual(svc._files["wdi-obs-part-1"]["content"], b"PARQUET_WDI_1")

        # Verify NBP control root resolution
        nbp_ctrl = resolve_nbp_control_root(storage, "prod-root-123", is_writer=False)
        self.assertEqual(svc._files[nbp_ctrl]["name"], "nbp")

        # Verify source campaigns kept exact state identity under 06_control
        self.assertEqual(svc._files["f-source-campaigns"]["name"], "source_campaigns")
        ctrl_folder = [f for f in svc._files.values() if f["name"] == "06_control" and "prod-root-123" in f.get("parents", [])][0]
        self.assertEqual(["f-ingestion-control"], svc._files["f-source-campaigns"]["parents"])

        # Verify wrappers moved to 05_archive
        archive_id = "f-05-archive"
        self.assertIn(archive_id, svc._files["f-bdl-wrapper"]["parents"])
        self.assertIn(archive_id, svc._files["f-wdi-wrapper"]["parents"])

        # Verify medallion navigation
        for src, rel_id in [("nbp", nbp_id), ("bdl", bdl_id), ("wdi", wdi_id)]:
            rel_root, direct = resolve_source_release_root(storage, "prod-root-123", src, is_writer=False)
            self.assertTrue(direct)
            store = DriveReleaseStore(storage, rel_root)
            manifest = read_current_release_manifest(store, rel_root)
            self.assertEqual(manifest["release_id"], rel_id)
            nav = verify_medallion_navigation(storage, "prod-root-123", src, manifest)
            self.assertEqual(nav["status"], "medallion_navigation_verified")

    def test_idempotent_rerun_on_canonical(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")

        # First apply
        engine.apply(plan=engine.plan(), confirmed=True)
        file_count_after_first = len(svc._files)

        # Second apply (idempotent rerun)
        rerun_result = engine.apply(plan=engine.plan(), confirmed=True)
        self.assertEqual(rerun_result["status"], "canonical_verified")
        self.assertEqual(len(svc._files), file_count_after_first)

    def test_interruption_and_resume_recovery(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")

        # Execute first 3 steps and simulate interruption
        interrupted_res = engine.apply(plan=engine.plan(), confirmed=True, stop_after_step=3)
        self.assertEqual(interrupted_res["status"], "interrupted")
        self.assertEqual(interrupted_res["steps_executed"], 3)

        # Resume with a fresh engine instance
        fresh_engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        resumed_result = fresh_engine.apply(resume=True, confirmed=True)
        self.assertEqual(resumed_result["status"], "migration_completed")
        self.assertEqual(detect_layout_mode(storage, "prod-root-123"), "canonical")

    def test_rollback_restores_exact_legacy_state(self):
        files, nbp_id, bdl_id, wdi_id = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")

        engine.apply(plan=engine.plan(), confirmed=True)
        self.assertEqual(detect_layout_mode(storage, "prod-root-123"), "canonical")

        # Rollback
        rollback_result = engine.rollback(confirmed=True)
        self.assertEqual(rollback_result["status"], "migration_rolled_back")
        self.assertEqual(detect_layout_mode(storage, "prod-root-123"), "legacy")

        # Verify legacy pointers and wrappers restored
        self.assertIn("prod-root-123", svc._files["f-root-pointer"]["parents"])
        self.assertIn("prod-root-123", svc._files["f-bdl-wrapper"]["parents"])
        self.assertIn("prod-root-123", svc._files["f-wdi-wrapper"]["parents"])

    def test_subsequent_publish_and_promotion_under_canonical_layout(self):
        files, nbp_id, bdl_id, wdi_id = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        engine.apply(plan=engine.plan(), confirmed=True)

        rel_root, direct = resolve_source_release_root(storage, "prod-root-123", "nbp", is_writer=True)
        self.assertTrue(direct)
        rel_store = DriveReleaseStore(storage, rel_root)
        current_manifest = read_current_release_manifest(rel_store, rel_root)
        expected_current = current_manifest["release_id"]

        # Stage a new retained release directly under releases/nbp
        new_rel_id = "22222222-3333-4444-5555-666666666666"
        new_manifest = {
            "format_version": 2,
            "release_id": new_rel_id,
            "code_sha": "f" * 40,
            "release_scope": "nbp_platform",
            "created_at_utc": "2026-09-14T01:00:00Z",
            "status": "validated",
            "tests": {"passed": True},
            "datasets": [
                {
                    "dataset_id": "dim_date",
                    "table_name": "dim_date",
                    "layer": "04_gold",
                    "row_count": 105,
                    "files": [{"id": "nbp-dim-date-pq-v2", "size": 1050, "sha256": "g" * 64}],
                }
            ],
            "artifacts": [],
        }
        rel_folder_id = "folder-" + new_rel_id
        svc._files[rel_folder_id] = {
            "id": rel_folder_id, "name": new_rel_id, "mimeType": "application/vnd.google-apps.folder",
            "parents": [rel_root], "trashed": False,
        }
        svc._files["nbp-dim-date-pq-v2"] = {
            "id": "nbp-dim-date-pq-v2", "name": "dim_date.parquet", "mimeType": "application/octet-stream",
            "parents": [rel_folder_id], "content": b"PARQUET_V2", "trashed": False,
        }
        manifest_bytes = json.dumps(new_manifest).encode("utf-8")
        svc._files["manifest-" + new_rel_id] = {
            "id": "manifest-" + new_rel_id, "name": "release.json", "mimeType": "application/json",
            "parents": [rel_folder_id], "content": manifest_bytes, "trashed": False,
        }

        # Promote retained release using promote_retained_release
        promote_res = promote_retained_release(
            rel_store,
            rel_root,
            target_release_id=new_rel_id,
            expected_current_release_id=expected_current,
            pre_promote_validator=lambda _s, _p: None,
            direct_releases=True,
        )
        self.assertEqual(promote_res["target_release_id"], new_rel_id)

        # Sync navigation
        sync_source_medallion_navigation(storage, "prod-root-123", "nbp", new_manifest)
        verify_res = verify_medallion_navigation(storage, "prod-root-123", "nbp", new_manifest)
        self.assertEqual(verify_res["status"], "medallion_navigation_verified")

    def test_navigation_pruning_obsolete_shortcuts(self):
        files, nbp_id, bdl_id, wdi_id = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        engine.apply(plan=engine.plan(), confirmed=True)

        # Initial WDI has multi-file part-0 and part-1
        rel_root, direct = resolve_source_release_root(storage, "prod-root-123", "wdi", is_writer=True)
        rel_store = DriveReleaseStore(storage, rel_root)
        wdi_manifest = read_current_release_manifest(rel_store, rel_root)

        # Update WDI manifest to only have 1 file (single-file transition)
        single_file_wdi_manifest = copy.deepcopy(wdi_manifest)
        single_file_wdi_manifest["release_id"] = "wdi-new-single-part-uuid"
        single_file_wdi_manifest["datasets"][0]["files"] = [
            {"id": "wdi-obs-single", "size": 8192, "sha256": "h" * 64}
        ]
        svc._files["wdi-obs-single"] = {
            "id": "wdi-obs-single", "name": "wdi_observations.parquet", "mimeType": "application/octet-stream",
            "parents": [rel_root], "content": b"WDI_SINGLE", "trashed": False,
        }

        sync_source_medallion_navigation(storage, "prod-root-123", "wdi", single_file_wdi_manifest)
        verify_res = verify_medallion_navigation(storage, "prod-root-123", "wdi", single_file_wdi_manifest)
        self.assertEqual(verify_res["status"], "medallion_navigation_verified")


if __name__ == "__main__":
    unittest.main()
