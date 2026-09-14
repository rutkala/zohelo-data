"""Unit and integration tests for Drive layout consolidation and recovery."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from drive_migration import (
    DriveMigrationEngine,
    DriftError,
    MigrationError,
    SafetyPinError,
)
from layout_resolution import (
    detect_layout_mode,
    resolve_nbp_control_root,
    resolve_source_release_root,
    AmbiguousLayoutError,
)
from medallion_navigation import (
    sync_source_medallion_navigation,
    verify_medallion_navigation,
    StaleNavigationError,
)
from release_protocol import (
    publish_release,
    promote_retained_release,
    read_current_release_manifest,
)


class MockDriveService:
    """In-memory mock Google Drive v3 files service."""

    def __init__(self, initial_files: dict[str, dict]):
        self._files = copy.deepcopy(initial_files)
        self._next_id = 1000

    def files(self):
        return self

    def list(self, q="", fields=None, pageSize=100, pageToken=None, supportsAllDrives=True, includeItemsFromAllDrives=True):
        mock_req = MagicMock()

        def execute(num_retries=0):
            matched = []
            for file_id, meta in self._files.items():
                if meta.get("trashed", False):
                    continue
                # Simple query parser for mock
                match = True
                if "in parents" in q:
                    # extract parent id
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
            return item

        mock_req.execute = execute
        return mock_req

    def get_media(self, fileId, supportsAllDrives=True):
        mock_req = MagicMock()

        def execute(num_retries=0):
            if fileId not in self._files:
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
                "shortcutDetails": body.get("shortcutDetails"),
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
            return {"id": fileId, "name": meta["name"], "parents": meta["parents"]}

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
    """Construct a full legacy Drive hierarchy for tests."""
    nbp_release_id = "11111111-2222-3333-4444-555555555555"
    bdl_release_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    wdi_release_id = "99999999-8888-7777-6666-555555555555"

    nbp_manifest = {
        "format_version": 2,
        "release_id": nbp_release_id,
        "code_sha": "a" * 40,
        "release_scope": "nbp_platform",
        "created_at_utc": "2026-09-14T00:00:00Z",
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
        "f-root-pointer": {"id": "f-root-pointer", "name": "current-release.json", "mimeType": "application/json", "parents": [root_id], "content": json.dumps({"format_version": 2, "release_id": nbp_release_id, "manifest_file_id": "f-nbp-manifest"}).encode()},
        # Legacy BDL: bdl-platform/ wrapper
        "f-bdl-wrapper": {"id": "f-bdl-wrapper", "name": "bdl-platform", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-bdl-releases": {"id": "f-bdl-releases", "name": "releases", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-bdl-wrapper"]},
        "f-bdl-rel-uuid": {"id": "f-bdl-rel-uuid", "name": bdl_release_id, "mimeType": "application/vnd.google-apps.folder", "parents": ["f-bdl-releases"]},
        "f-bdl-manifest": {"id": "f-bdl-manifest", "name": "release.json", "mimeType": "application/json", "parents": ["f-bdl-rel-uuid"], "content": json.dumps(bdl_manifest).encode()},
        "bdl-mart-pq": {"id": "bdl-mart-pq", "name": "mart_bdl_coverage.parquet", "mimeType": "application/octet-stream", "parents": ["f-bdl-rel-uuid"], "content": b"PARQUET_BDL"},
        "f-bdl-pointer": {"id": "f-bdl-pointer", "name": "current-release.json", "mimeType": "application/json", "parents": ["f-bdl-wrapper"], "content": json.dumps({"format_version": 2, "release_id": bdl_release_id, "manifest_file_id": "f-bdl-manifest"}).encode()},
        # Legacy WDI: wdi-platform/ wrapper
        "f-wdi-wrapper": {"id": "f-wdi-wrapper", "name": "wdi-platform", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-wdi-releases": {"id": "f-wdi-releases", "name": "releases", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-wdi-wrapper"]},
        "f-wdi-rel-uuid": {"id": "f-wdi-rel-uuid", "name": wdi_release_id, "mimeType": "application/vnd.google-apps.folder", "parents": ["f-wdi-releases"]},
        "f-wdi-manifest": {"id": "f-wdi-manifest", "name": "release.json", "mimeType": "application/json", "parents": ["f-wdi-rel-uuid"], "content": json.dumps(wdi_manifest).encode()},
        "wdi-obs-part-0": {"id": "wdi-obs-part-0", "name": "wdi_observations--part-0.parquet", "mimeType": "application/octet-stream", "parents": ["f-wdi-rel-uuid"], "content": b"PARQUET_WDI_0"},
        "wdi-obs-part-1": {"id": "wdi-obs-part-1", "name": "wdi_observations--part-1.parquet", "mimeType": "application/octet-stream", "parents": ["f-wdi-rel-uuid"], "content": b"PARQUET_WDI_1"},
        "f-wdi-pointer": {"id": "f-wdi-pointer", "name": "current-release.json", "mimeType": "application/json", "parents": ["f-wdi-wrapper"], "content": json.dumps({"format_version": 2, "release_id": wdi_release_id, "manifest_file_id": "f-wdi-manifest"}).encode()},
        # Ingestion control
        "f-ingestion-control": {"id": "f-ingestion-control", "name": "ingestion-control", "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-source-campaigns": {"id": "f-source-campaigns", "name": "source_campaigns", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-ingestion-control"]},
        "f-state-json": {"id": "f-state-json", "name": "state.json", "mimeType": "application/json", "parents": ["f-ingestion-control"], "content": json.dumps({"format_version": 1, "sources": {}}).encode()},
    }
    return files, nbp_release_id, bdl_release_id, wdi_release_id


def make_storage_manager_mock(files_dict, root_id="prod-root-123"):
    svc = MockDriveService(files_dict)
    storage = MagicMock()
    storage.drive_service = svc
    storage.resolve_root.return_value = root_id
    storage.authorize_writes.return_value = True

    def _list_exact(name, parent_id=None):
        res = svc.files().list(q=f"name='{name}' and '{parent_id or root_id}' in parents and trashed=false", mimeType="application/vnd.google-apps.folder").execute()
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
        # Verify no files were created or modified
        self.assertEqual(len(svc._files), initial_file_count)

    def test_drift_detection_aborts_apply(self):
        files, nbp_rel_id, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        plan = engine.plan()

        # Simulate drift: pointer changes release_id
        svc._files["f-root-pointer"]["content"] = json.dumps({
            "format_version": 2,
            "release_id": "drifted-rel-id-999",
            "manifest_file_id": "f-nbp-manifest",
        }).encode()

        with self.assertRaises(DriftError):
            engine.apply(plan)

    def test_conflicting_pointers_fail_closed(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        # Add a conflicting canonical releases/nbp pointer while root pointer exists
        files["f-nbp-canon-dir"] = {"id": "f-nbp-canon-dir", "name": "nbp", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-releases"]}
        files["f-nbp-canon-ptr"] = {"id": "f-nbp-canon-ptr", "name": "current-release.json", "mimeType": "application/json", "parents": ["f-nbp-canon-dir"], "content": b'{"format_version":2,"release_id":"conflicting-id"}'}

        storage, _ = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        with self.assertRaises(AmbiguousLayoutError):
            engine.plan()

    def test_full_apply_migration_and_unchanged_identities(self):
        files, nbp_id, bdl_id, wdi_id = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")

        plan = engine.plan()
        result = engine.apply(plan)

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

        # Verify source campaigns kept exact state identity
        self.assertEqual(svc._files["f-source-campaigns"]["name"], "source_campaigns")
        ctrl_folder = [f for f in svc._files.values() if f["name"] == "06_control" and "prod-root-123" in f.get("parents", [])][0]
        self.assertIn(ctrl_folder["id"], svc._files["f-source-campaigns"]["parents"])

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
        engine.apply()
        file_count_after_first = len(svc._files)

        # Second apply (idempotent rerun)
        rerun_result = engine.apply()
        self.assertEqual(rerun_result["status"], "canonical_verified")
        self.assertEqual(len(svc._files), file_count_after_first)

    def test_interruption_and_resume_recovery(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")

        plan = engine.plan()

        # Simulate interruption after step 3
        interrupted_plan = copy.deepcopy(plan)
        interrupted_plan["steps"][0]["status"] = "completed"
        interrupted_plan["steps"][1]["status"] = "completed"
        interrupted_plan["steps"][2]["status"] = "completed"
        interrupted_plan["status"] = "in_progress"

        # Save partial journal into Drive
        ctrl_id = storage.get_or_create_nested_folder(["06_control"], root_id="prod-root-123")
        engine._save_journal(interrupted_plan, ctrl_id)

        # Resume from journal
        resumed_result = engine.apply(resume=True)
        self.assertEqual(resumed_result["status"], "migration_completed")
        self.assertEqual(detect_layout_mode(storage, "prod-root-123"), "canonical")

    def test_rollback_restores_exact_legacy_state(self):
        files, nbp_id, bdl_id, wdi_id = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")

        # Apply migration
        engine.apply()
        self.assertEqual(detect_layout_mode(storage, "prod-root-123"), "canonical")

        # Rollback migration
        rollback_result = engine.rollback()
        self.assertEqual(rollback_result["status"], "migration_rolled_back")

        # Verify layout mode returned to legacy
        self.assertEqual(detect_layout_mode(storage, "prod-root-123"), "legacy")

        # Verify legacy wrappers are back under root
        self.assertIn("prod-root-123", svc._files["f-bdl-wrapper"]["parents"])
        self.assertIn("prod-root-123", svc._files["f-wdi-wrapper"]["parents"])
        self.assertEqual(svc._files["f-bdl-releases"]["name"], "releases")
        self.assertEqual(svc._files["f-wdi-releases"]["name"], "releases")

    def test_subsequent_publish_and_promotion_under_canonical_layout(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        engine.apply()

        # Publish next release for NBP under canonical layout
        rel_root, direct = resolve_source_release_root(storage, "prod-root-123", "nbp", is_writer=True)
        self.assertTrue(direct)
        rel_store = DriveReleaseStore(storage, rel_root)

        next_uuid = "22222222-3333-4444-5555-666666666666"
        new_manifest = {
            "format_version": 2,
            "release_id": next_uuid,
            "code_sha": "f" * 40,
            "release_scope": "nbp_platform",
            "created_at_utc": "2026-09-14T01:00:00Z",
            "datasets": [
                {
                    "dataset_id": "dim_date",
                    "table_name": "dim_date",
                    "layer": "04_gold",
                    "row_count": 105,
                    "files": [{"id": "nbp-dim-date-next-pq", "size": 1100, "sha256": "g" * 64}],
                }
            ],
            "artifacts": [],
        }

        # Add files for new release
        svc._files["f-nbp-next-uuid"] = {"id": "f-nbp-next-uuid", "name": next_uuid, "mimeType": "application/vnd.google-apps.folder", "parents": [rel_root]}
        svc._files["f-nbp-next-manifest"] = {"id": "f-nbp-next-manifest", "name": "release.json", "mimeType": "application/json", "parents": ["f-nbp-next-uuid"], "content": json.dumps(new_manifest).encode()}
        svc._files["nbp-dim-date-next-pq"] = {"id": "nbp-dim-date-next-pq", "name": "dim_date.parquet", "mimeType": "application/octet-stream", "parents": ["f-nbp-next-uuid"], "content": b"PARQUET_NBP_NEXT"}

        # Sync medallion navigation for next release
        sync_source_medallion_navigation(storage, "prod-root-123", "nbp", new_manifest)

        # Verify navigation points to new release
        nav = verify_medallion_navigation(storage, "prod-root-123", "nbp", new_manifest)
        self.assertEqual(nav["status"], "medallion_navigation_verified")


class WorkflowConcurrencyInvariantTests(unittest.TestCase):

    def test_workflow_concurrency_locks(self):
        import yaml

        gus_path = REPO_ROOT / ".github" / "workflows" / "source-gus-bdl.yml"
        wdi_path = REPO_ROOT / ".github" / "workflows" / "source-world-bank.yml"
        nbp_path = REPO_ROOT / ".github" / "workflows" / "daily-ingestion.yml"
        migrate_path = REPO_ROOT / ".github" / "workflows" / "migrate-drive-layout.yml"

        gus = yaml.safe_load(gus_path.read_text())
        wdi = yaml.safe_load(wdi_path.read_text())
        nbp = yaml.safe_load(nbp_path.read_text())
        migrate = yaml.safe_load(migrate_path.read_text())

        # Migration workflow has global lock zohelo-production-data
        self.assertEqual(migrate["concurrency"]["group"], "zohelo-production-data")
        self.assertFalse(migrate["concurrency"]["cancel-in-progress"])

        # NBP workflow has global lock zohelo-production-data
        self.assertEqual(nbp["concurrency"]["group"], "zohelo-production-data")
        self.assertFalse(nbp["concurrency"]["cancel-in-progress"])

        # BDL workflow keeps its own provider lock at workflow level
        self.assertEqual(gus["concurrency"]["group"], "zohelo-pipeline-gus_bdl")
        # BDL transform-and-release job uses global lock zohelo-production-data
        bdl_job_conc = gus["jobs"]["platform_transform_and_release"]["concurrency"]
        self.assertEqual(bdl_job_conc["group"], "zohelo-production-data")
        self.assertFalse(bdl_job_conc["cancel-in-progress"])

        # WDI workflow keeps its own provider lock at workflow level
        self.assertEqual(wdi["concurrency"]["group"], "zohelo-pipeline-world_bank_wdi")
        # WDI transform-and-release job uses global lock zohelo-production-data
        wdi_job_conc = wdi["jobs"]["platform_transform_and_release"]["concurrency"]
        self.assertEqual(wdi_job_conc["group"], "zohelo-production-data")
        self.assertFalse(wdi_job_conc["cancel-in-progress"])


if __name__ == "__main__":
    unittest.main()
