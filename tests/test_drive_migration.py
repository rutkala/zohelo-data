"""Tests for Google Drive layout migration engine, layout resolution, and medallion navigation."""
from __future__ import annotations

import copy
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import unittest
from unittest import mock
import tempfile
import httplib2
from googleapiclient.errors import HttpError
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


class InjectedDriveFailure(RuntimeError):
    pass


class _Request:
    def __init__(self, execute):
        self.execute = execute


class MockDriveService:
    """In-memory Drive v3 fake with durable effects and mutation fault injection."""

    def __init__(self, initial_files: dict[str, dict]):
        self._files = copy.deepcopy(initial_files)
        self._next_id = 1000
        self.failure_at = None
        self.failure_phase = None
        self.mutation_count = 0
        self.mutation_log = []
        self.page_size_override = None
        self.list_fault = None

    def files(self):
        return self

    def _mutate(self, label, effect):
        self.mutation_count += 1
        index = self.mutation_count
        self.mutation_log.append(label)
        if self.failure_at == index and self.failure_phase == "before":
            raise InjectedDriveFailure(f"before mutation {index}: {label}")
        result = effect()
        if self.failure_at == index and self.failure_phase == "after":
            raise InjectedDriveFailure(f"after mutation {index}: {label}")
        return result

    def clear_failure(self):
        self.failure_at = None
        self.failure_phase = None

    def list(self, q="", fields=None, pageSize=100, pageToken=None, supportsAllDrives=True, includeItemsFromAllDrives=True, **kwargs):
        def execute(num_retries=0):
            if self.list_fault == "non_object":
                return []
            if self.list_fault == "files_not_list":
                return {"files": {}}
            matched = []
            for file_id, meta in self._files.items():
                if meta.get("trashed", False):
                    continue
                match = True
                if "in parents" in q:
                    import re
                    found = re.search(r"'([^']+)' in parents", q)
                    if found and found.group(1) not in meta.get("parents", []):
                        match = False
                if match and "name=" in q:
                    import re
                    found = re.search(r"name='([^']+)'", q)
                    if found:
                        name = found.group(1).replace("\\'", "'").replace("\\\\", "\\")
                        if meta.get("name") != name:
                            match = False
                if match and "mimeType=" in q:
                    import re
                    found = re.search(r"mimeType='([^']+)'", q)
                    if found and meta.get("mimeType") != found.group(1):
                        match = False
                if match:
                    matched.append({k: copy.deepcopy(v) for k, v in meta.items() if k != "content"})
            matched.sort(key=lambda item: item["id"])
            if self.list_fault == "incomplete" and matched:
                matched[0].pop("mimeType", None)
            effective = self.page_size_override or pageSize
            try:
                offset = int(pageToken or "0")
            except ValueError:
                offset = 0
            page = matched[offset:offset + effective]
            next_offset = offset + effective
            next_token = str(next_offset) if next_offset < len(matched) else None
            if self.list_fault == "repeated_token" and matched:
                next_token = "repeat"
            response = {"files": page, "nextPageToken": next_token}
            if self.list_fault == "incomplete":
                response["incompleteSearch"] = True
            return response

        return _Request(execute)

    def get(self, fileId, fields=None, supportsAllDrives=True):
        def execute(num_retries=0):
            if fileId not in self._files:
                raise HttpError(httplib2.Response({"status": "404"}), b"not found")
            item = {k: copy.deepcopy(v) for k, v in self._files[fileId].items() if k != "content"}
            item.setdefault("ownedByMe", True)
            item.setdefault("trashed", False)
            return item

        return _Request(execute)

    def get_media(self, fileId, supportsAllDrives=True):
        def execute(num_retries=0):
            if fileId not in self._files:
                raise HttpError(httplib2.Response({"status": "404"}), b"not found")
            content = self._files[fileId].get("content")
            return b"{}" if content is None else content

        return _Request(execute)

    def create(self, body=None, media_body=None, fields=None, supportsAllDrives=True):
        def execute(num_retries=0):
            actual_body = body or {}
            file_id = actual_body.get("id")
            if not file_id:
                self._next_id += 1
                file_id = f"file-{self._next_id}"
            label = f"create:{actual_body.get('mimeType')}:{actual_body.get('name')}:{file_id}"

            def effect():
                if file_id in self._files:
                    raise HttpError(httplib2.Response({"status": "409"}), b"already exists")
                content = b""
                if media_body and hasattr(media_body, "_fd"):
                    media_body._fd.seek(0)
                    content = media_body._fd.read()
                self._files[file_id] = {
                    "id": file_id,
                    "name": actual_body.get("name", "untitled"),
                    "mimeType": actual_body.get("mimeType", "application/octet-stream"),
                    "parents": list(actual_body.get("parents", [])),
                    "trashed": False,
                    "content": content,
                    "shortcutDetails": copy.deepcopy(actual_body.get("shortcutDetails")),
                }
                return {k: copy.deepcopy(v) for k, v in self._files[file_id].items() if k != "content"}

            return self._mutate(label, effect)

        return _Request(execute)

    def update(self, fileId, body=None, media_body=None, addParents=None, removeParents=None, fields=None, supportsAllDrives=True):
        def execute(num_retries=0):
            if fileId not in self._files:
                raise HttpError(httplib2.Response({"status": "404"}), b"not found")
            kind = "move" if addParents or removeParents or (body and "name" in body) else "content"
            label = f"update:{kind}:{fileId}"

            def effect():
                meta = self._files[fileId]
                if body and "shortcutDetails" in body:
                    raise RuntimeError("Drive v3 does not allow shortcut target updates")
                if body and "name" in body:
                    meta["name"] = body["name"]
                if body and "trashed" in body:
                    meta["trashed"] = body["trashed"]
                if media_body and hasattr(media_body, "_fd"):
                    media_body._fd.seek(0)
                    meta["content"] = media_body._fd.read()
                parents = list(meta.get("parents", []))
                if removeParents:
                    remove = {value.strip() for value in removeParents.split(",")}
                    parents = [value for value in parents if value not in remove]
                if addParents:
                    for value in (part.strip() for part in addParents.split(",")):
                        if value and value not in parents:
                            parents.append(value)
                meta["parents"] = parents
                return {k: copy.deepcopy(v) for k, v in meta.items() if k != "content"}

            return self._mutate(label, effect)

        return _Request(execute)

    def delete(self, fileId, supportsAllDrives=True):
        def execute(num_retries=0):
            label = f"delete:{fileId}"

            def effect():
                if fileId not in self._files:
                    raise HttpError(httplib2.Response({"status": "404"}), b"not found")
                self._files[fileId]["trashed"] = True
                return {}

            return self._mutate(label, effect)

        return _Request(execute)

    def generateIds(self, count=1, space="drive", type="files"):
        def execute(num_retries=0):
            ids = []
            for _ in range(count):
                self._next_id += 1
                ids.append(f"gen-{type}-{self._next_id}")
            return {"ids": ids}

        return _Request(execute)


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

    # Production already has the shared control root.  NBP's legacy control
    # remains separate until the migration moves it under this root.
    files = {
        "f-06-control": {"id": "f-06-control", "name": "06_control",
                         "mimeType": "application/vnd.google-apps.folder", "parents": [root_id]},
        "f-source-campaigns": {"id": "f-source-campaigns", "name": "source_campaigns",
                               "mimeType": "application/vnd.google-apps.folder", "parents": ["f-06-control"]},
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
        "f-source-campaigns": {"id": "f-source-campaigns", "name": "source_campaigns", "mimeType": "application/vnd.google-apps.folder", "parents": ["f-06-control"]},
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
        self.assertEqual(["f-06-control"], svc._files["f-source-campaigns"]["parents"])

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



    def test_corrupt_or_duplicate_journal_fails_closed(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        interrupted = engine.apply(engine.plan(), confirmed=True, stop_after_step=0)
        journal_id = interrupted["journal_file_id"]
        # A malformed durable receipt is a recovery stop condition, never a cue to replan.
        svc._files[journal_id]["content"] = b"{not-json"
        with self.assertRaises(MigrationError):
            DriveMigrationEngine(storage, expected_root_id="prod-root-123").apply(
                resume=True, confirmed=True
            )

        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        interrupted = engine.apply(engine.plan(), confirmed=True, stop_after_step=0)
        journal_id = interrupted["journal_file_id"]
        original = svc._files[journal_id]
        svc._files["duplicate-journal"] = {
            **original, "id": "duplicate-journal", "parents": list(original["parents"]),
        }
        with self.assertRaises(AmbiguousLayoutError):
            DriveMigrationEngine(storage, expected_root_id="prod-root-123").apply(
                resume=True, confirmed=True
            )

    def test_apply_refuses_foreign_parent_on_pinned_move(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        plan = engine.plan()
        move = next(step for step in plan["steps"] if step["action"] in ("move", "move_and_rename"))
        svc._files[move["item_id"]]["parents"].append("foreign-parent")
        with self.assertRaises(DriftError):
            engine.apply(plan, confirmed=True)


    def test_resume_preserves_reviewed_plan_identity_without_nesting(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, _ = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        interrupted = engine.apply(engine.plan(), confirmed=True, stop_after_step=0)
        immutable_plan = copy.deepcopy(interrupted["journal"]["plan"])
        immutable_digest = interrupted["journal"]["plan_sha256"]
        resumed = DriveMigrationEngine(storage, expected_root_id="prod-root-123").apply(
            resume=True, confirmed=True
        )
        self.assertEqual(resumed["journal"]["plan"], immutable_plan)
        self.assertEqual(resumed["journal"]["plan_sha256"], immutable_digest)
        self.assertNotIn("plan", resumed["journal"]["plan"])


    def test_local_ahead_journal_recovers_when_remote_receipt_lags(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, _ = make_storage_manager_mock(files, "prod-root-123")
        with tempfile.TemporaryDirectory() as tmp:
            receipt = Path(tmp) / "journal.json"
            engine = DriveMigrationEngine(
                storage, expected_root_id="prod-root-123", journal_local_path=receipt
            )
            engine.apply(engine.plan(), confirmed=True, stop_after_step=0)
            local = json.loads(receipt.read_text())
            local["steps"][0]["status"] = "started"
            local["steps"][0]["started_at_utc"] = "2026-09-14T00:00:00+00:00"
            receipt.write_text(json.dumps(local))
            recovered = DriveMigrationEngine(
                storage, expected_root_id="prod-root-123", journal_local_path=receipt
            )._load_journal()
            self.assertEqual(recovered["steps"][0]["status"], "started")


    def test_folder_create_then_journal_save_failure_recovers_fresh_engine(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        with tempfile.TemporaryDirectory() as tmp:
            receipt = Path(tmp) / "journal.json"
            engine = DriveMigrationEngine(
                storage, expected_root_id="prod-root-123", journal_local_path=receipt
            )
            real_save = engine._save_journal
            failed = {"value": False}

            def fail_completion_receipt(journal, control_id):
                if (not failed["value"]
                        and any(step.get("status") == "completed" for step in journal["steps"])):
                    failed["value"] = True
                    raise RuntimeError("simulated journal upload failure")
                return real_save(journal, control_id)

            with mock.patch.object(engine, "_save_journal", side_effect=fail_completion_receipt):
                with self.assertRaisesRegex(RuntimeError, "journal upload failure"):
                    engine.apply(engine.plan(), confirmed=True)
            self.assertTrue(failed["value"])
            # Simulate a fresh worker that has only the durable Drive journal.
            receipt.unlink()
            resumed = DriveMigrationEngine(
                storage, expected_root_id="prod-root-123"
            ).apply(resume=True, confirmed=True)
            self.assertEqual(resumed["status"], "migration_completed")
            self.assertEqual(
                len([f for f in svc._files.values() if f["name"] == "releases" and not f.get("trashed")]),
                1,
            )

    def test_cli_failure_receipt_embeds_latest_full_journal(self):
        import migrate_drive_layout as migration_cli

        journal = {
            "schema_version": 2,
            "status": "interrupted",
            "plan_id": "plan-fixed",
            "steps": [{"step_id": "move", "status": "started"}],
        }
        with tempfile.TemporaryDirectory() as tmp:
            original_cwd = Path.cwd()
            try:
                tmp_path = Path(tmp)
                journal_path = tmp_path / "durable-journal.json"
                journal_path.write_text(json.dumps(journal), encoding="utf-8")
                os.chdir(tmp_path)
                args = mock.Mock(journal_path=journal_path, operation="resume")
                receipt = migration_cli.write_failure_receipt(
                    args, MigrationError("simulated failure"), "migration_failed"
                )
                self.assertEqual(journal, receipt["journal"])
                self.assertEqual(
                    receipt,
                    json.loads((tmp_path / "migration-error.json").read_text()),
                )
                self.assertEqual(
                    receipt,
                    json.loads((tmp_path / "operation-result.json").read_text()),
                )
                self.assertEqual(journal, json.loads(journal_path.read_text()))
            finally:
                os.chdir(original_cwd)

    def _assert_original_objects_unchanged(self, original, svc):
        for file_id, expected in original.items():
            self.assertIn(file_id, svc._files)
            self.assertEqual(svc._files[file_id], expected, file_id)
        active_extras = [
            item["id"] for file_id, item in svc._files.items()
            if file_id not in original
            and not item.get("trashed")
            and item.get("name") != "migration-journal.json"
        ]
        self.assertEqual([], active_extras)
        journals = [
            item for item in svc._files.values()
            if item.get("name") == "migration-journal.json"
            and not item.get("trashed")
        ]
        self.assertEqual(1, len(journals))
        self.assertEqual(["f-06-control"], journals[0]["parents"])

    def _all_fault_points(self, labels):
        return range(1, len(labels) + 1)

    def test_plan_contains_only_individually_pinned_navigation_actions(self):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, _ = make_storage_manager_mock(files, "prod-root-123")
        plan = DriveMigrationEngine(storage, "prod-root-123").plan()
        self.assertNotIn("sync_navigation", {step["action"] for step in plan["steps"]})
        nav = [step for step in plan["steps"] if step["step_id"].startswith("nav_")]
        self.assertEqual(26, len(nav))
        self.assertEqual(9, sum(step.get("object_kind") == "json" for step in nav))
        wdi_parts = [
            step for step in nav
            if step.get("object_kind") == "shortcut"
            and step.get("target_id", "").startswith("wdi-obs-part-")
        ]
        self.assertEqual({"wdi-obs-part-0", "wdi-obs-part-1"}, {
            step["target_id"] for step in wdi_parts
        })
        digest_input = copy.deepcopy(plan)
        expected = digest_input.pop("plan_sha256")
        self.assertEqual(
            expected,
            sha256(json.dumps(digest_input, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        )

    def test_strict_preflight_rejects_location_hash_mime_and_navigation_collisions(self):
        cases = []
        files, *_ = build_legacy_drive_state("prod-root-123")
        files["f-source-campaigns"]["parents"] = ["f-ingestion-control"]
        cases.append(files)
        files, *_ = build_legacy_drive_state("prod-root-123")
        files["f-nbp-snapshot"]["content"] = b"changed"
        cases.append(files)
        files, *_ = build_legacy_drive_state("prod-root-123")
        files["f-06-control"]["mimeType"] = "application/json"
        cases.append(files)
        files, *_ = build_legacy_drive_state("prod-root-123")
        files["f-nbp-rel-uuid"]["parents"] = ["wrong-release-root"]
        cases.append(files)
        files, *_ = build_legacy_drive_state("prod-root-123")
        files["invalid-release-child"] = {
            "id": "invalid-release-child", "name": "notes.txt",
            "mimeType": "text/plain", "parents": ["f-bdl-releases"],
            "content": b"unexpected",
        }
        cases.append(files)
        files, *_ = build_legacy_drive_state("prod-root-123")
        files["foreign-current"] = {
            "id": "foreign-current", "name": "current",
            "mimeType": "application/vnd.google-apps.folder",
            "parents": ["f-04-gold"],
        }
        files["foreign-nbp"] = {
            "id": "foreign-nbp", "name": "nbp",
            "mimeType": "application/vnd.google-apps.folder",
            "parents": ["foreign-current"],
        }
        files["foreign-child"] = {
            "id": "foreign-child", "name": "keep.txt",
            "mimeType": "text/plain", "parents": ["foreign-nbp"],
            "content": b"keep",
        }
        cases.append(files)
        for index, state in enumerate(cases):
            with self.subTest(case=index):
                storage, _ = make_storage_manager_mock(state, "prod-root-123")
                with self.assertRaises((MigrationError, LayoutResolutionError, AmbiguousLayoutError)):
                    DriveMigrationEngine(storage, "prod-root-123").plan()

    def test_all_retained_nbp_uuid_folders_move_and_restore_by_id(self):
        files, *_ = build_legacy_drive_state("prod-root-123")
        retained_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        files[retained_id] = {
            "id": retained_id,
            "name": retained_id,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": ["f-releases"],
            "trashed": False,
        }
        files["retained-payload"] = {
            "id": "retained-payload",
            "name": "retained.parquet",
            "mimeType": "application/octet-stream",
            "parents": [retained_id],
            "content": b"RETAINED_BYTES",
            "trashed": False,
        }
        original = copy.deepcopy(files)
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, "prod-root-123")
        plan = engine.plan()
        self.assertIn(
            retained_id,
            {step["item_id"] for step in plan["steps"] if step["action"] == "move"},
        )
        result = engine.apply(plan, confirmed=True)
        nbp_folder = next(
            step["item_id"] for step in result["journal"]["steps"]
            if step["step_id"] == "create_releases_nbp"
        )
        self.assertEqual([nbp_folder], svc._files[retained_id]["parents"])
        self.assertEqual(b"RETAINED_BYTES", svc._files["retained-payload"]["content"])
        DriveMigrationEngine(storage, "prod-root-123").rollback(confirmed=True)
        self._assert_original_objects_unchanged(original, svc)

    def test_rollback_rejects_new_release_child_before_any_reverse_mutation(self):
        files, *_ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, "prod-root-123")
        engine.apply(engine.plan(), confirmed=True)
        svc._files["new-staged-release"] = {
            "id": "new-staged-release",
            "name": "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
            "mimeType": "application/vnd.google-apps.folder",
            "parents": ["f-bdl-releases"],
            "trashed": False,
        }
        before_count = svc.mutation_count
        with self.assertRaises(DriftError):
            DriveMigrationEngine(storage, "prod-root-123").rollback(confirmed=True)
        self.assertEqual(before_count, svc.mutation_count)
        self.assertFalse(svc._files["new-staged-release"].get("trashed", False))
        self.assertEqual(["f-bdl-releases"], svc._files["new-staged-release"]["parents"])

    def test_bounded_pagination_accepts_multiple_pages_and_rejects_bad_pages(self):
        files, *_ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        svc.page_size_override = 1
        self.assertEqual(
            "planned", DriveMigrationEngine(storage, "prod-root-123").plan()["status"]
        )
        self.assertEqual("legacy", detect_layout_mode(storage, "prod-root-123"))
        for fault in ("repeated_token", "non_object", "files_not_list", "incomplete"):
            files, *_ = build_legacy_drive_state("prod-root-123")
            storage, svc = make_storage_manager_mock(files, "prod-root-123")
            svc.page_size_override = 1
            svc.list_fault = fault
            with self.subTest(path="engine", fault=fault), self.assertRaises(MigrationError):
                DriveMigrationEngine(storage, "prod-root-123").plan()
            with self.subTest(path="layout_helper", fault=fault), self.assertRaises(
                LayoutResolutionError
            ):
                detect_layout_mode(storage, "prod-root-123")

    def test_missing_and_wrong_root_journals_fail_closed(self):
        files, *_ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        result = DriveMigrationEngine(storage, "prod-root-123").apply(
            DriveMigrationEngine(storage, "prod-root-123").plan(),
            confirmed=True, stop_after_step=0,
        )
        journal_id = result["journal_file_id"]
        svc._files[journal_id]["trashed"] = True
        with self.assertRaises(MigrationError):
            DriveMigrationEngine(storage, "prod-root-123").apply(resume=True, confirmed=True)

        files, *_ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        result = DriveMigrationEngine(storage, "prod-root-123").apply(
            DriveMigrationEngine(storage, "prod-root-123").plan(),
            confirmed=True, stop_after_step=0,
        )
        journal_id = result["journal_file_id"]
        document = json.loads(svc._files[journal_id]["content"])
        document["expected_root_id"] = "foreign-root"
        svc._files[journal_id]["content"] = json.dumps(document).encode()
        with self.assertRaises(SafetyPinError):
            DriveMigrationEngine(storage, "prod-root-123").apply(resume=True, confirmed=True)

    def test_rollback_preflight_rejects_foreign_navigation_content_before_reverse(self):
        files, *_ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        original = copy.deepcopy(files)
        result = DriveMigrationEngine(storage, "prod-root-123").apply(
            DriveMigrationEngine(storage, "prod-root-123").plan(), confirmed=True
        )
        owned_folder = next(
            step for step in result["journal"]["steps"]
            if step["action"] == "ensure_object"
            and step["object_kind"] == "folder"
            and not step["preexisting"]
        )
        svc._files["foreign-during-migration"] = {
            "id": "foreign-during-migration", "name": "do-not-delete",
            "mimeType": "text/plain", "parents": [owned_folder["item_id"]],
            "content": b"foreign",
        }
        before = {
            file_id: (item["name"], list(item["parents"]), item.get("trashed", False))
            for file_id, item in svc._files.items()
        }
        with self.assertRaises(DriftError):
            DriveMigrationEngine(storage, "prod-root-123").rollback(confirmed=True)
        after = {
            file_id: (item["name"], list(item["parents"]), item.get("trashed", False))
            for file_id, item in svc._files.items()
        }
        self.assertEqual(before, after)
        self.assertEqual(b"foreign", svc._files["foreign-during-migration"]["content"])

    def test_apply_action_fault_matrix_recovers_fresh_and_rolls_back(self):
        baseline_files, *_ = build_legacy_drive_state("prod-root-123")
        baseline_storage, baseline_svc = make_storage_manager_mock(
            baseline_files, "prod-root-123"
        )
        baseline_engine = DriveMigrationEngine(baseline_storage, "prod-root-123")
        baseline_engine.apply(baseline_engine.plan(), confirmed=True)
        labels = baseline_svc.mutation_log
        self.assertEqual(110, len(labels))
        self.assertEqual(73, sum(label.startswith("update:content:") for label in labels))
        self.assertEqual(1, sum(
            label.startswith("create:")
            and ":migration-journal.json:" in label
            for label in labels
        ))
        points = self._all_fault_points(labels)
        for point in points:
            for phase in ("before", "after"):
                with self.subTest(point=point, phase=phase):
                    files, *_ = build_legacy_drive_state("prod-root-123")
                    original = copy.deepcopy(files)
                    storage, svc = make_storage_manager_mock(files, "prod-root-123")
                    engine = DriveMigrationEngine(storage, "prod-root-123")
                    plan = engine.plan()
                    reviewed = copy.deepcopy(plan)
                    svc.failure_at = point
                    svc.failure_phase = phase
                    with self.assertRaises(InjectedDriveFailure):
                        engine.apply(plan, confirmed=True)
                    svc.clear_failure()
                    fresh = DriveMigrationEngine(storage, "prod-root-123")
                    journals = [
                        item for item in svc._files.values()
                        if item["name"] == "migration-journal.json"
                        and not item.get("trashed")
                    ]
                    if journals:
                        resumed = fresh.apply(resume=True, confirmed=True)
                    else:
                        resumed = fresh.apply(copy.deepcopy(reviewed), confirmed=True)
                    self.assertEqual(reviewed, resumed["journal"]["plan"])
                    self.assertEqual(reviewed["plan_sha256"], resumed["journal"]["plan_sha256"])
                    DriveMigrationEngine(storage, "prod-root-123").rollback(confirmed=True)
                    self._assert_original_objects_unchanged(original, svc)

    def test_rollback_action_fault_matrix_recovers_fresh(self):
        baseline_files, *_ = build_legacy_drive_state("prod-root-123")
        baseline_storage, baseline_svc = make_storage_manager_mock(
            baseline_files, "prod-root-123"
        )
        baseline_engine = DriveMigrationEngine(baseline_storage, "prod-root-123")
        baseline_engine.apply(baseline_engine.plan(), confirmed=True)
        start = len(baseline_svc.mutation_log)
        baseline_engine.rollback(confirmed=True)
        rollback_labels = baseline_svc.mutation_log[start:]
        self.assertEqual(110, len(rollback_labels))
        self.assertEqual(
            74,
            sum(label.startswith("update:content:") for label in rollback_labels),
        )
        points = self._all_fault_points(rollback_labels)
        for point in points:
            for phase in ("before", "after"):
                with self.subTest(point=point, phase=phase):
                    files, *_ = build_legacy_drive_state("prod-root-123")
                    original = copy.deepcopy(files)
                    storage, svc = make_storage_manager_mock(files, "prod-root-123")
                    engine = DriveMigrationEngine(storage, "prod-root-123")
                    applied = engine.apply(engine.plan(), confirmed=True)
                    reviewed = copy.deepcopy(applied["journal"]["plan"])
                    svc.mutation_count = 0
                    svc.mutation_log = []
                    svc.failure_at = point
                    svc.failure_phase = phase
                    with self.assertRaises(InjectedDriveFailure):
                        engine.rollback(confirmed=True)
                    svc.clear_failure()
                    rolled_back = DriveMigrationEngine(
                        storage, "prod-root-123"
                    ).rollback(confirmed=True)
                    self.assertEqual("migration_rolled_back", rolled_back["status"])
                    self.assertEqual(reviewed, rolled_back["journal"]["plan"])
                    self._assert_original_objects_unchanged(original, svc)

if __name__ == "__main__":
    unittest.main()
