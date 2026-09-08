"""Focused checks for bounded Drive retries and campaign write sessions."""
from __future__ import annotations

import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from drive_release_store import DriveReleaseStore
from storage_manager import StorageManager


class _Request:
    def __init__(self, response):
        self.response = response
        self.retry_counts: list[int] = []

    def execute(self, *, num_retries=0):
        self.retry_counts.append(num_retries)
        return self.response


class _Files:
    def __init__(self):
        self.list_requests: list[_Request] = []
        self.get_requests: list[tuple[str, _Request]] = []
        self.generate_requests: list[_Request] = []
        self.create_requests: list[_Request] = []
        self.created_folder_ids: list[str] = []
        self._next_id = 1
        self.parent_metadata = {
            "id": "responses-root",
            "parents": ["selected-root"],
            "mimeType": StorageManager.FOLDER_MIME_TYPE,
            "trashed": False,
        }

    def list(self, **_kwargs):
        request = _Request({
            "files": [{
                "id": "selected-root",
                "name": "dev-root",
                "mimeType": StorageManager.FOLDER_MIME_TYPE,
                "trashed": False,
            }]
        })
        self.list_requests.append(request)
        return request

    def get(self, *, fileId, **_kwargs):
        request = _Request(dict(self.parent_metadata, id=fileId))
        self.get_requests.append((fileId, request))
        return request

    def generateIds(self, **_kwargs):
        file_id = f"reserved-{self._next_id}"
        self._next_id += 1
        request = _Request({"ids": [file_id]})
        self.generate_requests.append(request)
        return request

    def create(self, **kwargs):
        file_id = kwargs["body"].get("id")
        if file_id is None:
            file_id = f"folder-{len(self.created_folder_ids) + 1}"
            self.created_folder_ids.append(file_id)
        request = _Request({"id": file_id})
        self.create_requests.append(request)
        return request


class _Drive:
    def __init__(self, files):
        self._files = files

    def files(self):
        return self._files


def _manager(files, *, root_name="dev-root"):
    manager = object.__new__(StorageManager)
    manager.backend = "gdrive"
    manager.root_id = None
    manager.root_name = root_name
    manager.master_folder_name = root_name
    manager._configured_root_name = root_name
    manager._resolved_root_metadata = None
    manager.drive_service = _Drive(files)
    return manager


class DriveCampaignEfficiencyTests(unittest.TestCase):
    def test_store_session_reuses_verified_root_and_parent(self):
        files = _Files()
        manager = _manager(files)
        store = DriveReleaseStore(manager, "selected-root")

        self.assertEqual(
            store.create("first.json", b"first", "responses-root"), "reserved-1"
        )
        self.assertEqual(
            store.create("second.json", b"second", "responses-root"), "reserved-2"
        )

        self.assertEqual(len(files.list_requests), 1)
        self.assertEqual(
            [file_id for file_id, _ in files.get_requests], ["responses-root"]
        )
        self.assertEqual(len(files.create_requests), 2)
        self.assertTrue(all(r.retry_counts == [4] for r in files.list_requests))
        self.assertTrue(all(r.retry_counts == [4] for r in files.generate_requests))
        self.assertTrue(all(r.retry_counts == [4] for r in files.create_requests))

    def test_folder_created_below_verified_parent_joins_same_session(self):
        files = _Files()
        manager = _manager(files)
        store = DriveReleaseStore(manager, "selected-root")

        folder_id = store.mkdir("receipts", "responses-root")
        self.assertEqual(folder_id, "folder-1")
        store.create("receipt.json", b"receipt", folder_id)

        self.assertEqual(
            [file_id for file_id, _ in files.get_requests], ["responses-root"]
        )
        self.assertEqual(len(files.list_requests), 2)

    def test_foreign_parent_still_fails_before_object_allocation(self):
        files = _Files()
        files.parent_metadata = {
            "id": "outside-root",
            "parents": [],
            "mimeType": StorageManager.FOLDER_MIME_TYPE,
            "trashed": False,
        }
        store = DriveReleaseStore(_manager(files), "selected-root")

        for _ in range(2):
            with self.assertRaisesRegex(ValueError, "outside the selected root"):
                store.create("unsafe.json", b"unsafe", "outside-root")

        self.assertEqual(len(files.get_requests), 2)
        self.assertFalse(files.generate_requests)
        self.assertFalse(files.create_requests)

    def test_write_session_cannot_cross_storage_manager_instances(self):
        first = _manager(_Files())
        second = _manager(_Files())
        session = first.begin_write_session()

        with self.assertRaisesRegex(ValueError, "does not belong"):
            second.authorize_session_parent(session, "selected-root")

    def test_write_session_rejects_root_selector_or_service_drift(self):
        mutations = {
            "root_id": lambda manager: setattr(manager, "root_id", "another-root"),
            "root_name": lambda manager: setattr(manager, "root_name", "another-name"),
            "master_folder_name": lambda manager: setattr(
                manager, "master_folder_name", "another-name"
            ),
            "configured_root_name": lambda manager: setattr(
                manager, "_configured_root_name", "another-name"
            ),
            "backend": lambda manager: setattr(manager, "backend", "local"),
            "drive_service": lambda manager: setattr(
                manager, "drive_service", _Drive(_Files())
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                files = _Files()
                manager = _manager(files)
                store = DriveReleaseStore(manager, "selected-root")
                store.create("first.json", b"first", "responses-root")

                mutate(manager)
                with self.assertRaisesRegex(ValueError, "selection changed"):
                    store.create("second.json", b"second", "responses-root")

                self.assertEqual(len(files.generate_requests), 1)

    def test_changed_root_id_does_not_reuse_old_root_metadata(self):
        files = _Files()
        files.parent_metadata = {
            "id": "new-root",
            "name": "new-root-name",
            "mimeType": StorageManager.FOLDER_MIME_TYPE,
            "trashed": False,
        }
        manager = _manager(files, root_name="old-root-name")
        manager.root_id = "old-root"
        manager._resolved_root_metadata = {
            "id": "old-root",
            "name": "old-root-name",
            "mimeType": StorageManager.FOLDER_MIME_TYPE,
            "trashed": False,
        }

        manager.root_id = "new-root"

        self.assertEqual(manager.resolve_root(create=False), "new-root")
        self.assertEqual(manager.master_folder_name, "new-root-name")
        self.assertEqual([file_id for file_id, _ in files.get_requests], ["new-root"])

    def test_production_opt_in_is_checked_before_session_or_drive_lookup(self):
        files = _Files()
        manager = _manager(files, root_name="zohelo-data")

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(PermissionError):
                manager.begin_write_session()

        self.assertFalse(files.list_requests)


if __name__ == "__main__":
    unittest.main()
