import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import storage_manager


class _Request:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class _Files:
    def __init__(self, listings=None, metadata=None):
        self.listings = list(listings or [])
        self.metadata = metadata
        self.queries = []
        self.created = []

    def list(self, **kwargs):
        self.queries.append(kwargs["q"])
        return _Request(self.listings.pop(0) if self.listings else {"files": []})

    def get(self, **kwargs):
        return _Request(self.metadata)

    def create(self, **kwargs):
        self.created.append(kwargs)
        return _Request({"id": "created-id"})


class _Drive:
    def __init__(self, files):
        self._files = files

    def files(self):
        return self._files


class StorageBoundaryTests(unittest.TestCase):
    def _manager(self, **kwargs):
        with patch.object(storage_manager.StorageManager, "_authenticate_gdrive", return_value="unused"):
            manager = storage_manager.StorageManager(**kwargs)
        manager.drive_service = _Drive(self.files)
        return manager

    def setUp(self):
        self.files = _Files()

    def test_config_and_environment_select_a_dev_root_and_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "storage.yaml"
            config.write_text(
                "storage:\n"
                "  base_path: gdrive://config-root\n"
                "  zones:\n"
                "    landing: 01_landing\n"
                "    archive: 05_archive\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"ZOHELO_DRIVE_ROOT_NAME": "env-root"}, clear=True):
                manager = self._manager(config_path=config)
            self.assertEqual(manager.master_folder_name, "env-root")
            self.assertEqual(manager.zone_paths["archive"], ("05_archive",))
            self.assertEqual(manager.get_path("archive"), "gdrive://env-root/05_archive")

    def test_unsupported_config_protocol_fails_fast(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "storage.yaml"
            config.write_text(
                "storage:\n"
                "  protocol: s3\n"
                "  base_path: s3://some-bucket\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "only 'gdrive' is implemented"):
                self._manager(config_path=config)

    def test_read_only_resolution_does_not_create_missing_root_or_zone(self):
        manager = self._manager(root_name="dev-root")
        with self.assertRaisesRegex(ValueError, "read-only"):
            manager.resolve_root()
        self.assertFalse(self.files.created)

    def test_ambiguous_exact_folders_fail(self):
        self.files.listings = [{"files": [
            {"id": "one", "name": "dev-root", "mimeType": manager_mime()},
            {"id": "two", "name": "dev-root", "mimeType": manager_mime()},
        ]}]
        manager = self._manager(root_name="dev-root")
        with self.assertRaisesRegex(ValueError, "Ambiguous root"):
            manager.resolve_root()

    def test_root_id_is_verified_and_does_not_fallback_to_name(self):
        self.files.metadata = {"id": "root-id", "name": "isolated-root",
                               "mimeType": manager_mime(), "trashed": False}
        manager = self._manager(root_name="zohelo-data", root_id="root-id")
        self.assertEqual(manager.resolve_root(), "root-id")
        self.assertEqual(len(self.files.queries), 0)
        self.assertEqual(manager.master_folder_name, "isolated-root")

    def test_root_name_is_escaped_in_exact_drive_query(self):
        self.files.listings = [{"files": [{
            "id": "root-id", "name": "O'Reilly",
            "mimeType": manager_mime(), "trashed": False,
        }]}]
        manager = self._manager(root_name="O'Reilly")
        self.assertEqual(manager.resolve_root(), "root-id")
        escaped = manager._escape_drive_query_literal("O'Reilly")
        self.assertIn(f"name='{escaped}'", self.files.queries[0])

    def test_root_id_environment_override_is_authoritative(self):
        self.files.metadata = {"id": "env-id", "name": "env-root",
                               "mimeType": manager_mime(), "trashed": False}
        with patch.dict(os.environ, {"ZOHELO_DRIVE_ROOT_ID": "env-id"}, clear=True):
            manager = self._manager(root_name="config-root")
        self.assertEqual(manager.resolve_root(), "env-id")
        self.assertFalse(self.files.queries)

    def test_production_root_requires_opt_in_but_dev_root_is_allowed(self):
        manager = self._manager(root_name="zohelo-data")
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(PermissionError):
                manager.authorize_writes()
            with self.assertRaises(PermissionError):
                manager.init_infrastructure()
        self.assertFalse(self.files.queries)
        self.assertFalse(self.files.created)
        manager = self._manager(root_name="dev-root")
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(manager.authorize_writes())

    def test_nested_path_rejects_empty_traversal_and_slashes(self):
        manager = self._manager(root_name="dev-root")
        for segments in ([], [""], [".."], ["a/b"], ["a\\b"], [" \t"]):
            with self.subTest(segments=segments):
                with self.assertRaises(ValueError):
                    manager.get_or_create_nested_folder(segments, "root-id")

    def test_nested_creation_rejects_parent_outside_selected_root(self):
        self.files.metadata = {"id": "selected-root", "name": "dev-root",
                               "mimeType": manager_mime(), "trashed": False}
        manager = self._manager(root_name="dev-root", root_id="selected-root")
        with self.assertRaisesRegex(ValueError, "inside the selected root"):
            manager.get_or_create_nested_folder(["dataset"], "other-root")
        self.assertFalse(self.files.created)


def manager_mime():
    return storage_manager.StorageManager.FOLDER_MIME_TYPE


if __name__ == "__main__":
    unittest.main()
