"""Drive adapter must never replace datasets or delete existing objects."""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from drive_release_store import DriveReleaseStore
from transformation.bronze_builder import _get_zone_id, _list_files_recursively


class DriveReleaseStoreTests(unittest.TestCase):
    def setUp(self):
        self.storage = MagicMock()
        self.files = self.storage.drive_service.files.return_value
        self.store = DriveReleaseStore(self.storage, "platform-root")

    def test_replace_requires_exact_owned_pointer_identity(self):
        correct = {"id": "pointer", "name": "current-release.json", "mimeType": "application/json",
                   "parents": ["platform-root"], "ownedByMe": True, "trashed": False}
        for changed in ({"name": "nbp.parquet"}, {"parents": ["another-root"]},
                        {"mimeType": "application/vnd.google-apps.folder"}, {"ownedByMe": False}):
            with self.subTest(changed=changed):
                self.files.get.return_value.execute.return_value = {**correct, **changed}
                with self.assertRaisesRegex(ValueError, "Only the owned current-release"):
                    self.store.replace("pointer", b"{}")
                self.files.update.assert_not_called()
        self.files.get.return_value.execute.return_value = correct
        self.store.replace("pointer", b"{}")
        self.files.update.assert_called_once()
        self.files.delete.assert_not_called()

    def test_reserved_create_identity_survives_lost_response(self):
        self.files.generateIds.return_value.execute.return_value = {"ids": ["reserved-id"]}
        self.files.create.return_value.execute.side_effect = TimeoutError("response lost")
        self.files.get_media.return_value.execute.return_value = b"contents"
        self.assertEqual(self.store.create("candidate.parquet", b"contents", "release-folder"), "reserved-id")
        self.assertEqual(self.files.create.call_args.kwargs["body"]["id"], "reserved-id")
        self.files.delete.assert_not_called()
        self.files.update.assert_not_called()

    def test_bronze_zone_uses_selected_storage_root(self):
        self.storage.resolve_zone.return_value = "configured-bronze"
        self.assertEqual(_get_zone_id(self.storage, "02_bronze"), "configured-bronze")
        self.storage.resolve_zone.assert_called_once_with("02_bronze", create=True)
        self.storage._get_or_create_folder.assert_not_called()

    def test_nested_landing_file_retains_its_actual_archive_source_parent(self):
        self.files.list.return_value.execute.side_effect = [
            {"files": [{"id": "source-folder", "name": "nbp_gold_prices", "mimeType": "application/vnd.google-apps.folder"}]},
            {"files": [{"id": "raw-file", "name": "batch.json", "mimeType": "application/json"}]},
        ]
        [(item, dataset)] = _list_files_recursively(self.storage.drive_service, "landing-root")
        self.assertEqual(item["source_parent_id"], "source-folder")
        self.assertEqual(dataset, "nbp_gold_prices")


if __name__ == "__main__":
    unittest.main()
