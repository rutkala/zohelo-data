"""Tests for atomic Drive safe upload helper adhering to ADR 0009."""
import hashlib
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from drive_safe_upload import hash_file, safe_drive_upload


class TestDriveSafeUpload(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp_dir.name)
        self.sample_file = self.workspace / "test_data.parquet"
        self.sample_content = b"sample binary parquet data"
        self.sample_file.write_bytes(self.sample_content)
        self.sha256, self.md5 = hash_file(self.sample_file)
        self.size = len(self.sample_content)

        self.storage = MagicMock()
        self.files_mock = self.storage.drive_service.files.return_value

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_reused_matching_file(self):
        self.files_mock.list.return_value.execute.return_value = {
            "files": [
                {
                    "id": "existing-123",
                    "name": "test_data.parquet",
                    "size": str(self.size),
                    "md5Checksum": self.md5,
                    "appProperties": {"sha256": self.sha256},
                }
            ]
        }

        res = safe_drive_upload(
            self.storage, self.sample_file, "test_data.parquet", "parent-dir"
        )
        self.assertTrue(res["reused"])
        self.assertEqual(res["id"], "existing-123")
        self.files_mock.create.assert_not_called()
        self.files_mock.delete.assert_not_called()

    def test_upload_new_file(self):
        self.files_mock.list.return_value.execute.return_value = {"files": []}
        self.files_mock.create.return_value.execute.return_value = {
            "id": "new-file-456",
            "name": "test_data.parquet",
            "size": str(self.size),
            "md5Checksum": self.md5,
            "appProperties": {"sha256": self.sha256},
        }

        res = safe_drive_upload(
            self.storage, self.sample_file, "test_data.parquet", "parent-dir"
        )
        self.assertFalse(res["reused"])
        self.assertEqual(res["id"], "new-file-456")
        self.files_mock.create.assert_called_once()
        self.files_mock.delete.assert_not_called()

    def test_upload_with_superseded_file_upload_first_then_delete(self):
        self.files_mock.list.return_value.execute.return_value = {
            "files": [
                {
                    "id": "old-file-789",
                    "name": "test_data.parquet",
                    "size": "999",
                    "md5Checksum": "oldmd5",
                    "appProperties": {"sha256": "oldsha"},
                }
            ]
        }
        self.files_mock.create.return_value.execute.return_value = {
            "id": "new-file-456",
            "name": "test_data.parquet",
            "size": str(self.size),
            "md5Checksum": self.md5,
            "appProperties": {"sha256": self.sha256},
        }

        call_order = []
        self.files_mock.create.side_effect = lambda **k: MagicMock(execute=lambda **kw: call_order.append("create") or {
            "id": "new-file-456", "name": "test_data.parquet", "size": str(self.size),
            "md5Checksum": self.md5, "appProperties": {"sha256": self.sha256},
        })
        self.files_mock.delete.side_effect = lambda **k: MagicMock(execute=lambda **kw: call_order.append(f"delete_{k.get('fileId')}"))

        res = safe_drive_upload(
            self.storage, self.sample_file, "test_data.parquet", "parent-dir"
        )
        self.assertFalse(res["reused"])
        self.assertEqual(res["id"], "new-file-456")
        self.assertEqual(call_order, ["create", "delete_old-file-789"])

    def test_upload_failure_preserves_superseded_file(self):
        self.files_mock.list.return_value.execute.return_value = {
            "files": [
                {
                    "id": "old-file-789",
                    "name": "test_data.parquet",
                    "size": "999",
                    "md5Checksum": "oldmd5",
                    "appProperties": {"sha256": "oldsha"},
                }
            ]
        }
        self.files_mock.create.return_value.execute.side_effect = ConnectionResetError("network dropped")

        with self.assertRaises(ConnectionResetError):
            safe_drive_upload(
                self.storage, self.sample_file, "test_data.parquet", "parent-dir"
            )

        # Confirm old file was NEVER deleted
        self.files_mock.delete.assert_not_called()

    def test_verification_failure_deletes_candidate_preserves_superseded(self):
        self.files_mock.list.return_value.execute.return_value = {
            "files": [
                {
                    "id": "old-file-789",
                    "name": "test_data.parquet",
                    "size": "999",
                    "md5Checksum": "oldmd5",
                    "appProperties": {"sha256": "oldsha"},
                }
            ]
        }
        # Returned candidate has incorrect md5
        self.files_mock.create.return_value.execute.return_value = {
            "id": "corrupted-candidate-999",
            "name": "test_data.parquet",
            "size": str(self.size),
            "md5Checksum": "corrupted_md5",
            "appProperties": {"sha256": self.sha256},
        }

        deleted_ids = []
        self.files_mock.delete.side_effect = lambda **k: MagicMock(execute=lambda **kw: deleted_ids.append(k.get("fileId")))

        with self.assertRaises(RuntimeError) as ctx:
            safe_drive_upload(
                self.storage, self.sample_file, "test_data.parquet", "parent-dir"
            )

        self.assertIn("verification failed", str(ctx.exception))
        # Corrupted candidate was deleted, but superseded file was PRESERVED
        self.assertEqual(deleted_ids, ["corrupted-candidate-999"])

    def test_drive_lock_used(self):
        lock = threading.Lock()
        self.files_mock.list.return_value.execute.return_value = {"files": []}
        self.files_mock.create.return_value.execute.return_value = {
            "id": "new-file-456",
            "name": "test_data.parquet",
            "size": str(self.size),
            "md5Checksum": self.md5,
            "appProperties": {"sha256": self.sha256},
        }

        res = safe_drive_upload(
            self.storage, self.sample_file, "test_data.parquet", "parent-dir", drive_lock=lock
        )
        self.assertFalse(res["reused"])


if __name__ == "__main__":
    unittest.main()
