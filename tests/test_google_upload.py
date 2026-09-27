"""Exercise the probe's side-effect boundary against an in-memory Drive fake."""

import copy
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import httplib2
from googleapiclient.errors import HttpError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import check_google_upload as diagnostic


def http_error(status):
    return HttpError(httplib2.Response({"status": str(status)}), b'{"error":{"message":"private-provider-detail"}}')


class Request:
    def __init__(self, operation):
        self.operation = operation

    def execute(self, **kwargs):
        return self.operation()


class DriveFiles:
    """Existing data is represented by a sentinel ID that must never be touched."""

    allocated_id = "allocated-probe-id"

    def __init__(self, mode="success", roots=None):
        self.mode = mode
        self.roots = roots if roots is not None else {
            "files": [{"id": "platform-root-id", "capabilities": {"canAddChildren": True}}]
        }
        self.file = None
        self.payload = None
        self.created = 0
        self.deleted_ids = []
        self.read_ids = []
        self.updated = 0

    def list(self, **kwargs):
        return Request(lambda: copy.deepcopy(self.roots))

    def generateIds(self, **kwargs):
        return Request(lambda: {"ids": [self.allocated_id]})

    def create(self, *, body, media_body, **kwargs):
        def operation():
            self.created += 1
            if self.mode == "create_denied":
                raise http_error(403)
            self.file = copy.deepcopy(body)
            self.file["ownedByMe"] = True
            self.payload = media_body.getbytes(0, media_body.size())
            self.file["size"] = str(len(self.payload))
            if self.mode == "lost_create_response":
                raise TimeoutError("private-network-detail")
            if self.mode == "wrong_returned_id":
                return {"id": "existing-dataset-id"}
            return {"id": self.allocated_id}
        return Request(operation)

    def get(self, *, fileId, **kwargs):
        def operation():
            self.read_ids.append(fileId)
            if fileId != self.allocated_id:
                raise AssertionError("Probe inspected an unrelated file")
            if self.file is None:
                raise http_error(404)
            result = copy.deepcopy(self.file)
            if self.mode == "foreign_marker":
                result["appProperties"] = {"unrelated": "existing-dataset"}
            if self.mode == "folder_instead_of_file":
                result["mimeType"] = "application/vnd.google-apps.folder"
            if self.mode == "not_owned":
                result["ownedByMe"] = False
            return result
        return Request(operation)

    def get_media(self, *, fileId, **kwargs):
        def operation():
            if fileId != self.allocated_id:
                raise AssertionError("Probe downloaded an unrelated file")
            if self.mode == "corrupt_readback":
                return b"different data"
            return self.payload
        return Request(operation)

    def delete(self, *, fileId, **kwargs):
        def operation():
            if fileId != self.allocated_id:
                raise AssertionError("Probe deleted an unrelated file")
            self.deleted_ids.append(fileId)
            if self.mode == "delete_denied":
                raise http_error(403)
            if self.mode != "delete_not_effective":
                self.file = None
            return {}
        return Request(operation)


def run_probe(files, source="oauth_environment"):
    manager = MagicMock()
    manager.auth_source = source
    manager.master_folder_name = "zohelo-data"
    manager.drive_service.files.return_value = files
    factory = MagicMock(return_value=manager)
    report = diagnostic.check_upload(allow_write_test=True, storage_factory=factory)
    factory.assert_called_once_with(backend="gdrive", allow_interactive_auth=False)
    manager.init_infrastructure.assert_not_called()
    manager._get_or_create_folder.assert_not_called()
    return report


class GoogleUploadDiagnosticTests(unittest.TestCase):
    def test_default_never_authenticates_or_writes(self):
        factory = MagicMock()
        report = diagnostic.check_upload(storage_factory=factory)
        factory.assert_not_called()
        self.assertEqual(report["status"], "write_test_not_enabled")

    def test_success_verifies_upload_bytes_and_removal(self):
        files = DriveFiles()
        report = run_probe(files)
        self.assertEqual(report["status"], "upload_readback_cleanup_verified")
        for flag in ("upload_attempted", "upload_verified", "readback_verified", "cleanup_verified"):
            self.assertTrue(report[flag], flag)
        self.assertFalse(report["cleanup_required"])
        self.assertEqual(files.created, 1)
        self.assertLess(len(files.payload), 1024)
        self.assertEqual(files.deleted_ids, [files.allocated_id])
        self.assertIsNone(files.file)

    def test_missing_ambiguous_or_unwritable_root_never_creates_anything(self):
        for roots in (
            {"files": []},
            {"files": [{"id": "one"}, {"id": "two"}]},
            {"files": [{"id": "one"}], "nextPageToken": "more"},
            {"files": [{"id": "one", "capabilities": {"canAddChildren": False}}]},
        ):
            with self.subTest(roots=roots):
                files = DriveFiles(roots=roots)
                report = run_probe(files)
                self.assertNotEqual(report["status"], "upload_readback_cleanup_verified")
                self.assertEqual(files.created, 0)
                self.assertEqual(files.deleted_ids, [])

    def test_service_account_is_not_used_for_the_owner_upload_probe(self):
        files = DriveFiles()
        report = run_probe(files, source="service_account_json")
        self.assertEqual(report["status"], "oauth_required_for_upload_test")
        self.assertEqual(files.created, 0)

    def test_lost_create_response_still_cleans_up_the_allocated_file(self):
        files = DriveFiles("lost_create_response")
        report = run_probe(files)
        self.assertNotEqual(report["status"], "upload_readback_cleanup_verified")
        self.assertTrue(report["cleanup_verified"])
        self.assertEqual(files.deleted_ids, [files.allocated_id])
        self.assertIsNone(files.file)

    def test_corrupt_readback_is_a_failure_even_after_successful_cleanup(self):
        files = DriveFiles("corrupt_readback")
        report = run_probe(files)
        self.assertNotEqual(report["status"], "upload_readback_cleanup_verified")
        self.assertFalse(report["readback_verified"])
        self.assertTrue(report["cleanup_verified"])
        self.assertIsNone(files.file)

    def test_cleanup_refuses_unrelated_metadata_folders_or_other_owners(self):
        for mode in ("foreign_marker", "folder_instead_of_file", "not_owned"):
            with self.subTest(mode=mode):
                files = DriveFiles(mode)
                report = run_probe(files)
                self.assertNotEqual(report["status"], "upload_readback_cleanup_verified")
                self.assertFalse(report["cleanup_verified"])
                self.assertTrue(report["cleanup_required"])
                self.assertEqual(files.deleted_ids, [])

    def test_failed_or_unconfirmed_delete_cannot_be_reported_as_success(self):
        for mode in ("delete_denied", "delete_not_effective"):
            with self.subTest(mode=mode):
                files = DriveFiles(mode)
                report = run_probe(files)
                self.assertNotEqual(report["status"], "upload_readback_cleanup_verified")
                self.assertFalse(report["cleanup_verified"])
                self.assertTrue(report["cleanup_required"])

    def test_unexpected_returned_id_never_becomes_a_cleanup_target(self):
        files = DriveFiles("wrong_returned_id")
        report = run_probe(files)
        self.assertNotEqual(report["status"], "upload_readback_cleanup_verified")
        self.assertEqual(files.deleted_ids, [files.allocated_id])
        self.assertNotIn("existing-dataset-id", files.read_ids)

    def test_rejected_create_is_not_success_and_private_details_stay_out_of_report(self):
        files = DriveFiles("create_denied")
        report = run_probe(files)
        self.assertFalse(report["upload_verified"])
        self.assertEqual(files.deleted_ids, [])
        self.assertTrue(report["cleanup_verified"])
        serialized = json.dumps(report)
        for sensitive in ("private-provider-detail", "platform-root-id", files.allocated_id):
            self.assertNotIn(sensitive, serialized)


if __name__ == "__main__":
    unittest.main()
