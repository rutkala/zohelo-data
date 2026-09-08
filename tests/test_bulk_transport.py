"""Streaming and recovery tests for full-distribution transport."""
from __future__ import annotations

import hashlib
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion.bulk_transport import (
    BulkDriveRawStore,
    BulkHTTPStatusError,
    BulkResponseTooLargeError,
    BulkTransportError,
    DriveCapacityError,
    DriveIntegrityError,
    InvalidBulkRequestError,
    LocalDiskCapacityError,
    UncertainDriveWriteError,
    _StrictRedirectHandler,
    fetch_to_file,
)


class FakeHTTPResponse:
    def __init__(self, body=b"", *, status=200, headers=None, url="https://ec.europa.eu/file"):
        self.body = body
        self.status = status
        self.headers = headers or {}
        self.url = url
        self.position = 0
        self.read_sizes = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        if size < 0:
            size = len(self.body) - self.position
        result = self.body[self.position:self.position + size]
        self.position += len(result)
        return result

    def geturl(self):
        return self.url

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class FakeOpener:
    def __init__(self, response):
        self.response = response
        self.request = None
        self.timeout = None

    def open(self, request, timeout):
        self.request = request
        self.timeout = timeout
        return self.response


class HTTPBulkTransportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.path = self.root / "distribution.zip"

    def tearDown(self):
        self.temporary.cleanup()

    def test_streams_exact_bytes_and_returns_both_hashes_and_headers(self):
        body = b"abcdef" * 700_000
        response = FakeHTTPResponse(body, headers={
            "content-length": str(len(body)),
            "content-type": "application/zip",
            "etag": '"edition-1"',
            "last-modified": "Wed, 09 Sep 2026 12:00:00 GMT",
        })
        result = fetch_to_file(
            {"url": "https://ec.europa.eu/file", "params": {"lang": "en"}},
            self.path,
            {"ec.europa.eu"},
            timeout=30,
            max_bytes=len(body) + 1,
            opener=FakeOpener(response),
            chunk_size=1024 * 1024,
        )
        self.assertEqual(self.path.read_bytes(), body)
        self.assertEqual(result["sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(result["md5"], hashlib.md5(body, usedforsecurity=False).hexdigest())
        self.assertEqual(result["size_bytes"], len(body))
        self.assertEqual(result["response_headers"]["etag"], '"edition-1"')
        self.assertGreater(len(response.read_sizes), 4)
        self.assertTrue(all(size == 1024 * 1024 for size in response.read_sizes))

    def test_truncated_content_length_fails_without_replacing_prior_file(self):
        self.path.write_bytes(b"trusted previous")
        response = FakeHTTPResponse(b"short", headers={"content-length": "20"})
        with self.assertRaisesRegex(BulkTransportError, "Content-Length"):
            fetch_to_file(
                {"url": "https://ec.europa.eu/file"}, self.path,
                {"ec.europa.eu"}, 30, 100, opener=FakeOpener(response),
            )
        self.assertEqual(self.path.read_bytes(), b"trusted previous")

    def test_stream_limit_stops_unknown_length_response(self):
        response = FakeHTTPResponse(
            b"x" * 101, url="https://api.worldbank.org/v2/file"
        )
        with self.assertRaises(BulkResponseTooLargeError):
            fetch_to_file(
                {"url": "https://api.worldbank.org/v2/file"}, self.path,
                {"api.worldbank.org"}, 30, 100,
                opener=FakeOpener(response), chunk_size=17,
            )
        self.assertFalse(self.path.exists())

    def test_bad_final_redirect_host_is_rejected(self):
        response = FakeHTTPResponse(b"stolen", url="https://mirror.invalid/file")
        with self.assertRaises(InvalidBulkRequestError):
            fetch_to_file(
                {"url": "https://ec.europa.eu/file"}, self.path,
                {"ec.europa.eu"}, 30, 100, opener=FakeOpener(response),
            )

    def test_request_rejects_credentials_and_exact_host_subdomains(self):
        with self.assertRaisesRegex(InvalidBulkRequestError, "credential"):
            fetch_to_file(
                {"url": "https://ec.europa.eu/file", "params": {"api-key": "secret"}},
                self.path, {"ec.europa.eu"}, 30, 100,
                opener=FakeOpener(FakeHTTPResponse(b"x")),
            )

    def test_authenticated_redirect_cannot_forward_headers_to_another_host(self):
        handler = _StrictRedirectHandler(
            frozenset({"databank.worldbank.org", "databankfiles.worldbank.org"}),
            "databank.worldbank.org",
            True,
        )
        with self.assertRaisesRegex(InvalidBulkRequestError, "authenticated"):
            handler.redirect_request(
                None, None, 302, "Found", {},
                "https://databankfiles.worldbank.org/archive.zip",
            )
        with self.assertRaises(InvalidBulkRequestError):
            fetch_to_file(
                {"url": "https://evil.ec.europa.eu/file"}, self.path,
                {"ec.europa.eu"}, 30, 100,
                opener=FakeOpener(FakeHTTPResponse(b"x")),
            )

    def test_verified_previous_file_uses_conditional_get_and_accepts_304(self):
        body = b"complete previous object"
        first = fetch_to_file(
            {"url": "https://ec.europa.eu/file"}, self.path, {"ec.europa.eu"}, 30, 100,
            opener=FakeOpener(FakeHTTPResponse(body, headers={"etag": '"v1"'})),
        )
        opener = FakeOpener(FakeHTTPResponse(status=304, headers={"etag": '"v1"'}))
        second = fetch_to_file(
            {"url": "https://ec.europa.eu/file"}, self.path, {"ec.europa.eu"}, 30, 100,
            previous_descriptor=first, opener=opener,
        )
        self.assertEqual(opener.request.get_header("If-none-match"), '"v1"')
        self.assertTrue(second["not_modified"])
        self.assertEqual(second["sha256"], first["sha256"])
        self.assertEqual(self.path.read_bytes(), body)

    def test_tampered_previous_file_disables_conditional_request(self):
        body = b"first"
        previous = fetch_to_file(
            {"url": "https://ec.europa.eu/file"}, self.path, {"ec.europa.eu"}, 30, 100,
            opener=FakeOpener(FakeHTTPResponse(body, headers={"etag": '"v1"'})),
        )
        self.path.write_bytes(b"tampered")
        opener = FakeOpener(FakeHTTPResponse(b"replacement"))
        fetch_to_file(
            {"url": "https://ec.europa.eu/file"}, self.path, {"ec.europa.eu"}, 30, 100,
            previous_descriptor=previous, opener=opener,
        )
        self.assertIsNone(opener.request.get_header("If-none-match"))
        self.assertEqual(self.path.read_bytes(), b"replacement")

    def test_202_and_rate_or_size_statuses_expose_bounded_retry_fields(self):
        for status in (202, 413, 429):
            with self.subTest(status=status):
                response = FakeHTTPResponse(
                    b"ignored", status=status,
                    headers={"retry-after": "120", "location": "https://ec.europa.eu/job/1"},
                )
                with self.assertRaises(BulkHTTPStatusError) as raised:
                    fetch_to_file(
                        {"url": "https://ec.europa.eu/file"}, self.path,
                        {"ec.europa.eu"}, 30, 100, opener=FakeOpener(response),
                    )
                self.assertEqual(raised.exception.status_code, status)
                self.assertEqual(raised.exception.headers["retry-after"], "120")
                self.assertEqual(raised.exception.retry_info["retry_after"], "120")
                if status == 202:
                    self.assertEqual(
                        raised.exception.async_location, "https://ec.europa.eu/job/1"
                    )
                self.assertEqual(response.read_sizes, [])

    def test_explicitly_accepted_async_and_size_diagnostics_are_retained(self):
        for status, body in ((202, b"<pending/>"), (413, b"<too-large/>")):
            with self.subTest(status=status):
                target = self.root / f"diagnostic-{status}.xml"
                result = fetch_to_file(
                    {"url": "https://ec.europa.eu/file"}, target,
                    {"ec.europa.eu"}, 30, 100,
                    opener=FakeOpener(FakeHTTPResponse(
                        body, status=status,
                        headers={"content-length": str(len(body)), "retry-after": "300"},
                    )),
                    accepted_statuses=(200, 202, 413),
                )
                self.assertEqual(result["status_code"], status)
                self.assertEqual(target.read_bytes(), body)

    def test_reported_disk_headroom_failure_is_explicit(self):
        usage = type("Usage", (), {"free": 4})()
        with patch("ingestion.bulk_transport.shutil.disk_usage", return_value=usage):
            with self.assertRaises(LocalDiskCapacityError) as raised:
                fetch_to_file(
                    {"url": "https://ec.europa.eu/file"}, self.path,
                    {"ec.europa.eu"}, 30, 10,
                    opener=FakeOpener(FakeHTTPResponse(b"12345", headers={"content-length": "5"})),
                )
        self.assertEqual(raised.exception.required_bytes, 5)
        self.assertEqual(raised.exception.available_bytes, 4)


class FakeRequest:
    def __init__(self, action):
        self.action = action

    def execute(self, **_kwargs):
        return self.action()


class FakeUpload:
    def __init__(self, filename, mimetype=None, chunksize=None, resumable=None):
        self.filename = filename
        self.mimetype = mimetype
        self.chunksize = chunksize
        self.resumable = resumable


class FakeFiles:
    FOLDER = "application/vnd.google-apps.folder"

    def __init__(self):
        self.folders = {"selectedroot", "responsesroot"}
        self.objects = {}
        self.next_id = "allocated1"
        self.raise_before_create = False
        self.raise_after_create = False
        self.corrupt_download = False
        self.download_chunks = 0

    def get(self, *, fileId, fields):
        def action():
            if fileId in self.folders:
                return {"id": fileId, "mimeType": self.FOLDER,
                        "ownedByMe": True, "trashed": False}
            item = self.objects[fileId]
            data = item["data"]
            return {
                "id": fileId, "name": item["name"], "size": str(len(data)),
                "md5Checksum": hashlib.md5(data, usedforsecurity=False).hexdigest(),
                "mimeType": "application/octet-stream", "parents": ["responsesroot"],
                "ownedByMe": True, "trashed": False,
            }
        return FakeRequest(action)

    def list(self, **kwargs):
        query = kwargs["q"]
        match = re.search(r"name='([^']+)'", query)
        name = match.group(1)
        found = [{"id": file_id} for file_id, item in self.objects.items()
                 if item["name"] == name]
        return FakeRequest(lambda: {"files": found})

    def generateIds(self, **_kwargs):
        return FakeRequest(lambda: {"ids": [self.next_id]})

    def create(self, *, body, media_body, fields):
        def action():
            if self.raise_before_create:
                raise TimeoutError("request outcome unknown")
            self.objects[body["id"]] = {
                "name": body["name"], "data": Path(media_body.filename).read_bytes()
            }
            if self.raise_after_create:
                raise TimeoutError("response lost after commit")
            return {"id": body["id"]}
        return FakeRequest(action)

    def get_media(self, *, fileId):
        return self, fileId


class FakeDownload:
    def __init__(self, destination, request, chunksize):
        self.destination = destination
        self.files, self.file_id = request
        self.chunksize = chunksize
        self.position = 0

    def next_chunk(self, **_kwargs):
        data = self.files.objects[self.file_id]["data"]
        if self.files.corrupt_download and data:
            data = data[:-1] + bytes([data[-1] ^ 1])
        chunk = data[self.position:self.position + self.chunksize]
        self.position += len(chunk)
        self.destination.write(chunk)
        self.files.download_chunks += 1
        return None, self.position == len(data)


class FakeAbout:
    def __init__(self, storage):
        self.storage = storage

    def get(self, **_kwargs):
        return FakeRequest(lambda: {"storageQuota": dict(self.storage.quota)})


class FakeDriveService:
    def __init__(self, storage):
        self._files = FakeFiles()
        self._about = FakeAbout(storage)

    def files(self):
        return self._files

    def about(self):
        return self._about


class FakeStorage:
    def __init__(self):
        self.quota = {"limit": str(2 * 1024**3), "usage": "0"}
        self.drive_service = FakeDriveService(self)
        self.authorized = 0
        self.asserted = []
        self.session = None
        self.session_parents = []

    def resolve_root(self, *, create):
        if create:
            raise AssertionError("bulk transport must use an existing selected root")
        return "selectedroot"

    def authorize_writes(self):
        self.authorized += 1

    def _assert_parent_within_selected_root(self, parent_id):
        if parent_id not in {"selectedroot", "responsesroot"}:
            raise ValueError("outside selected root")
        self.asserted.append(parent_id)

    def begin_write_session(self):
        self.authorize_writes()
        self.session = object()
        return self.session

    def authorize_session_parent(self, session, parent_id):
        if session is not self.session or parent_id not in {"selectedroot", "responsesroot"}:
            raise ValueError("bad write session")
        self.session_parents.append(parent_id)


class DriveBulkTransportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.storage = FakeStorage()
        self.store = BulkDriveRawStore(
            self.storage, "eurostat", responses_root_id="responsesroot",
            chunk_size=1024 * 1024,
        )
        self.patches = (
            patch("googleapiclient.http.MediaFileUpload", FakeUpload),
            patch("googleapiclient.http.MediaIoBaseDownload", FakeDownload),
        )
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temporary.cleanup()

    def test_large_file_upload_is_content_addressed_and_stream_verified(self):
        body = (b"0123456789abcdef" * (1024 * 1024)) + b"tail"
        path = self.root / "large.zip"
        path.write_bytes(body)
        descriptor = self.store.put_file(path, {"dataset_id": "wdi"})
        digest = hashlib.sha256(body).hexdigest()
        self.assertEqual(descriptor, {
            "id": "allocated1", "sha256": digest,
            "md5": hashlib.md5(body, usedforsecurity=False).hexdigest(),
            "size_bytes": len(body), "name": f"raw-{digest}.bin",
            "metadata": {"dataset_id": "wdi"},
        })
        self.assertGreater(self.storage.drive_service._files.download_chunks, 16)
        self.assertEqual(self.storage.authorized, 1)
        self.assertIn("responsesroot", self.storage.session_parents)

    def test_uncertain_upload_is_accepted_only_after_known_id_readback(self):
        path = self.root / "archive.zip"
        path.write_bytes(b"complete archive")
        self.storage.drive_service._files.raise_after_create = True
        descriptor = self.store.put_file(path)
        self.assertEqual(descriptor["id"], "allocated1")
        self.assertEqual(self.store.verify(descriptor), descriptor)

    def test_unprovable_upload_failure_is_fail_closed_and_marked_uncertain(self):
        path = self.root / "archive.zip"
        path.write_bytes(b"complete archive")
        self.storage.drive_service._files.raise_before_create = True
        with self.assertRaises(UncertainDriveWriteError) as raised:
            self.store.put_file(path)
        self.assertTrue(raised.exception.uncertain)

    def test_remote_hash_mismatch_is_detected(self):
        path = self.root / "archive.zip"
        path.write_bytes(b"complete archive")
        descriptor = self.store.put_file(path)
        self.storage.drive_service._files.corrupt_download = True
        with self.assertRaisesRegex(DriveIntegrityError, "do not match"):
            self.store.verify(descriptor)

    def test_read_to_file_streams_and_verifies_before_replacement(self):
        source = self.root / "archive.zip"
        body = b"full source distribution" * 100_000
        source.write_bytes(body)
        descriptor = self.store.put_file(source)
        restored = self.root / "restored.zip"
        restored.write_bytes(b"old")
        returned = self.store.read_to_file(descriptor, restored)
        self.assertEqual(returned, descriptor)
        self.assertEqual(restored.read_bytes(), body)

    def test_live_drive_quota_uses_actual_file_size(self):
        path = self.root / "archive.zip"
        path.write_bytes(b"123456")
        self.storage.quota = {"limit": "10", "usage": "5"}
        with self.assertRaises(DriveCapacityError) as raised:
            self.store.put_file(path)
        self.assertEqual(raised.exception.required_bytes, 6)
        self.assertEqual(raised.exception.available_bytes, 5)
        self.assertEqual(self.storage.drive_service._files.objects, {})

    def test_credentials_are_rejected_from_durable_metadata(self):
        path = self.root / "archive.zip"
        path.write_bytes(b"complete")
        with self.assertRaisesRegex(InvalidBulkRequestError, "credential"):
            self.store.put_file(path, {"authorization": "Bearer secret"})


if __name__ == "__main__":
    unittest.main()
