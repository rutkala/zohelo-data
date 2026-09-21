"""Native-transfer and failure-boundary tests for the local GLEIF adapter."""
from __future__ import annotations

from hashlib import md5, sha256
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import ingestion.sources.gleif_bulk as gleif  # noqa: E402


PUBLISH_DATE = "2026-09-21 16:00:00"
MEMBERS = {"lei2": b"LEI2", "rr": b"RELA", "repex": b"EXCP"}


def discovery_document(*, sizes=None):
    sizes = sizes or {key: len(value) for key, value in MEMBERS.items()}
    snapshot = {"publish_date": PUBLISH_DATE}
    date_path = "2026/09/21/1278920"
    stamp = "20260921-1600"
    for key in gleif.PRODUCT_MEMBERS:
        snapshot[key] = {
            "type": key,
            "publish_date": PUBLISH_DATE,
            "full_file": {"csv": {
                "type": key,
                "format": "csv",
                "size": sizes[key],
                "url": (
                    f"https://goldencopy.gleif.org/storage/golden-copy-files/{date_path}/"
                    f"{stamp}-gleif-goldencopy-{key}-golden-copy.csv.zip"
                ),
                "delta_type": "GoldenCopy",
                "cdf_version": "LEI_3.1",
            }},
        }
    return {"data": [snapshot]}


def descriptor(body: bytes, url: str):
    return {
        "request_url": url, "final_url": url, "status_code": 200,
        "not_modified": False, "sha256": sha256(body).hexdigest(),
        "md5": md5(body, usedforsecurity=False).hexdigest(),
        "size_bytes": len(body), "response_headers": {"content_length": str(len(body))},
    }


def fetching(document, members=MEMBERS):
    raw = json.dumps(document).encode()

    def fake_fetch(request, path, *_args, **_kwargs):
        path = Path(path)
        url = request["url"]
        if url == gleif.DISCOVERY_URL:
            path.write_bytes(raw)
            return descriptor(raw, url)
        key = next(key for key in gleif.PRODUCT_MEMBERS if f"-{key}-golden-copy.csv.zip" in url)
        body = members[key]
        path.write_bytes(body)
        return descriptor(body, url)

    return fake_fetch


class GleifBulkTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_dataset_definitions_keep_current_three_member_product(self):
        self.assertEqual({item["dataset_key"] for item in gleif.GLEIF_DATASETS}, set(gleif.PRODUCT_MEMBERS))

    def test_discovery_rejects_invalid_or_unrepresentable_publish_time(self):
        for publish_date in ("2026-02-30 16:00:00", "2026-09-21 16:00:01"):
            with self.subTest(publish_date=publish_date):
                document = discovery_document()
                document["data"][0]["publish_date"] = publish_date
                for key in gleif.PRODUCT_MEMBERS:
                    document["data"][0][key]["publish_date"] = publish_date
                with self.assertRaises(gleif.GleifBulkError):
                    gleif._parse_discovery(json.dumps(document).encode())

    def test_unknown_empty_and_duplicate_selection_fail_before_workspace_io(self):
        missing = self.workspace / "not-created"
        for selection in ([], ["lei2", "lei2"], ["unknown"]):
            with self.subTest(selection=selection):
                with self.assertRaises(gleif.GleifBulkError):
                    gleif.run_gleif_ingestion(missing, datasets=selection, skip_upload=True)
        self.assertFalse(missing.exists())

    @patch.object(gleif, "fetch_to_file")
    def test_upload_flags_fail_closed_before_workspace_or_network(self, fetch):
        missing = self.workspace / "not-created"
        for kwargs in ({"allow_codespace": True}, {"allow_production_write": True}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(gleif.GleifBulkError, "publication disabled"):
                    gleif.run_gleif_ingestion(missing, **kwargs)
        self.assertFalse(missing.exists())
        fetch.assert_not_called()

    @patch.object(gleif, "fetch_to_file")
    def test_local_current_product_is_cached_as_native_files(self, fetch):
        fetch.side_effect = fetching(discovery_document())
        result = gleif.run_gleif_ingestion(self.workspace, skip_upload=True)
        self.assertEqual(result["status"], "downloaded_locally")
        self.assertTrue(result["complete_current_product"])
        self.assertEqual(result["selected_members"], list(gleif.PRODUCT_MEMBERS))
        for key, body in MEMBERS.items():
            self.assertEqual(gleif._member_path(self.workspace, {"provider_publish_date": PUBLISH_DATE}, key).read_bytes(), body)
        cache = json.loads((self.workspace / gleif.CACHE_NAME).read_text())
        self.assertEqual(set(cache["records"]), set(gleif.PRODUCT_MEMBERS))

    @patch.object(gleif, "fetch_to_file")
    def test_provider_size_mismatch_cannot_replace_prior_good_local_file(self, fetch):
        document = discovery_document(sizes={"lei2": 20, "rr": 4, "repex": 4})
        snapshot = gleif._parse_discovery(json.dumps(document).encode())
        trusted = gleif._member_path(self.workspace, snapshot, "lei2")
        trusted.write_bytes(b"trusted native bytes")
        fetch.side_effect = fetching(document)
        with self.assertRaisesRegex(gleif.GleifBulkError, "bytes do not match"):
            gleif.run_gleif_ingestion(self.workspace, datasets=["lei2"], skip_upload=True)
        self.assertEqual(trusted.read_bytes(), b"trusted native bytes")

    @patch.object(gleif, "fetch_to_file")
    def test_skip_download_requires_verified_untampered_cache(self, fetch):
        fetch.side_effect = fetching(discovery_document())
        gleif.run_gleif_ingestion(self.workspace, datasets=["rr"], skip_upload=True)
        rr = gleif._member_path(self.workspace, {"provider_publish_date": PUBLISH_DATE}, "rr")
        rr.write_bytes(b"tampered")
        with self.assertRaisesRegex(gleif.GleifBulkError, "do not match"):
            gleif.run_gleif_ingestion(self.workspace, datasets=["rr"], skip_download=True, skip_upload=True)
        with self.assertRaisesRegex(gleif.GleifBulkError, "missing lei2 verification"):
            gleif.run_gleif_ingestion(self.workspace, datasets=["lei2"], skip_download=True, skip_upload=True)

    @patch.object(gleif, "fetch_to_file")
    def test_skip_download_rejects_cached_metadata_not_bound_to_discovery_bytes(self, fetch):
        fetch.side_effect = fetching(discovery_document())
        gleif.run_gleif_ingestion(self.workspace, datasets=["rr"], skip_upload=True)
        cache_path = self.workspace / gleif.CACHE_NAME
        cache = json.loads(cache_path.read_text())
        cache["snapshot"]["members"]["rr"]["cdf_version"] = "invented-version"
        cache_path.write_text(json.dumps(cache))
        with self.assertRaisesRegex(gleif.GleifBulkError, "discovery bytes do not match"):
            gleif.run_gleif_ingestion(self.workspace, datasets=["rr"], skip_download=True, skip_upload=True)

    @patch.object(gleif, "fetch_to_file")
    def test_failure_on_third_member_reuses_first_two_verified_cache_records(self, fetch):
        document = discovery_document()
        first = fetching(document)

        def fail_third(request, path, *args, **kwargs):
            if "-repex-golden-copy.csv.zip" in request["url"]:
                raise gleif.BulkTransportError("simulated network loss")
            return first(request, path, *args, **kwargs)

        fetch.side_effect = fail_third
        with self.assertRaisesRegex(gleif.GleifBulkError, "repex download failed"):
            gleif.run_gleif_ingestion(self.workspace, skip_upload=True)
        cache = json.loads((self.workspace / gleif.CACHE_NAME).read_text())
        self.assertEqual(set(cache["records"]), {"lei2", "rr"})

        fetch.side_effect = fetching(document)
        gleif.run_gleif_ingestion(self.workspace, skip_upload=True)
        self.assertEqual(fetch.call_count, 6)  # initial discovery/three members, then discovery/retry only
        retry_urls = [call.args[0]["url"] for call in fetch.call_args_list[-2:]]
        self.assertEqual(retry_urls[0], gleif.DISCOVERY_URL)
        self.assertIn("-repex-golden-copy.csv.zip", retry_urls[1])

    @patch("fcntl.flock", side_effect=BlockingIOError())
    @patch.object(gleif, "fetch_to_file")
    def test_concurrent_workspace_owner_fails_before_network_or_cache_io(self, fetch, _flock):
        with self.assertRaisesRegex(gleif.GleifBulkError, "already owns this workspace"):
            gleif.run_gleif_ingestion(self.workspace, skip_upload=True)
        fetch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
