"""Native-transfer and failure-boundary tests for the GLEIF adapter."""
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
from ingestion.bulk_transport import DriveIntegrityError  # noqa: E402


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
        "request_url": url,
        "final_url": url,
        "status_code": 200,
        "not_modified": False,
        "sha256": sha256(body).hexdigest(),
        "md5": md5(body, usedforsecurity=False).hexdigest(),
        "size_bytes": len(body),
        "response_headers": {"content_length": str(len(body))},
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


class FakeRawStore:
    def __init__(self, *_args):
        self.put = []
        self.verify_calls = []
        self.fail_upload = False
        self.mismatch = False

    def put_file(self, path, metadata):
        if self.fail_upload:
            raise DriveIntegrityError("simulated upload failure")
        body = Path(path).read_bytes()
        result = {
            "id": f"raw-{len(self.put)}",
            "name": f"raw-{sha256(body).hexdigest()}.bin",
            "sha256": sha256(body).hexdigest(),
            "md5": md5(body, usedforsecurity=False).hexdigest(),
            "size_bytes": len(body),
            "metadata": metadata,
        }
        self.put.append(result)
        if self.mismatch:
            result = {**result, "sha256": "0" * 64}
        return result

    def verify(self, item):
        self.verify_calls.append(item)
        return dict(item)


class FakeCampaignStore:
    def __init__(self, *_args):
        self.receipts = []

    def put_receipt(self, receipt):
        self.receipts.append(receipt)
        raw = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        return {"id": "receipt-1", "sha256": sha256(raw).hexdigest(), "size_bytes": len(raw)}

    def read_receipt(self, _descriptor):
        return self.receipts[-1]


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
    def test_local_current_product_is_cached_as_native_files(self, fetch):
        fetch.side_effect = fetching(discovery_document())
        result = gleif.run_gleif_ingestion(self.workspace, skip_upload=True)
        self.assertEqual(result["status"], "downloaded_locally")
        self.assertTrue(result["complete_current_product"])
        self.assertEqual(result["selected_members"], list(gleif.PRODUCT_MEMBERS))
        snapshot = gleif._download_snapshot  # prove target paths are snapshot-isolated below
        del snapshot
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

    @patch.object(gleif, "DriveCampaignStore")
    @patch.object(gleif, "BulkDriveRawStore")
    @patch.object(gleif, "StorageManager")
    @patch.object(gleif, "fetch_to_file")
    def test_full_product_receipt_pins_all_native_descriptors_after_verify(self, fetch, storage, raw_class, campaign_class):
        fetch.side_effect = fetching(discovery_document())
        raw = FakeRawStore()
        campaign = FakeCampaignStore()
        raw_class.return_value = raw
        campaign_class.return_value = campaign
        result = gleif.run_gleif_ingestion(self.workspace, allow_production_write=True)
        self.assertEqual(result["status"], "published_native")
        self.assertTrue(result["complete_current_product"])
        self.assertEqual(len(raw.verify_calls), 4)
        receipt = campaign.receipts[0]
        self.assertEqual(receipt["product"]["completion_status"], "complete_current_product")
        self.assertFalse(receipt["product"]["provider_history_verified"])
        self.assertEqual([item["dataset_key"] for item in receipt["archives"]], list(gleif.PRODUCT_MEMBERS))
        self.assertTrue(all(item["native"]["name"].startswith("raw-") for item in receipt["archives"]))
        self.assertIn("discovery_native", receipt["provider_snapshot"])
        storage.assert_called_once_with(allow_interactive_auth=False)

    @patch.object(gleif, "DriveCampaignStore")
    @patch.object(gleif, "BulkDriveRawStore")
    @patch.object(gleif, "StorageManager")
    @patch.object(gleif, "fetch_to_file")
    def test_subset_receipt_is_explicitly_not_product_completion(self, fetch, _storage, raw_class, campaign_class):
        fetch.side_effect = fetching(discovery_document())
        raw = FakeRawStore()
        campaign = FakeCampaignStore()
        raw_class.return_value = raw
        campaign_class.return_value = campaign
        result = gleif.run_gleif_ingestion(self.workspace, datasets=["rr"], allow_codespace=True)
        self.assertFalse(result["complete_current_product"])
        self.assertEqual(campaign.receipts[0]["product"]["completion_status"], "incomplete_selected_subset")

    @patch.object(gleif, "DriveCampaignStore")
    @patch.object(gleif, "BulkDriveRawStore")
    @patch.object(gleif, "StorageManager")
    @patch.object(gleif, "fetch_to_file")
    def test_upload_failure_writes_no_completion_receipt(self, fetch, _storage, raw_class, campaign_class):
        fetch.side_effect = fetching(discovery_document())
        raw = FakeRawStore()
        raw.fail_upload = True
        campaign = FakeCampaignStore()
        raw_class.return_value = raw
        campaign_class.return_value = campaign
        with self.assertRaisesRegex(DriveIntegrityError, "upload failure"):
            gleif.run_gleif_ingestion(self.workspace, datasets=["rr"], allow_production_write=True)
        self.assertEqual(campaign.receipts, [])

    @patch.object(gleif, "DriveCampaignStore")
    @patch.object(gleif, "BulkDriveRawStore")
    @patch.object(gleif, "StorageManager")
    @patch.object(gleif, "fetch_to_file")
    def test_remote_mismatch_retains_prior_receipt_and_does_not_complete(self, fetch, _storage, raw_class, campaign_class):
        fetch.side_effect = fetching(discovery_document())
        raw = FakeRawStore()
        raw.mismatch = True
        campaign = FakeCampaignStore()
        campaign.receipts.append({"prior": "trusted receipt"})
        raw_class.return_value = raw
        campaign_class.return_value = campaign
        before = __import__("os").environ.get("ZOHELO_ALLOW_PRODUCTION_WRITES")
        with self.assertRaisesRegex(DriveIntegrityError, "discovery bytes differ"):
            gleif.run_gleif_ingestion(self.workspace, datasets=["rr"], allow_production_write=True)
        self.assertEqual(campaign.receipts, [{"prior": "trusted receipt"}])
        self.assertEqual(__import__("os").environ.get("ZOHELO_ALLOW_PRODUCTION_WRITES"), before)

    @patch.object(gleif, "fetch_to_file")
    def test_failure_on_third_member_reuses_first_two_verified_cache_records(self, fetch):
        document = discovery_document()
        first = fetching(document)
        calls = []

        def fail_third(request, path, *args, **kwargs):
            calls.append(request["url"])
            if "-repex-golden-copy.csv.zip" in request["url"]:
                raise gleif.BulkTransportError("simulated network loss")
            return first(request, path, *args, **kwargs)

        fetch.side_effect = fail_third
        with self.assertRaisesRegex(gleif.GleifBulkError, "repex download failed"):
            gleif.run_gleif_ingestion(self.workspace, skip_upload=True)
        cache = json.loads((self.workspace / gleif.CACHE_NAME).read_text())
        self.assertEqual(set(cache["records"]), {"lei2", "rr"})

        calls.clear()
        fetch.side_effect = fetching(document)
        gleif.run_gleif_ingestion(self.workspace, skip_upload=True)
        self.assertEqual(calls, [])  # fetching replacement does not record calls
        self.assertEqual(fetch.call_count, 6)  # first discovery/three members, then discovery/retry only
        retry_urls = [call.args[0]["url"] for call in fetch.call_args_list[-2:]]
        self.assertEqual(retry_urls, [gleif.DISCOVERY_URL, next(
            item["url"] for item in discovery_document()["data"][0]["repex"]["full_file"].values()
        )])

    @patch.object(gleif, "DriveCampaignStore")
    @patch.object(gleif, "BulkDriveRawStore")
    @patch.object(gleif, "StorageManager")
    @patch.object(gleif, "fetch_to_file")
    def test_explicit_write_flag_never_mutates_storage_guard_environment(self, fetch, _storage, raw_class, campaign_class):
        fetch.side_effect = fetching(discovery_document())
        raw_class.return_value = FakeRawStore()
        campaign_class.return_value = FakeCampaignStore()
        before = __import__("os").environ.get("ZOHELO_ALLOW_PRODUCTION_WRITES")
        gleif.run_gleif_ingestion(self.workspace, datasets=["rr"], allow_production_write=True)
        self.assertEqual(__import__("os").environ.get("ZOHELO_ALLOW_PRODUCTION_WRITES"), before)

    @patch("fcntl.flock", side_effect=BlockingIOError())
    @patch.object(gleif, "fetch_to_file")
    def test_concurrent_workspace_owner_fails_before_network_or_cache_io(self, fetch, _flock):
        with self.assertRaisesRegex(gleif.GleifBulkError, "already owns this workspace"):
            gleif.run_gleif_ingestion(self.workspace, skip_upload=True)
        fetch.assert_not_called()

    @patch.dict("os.environ", {"GITHUB_ACTIONS": "true"}, clear=False)
    @patch.object(gleif, "StorageManager")
    @patch.object(gleif, "fetch_to_file")
    def test_actions_environment_alone_never_enables_drive_write(self, fetch, storage):
        fetch.side_effect = fetching(discovery_document())
        result = gleif.run_gleif_ingestion(self.workspace)
        self.assertEqual(result["status"], "downloaded_locally")
        storage.assert_not_called()


if __name__ == "__main__":
    unittest.main()
