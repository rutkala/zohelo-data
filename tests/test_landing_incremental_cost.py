"""Regression tests for bounded-cost incremental Landing publication."""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import tempfile
import unittest

from tests.test_landing_publication import FixtureAdapter, accepted, save_state

from ingestion.landing_publication import LandingPublicationError, publish_landing, verify_landing
from ingestion.source_campaign_store import CampaignStoreError, LocalCampaignStore


class LandingIncrementalCostTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.adapter = FixtureAdapter()

    def tearDown(self):
        self.temporary.cleanup()

    def _published_one(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        first_descriptor, _ = accepted(store, self.adapter, 1)
        save_state(store, [first_descriptor])
        manifest = publish_landing(store, self.adapter, "first")
        return first_descriptor, manifest

    def _add_second(self, first_descriptor):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        second_descriptor, _ = accepted(store, self.adapter, 2)
        save_state(store, [first_descriptor, second_descriptor])
        return second_descriptor

    def _pointer_path(self):
        return (
            self.root
            / "06_control/source_campaigns/landing_fixture/current-landing.json"
        )

    def test_increment_reads_only_new_receipt_raw_and_parquet(self):
        first_descriptor, first = self._published_one()
        second_descriptor = self._add_second(first_descriptor)
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        landing_reads = []
        receipt_reads = []
        raw_reads = []
        read_landing = store.read_landing_object
        read_receipt = store.read_receipt
        read_raw = store.read_raw

        def observed_landing(descriptor, **kwargs):
            landing_reads.append(descriptor["name"])
            return read_landing(descriptor, **kwargs)

        def observed_receipt(descriptor):
            receipt_reads.append(descriptor["id"])
            return read_receipt(descriptor)

        def observed_raw(descriptor):
            raw_reads.append(descriptor["id"])
            return read_raw(descriptor)

        store.read_landing_object = observed_landing
        store.read_receipt = observed_receipt
        store.read_raw = observed_raw
        second = publish_landing(store, self.adapter, "second")

        old_files = {item["name"] for item in first["files"]}
        new_files = [
            item["name"] for item in second["files"] if item["name"] not in old_files
        ]
        self.assertEqual(len(new_files), 1)
        self.assertTrue(old_files.isdisjoint(landing_reads))
        self.assertEqual(landing_reads.count(new_files[0]), 1)
        self.assertEqual(receipt_reads, [second_descriptor["id"]])
        self.assertEqual(len(raw_reads), 1)

    def test_changed_accepted_prefix_is_rejected_without_old_object_reads(self):
        first_descriptor, _ = self._published_one()
        second_descriptor = self._add_second(first_descriptor)
        changed_first = deepcopy(first_descriptor)
        changed_first["sha256"] = "f" * 64
        state_store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        save_state(state_store, [changed_first, second_descriptor])
        pointer_before = self._pointer_path().read_bytes()
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        old_evidence_reads = []
        store.read_receipt = lambda descriptor: old_evidence_reads.append(
            descriptor["id"]
        )
        store.read_raw = lambda descriptor: old_evidence_reads.append(descriptor["id"])

        with self.assertRaisesRegex(LandingPublicationError, "checkpoint"):
            publish_landing(store, self.adapter, "changed-prefix")

        self.assertEqual(old_evidence_reads, [])
        self.assertEqual(self._pointer_path().read_bytes(), pointer_before)

    def test_malformed_previous_manifest_descriptor_blocks_append(self):
        first_descriptor, _ = self._published_one()
        self._add_second(first_descriptor)
        pointer_path = self._pointer_path()
        pointer = json.loads(pointer_path.read_bytes())
        manifest_path = self.root / pointer["manifest_file_id"]
        manifest = json.loads(manifest_path.read_bytes())
        manifest["files"][0]["size"] = 0
        malformed = json.dumps(
            manifest, sort_keys=True, separators=(",", ":")
        ).encode()
        manifest_path.write_bytes(malformed)
        pointer["manifest_size_bytes"] = len(malformed)
        pointer["manifest_sha256"] = sha256(malformed).hexdigest()
        pointer_path.write_bytes(
            json.dumps(pointer, sort_keys=True, separators=(",", ":")).encode()
        )
        pointer_before = pointer_path.read_bytes()

        with self.assertRaisesRegex(LandingPublicationError, "file size"):
            publish_landing(
                LocalCampaignStore(self.root, self.adapter.SOURCE_ID),
                self.adapter,
                "malformed-previous",
            )

        self.assertEqual(pointer_path.read_bytes(), pointer_before)

    def test_new_fragment_readback_failure_does_not_promote(self):
        first_descriptor, first = self._published_one()
        self._add_second(first_descriptor)
        pointer_before = self._pointer_path().read_bytes()
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        read_landing = store.read_landing_object
        old_names = {item["name"] for item in first["files"]}
        failed_names = []

        def fail_new_fragment(descriptor, **kwargs):
            name = descriptor["name"]
            if name.endswith(".parquet") and name not in old_names:
                failed_names.append(name)
                raise CampaignStoreError("injected new fragment readback failure")
            return read_landing(descriptor, **kwargs)

        store.read_landing_object = fail_new_fragment
        with self.assertRaisesRegex(CampaignStoreError, "readback failure"):
            publish_landing(store, self.adapter, "failed-readback")

        self.assertEqual(len(failed_names), 1)
        self.assertEqual(self._pointer_path().read_bytes(), pointer_before)

    def test_explicit_fresh_verify_reads_old_fragments_and_detects_corruption(self):
        first_descriptor, first = self._published_one()
        self._add_second(first_descriptor)
        second = publish_landing(
            LocalCampaignStore(self.root, self.adapter.SOURCE_ID),
            self.adapter,
            "second",
        )
        old_file = first["files"][0]
        old_path = self.root / old_file["id"]
        payload = old_path.read_bytes()
        old_path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        observed = []
        read_landing = store.read_landing_object

        def observed_read(descriptor, **kwargs):
            observed.append(descriptor["name"])
            return read_landing(descriptor, **kwargs)

        store.read_landing_object = observed_read
        with self.assertRaisesRegex(CampaignStoreError, "does not match"):
            verify_landing(store)

        self.assertIn(old_file["name"], observed)
        self.assertEqual(second["format_version"], 1)


if __name__ == "__main__":
    unittest.main()
