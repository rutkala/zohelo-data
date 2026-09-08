"""Acceptance tests for incremental queryable source Landing snapshots."""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from datetime import date
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.landing_publication import (  # noqa: E402
    LANDING_COLUMNS,
    LandingPublicationError,
    MAX_NEW_RESPONSES,
    publish_landing,
    verify_landing,
)
from ingestion.source_campaign import new_state  # noqa: E402
from ingestion.source_campaign_store import (  # noqa: E402
    CampaignStoreError,
    DriveCampaignStore,
    LocalCampaignStore,
)


class FixtureAdapter:
    SOURCE_ID = "landing_fixture"

    def request_for(self, task):
        return {
            "url": "https://example.test/responses",
            "params": {"task": task["id"]},
        }

    def interpret(self, task, body, today):
        payload = json.loads(body)
        if payload["task"] != task["id"]:
            raise ValueError("body/task mismatch")
        return {
            "record_count": len(payload["records"]),
            "next_tasks": [],
            "metadata": {"representation": payload["representation"]},
        }


class _Executed:
    def __init__(self, value):
        self.value = value

    def execute(self, **_kwargs):
        return self.value


class _Media:
    def __init__(self, stream, **_kwargs):
        self.data = stream.read()


class _Download:
    def __init__(self, stream, request, **_kwargs):
        self.stream = stream
        self.request = request

    def next_chunk(self, **_kwargs):
        self.stream.write(self.request.data)
        return None, True


class _MediaRequest:
    def __init__(self, data):
        self.data = data


class _DriveFiles:
    def __init__(self):
        self.next_id = 1
        self.objects = {
            "selectedroot": {
                "id": "selectedroot", "name": "root", "mimeType": "folder",
                "parents": [], "ownedByMe": True, "trashed": False,
            },
            "landingroot": {
                "id": "landingroot", "name": "01_landing", "mimeType": "folder",
                "parents": ["selectedroot"], "ownedByMe": True, "trashed": False,
            },
        }

    def _id(self):
        value = f"driveid{self.next_id}"
        self.next_id += 1
        return value

    def list(self, **kwargs):
        query = kwargs["q"]
        name = re.search(r"name='([^']+)'", query).group(1)
        parent = re.search(r"and '([^']+)' in parents", query).group(1)
        files = [
            {"id": key, "mimeType": item["mimeType"]}
            for key, item in self.objects.items()
            if item["name"] == name
            and item["parents"] == [parent]
            and item["trashed"] is False
        ]
        return _Executed({"files": files})

    def generateIds(self, **_kwargs):
        return _Executed({"ids": [self._id()]})

    def create(self, *, body, media_body=None, **_kwargs):
        object_id = body.get("id", self._id())
        self.objects[object_id] = {
            **body,
            "id": object_id,
            "data": b"" if media_body is None else media_body.data,
            "ownedByMe": True,
            "trashed": False,
        }
        return _Executed({"id": object_id})

    def get(self, *, fileId, **_kwargs):
        item = self.objects[fileId]
        return _Executed(
            {
                key: value
                for key, value in item.items()
                if key != "data"
            }
            | {"size": str(len(item.get("data", b"")))}
        )

    def get_media(self, *, fileId):
        return _MediaRequest(self.objects[fileId]["data"])

    def update(self, *, fileId, media_body, **_kwargs):
        self.objects[fileId]["data"] = media_body.data
        return _Executed({"id": fileId})


class _DriveService:
    def __init__(self, files):
        self._files = files

    def files(self):
        return self._files


class _DriveStorage:
    def __init__(self):
        self.files = _DriveFiles()
        self.drive_service = _DriveService(self.files)

    def authorize_writes(self):
        return None

    def _assert_parent_within_selected_root(self, parent_id):
        if parent_id not in self.files.objects:
            raise ValueError("parent outside selected root")

    def resolve_root(self, *, create):
        return "selectedroot"

    def resolve_zone(self, name, *, create):
        if name != "landing":
            raise ValueError("unexpected zone")
        return "landingroot"

    def get_or_create_nested_folder(self, segments, *, root_id):
        parent = root_id
        for name in segments:
            found = [
                key for key, item in self.files.objects.items()
                if item["name"] == name and item["parents"] == [parent]
            ]
            if found:
                parent = found[0]
                continue
            object_id = self.files._id()
            self.files.objects[object_id] = {
                "id": object_id, "name": name, "mimeType": "folder",
                "parents": [parent], "ownedByMe": True, "trashed": False,
            }
            parent = object_id
        return parent

def accepted(store, adapter, number):
    task = {
        "id": f"page:{number}",
        "lane": "recent" if number % 2 else "history",
        "kind": "fixture_page",
        "cursor": {"page": number},
    }
    body = json.dumps(
        {
            "task": task["id"],
            "records": list(range(number % 3 + 1)),
            "representation": number,
        },
        separators=(",", ":"),
    ).encode()
    raw = store.put_raw(body, {"task_id": task["id"], "source_id": adapter.SOURCE_ID})
    receipt = {
        "schema_version": 1,
        "source_id": adapter.SOURCE_ID,
        "task": task,
        "request": adapter.request_for(task),
        "http_status": 200,
        "headers": {"content-type": "application/json", "etag": f"etag-{number}"},
        "retrieved_at_utc": f"2026-09-{number % 20 + 1:02d}T12:00:00+00:00",
        "raw": raw,
        "record_count": len(json.loads(body)["records"]),
        "metadata": {"representation": number},
        "code_sha": "a" * 40,
        "accepted": True,
    }
    descriptor = store.put_receipt(receipt)
    return {"task_id": task["id"], **descriptor}, body


def save_state(store, receipts, *, rejected=()):
    state = new_state(store.source_id, date(2026, 9, 1))
    state["receipts"] = deepcopy(receipts)
    state["rejected_receipts"] = deepcopy(list(rejected))
    state["accepted_responses"] = len(receipts)
    store.save(state)


class LandingPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.adapter = FixtureAdapter()

    def tearDown(self):
        self.temporary.cleanup()

    def test_real_local_store_publishes_typed_payload_queryable_by_duckdb(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        descriptor, expected_body = accepted(store, self.adapter, 1)
        save_state(store, [descriptor])

        manifest = publish_landing(store, self.adapter, "b" * 40)

        self.assertEqual(manifest["row_count"], 1)
        self.assertEqual(manifest["pending_publication_count"], 0)
        self.assertEqual(
            manifest["columns"],
            [{"name": name, "type": kind} for name, kind in LANDING_COLUMNS],
        )
        self.assertFalse(
            (self.root / "06_control/nbp/current-release.json").exists()
        )
        pointer_path = (
            self.root
            / "06_control/source_campaigns/landing_fixture/current-landing.json"
        )
        self.assertTrue(pointer_path.is_file())
        parquet_path = self.root / manifest["files"][0]["id"]
        connection = duckdb.connect(":memory:")
        try:
            row = connection.execute(
                "SELECT source_id, task_id, lane, task_kind, record_count, "
                "raw_sha256, raw_size_bytes, request_json, metadata_json, "
                "payload_utf8, content_type FROM read_parquet(?)",
                [str(parquet_path)],
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(row[0:5], ("landing_fixture", "page:1", "recent", "fixture_page", 2))
        self.assertEqual(row[5], sha256(expected_body).hexdigest())
        self.assertEqual(row[6], len(expected_body))
        self.assertEqual(json.loads(row[7]), self.adapter.request_for({"id": "page:1"}))
        self.assertEqual(json.loads(row[8]), {"representation": 1})
        self.assertEqual(row[9].encode(), expected_body)
        self.assertEqual(row[10], "application/json")

    def test_noop_keeps_exact_snapshot_and_creates_no_objects(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        descriptor, _ = accepted(store, self.adapter, 1)
        save_state(store, [descriptor])
        first = publish_landing(store, self.adapter, "first")
        publication_root = (
            self.root
            / "06_control/source_campaigns/landing_fixture/landing_publications"
        )
        before = {path.name for path in publication_root.iterdir()}
        pointer_before = (
            self.root
            / "06_control/source_campaigns/landing_fixture/current-landing.json"
        ).read_bytes()

        second = publish_landing(store, self.adapter, "second")

        self.assertEqual(second, first)
        self.assertEqual({path.name for path in publication_root.iterdir()}, before)
        self.assertEqual(
            (
                self.root
                / "06_control/source_campaigns/landing_fixture/current-landing.json"
            ).read_bytes(),
            pointer_before,
        )

    def test_restart_appends_only_new_raw_and_preserves_prior_fragments(self):
        first_store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        first_descriptor, _ = accepted(first_store, self.adapter, 1)
        save_state(first_store, [first_descriptor])
        first = publish_landing(first_store, self.adapter, "first")

        campaign_store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        second_descriptor, _ = accepted(campaign_store, self.adapter, 2)
        save_state(campaign_store, [first_descriptor, second_descriptor])
        restarted = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        read_raw = restarted.read_raw
        raw_reads = []

        def observed_read_raw(descriptor):
            raw_reads.append(descriptor["id"])
            return read_raw(descriptor)

        restarted.read_raw = observed_read_raw
        second = publish_landing(restarted, self.adapter, "second")

        self.assertEqual(len(raw_reads), 1)
        self.assertEqual(second["row_count"], 2)
        self.assertEqual(second["files"][: len(first["files"])], first["files"])
        fresh = verify_landing(LocalCampaignStore(self.root, self.adapter.SOURCE_ID))
        self.assertEqual(fresh["snapshot_id"], second["snapshot_id"])

    def test_large_backlog_is_published_in_bounded_complete_prefixes(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        receipts = [accepted(store, self.adapter, number)[0] for number in range(1, 31)]
        save_state(store, receipts)

        first = publish_landing(store, self.adapter, "first")
        self.assertEqual(first["published_response_count"], MAX_NEW_RESPONSES)
        self.assertEqual(first["pending_publication_count"], 30 - MAX_NEW_RESPONSES)
        second = publish_landing(
            LocalCampaignStore(self.root, self.adapter.SOURCE_ID), self.adapter, "second"
        )
        self.assertEqual(second["published_response_count"], 30)
        self.assertEqual(second["pending_publication_count"], 0)
        self.assertGreater(len(second["files"]), len(first["files"]))

    def test_receipt_and_raw_tampering_are_rejected_before_any_pointer(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        descriptor, _ = accepted(store, self.adapter, 1)
        save_state(store, [descriptor])
        receipt = store.read_receipt(descriptor)
        (self.root / receipt["raw"]["id"]).write_bytes(b'{"changed":true}')

        with self.assertRaisesRegex(CampaignStoreError, "size|checksum"):
            publish_landing(store, self.adapter, "tamper")
        self.assertFalse(
            (
                self.root
                / "06_control/source_campaigns/landing_fixture/current-landing.json"
            ).exists()
        )

    def test_adapter_replay_and_request_auth_fields_are_admission_gates(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        descriptor, _ = accepted(store, self.adapter, 1)
        receipt = store.read_receipt(descriptor)
        receipt["metadata"] = {"representation": 999}
        changed = store.put_receipt(receipt)
        save_state(store, [{"task_id": "page:1", **changed}])
        with self.assertRaisesRegex(LandingPublicationError, "replay"):
            publish_landing(store, self.adapter, "replay")

        second_root = self.root / "auth"
        auth_store = LocalCampaignStore(second_root, self.adapter.SOURCE_ID)
        descriptor, _ = accepted(auth_store, self.adapter, 1)
        receipt = auth_store.read_receipt(descriptor)
        receipt["request"]["params"]["api_key"] = "must-not-publish"
        changed = auth_store.put_receipt(receipt)
        save_state(auth_store, [{"task_id": "page:1", **changed}])
        with self.assertRaisesRegex(LandingPublicationError, "request does not replay|authentication"):
            publish_landing(auth_store, self.adapter, "auth")

    def test_checkpoint_detects_reordered_accepted_membership(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        receipts = [accepted(store, self.adapter, number)[0] for number in (1, 2)]
        save_state(store, receipts)
        publish_landing(store, self.adapter, "first")
        changed = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        save_state(changed, list(reversed(receipts)))

        with self.assertRaisesRegex(LandingPublicationError, "checkpoint"):
            publish_landing(
                LocalCampaignStore(self.root, self.adapter.SOURCE_ID),
                self.adapter,
                "second",
            )

    def test_pointer_failure_retains_previous_complete_snapshot(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        first_descriptor, _ = accepted(store, self.adapter, 1)
        save_state(store, [first_descriptor])
        first = publish_landing(store, self.adapter, "first")

        next_store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        second_descriptor, _ = accepted(next_store, self.adapter, 2)
        save_state(next_store, [first_descriptor, second_descriptor])
        failing = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        original_replace = failing._store.replace

        def fail_landing_pointer(file_id, data):
            if file_id.endswith("current-landing.json"):
                raise OSError("injected pointer failure")
            return original_replace(file_id, data)

        failing._store.replace = fail_landing_pointer
        with self.assertRaisesRegex(CampaignStoreError, "previous trusted snapshot was retained"):
            publish_landing(failing, self.adapter, "second")

        current = verify_landing(LocalCampaignStore(self.root, self.adapter.SOURCE_ID))
        self.assertEqual(current["snapshot_id"], first["snapshot_id"])
        self.assertEqual(current["row_count"], 1)

    def test_malformed_candidate_is_rejected_before_pointer_promotion(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        first_descriptor, _ = accepted(store, self.adapter, 1)
        save_state(store, [first_descriptor])
        first = publish_landing(store, self.adapter, "first")

        next_store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        second_descriptor, _ = accepted(next_store, self.adapter, 2)
        save_state(next_store, [first_descriptor, second_descriptor])
        candidate_store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        real_put = candidate_store.put_landing_object

        def corrupt_manifest(name, data, **kwargs):
            if name.startswith("manifest-"):
                manifest = json.loads(data)
                manifest["row_count"] += 1
                data = json.dumps(
                    manifest, sort_keys=True, separators=(",", ":")
                ).encode()
            return real_put(name, data, **kwargs)

        candidate_store.put_landing_object = corrupt_manifest
        with self.assertRaisesRegex(LandingPublicationError, "counts|row count"):
            publish_landing(candidate_store, self.adapter, "second")

        current = verify_landing(LocalCampaignStore(self.root, self.adapter.SOURCE_ID))
        self.assertEqual(current["snapshot_id"], first["snapshot_id"])

    def test_fresh_verifier_rejects_parquet_and_manifest_tampering(self):
        store = LocalCampaignStore(self.root, self.adapter.SOURCE_ID)
        descriptor, _ = accepted(store, self.adapter, 1)
        save_state(store, [descriptor])
        manifest = publish_landing(store, self.adapter, "first")
        parquet = self.root / manifest["files"][0]["id"]
        original = parquet.read_bytes()
        parquet.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
        with self.assertRaisesRegex(CampaignStoreError, "does not match"):
            verify_landing(LocalCampaignStore(self.root, self.adapter.SOURCE_ID))

        parquet.write_bytes(original)
        pointer_path = (
            self.root
            / "06_control/source_campaigns/landing_fixture/current-landing.json"
        )
        pointer = json.loads(pointer_path.read_bytes())
        manifest_path = self.root / pointer["manifest_file_id"]
        manifest_path.write_bytes(manifest_path.read_bytes() + b" ")
        with self.assertRaisesRegex(CampaignStoreError, "does not match"):
            verify_landing(LocalCampaignStore(self.root, self.adapter.SOURCE_ID))

    def test_real_drive_state_transport_supports_two_promotions_and_cold_verify(self):
        storage = _DriveStorage()
        media_patches = (
            patch("drive_release_store.MediaIoBaseUpload", _Media),
            patch("ingestion.drive_state_store.MediaIoBaseUpload", _Media),
            patch("ingestion.drive_state_store.MediaIoBaseDownload", _Download),
        )
        with media_patches[0], media_patches[1], media_patches[2]:
            first_store = DriveCampaignStore(storage, self.adapter.SOURCE_ID)
            first_descriptor, _ = accepted(first_store, self.adapter, 1)
            save_state(first_store, [first_descriptor])
            first = publish_landing(first_store, self.adapter, "first")

            second_store = DriveCampaignStore(storage, self.adapter.SOURCE_ID)
            second_descriptor, _ = accepted(second_store, self.adapter, 2)
            save_state(second_store, [first_descriptor, second_descriptor])
            second = publish_landing(second_store, self.adapter, "second")

            cold = verify_landing(
                DriveCampaignStore(storage, self.adapter.SOURCE_ID)
            )
        self.assertNotEqual(first["snapshot_id"], second["snapshot_id"])
        self.assertEqual(cold["snapshot_id"], second["snapshot_id"])
        self.assertEqual(cold["row_count"], 2)
        landing_pointers = [
            item for item in storage.files.objects.values()
            if item["name"] == "current-landing.json"
        ]
        self.assertEqual(len(landing_pointers), 1)


if __name__ == "__main__":
    unittest.main()
