"""Safety and recovery tests for source-scoped campaign persistence."""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion.source_campaign_store import (
    CampaignCapacityError,
    CampaignStoreError,
    DriveCampaignStore,
    LocalCampaignStore,
    MAX_RAW_BYTES,
    MAX_STATE_BYTES,
    UncertainCampaignWriteError,
)


class MemoryDriveObjects:
    """Drive-shaped object transport with deterministic failure injection."""

    def __init__(self) -> None:
        self.next_id = 1
        self.files: dict[str, dict] = {}
        self.folders: dict[str, dict] = {
            "selectedroot": {"name": "selected-root", "parent": None},
            "landingroot": {"name": "01_landing", "parent": "selectedroot"},
            "nbpcontrol": {"name": "nbp", "parent": "selectedroot"},
        }
        self.fail_create_name: str | None = None
        self.raise_after_create_name: str | None = None
        self.fail_replace = False
        self.raise_after_replace = False
        self.on_create = None
        self.on_replace = None

    def _id(self) -> str:
        value = f"object{self.next_id}"
        self.next_id += 1
        return value

    def find(self, name: str, parent_id: str) -> list[str]:
        files = [
            object_id
            for object_id, item in self.files.items()
            if item["name"] == name and item["parent"] == parent_id
        ]
        folders = [
            object_id
            for object_id, item in self.folders.items()
            if item["name"] == name and item["parent"] == parent_id
        ]
        return files + folders

    def create(self, name: str, data: bytes, parent_id: str) -> str:
        if name == self.fail_create_name:
            raise OSError("injected create failure")
        object_id = self._id()
        self.files[object_id] = {"name": name, "data": data, "parent": parent_id}
        if self.on_create is not None:
            self.on_create(name, object_id)
        if name == self.raise_after_create_name:
            raise TimeoutError("created but response was lost")
        return object_id

    def read(self, file_id: str) -> bytes:
        return self.files[file_id]["data"]

    def replace(self, file_id: str, data: bytes) -> None:
        if self.fail_replace:
            raise OSError("injected replacement failure")
        self.files[file_id]["data"] = data
        if self.on_replace is not None:
            self.on_replace(file_id)
        if self.raise_after_replace:
            raise TimeoutError("replacement response was lost")

    def mkdir(self, name: str, parent_id: str) -> str:
        object_id = self._id()
        self.folders[object_id] = {"name": name, "parent": parent_id}
        return object_id


class FakeStorage:
    def __init__(self, objects: MemoryDriveObjects) -> None:
        self.objects = objects
        self.nested_calls: list[tuple[list[str], str]] = []

    def resolve_root(self, *, create: bool) -> str:
        self.root_create = create
        return "selectedroot"

    def resolve_zone(self, name: str, *, create: bool) -> str:
        self.zone_call = (name, create)
        return "landingroot"

    def get_or_create_nested_folder(self, segments: list[str], *, root_id: str) -> str:
        self.nested_calls.append((list(segments), root_id))
        parent = root_id
        for segment in segments:
            found = [
                item for item in self.objects.find(segment, parent)
                if item in self.objects.folders
            ]
            if len(found) > 1:
                raise ValueError("ambiguous fake Drive folder")
            parent = found[0] if found else self.objects.mkdir(segment, parent)
        return parent


def drive_store(objects: MemoryDriveObjects, source_id: str = "world_bank"):
    storage = FakeStorage(objects)
    fake_module = ModuleType("ingestion.drive_state_store")
    fake_module.DriveStateStore = lambda _storage, _root, _control: objects
    with patch.dict(sys.modules, {"ingestion.drive_state_store": fake_module}):
        store = DriveCampaignStore(storage, source_id)
    return store, storage


class LocalCampaignStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = LocalCampaignStore(self.root, "world_bank")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_state_and_raw_roundtrip_are_immutable_and_verified(self):
        self.assertIsNone(self.store.load())
        state = {"quota": {"reserved": 3}, "cursor": "2026-09-08"}
        self.store.save(state)
        self.assertEqual(LocalCampaignStore(self.root, "world_bank").load(), state)

        body = b'{"value":1}\n'
        descriptor = self.store.put_raw(body, {"kind": "accepted_receipt", "status": 200})
        self.assertEqual(
            descriptor,
            {
                "id": descriptor["id"],
                "sha256": sha256(body).hexdigest(),
                "size_bytes": len(body),
            },
        )
        self.assertEqual(self.store.read_raw(descriptor), body)
        self.assertTrue((self.root / descriptor["id"]).is_file())

        # Identical bytes reuse the exact immutable response, while each
        # distinct accepted receipt remains durable and content addressed.
        repeated = self.store.put_raw(body, {"kind": "accepted_receipt", "status": 304})
        self.assertEqual(repeated, descriptor)
        receipts = list(
            (self.root / "06_control/source_campaigns/world_bank/receipts").glob("*.json")
        )
        self.assertEqual(len(receipts), 2)

    def test_accepted_receipt_roundtrip_supports_fresh_process_raw_replay(self):
        raw_descriptor = self.store.put_raw(
            b'{"value":1}', {"task_id": "recent:2026-09-08"}
        )
        accepted = {
            "schema_version": 1,
            "source_id": "world_bank",
            "task": {"id": "recent:2026-09-08"},
            "raw": raw_descriptor,
            "record_count": 1,
            "accepted": True,
        }
        receipt_descriptor = self.store.put_receipt(accepted)

        fresh = LocalCampaignStore(self.root, "world_bank")
        restored = fresh.read_receipt(receipt_descriptor)
        self.assertEqual(restored, accepted)
        self.assertEqual(fresh.read_raw(restored["raw"]), b'{"value":1}')

    def test_receipt_tampering_and_cross_source_descriptor_are_rejected(self):
        receipt = {
            "schema_version": 1,
            "source_id": "world_bank",
            "accepted": True,
        }
        descriptor = self.store.put_receipt(receipt)
        other = LocalCampaignStore(self.root, "eurostat")
        with self.assertRaisesRegex(CampaignStoreError, "outside this source"):
            other.read_receipt(descriptor)

        receipt_path = self.root / descriptor["id"]
        original = receipt_path.read_bytes()
        receipt_path.write_bytes(original.replace(b"true", b"null"))
        with self.assertRaisesRegex(CampaignStoreError, "size|checksum"):
            self.store.read_receipt(descriptor)

    def test_tampering_is_detected_for_raw_and_state(self):
        descriptor = self.store.put_raw(b"exact bytes", {"kind": "accepted_receipt"})
        (self.root / descriptor["id"]).write_bytes(b"changed")
        with self.assertRaisesRegex(CampaignStoreError, "size|checksum"):
            self.store.read_raw(descriptor)

        self.store.save({"cursor": 1})
        pointer_path = (
            self.root
            / "06_control/source_campaigns/world_bank/current-ingestion-state.json"
        )
        pointer = json.loads(pointer_path.read_bytes())
        (self.root / pointer["state_file_id"]).write_bytes(b'{"cursor":2}')
        with self.assertRaisesRegex(CampaignStoreError, "does not match"):
            LocalCampaignStore(self.root, "world_bank").load()

    def test_stale_loaded_writer_cannot_replace_a_newer_pointer(self):
        self.store.save({"cursor": 1})
        stale = LocalCampaignStore(self.root, "world_bank")
        self.assertEqual(stale.load(), {"cursor": 1})
        current = LocalCampaignStore(self.root, "world_bank")
        current.save({"cursor": 2})

        with self.assertRaisesRegex(CampaignStoreError, "changed since"):
            stale.save({"cursor": 3})
        self.assertEqual(LocalCampaignStore(self.root, "world_bank").load(), {"cursor": 2})

    def test_failed_pointer_replace_preserves_previous_trusted_state(self):
        self.store.save({"cursor": 1})
        self.assertEqual(self.store.load(), {"cursor": 1})
        previous_pointer = (
            self.root
            / "06_control/source_campaigns/world_bank/current-ingestion-state.json"
        ).read_bytes()

        def fail_replace(_file_id, _data):
            raise OSError("disk full")

        self.store._store.replace = fail_replace
        with self.assertRaisesRegex(CampaignStoreError, "previous trusted state was retained"):
            self.store.save({"cursor": 2})
        self.assertEqual(
            (
                self.root
                / "06_control/source_campaigns/world_bank/current-ingestion-state.json"
            ).read_bytes(),
            previous_pointer,
        )
        self.assertEqual(LocalCampaignStore(self.root, "world_bank").load(), {"cursor": 1})

    def test_source_namespaces_and_descriptors_cannot_cross(self):
        descriptor = self.store.put_raw(b"same", {"kind": "accepted_receipt"})
        other = LocalCampaignStore(self.root, "eurostat")
        other_descriptor = other.put_raw(b"same", {"kind": "accepted_receipt"})
        self.assertNotEqual(descriptor["id"], other_descriptor["id"])
        with self.assertRaisesRegex(CampaignStoreError, "outside this source"):
            other.read_raw(descriptor)
        other.save({"cursor": "other"})
        self.store.save({"cursor": "first"})
        self.assertEqual(other.load(), {"cursor": "other"})
        self.assertEqual(self.store.load(), {"cursor": "first"})

    def test_local_symlink_cannot_redirect_a_descriptor_to_another_source(self):
        descriptor = self.store.put_raw(b"shared", {"kind": "accepted_receipt"})
        other = LocalCampaignStore(self.root, "eurostat")
        other_descriptor = other.put_raw(b"shared", {"kind": "accepted_receipt"})
        raw_path = self.root / descriptor["id"]
        raw_path.unlink()
        raw_path.symlink_to(self.root / other_descriptor["id"])
        with self.assertRaisesRegex(CampaignStoreError, "symbolic link"):
            self.store.read_raw(descriptor)

    def test_unsafe_source_ids_and_capacity_limits_fail_clearly(self):
        for source_id in ("../escape", "UPPER", "has-dash", "a/b", ""):
            with self.subTest(source_id=source_id):
                with self.assertRaisesRegex(CampaignStoreError, "unsafe source_id"):
                    LocalCampaignStore(self.root, source_id)
        with self.assertRaisesRegex(CampaignCapacityError, "4 MiB"):
            self.store.save({"payload": "x" * MAX_STATE_BYTES})
        with self.assertRaisesRegex(CampaignCapacityError, "8 MiB"):
            self.store.put_raw(
                b"x" * (MAX_RAW_BYTES + 1), {"kind": "accepted_receipt"}
            )


class DriveCampaignStoreTests(unittest.TestCase):
    def test_selected_root_paths_and_nbp_pointer_are_isolated(self):
        objects = MemoryDriveObjects()
        objects.files["nbppointer"] = {
            "name": "current-ingestion-state.json",
            "parent": "nbpcontrol",
            "data": b"NBP pointer bytes",
        }
        store, storage = drive_store(objects)
        self.assertEqual(
            storage.nested_calls,
            [
                (["06_control", "source_campaigns", "world_bank"], "selectedroot"),
                (["world_bank", "responses"], "landingroot"),
            ],
        )
        self.assertEqual(storage.zone_call, ("landing", True))

        store.save({"cursor": 1})
        descriptor = store.put_raw(b"drive bytes", {"kind": "accepted_receipt"})
        self.assertEqual(store.read_raw(descriptor), b"drive bytes")
        accepted = {
            "schema_version": 1,
            "source_id": "world_bank",
            "raw": descriptor,
            "accepted": True,
        }
        receipt_descriptor = store.put_receipt(accepted)
        self.assertEqual(drive_store(objects)[0].read_receipt(receipt_descriptor), accepted)
        self.assertEqual(objects.files["nbppointer"]["data"], b"NBP pointer bytes")

    def test_ambiguous_pointer_stops_without_promotion(self):
        objects = MemoryDriveObjects()
        store, _ = drive_store(objects)
        store.save({"cursor": 1})
        pointer = objects.find("current-ingestion-state.json", store._control_root_id)[0]
        duplicate = objects._id()
        objects.files[duplicate] = dict(objects.files[pointer])
        with self.assertRaisesRegex(CampaignStoreError, "ambiguous current"):
            drive_store(objects)[0].load()

    def test_lost_immutable_write_reply_is_verified_by_exact_bytes(self):
        objects = MemoryDriveObjects()
        body = b"exact response"
        objects.raise_after_create_name = f"response-{sha256(body).hexdigest()}.bin"
        store, _ = drive_store(objects)
        descriptor = store.put_raw(body, {"kind": "accepted_receipt"})
        self.assertEqual(store.read_raw(descriptor), body)

    def test_unverifiable_raw_write_exposes_uncertain_flag(self):
        objects = MemoryDriveObjects()
        body = b"cannot prove"
        objects.fail_create_name = f"response-{sha256(body).hexdigest()}.bin"
        store, _ = drive_store(objects)
        with self.assertRaises(UncertainCampaignWriteError) as raised:
            store.put_raw(body, {"kind": "accepted_receipt"})
        self.assertTrue(raised.exception.uncertain)

    def test_lost_pointer_reply_is_success_only_after_exact_readback(self):
        objects = MemoryDriveObjects()
        store, _ = drive_store(objects)
        store.save({"cursor": 1})
        objects.raise_after_replace = True
        store.save({"cursor": 2})
        self.assertEqual(drive_store(objects)[0].load(), {"cursor": 2})

    def test_pointer_byte_drift_during_snapshot_upload_stops_promotion(self):
        objects = MemoryDriveObjects()
        store, _ = drive_store(objects)
        store.save({"cursor": 1})
        self.assertEqual(store.load(), {"cursor": 1})
        pointer_id = objects.find(
            "current-ingestion-state.json", store._control_root_id
        )[0]
        previous = json.loads(objects.files[pointer_id]["data"])
        competing = {**previous, "writer": "competing-operation"}
        competing_raw = json.dumps(
            competing, sort_keys=True, separators=(",", ":")
        ).encode()

        def drift_on_snapshot(name, _object_id):
            if name.startswith("state-"):
                objects.files[pointer_id]["data"] = competing_raw

        objects.on_create = drift_on_snapshot
        with self.assertRaisesRegex(CampaignStoreError, "changed during state upload"):
            store.save({"cursor": 2})
        self.assertEqual(objects.files[pointer_id]["data"], competing_raw)

    def test_ambiguous_pointer_readback_after_write_is_uncertain(self):
        objects = MemoryDriveObjects()
        store, _ = drive_store(objects)
        store.save({"cursor": 1})

        def duplicate_pointer(pointer_id):
            duplicate = objects._id()
            objects.files[duplicate] = dict(objects.files[pointer_id])

        objects.on_replace = duplicate_pointer
        with self.assertRaises(CampaignStoreError) as raised:
            store.save({"cursor": 2})
        self.assertTrue(raised.exception.uncertain)


if __name__ == "__main__":
    unittest.main()
