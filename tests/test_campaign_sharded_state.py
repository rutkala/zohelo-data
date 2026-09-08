"""Migration and integrity tests for the scalable campaign state format."""
from __future__ import annotations

from copy import deepcopy
from datetime import date
from hashlib import sha256
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion import source_campaign_store as campaign_store  # noqa: E402
from ingestion.source_campaign import new_state  # noqa: E402
from ingestion.source_campaign_store import (  # noqa: E402
    CampaignCapacityError,
    CampaignStoreError,
    LocalCampaignStore,
    UncertainCampaignWriteError,
)


def canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def task(number: int, lane: str = "history", payload: str = "") -> dict:
    return {
        "id": f"task:{number:06d}",
        "lane": lane,
        "kind": f"{lane}_page",
        "cursor": {"page": number, "payload": payload},
    }


class ShardedCampaignStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _store(self, source_id: str = "fixture", **kwargs) -> LocalCampaignStore:
        return LocalCampaignStore(self.root, source_id, **kwargs)

    def _state(self) -> dict:
        state = new_state("fixture", date(2026, 9, 8))
        state["pending"] = [task(1), task(2, "recent")]
        state["completed"] = {"old:1": "2026-09-08T01:02:03+00:00"}
        state["recent_roots"] = {"series:2": task(2, "recent")}
        state["quota_attempts"] = [100.0, 200.0]
        state["provider_retry_at"] = 300.0
        return state

    def _install_v1_pointer(self, state: dict) -> tuple[Path, bytes]:
        store = self._store()
        states = Path(store._states_root())
        # Local object IDs are relative to the selected local root.
        snapshot_name = f"state-{uuid4()}.json"
        snapshot_path = self.root / states / snapshot_name
        state_raw = canonical(state)
        snapshot_path.write_bytes(state_raw)
        pointer = {
            "format_version": 1,
            "source_id": "fixture",
            "state_file_id": snapshot_path.relative_to(self.root).as_posix(),
            "state_file_name": snapshot_name,
            "state_sha256": sha256(state_raw).hexdigest(),
            "state_size_bytes": len(state_raw),
        }
        pointer_path = (
            self.root
            / "06_control/source_campaigns/fixture/current-ingestion-state.json"
        )
        pointer_raw = canonical(pointer)
        pointer_path.write_bytes(pointer_raw)
        return snapshot_path, pointer_raw

    def _pointer(self) -> tuple[Path, dict]:
        path = (
            self.root
            / "06_control/source_campaigns/fixture/current-ingestion-state.json"
        )
        return path, json.loads(path.read_bytes())

    def test_first_save_atomically_migrates_v1_and_preserves_external_evidence(self):
        store = self._store()
        raw_descriptor = store.put_raw(b'{"retained":true}', {"task_id": "old:1"})
        receipt_descriptor = store.put_receipt(
            {"source_id": "fixture", "accepted": True, "raw": raw_descriptor}
        )
        state = self._state()
        state["receipts"] = [{"task_id": "old:1", **receipt_descriptor}]
        state["accepted_responses"] = 1
        snapshot_path, old_pointer = self._install_v1_pointer(state)
        landing_pointer = (
            self.root / "06_control/source_campaigns/fixture/current-landing.json"
        )
        landing_pointer.write_bytes(b"retained Landing pointer bytes")
        raw_path = self.root / raw_descriptor["id"]
        receipt_path = self.root / receipt_descriptor["id"]
        raw_before, receipt_before = raw_path.read_bytes(), receipt_path.read_bytes()

        migrating = self._store()
        self.assertEqual(migrating.load(), state)
        migrating.save(deepcopy(state))

        _, pointer = self._pointer()
        self.assertEqual(pointer["format_version"], 2)
        self.assertEqual(self._store().load(), state)
        self.assertTrue(snapshot_path.exists())
        self.assertNotEqual(canonical(pointer), old_pointer)
        self.assertEqual(raw_path.read_bytes(), raw_before)
        self.assertEqual(receipt_path.read_bytes(), receipt_before)
        self.assertEqual(landing_pointer.read_bytes(), b"retained Landing pointer bytes")

    def test_v1_migration_pointer_failure_leaves_v1_authoritative(self):
        state = self._state()
        snapshot_path, pointer_before = self._install_v1_pointer(state)
        store = self._store()
        self.assertEqual(store.load(), state)

        def fail_replace(_file_id: str, _data: bytes) -> None:
            raise OSError("injected")

        store._store.replace = fail_replace
        with self.assertRaisesRegex(CampaignStoreError, "previous trusted state was retained"):
            store.save(state)
        pointer_path, pointer = self._pointer()
        self.assertEqual(pointer_path.read_bytes(), pointer_before)
        self.assertEqual(pointer["format_version"], 1)
        self.assertTrue(snapshot_path.exists())
        self.assertEqual(self._store().load(), state)

    def test_v1_snapshot_with_cross_source_identity_is_rejected(self):
        state = self._state()
        state["source_id"] = "other"
        self._install_v1_pointer(state)

        with self.assertRaisesRegex(CampaignStoreError, "source identity"):
            self._store().load()

    def test_all_collection_shapes_are_validated_before_first_shard_write(self):
        state = self._state()
        state["completed"] = []
        store = self._store()
        states_root = (
            self.root / "06_control/source_campaigns/fixture/states"
        )

        with self.assertRaisesRegex(CampaignStoreError, "completed must be an object"):
            store.save(state)
        self.assertFalse(states_root.exists())

        state["pending"][0]["cursor"]["changed"] = True
        with self.assertRaisesRegex(CampaignStoreError, "completed must be an object"):
            store.save(state)
        self.assertFalse(states_root.exists())

    def test_post_create_verification_failure_is_marked_uncertain(self):
        state = self._state()
        store = self._store()
        original_create = store._store.create
        original_find = store._store.find
        created_names: set[str] = set()
        fail_verification = True

        def observed_create(name: str, data: bytes, parent_id: str) -> str:
            object_id = original_create(name, data, parent_id)
            created_names.add(name)
            return object_id

        def fail_first_post_create_find(name: str, parent_id: str) -> list[str]:
            nonlocal fail_verification
            if fail_verification and name in created_names:
                fail_verification = False
                raise OSError("injected post-create lookup failure")
            return original_find(name, parent_id)

        store._store.create = observed_create
        store._store.find = fail_first_post_create_find
        with self.assertRaises(UncertainCampaignWriteError) as raised:
            store.save(state)
        self.assertTrue(raised.exception.uncertain)
        pointer_path = (
            self.root
            / "06_control/source_campaigns/fixture/current-ingestion-state.json"
        )
        self.assertFalse(pointer_path.exists())
        states_root = self.root / "06_control/source_campaigns/fixture/states"
        self.assertEqual(len(list(states_root.glob("state-*.json"))), 1)

    def test_one_task_change_reuses_every_unaffected_content_addressed_shard(self):
        state = self._state()
        state["pending"] = [task(index) for index in range(600)]
        store = self._store()
        store.save(state)
        states_root = self.root / "06_control/source_campaigns/fixture/states"
        before = {path.name for path in states_root.iterdir()}

        loaded = store.load()
        loaded["pending"][300]["cursor"]["changed"] = True
        store.save(loaded)
        after = {path.name for path in states_root.iterdir()}

        # One hash-prefix shard and its manifest change. All other immutable
        # objects are addressed by their content and reused without replacement.
        self.assertEqual(len(after - before), 2)
        self.assertEqual(self._store().load(), loaded)
        pointer_before = self._pointer()[0].read_bytes()
        object_count = len(after)
        store.save(deepcopy(loaded))
        self.assertEqual(self._pointer()[0].read_bytes(), pointer_before)
        self.assertEqual(len(list(states_root.iterdir())), object_count)

    def test_cached_load_checks_pointer_but_skips_unchanged_shard_downloads(self):
        state = self._state()
        state["pending"] = [task(index) for index in range(100)]
        store = self._store()
        store.save(state)
        self.assertEqual(store.load(), state)
        original_read = store._store.read
        reads: list[str] = []

        def observed_read(object_id: str) -> bytes:
            reads.append(object_id)
            return original_read(object_id)

        store._store.read = observed_read
        first = store.load_cached()
        first["pending"].clear()
        second = store.load_cached()

        pointer_id = self._pointer()[1]
        self.assertEqual(len(reads), 2)  # one exact pointer read per cached load
        self.assertEqual(second, state)
        self.assertNotEqual(pointer_id["manifest_file_id"], reads[0])

    def test_receipt_segments_roll_and_preserve_exact_order(self):
        state = self._state()
        state["receipts"] = [
            {"task_id": f"receipt:{index}", "id": f"id-{index}", "sha256": "a" * 64, "size_bytes": 1}
            for index in range(campaign_store.RECEIPT_SEGMENT_ITEMS)
        ]
        store = self._store()
        store.save(state)
        first_pointer = self._pointer()[1]
        first_manifest = json.loads(
            (self.root / first_pointer["manifest_file_id"]).read_bytes()
        )
        first_segment = first_manifest["collections"]["receipts"][0]

        loaded = store.load()
        loaded["receipts"].append(
            {"task_id": "receipt:new", "id": "id-new", "sha256": "b" * 64, "size_bytes": 2}
        )
        store.save(loaded)
        second_pointer = self._pointer()[1]
        second_manifest = json.loads(
            (self.root / second_pointer["manifest_file_id"]).read_bytes()
        )
        self.assertEqual(len(second_manifest["collections"]["receipts"]), 1)
        second_head = second_manifest["collections"]["receipts"][0]
        second_segment = json.loads((self.root / second_head["id"]).read_bytes())
        self.assertEqual(second_segment["previous"], first_segment)
        self.assertEqual(second_segment["total_items"], len(loaded["receipts"]))
        self.assertEqual(self._store().load()["receipts"], loaded["receipts"])

    def test_hash_bucket_splits_adaptively_and_round_trips_order(self):
        colliding: list[dict] = []
        wanted_prefix = None
        candidate = 0
        while len(colliding) < 12:
            item = task(candidate, payload="x" * 220)
            prefix = sha256(item["id"].encode()).hexdigest()[:1]
            wanted_prefix = wanted_prefix or prefix
            if prefix == wanted_prefix:
                colliding.append(item)
            candidate += 1
        state = self._state()
        state["pending"] = colliding

        with patch.object(campaign_store, "MAX_STATE_SHARD_BYTES", 1300):
            self._store().save(state)
        _, pointer = self._pointer()
        manifest = json.loads((self.root / pointer["manifest_file_id"]).read_bytes())
        keys = [item["key"] for item in manifest["collections"]["pending"]]
        self.assertGreater(len(keys), 1)
        self.assertTrue(all(len(key) > 1 for key in keys))
        self.assertEqual(self._store().load()["pending"], colliding)

    def test_sharded_collections_can_exceed_the_legacy_four_mib_snapshot(self):
        state = self._state()
        state["pending"] = [task(index, payload="x" * 1100) for index in range(4000)]
        self.assertGreater(len(canonical(state)), campaign_store.MAX_STATE_BYTES)

        self._store().save(state)

        _, pointer = self._pointer()
        self.assertEqual(pointer["format_version"], 2)
        self.assertLessEqual(
            pointer["manifest_size_bytes"], campaign_store.MAX_STATE_MANIFEST_BYTES
        )
        self.assertEqual(self._store().load(), state)

    def test_state_shard_byte_tampering_is_detected_before_materialization(self):
        state = self._state()
        self._store().save(state)
        _, pointer = self._pointer()
        manifest = json.loads((self.root / pointer["manifest_file_id"]).read_bytes())
        shard = manifest["collections"]["pending"][0]
        (self.root / shard["id"]).write_bytes(b'{"tampered":true}')

        with self.assertRaisesRegex(CampaignStoreError, "does not match its descriptor"):
            self._store().load()

    def test_duplicate_descriptor_and_cross_namespace_descriptor_fail_closed(self):
        state = self._state()
        store = self._store()
        store.save(state)
        pointer_path, pointer = self._pointer()
        manifest_path = self.root / pointer["manifest_file_id"]
        manifest = json.loads(manifest_path.read_bytes())
        descriptor = manifest["collections"]["pending"][0]
        manifest["collections"]["pending"].append(deepcopy(descriptor))
        changed = canonical(manifest)
        digest = sha256(changed).hexdigest()
        name = f"state-manifest-root-{digest}.json"
        changed_path = manifest_path.with_name(name)
        changed_path.write_bytes(changed)
        relative = changed_path.relative_to(self.root).as_posix()
        for prefix in ("manifest_", "state_"):
            pointer[f"{prefix}file_id"] = relative
            pointer[f"{prefix}file_name"] = name
            pointer[f"{prefix}sha256"] = digest
            pointer[f"{prefix}size_bytes"] = len(changed)
        pointer_path.write_bytes(canonical(pointer))

        with self.assertRaisesRegex(CampaignStoreError, "duplicate shard keys|reuses|root shard"):
            self._store().load()

        # A descriptor cannot redirect through an object in another source's
        # state folder, even when the bytes and declared digest are exact.
        clean_root = self.root / "cross"
        own = LocalCampaignStore(clean_root, "fixture")
        own.save(state)
        other = LocalCampaignStore(clean_root, "other")
        other.save({"pending": [task(99)]})
        own_pointer_path = clean_root / "06_control/source_campaigns/fixture/current-ingestion-state.json"
        own_pointer = json.loads(own_pointer_path.read_bytes())
        own_manifest_path = clean_root / own_pointer["manifest_file_id"]
        own_manifest = json.loads(own_manifest_path.read_bytes())
        other_pointer = json.loads(
            (clean_root / "06_control/source_campaigns/other/current-ingestion-state.json").read_bytes()
        )
        other_manifest = json.loads((clean_root / other_pointer["manifest_file_id"]).read_bytes())
        foreign = other_manifest["collections"]["pending"][0]
        own_manifest["collections"]["pending"][0] = foreign
        changed = canonical(own_manifest)
        digest = sha256(changed).hexdigest()
        name = f"state-manifest-root-{digest}.json"
        changed_path = own_manifest_path.with_name(name)
        changed_path.write_bytes(changed)
        relative = changed_path.relative_to(clean_root).as_posix()
        for prefix in ("manifest_", "state_"):
            own_pointer[f"{prefix}file_id"] = relative
            own_pointer[f"{prefix}file_name"] = name
            own_pointer[f"{prefix}sha256"] = digest
            own_pointer[f"{prefix}size_bytes"] = len(changed)
        own_pointer_path.write_bytes(canonical(own_pointer))
        with self.assertRaisesRegex(CampaignStoreError, "outside its source namespace"):
            LocalCampaignStore(clean_root, "fixture").load()

    def test_optional_materialization_budget_is_an_operator_resource_guard(self):
        state = self._state()
        self._store().save(state)
        with self.assertRaisesRegex(CampaignCapacityError, "configured materialized-runner"):
            self._store(max_materialized_bytes=10).load()


if __name__ == "__main__":
    unittest.main()
