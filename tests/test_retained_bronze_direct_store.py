"""No-Landing, no-copy, exact-reference and bounded-reader regressions."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT / 'scripts'))
from retained_bronze_store import RetainedBronzeDriveStore
from ingestion.source_campaign_store import CampaignStoreError
import audit_retained_dbw_bronze as audit


class MemoryTransport:
    def __init__(self):
        self.items = {'root': {'name': 'root', 'parent': None, 'data': None}}
        self.created = []
    def find(self, name, parent):
        return [key for key, item in self.items.items() if item['name'] == name and item['parent'] == parent]
    def mkdir(self, name, parent):
        return self.create(name, None, parent)
    def create(self, name, data, parent):
        key = 'id-' + str(len(self.items))
        self.items[key] = {'name': name, 'parent': parent, 'data': data}
        self.created.append((name, parent, data))
        return key
    def replace(self, key, data):
        self.items[key]['data'] = data
    def read(self, key):
        return self.items[key]['data']
    def list_metadata(self, parent):
        return [{'id': key, 'name': item['name'], 'size': len(item['data']),
                 'sha256': hashlib.sha256(item['data']).hexdigest(), 'trashed': False,
                 'kind': 'application/octet-stream'}
                for key, item in self.items.items() if item['parent'] == parent and item['data'] is not None]


class Storage:
    def __init__(self, transport):
        self.transport = transport
        self.zone_requests = []
        self.root_requests = []
        self.bronze = transport.mkdir('02_bronze', 'root')
        transport.mkdir('gus_dbw', self.bronze)
    def resolve_root(self, *, create):
        self.root_requests.append(create)
        return 'root'
    def resolve_zone(self, zone, *, create):
        self.zone_requests.append((zone, create))
        if zone != 'bronze':
            raise AssertionError('Publication must never initialize Landing')
        return self.bronze
    def get_or_create_nested_folder(self, names, *, root_id):
        parent = root_id
        for name in names:
            existing = self.transport.find(name, parent)
            parent = existing[0] if existing else self.transport.mkdir(name, parent)
        return parent


class DirectStoreTests(unittest.TestCase):
    def setUp(self):
        self.transport = MemoryTransport()
        self.storage = Storage(self.transport)
        self.raw = b'existing-reviewed-parquet-fixture'
        self.reference = {'id': 'original-7', 'name': 'part_7.parquet', 'size': len(self.raw),
                          'sha256': hashlib.sha256(self.raw).hexdigest(), 'path': 'observations/part_7.parquet'}
        self.inventory = {'objects': [self.reference], 'inventory_sha256': 'a'*64}
        with patch('retained_bronze_store.validate_audit', return_value=({}, self.inventory, 'b'*64)), \
             patch('ingestion.drive_state_store.DriveStateStore', return_value=self.transport):
            self.store = RetainedBronzeDriveStore(self.storage, Path('fixture'), lambda: None)

    def test_constructor_never_resolves_landing_and_refuses_source_responses(self):
        self.assertEqual(self.storage.root_requests, [False])
        self.assertEqual(self.storage.zone_requests, [])
        self.assertNotIn('responses', [entry[0] for entry in self.transport.created])
        with self.assertRaisesRegex(CampaignStoreError, 'cannot receive'):
            self.store.put_raw(b'x', {})

    def test_original_registration_preserves_id_name_and_hash_without_any_upload(self):
        before = list(self.transport.created)
        result = self.store.put_or_reuse_landing_object('observations-7-1', 'parquet', self.raw)
        self.assertEqual(result, {k: self.reference[k] for k in ('id','name','size','sha256')})
        self.assertEqual(self.transport.created, before)
        self.assertEqual(self.storage.zone_requests, [])
        with self.assertRaisesRegex(CampaignStoreError, 'bytes differ'):
            self.store.put_or_reuse_landing_object('observations-7-1', 'parquet', b'changed')

    def test_changed_or_unreviewed_reference_fails(self):
        for key, value in [('id', 'not-reviewed'), ('name', 'part_8.parquet'), ('size', 42), ('sha256', 'c'*64)]:
            changed = dict(self.reference); changed[key] = value
            with self.subTest(key=key), self.assertRaises(CampaignStoreError):
                self.store.assert_reference(changed)

    def test_new_query_parts_are_under_bronze_and_retries_reuse_exact_bytes(self):
        self.store.acquire_publication_owner('fixture-owner')
        first = self.store.put_or_reuse_landing_object('observations-8-1', 'parquet', b'bounded-query-part')
        parent = self.transport.items[first['id']]['parent']
        self.assertEqual(parent, self.store._derived_root_id)
        names = [entry[0] for entry in self.transport.created]
        self.assertIn('query_parts', names)
        self.assertNotIn('01_landing', names)
        self.assertNotIn('responses', names)
        before = len(self.transport.created)
        second = self.store.put_or_reuse_landing_object('observations-8-1', 'parquet', b'bounded-query-part')
        self.assertEqual(first, second)
        self.assertEqual(before, len(self.transport.created))
        self.assertEqual(self.store.read_landing_object(first), b'bounded-query-part')
        self.transport.items[first['id']]['data'] = b'tampered'
        with self.assertRaisesRegex(CampaignStoreError, 'pinned descriptor'):
            self.store.read_landing_object(first)

    def test_original_inventory_drift_stops_before_pointer_promotion(self):
        self.store.retained_publication_guard = Mock()
        with patch.object(self.store, 'verify_original_inventory', side_effect=CampaignStoreError('changed')):
            before = list(self.transport.created)
            with self.assertRaisesRegex(CampaignStoreError, 'changed'):
                self.store.promote_landing_pointer({})
            self.assertEqual(before, self.transport.created)

    def test_guard_denial_precedes_any_storage_initialization(self):
        with patch('retained_bronze_store.validate_audit', return_value=({}, self.inventory, 'b'*64)):
            storage = Mock()
            with self.assertRaisesRegex(RuntimeError, 'owned'):
                RetainedBronzeDriveStore(storage, Path('fixture'), Mock(side_effect=RuntimeError('owned')))
            storage.resolve_root.assert_not_called()


class DirectPublicationIntegrationTests(unittest.TestCase):
    def test_full_registration_upgrades_v1_without_reuploading_existing_parquet(self):
        import duckdb
        import test_retained_dbw_publication as fixtures
        import retained_dbw_publication as publication
        from ingestion.source_campaign_store import _CampaignStore
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package = fixtures.RetainedPublicationTests().fixture(root)
            inventory_path = package / "descriptor-inventory.json"
            inventory = json.loads(inventory_path.read_text())
            by_path = {item["path"]: item for item in inventory["objects"]}
            cache = package / "verified-cache"
            with duckdb.connect() as con:
                con.execute("SET threads=1")
                for identity in range(4, 1551):
                    target = cache / f"observations/part_{identity}.parquet"
                    con.execute(
                        f"COPY (SELECT * REPLACE ({identity}::BIGINT AS indicator_id) FROM read_parquet($source)) TO $destination (FORMAT PARQUET)",
                        {"source": str(cache / "observations/part_1.parquet"), "destination": str(target)},
                    )
                    raw = target.read_bytes()
                    by_path[f"observations/part_{identity}.parquet"].update(
                        size=len(raw), sha256=hashlib.sha256(raw).hexdigest(), md5=hashlib.md5(raw).hexdigest(),
                    )
            for n, item in enumerate(inventory["objects"]):
                item["id"] = f"original-{n}"
            inventory["total_bytes"] = sum(item["size"] for item in inventory["objects"])
            inventory["inventory_sha256"] = hashlib.sha256(publication.canonical(inventory["objects"])).hexdigest()
            inventory_path.write_text(json.dumps(inventory))
            report_path = package / "audit-report.json"
            report = json.loads(report_path.read_text())
            report["inventory_sha256"] = inventory["inventory_sha256"]
            report["measured_parquet_rows"]["observations"] = 1550
            report_path.write_text(json.dumps(report))
            (package / "run-status.json").write_text(json.dumps({
                "status":"complete", "run_id":report["run_id"], "inventory_sha256":inventory["inventory_sha256"],
            }))
            transport = MemoryTransport()
            storage = Storage(transport)
            control = storage.get_or_create_nested_folder(["06_control", "source_campaigns", publication.SOURCE_ID], root_id="root")
            legacy = _CampaignStore(transport, publication.SOURCE_ID, control, "root")
            first = publication.publish_retained_bronze(legacy, package, root / "legacy", "a"*40, max_indicators=1)
            self.assertEqual(first["format_version"], 1)
            self.assertEqual(first["published_indicator_count"], 1)
            previous_payloads = {key: dict(value) for key,value in transport.items.items() if value["name"].endswith('.parquet')}
            before = len(transport.created)
            with patch.object(publication, 'REVIEWED_INVENTORY_SHA256', inventory["inventory_sha256"]), \
                 patch.object(publication, 'REVIEWED_AUDIT_REPORT_SHA256', publication.file_sha(report_path)), \
                 patch('ingestion.drive_state_store.DriveStateStore', return_value=transport):
                store = RetainedBronzeDriveStore(storage, package, lambda: None)
                with patch.object(store, 'verify_original_inventory'):
                    result = publication.publish_retained_bronze(store, package, root / "direct", "b"*40)
                    self.assertEqual(result["format_version"], 2)
                    self.assertEqual(result["published_indicator_count"], 1550)
                    self.assertEqual(result["pending_indicator_count"], 0)
                    index = json.loads(store.read_landing_object(result["indicator_index"]))
                    for item in index["indicators"]:
                        expected = by_path[f"observations/part_{item['indicator_id']}.parquet"]
                        self.assertEqual(item["parts"][0]["id"], expected["id"])
                        self.assertEqual(item["parts"][0]["name"], expected["name"])
                    self.assertFalse(any(name.endswith('.parquet') for name,_,_ in transport.created[before:]))
                    self.assertTrue(all(transport.items[key] == value for key,value in previous_payloads.items()))
                    same = publication.publish_retained_bronze(store, package, root / "resume", "b"*40)
                    self.assertEqual(same["snapshot_id"], result["snapshot_id"])
                    self.assertNotIn('landing', [zone for zone,_ in storage.zone_requests])


class PrefetchTests(unittest.TestCase):
    def test_bounded_readers_and_failures_are_not_successful_restores(self):
        import threading
        from time import sleep
        state = {'active':0, 'peak':0}
        lock = threading.Lock()
        readers = {}
        def restore(reader, item, path):
            thread = threading.get_ident()
            with lock:
                readers.setdefault(thread, set()).add(id(reader))
                state['active'] += 1
                state['peak'] = max(state['peak'], state['active'])
            try:
                sleep(0.005)
                if item['path'] == 'bad': raise ValueError('checksum differs')
                return item['path'] == 'cached'
            finally:
                with lock: state['active'] -= 1
        with tempfile.TemporaryDirectory() as tmp, patch.object(audit, 'restore_verified', side_effect=restore):
            entries = [{'path':str(i)} for i in range(16)] + [{'path':'cached'}]
            result = audit.prefetch_verified(None, entries, Path(tmp), reader_factory=object)
            self.assertEqual(len(result), 17)
            self.assertTrue(result['cached'])
            self.assertLessEqual(state['peak'], 4)
            self.assertTrue(all(len(values) == 1 for values in readers.values()))
            with self.assertRaisesRegex(ValueError, 'checksum'):
                audit.prefetch_verified(None, entries + [{'path':'bad'}], Path(tmp), reader_factory=object)
            self.assertEqual(state['active'], 0)
            for n in (0, 5, True):
                with self.assertRaises(audit.RetainedDbwAuditError):
                    audit.prefetch_verified(None, entries, Path(tmp), workers=n, reader_factory=object)

if __name__ == '__main__':
    unittest.main()
