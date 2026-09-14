
import copy
import json
import unittest
from unittest import mock
from tests.test_drive_migration import build_legacy_drive_state, make_storage_manager_mock
from drive_migration import DriveMigrationEngine
from drive_release_store import DriveReleaseStore
from layout_resolution import resolve_source_release_root
import medallion_navigation as navigation
from medallion_navigation import (
    NavigationError, SHORTCUT_MIME_TYPE, StaleNavigationError,
    sync_source_medallion_navigation, verify_medallion_navigation,
)
from release_protocol import read_current_release_manifest

class NavigationRecoveryTests(unittest.TestCase):
    def canonical(self, source):
        files, _, _, _ = build_legacy_drive_state("prod-root-123")
        storage, svc = make_storage_manager_mock(files, "prod-root-123")
        engine = DriveMigrationEngine(storage, expected_root_id="prod-root-123")
        engine.apply(plan=engine.plan(), confirmed=True)
        root, _ = resolve_source_release_root(storage, "prod-root-123", source, is_writer=True)
        manifest = read_current_release_manifest(DriveReleaseStore(storage, root), root)
        return storage, svc, manifest

    def target(self, svc, manifest, release, target):
        result = copy.deepcopy(manifest)
        result["release_id"] = release
        item = result["datasets"][0]["files"][0]
        item["id"] = target
        svc._files[target] = {"id": target, "name": item.get("name", target+".parquet"),
            "mimeType": "application/octet-stream", "parents": [],
            "content": target.encode(), "trashed": False}
        return result

    def test_pending_repairs_same_and_later_release(self):
        storage, svc, manifest = self.canonical("nbp")
        first = self.target(svc, manifest, "nbp-retry-one", "nbp-target-one")
        original = navigation._create_shortcut
        with mock.patch.object(navigation, "_create_shortcut", side_effect=RuntimeError("before")):
            with self.assertRaises(RuntimeError):
                sync_source_medallion_navigation(storage, "prod-root-123", "nbp", first)
        pending = [json.loads(x["content"]) for x in svc._files.values()
            if x.get("name") == "navigation-index.json" and not x.get("trashed")
            and json.loads(x["content"]).get("source_id") == "nbp"]
        self.assertTrue(pending)
        self.assertTrue(all(x["status"] == "pending" for x in pending))
        sync_source_medallion_navigation(storage, "prod-root-123", "nbp", first)
        verify_medallion_navigation(storage, "prod-root-123", "nbp", first)

        second = self.target(svc, first, "nbp-retry-two", "nbp-target-two")
        def create_then_fail(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError("lost response")
        with mock.patch.object(navigation, "_create_shortcut", side_effect=create_then_fail):
            with self.assertRaises(RuntimeError):
                sync_source_medallion_navigation(storage, "prod-root-123", "nbp", second)
        third = self.target(svc, second, "nbp-retry-three", "nbp-target-three")
        sync_source_medallion_navigation(storage, "prod-root-123", "nbp", third)
        verify_medallion_navigation(storage, "prod-root-123", "nbp", third)
        self.assertFalse(any(not x.get("trashed") and x.get("mimeType") == SHORTCUT_MIME_TYPE
            and (x.get("shortcutDetails") or {}).get("targetId") == "nbp-target-two"
            for x in svc._files.values()))

    def test_multipart_foreign_child_blocks_before_mutation_and_owned_stale_prunes(self):
        storage, svc, manifest = self.canonical("wdi")
        indexes = [json.loads(x["content"]) for x in svc._files.values()
            if x.get("name") == "navigation-index.json" and not x.get("trashed")
            and json.loads(x["content"]).get("source_id") == "wdi"]
        multipart = next(t for i in indexes for t in i["tables"].values() if t["is_multi_part"])
        foreign = "foreign-multipart-child"
        svc._files[foreign] = {"id": foreign, "name": "foreign.txt", "mimeType": "text/plain",
            "parents": [multipart["container_id"]], "content": b"foreign", "trashed": False}
        svc.mutation_count = 0
        with self.assertRaises(NavigationError):
            sync_source_medallion_navigation(storage, "prod-root-123", "wdi", manifest)
        self.assertFalse(svc._files[foreign]["trashed"])
        self.assertEqual(0, svc.mutation_count)

        svc._files[foreign]["trashed"] = True
        stale = {multipart["container_id"], *(x["shortcut_id"] for x in multipart["shortcuts"])}
        single = copy.deepcopy(manifest)
        single["release_id"] = "wdi-owned-cleanup"
        item = single["datasets"][0]["files"][0]
        item["id"] = "wdi-cleanup-target"
        single["datasets"][0]["files"] = [item]
        svc._files["wdi-cleanup-target"] = {"id": "wdi-cleanup-target",
            "name": "cleanup.parquet", "mimeType": "application/octet-stream",
            "parents": [], "content": b"cleanup", "trashed": False}
        sync_source_medallion_navigation(storage, "prod-root-123", "wdi", single)
        self.assertTrue(all(svc._files[x]["trashed"] for x in stale))

    def test_verification_rejects_index_and_physical_drift(self):
        storage, svc, manifest = self.canonical("nbp")
        baseline = copy.deepcopy(svc._files)
        def index():
            return next(x for x in svc._files.values()
                if x.get("name") == "navigation-index.json" and not x.get("trashed")
                and json.loads(x["content"]).get("source_id") == "nbp")
        for variant in ("status", "source", "layer", "extra_table", "extra_physical"):
            with self.subTest(variant=variant):
                svc._files = copy.deepcopy(baseline)
                item = index()
                doc = json.loads(item["content"])
                if variant == "status": doc["status"] = "pending"
                elif variant == "source": doc["source_id"] = "bdl"
                elif variant == "layer": doc["layer"] = "wrong"
                elif variant == "extra_table":
                    doc["tables"]["foreign"] = {"is_multi_part": False, "file_count": 1,
                        "shortcuts": [{"name": "foreign.parquet", "target_id": "foreign",
                            "shortcut_id": "foreign-shortcut"}]}
                else:
                    svc._files["extra-physical"] = {"id": "extra-physical", "name": "extra.parquet",
                        "mimeType": SHORTCUT_MIME_TYPE, "parents": [item["parents"][0]],
                        "shortcutDetails": {"targetId": "foreign"}, "content": b"", "trashed": False}
                if variant != "extra_physical":
                    item["content"] = json.dumps(doc).encode()
                with self.assertRaises((NavigationError, StaleNavigationError)):
                    verify_medallion_navigation(storage, "prod-root-123", "nbp", manifest)

if __name__ == "__main__":
    unittest.main()
