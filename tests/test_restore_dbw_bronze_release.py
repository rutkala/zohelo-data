"""Tests for the DBW Bronze consumer restore contract."""
import importlib.util
import json
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/restore_dbw_bronze_release.py"
SPEC = importlib.util.spec_from_file_location("restore_dbw_bronze_release", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class RestoreDbwBronzeReleaseTests(unittest.TestCase):
    def test_completion_requires_both_complete_partition_sets(self):
        release_id = "b" * 64
        marker = {
            "schema_version": 1,
            "record_type": "gus_dbw_bronze_completion",
            "source_id": "gus_dbw",
            "status": "complete_native_snapshot",
            "release_id": release_id,
            "completed_indicators": 2,
            "observation_partitions": 2,
            "dictionary_partitions": 2,
            "observation_inventory_sha256": "a" * 64,
            "dictionary_inventory_sha256": "b" * 64,
        }
        self.assertEqual(
            MODULE._completion(json.dumps(marker).encode(), release_id), marker
        )
        marker["dictionary_partitions"] = 1
        with self.assertRaisesRegex(RuntimeError, "reconcile all partitions"):
            MODULE._completion(json.dumps(marker).encode(), release_id)

    def test_restore_entrypoint_is_the_dbt_local_materialization_boundary(self):
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('storage.resolve_zone("bronze", create=False)', source)
        self.assertIn("get_media", source)
        self.assertIn("hashlib.sha256(raw)", source)
        self.assertIn('os.replace(staging, target)', source)
        self.assertIn('"02_bronze" / "gus_dbw" / "releases"', source)


if __name__ == "__main__":
    unittest.main()
