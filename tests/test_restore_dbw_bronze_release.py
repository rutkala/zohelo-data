"""Tests for the DBW Bronze consumer restore contract."""
import importlib.util
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/restore_dbw_bronze_release.py"
SPEC = importlib.util.spec_from_file_location("restore_dbw_bronze_release", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class RestoreDbwBronzeReleaseTests(unittest.TestCase):
    def test_failed_restore_discards_nonresumable_staging_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp) / ".restore-release-attempt"
            (staging / "observations").mkdir(parents=True)
            (staging / "observations/part_7.parquet").write_bytes(b"partial")
            MODULE._discard_incomplete_staging(staging)
            self.assertFalse(staging.exists())

    def test_verified_download_streams_chunks_without_buffering_partition(self):
        raw = b"verified-partition-bytes"
        digest = hashlib.sha256(raw).hexdigest()
        item = {
            "id": "partition",
            "name": "part_7.parquet",
            "size": str(len(raw)),
            "md5Checksum": hashlib.md5(raw).hexdigest(),
            "sha256Checksum": digest,
            "appProperties": {"sha256": digest},
        }
        storage = MagicMock()
        request = object()
        storage.drive_service.files.return_value.get_media.return_value = request

        class FakeDownloader:
            calls = 0

            def __init__(self, handle, supplied_request, *, chunksize):
                self.handle = handle
                self.request = supplied_request
                self.chunksize = chunksize
                self.offset = 0

            def next_chunk(self, *, num_retries):
                type(self).calls += 1
                end = min(self.offset + 5, len(raw))
                self.handle.write(raw[self.offset:end])
                self.offset = end
                return None, self.offset == len(raw)

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            MODULE, "MediaIoBaseDownload", FakeDownloader
        ):
            path = Path(tmp) / "part_7.parquet"
            self.assertIsNone(MODULE._download_verified(storage, item, path))
            self.assertEqual(path.read_bytes(), raw)
        self.assertGreater(FakeDownloader.calls, 1)

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
            "observation_content_inventory_sha256": "c" * 64,
            "dictionary_content_inventory_sha256": "d" * 64,
            "taxonomy_sha256": "e" * 64,
            "metadata_sha256": "f" * 64,
            "consolidated_dictionary_sha256": "0" * 64,
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
        self.assertIn("MediaIoBaseDownload", source)
        self.assertIn("DOWNLOAD_CHUNK_BYTES", source)
        self.assertNotIn("get_media(fileId=item[\"id\"]).execute", source)
        self.assertNotIn("hashlib.sha256(path.read_bytes())", source)
        self.assertIn("preserve_completed_staging = True", source)
        self.assertIn("_discard_incomplete_staging(staging)", source)
        self.assertIn('os.replace(staging, target)', source)
        self.assertIn('"02_bronze" / "gus_dbw" / "releases"', source)
        self.assertIn("len(observations) != completed", source)
        self.assertIn("len(dictionary_parts) != completed", source)
        self.assertIn("observation_content_inventory_sha256", source)
        self.assertIn("dictionary_content_inventory_sha256", source)
        self.assertIn("consolidated_dictionary_sha256", source)


if __name__ == "__main__":
    unittest.main()
