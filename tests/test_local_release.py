import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from local_release import CachedReadStore, materialize_database, restore_local_release
from release_validation import ReleaseValidationError


class Store:
    def __init__(self, data):
        self.data = data
        self.reads = 0

    def read(self, file_id):
        self.reads += 1
        return self.data[file_id]


class LocalReleaseTests(unittest.TestCase):
    def test_cache_pins_bytes_and_enforces_total_budget(self):
        underlying = Store({"one": b"first", "two": b"next"})
        store = CachedReadStore(underlying, max_bytes=8)
        self.assertEqual(store.read("one"), b"first")
        underlying.data["one"] = b"changed"
        self.assertEqual(store.read("one"), b"first")
        self.assertEqual(underlying.reads, 1)
        with self.assertRaisesRegex(ValueError, "budget"):
            store.read("two")

    def test_materializes_portable_named_tables_without_parquet_dependency(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            source = workspace / "source.parquet"
            with duckdb.connect() as connection:
                connection.sql("SELECT DATE '2026-09-04' AS effective_date, 3.75::DOUBLE AS mid").write_parquet(str(source))
            data = source.read_bytes()
            source.unlink()
            manifest = {"datasets": [{"dataset_id": "fact_fx_quotes", "layer": "04_gold",
                "table_name": "fact_fx_quotes", "row_count": 1,
                "date_column": "effective_date", "min_date": "2026-09-04", "max_date": "2026-09-04",
                "columns": [{"name": "effective_date", "type": "DATE"}, {"name": "mid", "type": "DOUBLE"}],
                "files": [{"id": "data", "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}]}]}
            database = materialize_database(manifest, Store({"data": data}), workspace)
            self.assertEqual(list(workspace.glob("*.parquet")), [])
            with duckdb.connect(str(database), read_only=True) as connection:
                self.assertEqual(connection.execute('SELECT mid FROM "04_gold".fact_fx_quotes').fetchone(), (3.75,))
            manifest["datasets"][0]["row_count"] = 2
            other = workspace / "other"
            other.mkdir()
            with self.assertRaisesRegex(ReleaseValidationError, "metadata"):
                materialize_database(manifest, Store({"data": data}), other)

    def test_existing_workspace_is_rejected_before_remote_read(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch("local_release.restore_current_release") as restore:
                with self.assertRaisesRegex(ValueError, "never overwritten"):
                    restore_local_release(Store({}), "root", temporary)
                restore.assert_not_called()
