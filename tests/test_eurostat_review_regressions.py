"""Credential-free regressions for the two final PR 103 review findings."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from hashlib import sha256
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import duckdb
from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import eurostat_platform


class PinnedSnapshotTests(unittest.TestCase):
    def test_download_does_not_read_a_new_current_pointer(self):
        payload = b"pinned A"
        manifest = {"snapshot_id": "A", "files": [
            {"id": "A-1", "size": len(payload), "sha256": sha256(payload).hexdigest()}
        ]}
        original = deepcopy(manifest)
        store = Mock()
        store.read_landing_object.return_value = payload
        with tempfile.TemporaryDirectory() as directory, patch.object(
            eurostat_platform, "verify_landing", side_effect=AssertionError("Pointer read twice")
        ) as verify:
            paths = eurostat_platform._download_landing_snapshot(store, Path(directory), manifest)
            self.assertEqual([payload], [p.read_bytes() for p in paths])
            verify.assert_not_called()
        store.read_landing_object.assert_called_once_with(original["files"][0])
        self.assertEqual(original, manifest)

    def test_missing_pinned_manifest_fails_without_reading_storage(self):
        store = Mock()
        with tempfile.TemporaryDirectory() as directory:
            for manifest in (None, {}, {"snapshot_id": "A", "files": []}):
                with self.subTest(manifest=manifest), self.assertRaisesRegex(RuntimeError, "verified"):
                    eurostat_platform._download_landing_snapshot(store, Path(directory), manifest)
        store.read_landing_object.assert_not_called()

    def test_descriptor_verification_failure_is_not_swallowed(self):
        store = Mock()
        store.read_landing_object.side_effect = ValueError("fingerprint changed")
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(ValueError, "fingerprint"):
            eurostat_platform._download_landing_snapshot(
                store, Path(directory), {"snapshot_id": "A", "files": [{"id": "A-1"}]}
            )


class SourceMetadataRevisionTests(unittest.TestCase):
    def model_rows(self, timestamps):
        template = (ROOT / "models/silver/eurostat_observation_revisions.sql").read_text()
        sql = Environment(undefined=StrictUndefined).from_string(template).render(
            config=lambda **_: "", var=lambda *_: True, ref=lambda _: "source_rows"
        )
        with duckdb.connect() as connection:
            connection.execute("""create table source_rows (
                dataset_id varchar, dimension_key_sha256 varchar, dataset_label varchar,
                geo_code varchar, freq_code varchar, unit_code varchar, period_key varchar,
                dimension_key_json varchar, value_json varchar, value_numeric double,
                status_code varchar, is_missing boolean, source_updated_at timestamp,
                retrieved_at_utc timestamp, response_sha256 varchar, task_id varchar
            )""")
            for index, timestamp in enumerate(timestamps, 1):
                connection.execute(
                    "insert into source_rows values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ["demo", "same-cell", "Demo", "PL", "A", "NR", "2025", "{}",
                     "42", 42, None, False, timestamp, datetime(2026, 9, index),
                     str(index) * 64, f"task-{index}"]
                )
            connection.execute("create table revisions as " + sql)
            return connection.execute("""select revision_number, source_updated_at,
                replayed_response_count, is_current, revision_event_type
                from revisions order by revision_number""").fetchall()

    def test_timestamp_only_update_is_a_metadata_revision_and_replay_stays_one_run(self):
        first, second = datetime(2026, 8, 1), datetime(2026, 8, 2)
        self.assertEqual(self.model_rows([first, second, second]), [
            (1, first, 1, False, "first_observed"),
            (2, second, 2, True, "observed_metadata_changed"),
        ])

    def test_metadata_reversion_and_null_transitions_remain_observable(self):
        stamp = datetime(2026, 8, 1)
        self.assertEqual(self.model_rows([None, stamp, None]), [
            (1, None, 1, False, "first_observed"),
            (2, stamp, 1, False, "observed_metadata_changed"),
            (3, None, 1, True, "observed_metadata_changed"),
        ])


if __name__ == "__main__":
    unittest.main()
