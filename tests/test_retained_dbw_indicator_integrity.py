"""Retained query publication must bind indicator labels to actual row identities."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import test_retained_dbw_publication as fixtures
from test_retained_dbw_publication import descriptor
from retained_dbw_publication import canonical, publish_retained_bronze
from ingestion.source_campaign_store import CampaignStoreError, LocalCampaignStore


def repin_fixture(audit: Path, relative_path: str) -> None:
    """Make changed fixture bytes internally audited, never approve production hashes."""
    path = audit / "descriptor-inventory.json"
    inventory = json.loads(path.read_text())
    replacement = descriptor(relative_path, audit / "verified-cache" / relative_path)
    inventory["objects"] = [replacement if item["path"] == relative_path else item
                            for item in inventory["objects"]]
    sha = hashlib.sha256(canonical(inventory["objects"])).hexdigest()
    inventory.update(inventory_sha256=sha, total_bytes=sum(x["size"] for x in inventory["objects"]))
    path.write_text(json.dumps(inventory))
    for name in ("audit-report.json", "run-status.json"):
        path = audit / name
        value = json.loads(path.read_text()); value["inventory_sha256"] = sha
        path.write_text(json.dumps(value))


class RetainedIndicatorIntegrityTests(unittest.TestCase):
    def test_partition_rows_must_match_the_indexed_indicator(self):
        for expression in ("999::BIGINT", "NULL::BIGINT", "CASE WHEN n=0 THEN 1 ELSE 999 END::BIGINT"):
            with self.subTest(expression=expression), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                audit = fixtures.RetainedPublicationTests().fixture(root)
                relative = "observations/part_1.parquet"
                source = audit / "verified-cache" / relative
                changed = source.with_suffix(".new.parquet")
                con = duckdb.connect()
                try:
                    con.execute("COPY (SELECT p.* REPLACE (" + expression +
                                " AS indicator_id) FROM read_parquet($source) p CROSS JOIN range(2) t(n)) "
                                "TO $output (FORMAT PARQUET)", {"source": str(source), "output": str(changed)})
                finally:
                    con.close()
                changed.replace(source)
                repin_fixture(audit, relative)
                store = LocalCampaignStore(root / "store", "gus_dbw_retained_bronze")
                with patch.object(store, "promote_landing_pointer", wraps=store.promote_landing_pointer) as promote:
                    with self.assertRaisesRegex(CampaignStoreError, "indicator identity"):
                        publish_retained_bronze(store, audit, root / "work", "a" * 40, max_indicators=1)
                    promote.assert_not_called()
                self.assertIsNone(store.load_landing_pointer())

    def test_oversized_fragment_fallback_writes_exact_offset_slice(self):
        from retained_dbw_publication import _write_slice
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.parquet"
            output = root / "slice.parquet"
            fixtures.parquet(source, "SELECT i::BIGINT id FROM range(12) t(i)")
            _write_slice(source, output, 3, 4)
            con = duckdb.connect()
            try:
                rows = con.execute("SELECT * FROM read_parquet(?)", [str(output)]).fetchall()
            finally:
                con.close()
            self.assertEqual(rows, [(3,), (4,), (5,), (6,)])
            self.assertEqual(list(root.glob("duckdb-spill-*")), [])

    def test_real_repack_uses_fallback_and_preserves_all_rows(self):
        import retained_dbw_publication as publication
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "wide.parquet"
            fixtures.parquet(source, "SELECT i::BIGINT id, repeat(md5(i::VARCHAR), 3) payload "
                             "FROM range(512) t(i)")
            original = source.read_bytes()
            with patch.object(publication, "MAX_LANDING_FILE_BYTES", 4096), \
                 patch.object(publication, "_write_slice", wraps=publication._write_slice) as split:
                parts = list(publication.bounded_fragments(
                    source, [("id", "BIGINT"), ("payload", "VARCHAR")], root))
                self.assertGreater(split.call_count, 0)
            self.assertGreater(len(parts), 1)
            self.assertTrue(all(path.stat().st_size <= 4096 for path, _ in parts))
            self.assertEqual(sum(rows for _, rows in parts), 512)
            publication._verify_repack(source, [path for path, _ in parts], 512)
            self.assertEqual(source.read_bytes(), original)

    def test_slice_failure_removes_disposable_spill_directory(self):
        from retained_dbw_publication import _write_slice
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(duckdb.Error):
                _write_slice(root / "absent.parquet", root / "output.parquet", 0, 1)
            self.assertEqual(list(root.glob("duckdb-spill-*")), [])

    def test_bad_next_partition_preserves_previous_snapshot_and_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            audit = fixtures.RetainedPublicationTests().fixture(root)
            relative = "observations/part_2.parquet"
            source = audit / "verified-cache" / relative
            changed = source.with_suffix(".new.parquet")
            con = duckdb.connect()
            try:
                con.execute("COPY (SELECT * REPLACE (999::BIGINT AS indicator_id) "
                            "FROM read_parquet($source)) TO $output (FORMAT PARQUET)",
                            {"source": str(source), "output": str(changed)})
            finally:
                con.close()
            changed.replace(source)
            repin_fixture(audit, relative)
            before = source.read_bytes()
            store = LocalCampaignStore(root / "store", "gus_dbw_retained_bronze")
            first = publish_retained_bronze(store, audit, root / "work", "a" * 40, max_indicators=1)
            pointer = store.load_landing_pointer()
            with patch.object(store, "promote_landing_pointer", wraps=store.promote_landing_pointer) as promote:
                with self.assertRaisesRegex(CampaignStoreError, "indicator identity"):
                    publish_retained_bronze(store, audit, root / "work", "a" * 40, max_indicators=1)
                promote.assert_not_called()
            self.assertEqual(first["published_indicator_count"], 1)
            self.assertEqual(store.load_landing_pointer(), pointer)
            self.assertEqual(source.read_bytes(), before)
