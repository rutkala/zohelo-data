"""Offline acceptance for the read-only retained DBW Bronze diagnostic."""
import hashlib
import io
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import duckdb


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/audit_retained_dbw_bronze.py"
SPEC = importlib.util.spec_from_file_location("audit_retained_dbw_bronze", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def item(name: str, object_id: str, raw: bytes = b"verified") -> dict:
    sha256_hex = hashlib.sha256(raw).hexdigest()
    return {
        "id": object_id,
        "name": name,
        "size": str(len(raw)),
        "md5Checksum": hashlib.md5(raw).hexdigest(),
        "sha256Checksum": sha256_hex,
        "appProperties": {"sha256": sha256_hex},
        "trashed": False,
    }


def inventory(indicators=(7, 8)) -> dict:
    return {
        "receipts": [item(f"{value}.json", f"receipt-{value}") for value in indicators],
        "observations": [
            item(f"part_{value}.parquet", f"observation-{value}") for value in indicators
        ],
        "dictionaries": [item("br_dbw_dictionaries.parquet", "dictionaries")],
        "taxonomy": [item("br_dbw_indicators.parquet", "taxonomy")],
        "metadata": [item("br_dbw_metadata.parquet", "metadata")],
    }


class RetainedDbwAuditTests(unittest.TestCase):
    def test_output_directory_lock_prevents_concurrent_audits(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            with (output / "audit.lock").open("a") as lock:
                MODULE.fcntl.flock(lock, MODULE.fcntl.LOCK_EX | MODULE.fcntl.LOCK_NB)
                with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "owns this output"):
                    MODULE.audit_retained_dbw(mock.Mock(), output)

    def test_stream_writer_rejects_bytes_beyond_pinned_size(self):
        stream = io.BytesIO()
        writer = MODULE._BoundedWriter(stream, 4)
        self.assertEqual(writer.write(b"1234"), 4)
        with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "exceeded"):
            writer.write(b"5")
        self.assertEqual(stream.getvalue(), b"1234")

    def test_folder_ambiguity_across_pages_fails_closed(self):
        storage = mock.MagicMock()
        execute = storage.drive_service.files.return_value.list.return_value.execute
        execute.side_effect = [
            {"nextPageToken": "next", "files": [{"id": "one", "name": "gus_dbw"}]},
            {"files": [{"id": "two", "name": "gus_dbw"}]},
        ]
        with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "found 2"):
            MODULE._folder(storage, "parent", "gus_dbw")

    def test_reconciled_indicator_sets_are_pinned_deterministically(self):
        descriptors, indicator_ids = MODULE.validate_inventory(
            inventory(), expected_indicator_count=2
        )
        self.assertEqual(indicator_ids, {7, 8})
        first = MODULE.inventory_document(descriptors)
        second = MODULE.inventory_document(descriptors)
        self.assertEqual(first["inventory_sha256"], second["inventory_sha256"])
        self.assertEqual(first["object_count"], 7)

    def test_missing_partition_rejects_apparently_complete_receipts(self):
        value = inventory()
        value["observations"].pop()
        with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "do not reconcile"):
            MODULE.validate_inventory(value, expected_indicator_count=2)

    def test_duplicate_remote_name_is_rejected(self):
        value = inventory()
        value["observations"].append(
            item("part_7.parquet", "second-observation-7")
        )
        with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "Duplicate retained DBW path"):
            MODULE.validate_inventory(value, expected_indicator_count=2)

    def test_truncated_transfer_preserves_prior_local_file(self):
        expected = b"complete remote bytes"
        descriptor = MODULE._descriptor(item("part_7.parquet", "remote", expected), "observations/part_7.parquet")
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "part_7.parquet"
            target.write_bytes(b"prior cache bytes")

            def truncated(_storage, _descriptor, temporary):
                temporary.write_bytes(expected[:-2])

            with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "byte verification"):
                MODULE.restore_verified(mock.Mock(), descriptor, target, truncated)
            self.assertEqual(target.read_bytes(), b"prior cache bytes")
            self.assertEqual(list(target.parent.glob(".*.tmp")), [])

    def test_hash_mismatch_preserves_prior_local_file(self):
        expected = b"expected bytes"
        descriptor = MODULE._descriptor(item("part_7.parquet", "remote", expected), "observations/part_7.parquet")
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "part_7.parquet"
            target.write_bytes(b"prior cache bytes")

            def wrong_hash(_storage, _descriptor, temporary):
                temporary.write_bytes(b"different bytes")

            with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "byte verification"):
                MODULE.restore_verified(mock.Mock(), descriptor, target, wrong_hash)
            self.assertEqual(target.read_bytes(), b"prior cache bytes")

    def test_restart_rehashes_and_reuses_verified_cache(self):
        raw = b"verified cache"
        descriptor = MODULE._descriptor(item("part_7.parquet", "remote", raw), "observations/part_7.parquet")
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "part_7.parquet"
            target.write_bytes(raw)
            download = mock.Mock(side_effect=AssertionError("download must not run"))
            self.assertTrue(MODULE.restore_verified(mock.Mock(), descriptor, target, download))
            download.assert_not_called()

    def test_receipt_content_must_match_filename_identity(self):
        receipt = {
            "indicator_id": 8,
            "name": "indicator",
            "status": "completed",
            "files_landed": ["native.zip"],
            "updated_at_utc": "2026-09-20T00:00:00Z",
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "7.json"
            path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "completed indicator"):
                MODULE.validate_receipt(path, 7)

    def test_non_object_receipt_fails_as_diagnostic_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "7.json"
            path.write_text("[]")
            with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "completed indicator"):
                MODULE.validate_receipt(path, 7)

    def test_parquet_footer_schema_and_rows_are_measured(self):
        schema = MODULE.EXPECTED_SCHEMAS["dictionaries"]
        columns = ", ".join(f'"{name}" {kind}' for name, kind in schema)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "br_dbw_dictionaries.parquet"
            connection = duckdb.connect(":memory:")
            connection.execute(f"CREATE TABLE fixture ({columns})")
            connection.execute(f"COPY fixture TO '{path}' (FORMAT PARQUET)")
            connection.close()
            evidence = MODULE.parquet_evidence(path, schema)
        self.assertEqual(evidence["rows"], 0)
        self.assertEqual(
            evidence["columns"],
            [{"name": name, "type": kind} for name, kind in schema],
        )

    def test_remote_inventory_drift_prevents_diagnostic_acceptance(self):
        first, indicator_ids = MODULE.validate_inventory(
            inventory((7,)), expected_indicator_count=1
        )
        changed_groups = inventory((7,))
        changed_groups["observations"][0] = item(
            "part_7.parquet", "observation-7", b"changed bytes"
        )
        changed, _ = MODULE.validate_inventory(
            changed_groups, expected_indicator_count=1
        )
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            MODULE, "discover", return_value={}
        ), mock.patch.object(
            MODULE, "validate_inventory", side_effect=[(first, indicator_ids), (changed, indicator_ids)]
        ), mock.patch.object(
            MODULE, "restore_verified", return_value=True
        ), mock.patch.object(
            MODULE, "validate_receipt", return_value={"files_landed": ["native.zip"]}
        ), mock.patch.object(
            MODULE,
            "parquet_evidence",
            return_value={"rows": 1, "columns": []},
        ):
            with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "changed during"):
                MODULE.audit_retained_dbw(mock.Mock(), Path(tmp))
            self.assertFalse((Path(tmp) / "audit-report.json").exists())
            status = json.loads((Path(tmp) / "run-status.json").read_text())
            self.assertEqual(status["status"], "failed")
            progress = json.loads((Path(tmp) / "progress.json").read_text())
            self.assertEqual(progress["objects_verified"], 5)

    def test_insufficient_disk_space_stops_before_restore(self):
        descriptors, _ = MODULE.validate_inventory(
            inventory((7,)), expected_indicator_count=1
        )
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            MODULE.shutil, "disk_usage", return_value=mock.Mock(free=0)
        ):
            with self.assertRaisesRegex(MODULE.RetainedDbwAuditError, "disk space"):
                MODULE._require_disk_space(Path(tmp) / "cache", descriptors)

    def test_failed_new_run_marks_status_while_retaining_prior_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            prior = {"run_id": "prior-success", "status": "retained_outputs_audited"}
            (output / "audit-report.json").write_text(json.dumps(prior))
            with mock.patch.object(
                MODULE, "_audit_locked", side_effect=MODULE.RetainedDbwAuditError("fixture")
            ):
                with self.assertRaises(MODULE.RetainedDbwAuditError):
                    MODULE.audit_retained_dbw(mock.Mock(), output)
            self.assertEqual(json.loads((output / "audit-report.json").read_text()), prior)
            current = json.loads((output / "run-status.json").read_text())
            self.assertEqual(current["status"], "failed")
            self.assertNotEqual(current["run_id"], "prior-success")

    def test_fixed_relations_are_verified_before_observations(self):
        descriptors, indicator_ids = MODULE.validate_inventory(
            inventory((7,)), expected_indicator_count=1
        )
        restored = []

        def restore(_storage, descriptor, _path):
            restored.append(descriptor["path"])
            return True

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            MODULE, "discover", return_value={}
        ), mock.patch.object(
            MODULE, "validate_inventory", return_value=(descriptors, indicator_ids)
        ), mock.patch.object(
            MODULE, "restore_verified", side_effect=restore
        ), mock.patch.object(
            MODULE, "validate_receipt", return_value={"files_landed": ["native.zip"]}
        ), mock.patch.object(
            MODULE, "parquet_evidence", return_value={"rows": 1, "columns": []}
        ):
            MODULE.audit_retained_dbw(mock.Mock(), Path(tmp))
        self.assertEqual(
            restored,
            [
                "receipts/7.json",
                "dictionaries/br_dbw_dictionaries.parquet",
                "taxonomy/br_dbw_indicators.parquet",
                "metadata/br_dbw_metadata.parquet",
                "observations/part_7.parquet",
            ],
        )

    def test_source_contains_no_drive_mutation_or_release_marker_path(self):
        source = SCRIPT.read_text(encoding="utf-8")
        for forbidden in ("files().create", "files().update", "files().delete", "begin_write_session", "authorize_writes"):
            self.assertNotIn(forbidden, source)
        self.assertNotIn("bronze-complete", source)
        self.assertIn('resolve_zone("landing", create=False)', source)
        self.assertIn('resolve_zone("bronze", create=False)', source)


if __name__ == "__main__":
    unittest.main()
