"""Offline checks for cold-runner DBW preparation; no remote calls or writes."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "tests") not in sys.path:
    sys.path.insert(0, str(ROOT / "tests"))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))
SCRIPT = ROOT / "scripts/prepare_retained_dbw_release.py"
SPEC = importlib.util.spec_from_file_location("prepare_retained_dbw_release", SCRIPT)
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


class PrepareRetainedDbwTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "restore"
        self.raw, self.baseline = M.reviewed_report()
        self.storage = Mock()
        self.fresh = copy.deepcopy(self.baseline)
        self.fresh.update(run_id="fresh-restore", audited_at_utc="2026-09-22T00:00:00Z",
                          cache_files_reused=0, cache_files_downloaded=3103)
        self.observed = {"inventory_sha256": M.REVIEWED_INVENTORY_SHA256,
                         "object_count": self.baseline["remote_object_count"],
                         "total_bytes": self.baseline["remote_total_bytes"]}
        def audit_fixture(_storage, path, **kwargs):
            path.mkdir(parents=True)
            (path / "verified-cache").mkdir()
            (path / "audit-report.json").write_text(json.dumps(self.fresh))
            (path / "run-status.json").write_text(json.dumps({
                "run_id": "fresh-restore", "status": "complete"}))
            return self.fresh
        patches = [patch.object(M.audit, "discover", return_value={}),
                   patch.object(M.audit, "validate_inventory", return_value=({}, set())),
                   patch.object(M.audit, "inventory_document", return_value=self.observed),
                   patch.object(M.audit, "audit_retained_dbw", side_effect=audit_fixture),
                   patch.object(M, "validate_audit")]
        self.mocks = [self.enterContext(patcher) for patcher in patches]

    def test_checked_in_baseline_is_still_exactly_reviewed(self):
        self.assertEqual(self.baseline["inventory_sha256"], M.REVIEWED_INVENTORY_SHA256)
        self.assertEqual(self.baseline["remote_object_count"], 3103)
        self.assertEqual(self.baseline["measured_parquet_rows"]["observations"], 879999727)

    def test_restore_preserves_original_report_and_separate_fresh_evidence(self):
        result = M.prepare(self.storage, self.output)
        self.assertEqual(result["status"], "verified")
        self.assertFalse(result["publication_performed"])
        self.assertEqual((self.output / "audit/audit-report.json").read_bytes(), self.raw)
        fresh = json.loads((self.output / "fresh-audit-report.json").read_text())
        self.assertEqual(fresh["run_id"], "fresh-restore")
        status = json.loads((self.output / "audit/run-status.json").read_text())
        self.assertEqual(status["run_id"], self.baseline["run_id"])
        self.assertEqual(status["restore_run_id"], "fresh-restore")
        self.assertEqual(status["kind"], "restored_reviewed_audit")
        self.mocks[-1].assert_called_once_with(self.output / "audit", require_reviewed_snapshot=True)
        self.assertEqual(self.storage.mock_calls, [])

    def test_real_publisher_validator_accepts_reconstructed_fixture_package(self):
        import hashlib
        import retained_dbw_publication as publication
        from test_retained_dbw_publication import RetainedPublicationTests
        fixture = RetainedPublicationTests().fixture(Path(self.temp.name) / "producer")
        report = {**self.baseline, **json.loads((fixture / "audit-report.json").read_text())}
        inventory = json.loads((fixture / "descriptor-inventory.json").read_text())
        report.update(remote_object_count=inventory["object_count"],
                      remote_total_bytes=inventory["total_bytes"])
        raw = json.dumps(report).encode()
        baseline = Path(self.temp.name) / "reviewed-fixture.json"
        baseline.write_bytes(raw)
        report_sha = hashlib.sha256(raw).hexdigest()
        inventory_sha = inventory["inventory_sha256"]
        self.observed.update(inventory_sha256=inventory_sha,
                             object_count=inventory["object_count"],
                             total_bytes=inventory["total_bytes"])
        fresh = {**report, "run_id": "fresh-fixture"}
        def restore_fixture(_storage, output, **kwargs):
            output.mkdir()
            (output / "descriptor-inventory.json").write_text(json.dumps(inventory))
            (output / "audit-report.json").write_text(json.dumps(fresh))
            (output / "run-status.json").write_text(json.dumps({"run_id": "fresh-fixture"}))
            return fresh
        with patch.object(M, "REVIEWED_AUDIT_REPORT_SHA256", report_sha), patch.object(
            M, "REVIEWED_INVENTORY_SHA256", inventory_sha
        ), patch.object(publication, "REVIEWED_AUDIT_REPORT_SHA256", report_sha), patch.object(
            publication, "REVIEWED_INVENTORY_SHA256", inventory_sha
        ), patch.object(M, "validate_audit", side_effect=publication.validate_audit):
            self.mocks[3].side_effect = restore_fixture
            result = M.prepare(self.storage, self.output, baseline=baseline)
            self.assertEqual(result["status"], "verified")
            accepted, _, _ = publication.validate_audit(self.output / "audit", require_reviewed_snapshot=True)
            self.assertEqual(accepted["run_id"], report["run_id"])

    def test_changed_inventory_stops_before_payload_restore(self):
        self.observed["inventory_sha256"] = "0" * 64
        with self.assertRaisesRegex(M.PreparationError, "Drive inventory differs"):
            M.prepare(self.storage, self.output)
        self.mocks[3].assert_not_called()
        self.assertEqual(json.loads((self.output / "restore-evidence.json").read_text())["status"], "failed")

    def test_changed_inventory_counts_stop_before_payload_restore(self):
        self.observed["total_bytes"] += 1
        with self.assertRaises(M.PreparationError):
            M.prepare(self.storage, self.output)
        self.mocks[3].assert_not_called()

    def test_unreviewed_report_stops_before_remote_or_output_work(self):
        bad = Path(self.temp.name) / "tampered.json"
        bad.write_bytes(self.raw + b" ")
        with self.assertRaisesRegex(M.PreparationError, "SHA-256"):
            M.prepare(self.storage, self.output, baseline=bad)
        self.mocks[0].assert_not_called()
        self.assertFalse(self.output.exists())

    def test_changed_rows_fail_and_make_package_ineligible(self):
        self.fresh["measured_parquet_rows"]["observations"] -= 1
        with self.assertRaisesRegex(M.PreparationError, "measured_parquet_rows"):
            M.prepare(self.storage, self.output)
        status = json.loads((self.output / "audit/run-status.json").read_text())
        self.assertEqual(status["status"], "failed")
        self.mocks[-1].assert_not_called()

    def test_publisher_compatibility_failure_never_leaves_complete_status(self):
        self.mocks[-1].side_effect = RuntimeError("incompatible")
        with self.assertRaisesRegex(RuntimeError, "incompatible"):
            M.prepare(self.storage, self.output)
        status = json.loads((self.output / "audit/run-status.json").read_text())
        self.assertEqual(status["status"], "failed")
        self.assertTrue((self.output / "fresh-audit-report.json").exists())

    def test_existing_output_is_never_overwritten(self):
        self.output.mkdir()
        sentinel = self.output / "retained.txt"
        sentinel.write_text("preserve")
        with self.assertRaisesRegex(M.PreparationError, "new output"):
            M.prepare(self.storage, self.output)
        self.assertEqual(sentinel.read_text(), "preserve")
        self.mocks[0].assert_not_called()

    def test_audit_failure_keeps_failure_evidence(self):
        self.mocks[3].side_effect = RuntimeError("transfer failure")
        with self.assertRaisesRegex(RuntimeError, "transfer failure"):
            M.prepare(self.storage, self.output)
        result = json.loads((self.output / "restore-evidence.json").read_text())
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["publication_performed"])

    def test_workflow_is_main_dispatch_read_only_and_uploads_only_summaries(self):
        path = ROOT / ".github/workflows/dbw-release-preflight.yml"
        value = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(value["on"]), {"workflow_dispatch"})
        self.assertEqual(value["permissions"], {"contents": "read"})
        job = value["jobs"]["restore_reviewed_inputs"]
        self.assertIn("github.ref == 'refs/heads/main'", job["if"])
        text = path.read_text()
        self.assertNotIn("allow-production-write", text)
        self.assertNotIn("publish_retained_dbw_bronze.py", text)
        upload = job["steps"][-1]["with"]["path"]
        self.assertNotIn("verified-cache", upload)
        self.assertNotIn("descriptor-inventory", upload)
        self.assertNotIn("*", upload)


if __name__ == "__main__":
    unittest.main()
