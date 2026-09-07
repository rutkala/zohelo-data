"""Capacity facts distinguish real bounds, deduplication, and unknown inventory."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from capacity_report import (FOLDER_MIME, RETENTION_POLICY, account_storage_quota,
                             capacity_report, inventory_project, process_memory_report)
from check_platform_health import read_drive_health


def state_with_sizes(*items):
    source_id = "nbp_gold_prices"
    return {"format_version": 1, "sources": {source_id: {"successful_responses": [
        {"source_id": source_id, "ingestion_sequence": index + 1,
         "raw_file_id": file_id, "size_bytes": size}
        for index, (file_id, size) in enumerate(items)
    ]}}}


class CapacityReportTests(unittest.TestCase):
    def test_descriptor_build_bound_is_not_confused_with_unique_raw_storage(self):
        state = state_with_sizes(("raw-one", 40), ("raw-one", 40), ("raw-two", 10))
        report = capacity_report(state, max_raw_bytes=100, max_observation_batches=10)
        self.assertEqual(report["successful_descriptor_count"], 3)
        self.assertEqual(report["declared_raw_bytes"], 90)
        self.assertEqual(report["unique_raw_file_bytes"], 50)
        self.assertEqual(report["unique_raw_file_count"], 2)
        self.assertEqual(report["capacity"]["declared_raw_bytes"]["headroom"], 10)
        self.assertEqual(report["capacity"]["declared_raw_bytes"]["status"], "warning")
        self.assertEqual(report["canonical_state_bytes"], len(json.dumps(
            state, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")))
        self.assertEqual(report["retention_policy"], RETENTION_POLICY)
        self.assertEqual(report["forecast"], "not_estimated_no_measured_growth_rate")

    def test_unknown_sizes_never_become_zero_or_false_headroom(self):
        report = capacity_report(state_with_sizes(("one", 10), ("two", None)))
        self.assertIsNone(report["declared_raw_bytes"])
        self.assertIsNone(report["unique_raw_file_bytes"])
        self.assertIsNone(report["capacity"]["declared_raw_bytes"]["headroom"])
        self.assertEqual(report["known_declared_raw_bytes_lower_bound"], 10)
        self.assertEqual(report["status"], "attention_required")

    def test_conflicting_file_metadata_is_visible_and_not_deduplicated_as_safe(self):
        report = capacity_report(state_with_sizes(("one", 10), ("one", 20)))
        self.assertEqual(report["declared_raw_bytes"], 30)
        self.assertIsNone(report["unique_raw_file_bytes"])
        self.assertIn("raw_file_size_conflict", report["warnings"])

    def test_warning_threshold_and_exceeded_capacity_are_explicit(self):
        below = capacity_report(state_with_sizes(("one", 69)), max_raw_bytes=100)
        at = capacity_report(state_with_sizes(("one", 70)), max_raw_bytes=100)
        over = capacity_report(state_with_sizes(("one", 101)), max_raw_bytes=100)
        self.assertEqual(below["capacity"]["declared_raw_bytes"]["status"], "within_limit")
        self.assertEqual(at["capacity"]["declared_raw_bytes"]["status"], "warning")
        self.assertEqual(over["capacity"]["declared_raw_bytes"]["status"], "exceeded")
        self.assertEqual(over["capacity"]["declared_raw_bytes"]["headroom"], -1)

    def test_account_quota_does_not_claim_missing_limits_are_unlimited(self):
        service = MagicMock()
        service.about().get().execute.return_value = {"storageQuota": {"usage": "25", "usageInDrive": "20"}}
        report = account_storage_quota(service)
        self.assertIsNone(report["limit_bytes"])
        self.assertIsNone(report["available_bytes"])
        self.assertEqual(report["usage_bytes"], 25)
        service.about().get.assert_called_with(fields="storageQuota")
        service.about().get().execute.side_effect = RuntimeError("private identity must not be emitted")
        unavailable = account_storage_quota(service)
        self.assertEqual(unavailable["status"], "unavailable")
        self.assertNotIn("private", json.dumps(unavailable))

    def test_project_inventory_is_bounded_and_reports_known_bytes_as_lower_bound(self):
        files = MagicMock()
        files.list().execute.side_effect = [
            {"files": [{"id": "releases", "name": "releases", "mimeType": FOLDER_MIME},
                       {"id": "other", "name": "unknown.txt", "mimeType": "text/plain"}]},
            {"files": [{"id": "one", "name": "one.parquet", "mimeType": "application/octet-stream", "size": "30"}],
             "nextPageToken": "next"},
        ]
        report = inventory_project(files, "root", max_files=3)
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["inspected_entries"], 3)
        self.assertEqual(report["known_file_bytes_lower_bound"], 30)
        self.assertEqual(report["unknown_size_file_count"], 1)
        self.assertIsNone(report["total_file_bytes"])
        self.assertEqual(report["categories"]["releases"]["known_file_bytes"], 30)
        self.assertEqual(files.list().execute.call_count, 2)
        files.create.assert_not_called()
        files.update.assert_not_called()
        files.delete.assert_not_called()

    def test_complete_inventory_does_not_follow_shortcuts(self):
        files = MagicMock()
        files.list().execute.return_value = {"files": [
            {"id": "shortcut", "name": "external", "mimeType": "application/vnd.google-apps.shortcut"},
            {"id": "data", "name": "data.bin", "mimeType": "application/octet-stream", "size": "5"},
        ]}
        report = inventory_project(files, "root")
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["list_requests"], 1)
        self.assertEqual(report["known_file_bytes_lower_bound"], 5)
        self.assertIsNone(report["total_file_bytes"])

    def test_inventory_metadata_failure_preserves_partial_evidence(self):
        files = MagicMock()
        files.list().execute.side_effect = [
            {"files": [{"id": "one", "name": "one.bin", "size": "7"}], "nextPageToken": "next"},
            RuntimeError("private API payload"),
        ]
        report = inventory_project(files, "root")
        self.assertEqual(report["known_file_bytes_lower_bound"], 7)
        self.assertIn("drive_metadata_request_failed", report["incomplete_reasons"])
        self.assertNotIn("private", json.dumps(report))

    def test_memory_high_water_values_are_not_a_process_tree_peak(self):
        report = process_memory_report()
        self.assertIsNone(report["process_tree_peak_bytes"])
        if sys.platform.startswith("linux"):
            self.assertGreater(report["parent_process_high_water_bytes"], 0)
            self.assertIn("not_a_sum", report["interpretation"])

    def test_drive_health_uses_effective_source_config_and_never_creates_folders(self):
        from ingestion.nbp_state import new_state
        manager = MagicMock()
        manager.resolve_root.return_value = "selected-root"
        manager._list_exact_folders.return_value = [{"id": "control"}]
        manager.drive_service.files().list().execute.return_value = {"files": []}
        manager.drive_service.about().get().execute.return_value = {
            "storageQuota": {"limit": "1000", "usage": "20"}}
        factory = MagicMock(return_value=manager)

        def loaded(_store, control, specs):
            # Read and validate the actual production config, not a replacement
            # test mapping. A metadata-only sources.yaml cannot satisfy this.
            self.assertEqual(control, "control")
            self.assertEqual(len(specs), 4)
            self.assertTrue(all("{start_date}" in spec.endpoint_template for spec in specs.values()))
            state = new_state(specs, datetime(2026, 9, 7, tzinfo=timezone.utc))
            return SimpleNamespace(state=state, snapshot_file_id="existing-state")

        with patch("ingestion.nbp_state.load_state", side_effect=loaded):
            report = read_drive_health(storage_factory=factory)
        self.assertEqual(report["account_storage"]["available_bytes"], 980)
        self.assertEqual(report["project_inventory"]["status"], "complete")
        factory.assert_called_once_with(backend="gdrive", allow_interactive_auth=False)
        manager.resolve_root.assert_called_once_with(create=False)
        manager.authorize_writes.assert_not_called()
        manager.get_or_create_nested_folder.assert_not_called()
        manager.drive_service.files().create.assert_not_called()
        manager.drive_service.files().update.assert_not_called()
        manager.drive_service.files().delete.assert_not_called()

    def test_local_cli_does_not_authenticate_or_disclose_raw_file_ids(self):
        temporary_root = ROOT / ".local/test-tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=temporary_root, prefix="capacity-") as temporary:
            path = Path(temporary) / "state.json"
            path.write_text(json.dumps(state_with_sizes(("private-raw-id", 10))))
            summary = Path(temporary) / "summary.md"
            summary.write_text("Existing workflow evidence\n")
            result = subprocess.run([sys.executable, str(ROOT / "scripts/check_platform_health.py"),
                                     "--local-state", str(path)], cwd=ROOT, text=True,
                                    env=dict(os.environ, GITHUB_STEP_SUMMARY=str(summary)),
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    timeout=30, check=False)
            self.assertEqual(result.returncode, 0, result.stdout)
            report = json.loads(result.stdout)
            self.assertEqual(report["account_storage"]["status"], "not_requested")
            self.assertNotIn("private-raw-id", result.stdout)
            self.assertTrue(summary.read_text().startswith("Existing workflow evidence\n"))
            self.assertIn("Platform capacity and retained storage", summary.read_text())
            self.assertNotIn("private-raw-id", summary.read_text())


if __name__ == "__main__":
    unittest.main()
