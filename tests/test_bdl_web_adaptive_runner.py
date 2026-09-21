"""Unit tests for BDL Web full-history adaptive runner.

Verifies:
1. Complete catalogue traversal without campaign runtime cutoff (> 18,600s).
2. Clean interruption handling when max_seconds is explicitly provided.
3. Exclusive writer ownership and heartbeat-guarded lock.
4. Standalone Codespace authorization and secret boundary checks.
5. Fast-path whole export with graceful fallback to exact partitions on large-selection error.
6. Fair traversal across unvisited subgroups before deep slicing/retrying.
7. Resume vs reload mode invariants and corrupt receipt rejection.
"""
from contextlib import ExitStack, redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import bdl_web_adaptive as adaptive
import bdl_web_partitions as parts
import bdl_web_queue as queue
import bdl_bulk_plan


def make_candidate(number: int) -> dict:
    return {
        "category_id": "K1",
        "group_id": "G2",
        "subgroup_id": f"P{number}",
        "subgroup_name": f"Subgroup {number}",
        "web_catalogue_verified": True,
        "url": f"{queue.WEB}/dane/podgrup/wymiary/1/2/{number}",
    }


class FakeDriveControl:
    """In-memory mock for DriveControl preserving exact json serialization."""
    records = {}

    def __init__(self, storage, parent, name):
        self.storage = storage
        self.parent = parent
        self.name = name
        self.key = f"{parent}/{name}"
        self.file_id = f"fake-file-{self.name}"
        self.observed = FakeDriveControl.records.get(self.key)

    def load(self):
        if self.observed is None:
            return None
        value = json.loads(self.observed.decode("utf-8"))
        if value.get("source_id") != "gus_bdl" or value.get("format_version") != 1 or value.get("transport") != "web_ui":
            raise RuntimeError("Unexpected BDL Web checkpoint identity")
        return value

    def save(self, value):
        raw = queue.rendered(value)
        FakeDriveControl.records[self.key] = raw
        self.observed = raw


class BdlWebAdaptiveRunnerTests(unittest.TestCase):
    def test_worker_proxy_is_forwarded_without_changing_default_call_shape(self):
        item = {"subgroup_id": "P1", "url": "https://example.test"}
        node = {"id": "node", "scope": {"kind": "download"}}
        workspace = Path("/tmp/work")
        with patch.object(adaptive, "invoke_selection", return_value={"status": "download"}) as invoke:
            adaptive.invoke_selection_for_worker(item, node, workspace, 60)
            invoke.assert_called_once_with(item, node, workspace, 60)
        with patch.object(adaptive, "invoke_selection", return_value={"status": "download"}) as invoke:
            adaptive.invoke_selection_for_worker(
                item, node, workspace, 60, proxy="http://127.0.0.1:8081"
            )
            invoke.assert_called_once_with(
                item, node, workspace, 60, proxy="http://127.0.0.1:8081"
            )

    def setUp(self):
        FakeDriveControl.records = {}

    def setup_environment(self, directory: Path, candidates: dict, legacy: set = None):
        state = queue.new_state()
        state["candidates"] = deepcopy(candidates)
        state["discovery_pending"] = []
        state["discovery_completed"] = {"https://bdl.stat.gov.pl/bdl/metadane/kategorie": {"row_count": len(candidates)}}
        state["catalogue_errors"] = {}

        FakeDriveControl.records["control/web-queue-v1.json"] = queue.rendered(state)

        stack = ExitStack()
        self.addCleanup(stack.close)

        stack.enter_context(patch.dict(os.environ, {
            "GITHUB_ACTIONS": "true",
            "GITHUB_REF": "refs/heads/main",
            "GUS_BDL_WEB_EMAIL": "test-user@example.com",
            "GUS_BDL_WEB_PASSWORD": "test-password",
            "GITHUB_STEP_SUMMARY": str(directory / "step-summary.md"),
        }))

        mock_storage = Mock()
        mock_storage.drive_service = Mock()
        mock_storage.authorize_writes = Mock()
        mock_storage.resolve_root = Mock(return_value="root-id")
        mock_storage.begin_write_session = Mock(return_value="session-1")
        mock_storage.resolve_zone = Mock(return_value="landing-id")

        stack.enter_context(patch("storage_manager.StorageManager", return_value=mock_storage))
        stack.enter_context(patch.object(bdl_bulk_plan, "_bulk_roots", return_value=("bulk", "control")))
        stack.enter_context(patch.object(bdl_bulk_plan, "_durable_status", return_value=(set(legacy or ()), set(legacy or ()))))
        stack.enter_context(patch.object(adaptive, "DriveControl", FakeDriveControl))
        stack.enter_context(patch.object(adaptive, "invoke", return_value={"url": "", "complete": True, "records": [], "expected_count": 0, "pages": 1}))
        stack.enter_context(redirect_stdout(io.StringIO()))

        return mock_storage

    def test_full_history_exceeds_18600_seconds_without_stopping(self):
        """Runner must not stop at 18,600s when max_seconds=None; it finishes all candidates."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            candidates = {f"P{i}": make_candidate(i) for i in range(1, 4)}
            self.setup_environment(workspace, candidates)

            clock = [0.0]
            def fake_time():
                t = clock[0]
                clock[0] += 5000.0
                return t

            def fake_persist(storage, session, landing_root, control, plan, node, result, ws, part_store):
                subgroup = plan["subgroup_id"]
                raw = f"archive-{subgroup}".encode()
                h = sha256(raw).hexdigest()
                receipt = {
                    "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                    "record_type": "native_partition_receipt", "subgroup_id": subgroup,
                    "selection_id": node["id"], "selection": node["scope"],
                    "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                    "archive_object": {"id": f"drive-{subgroup}", "size": len(raw), "sha256": h, "md5": "b" * 32},
                    "completed_at_utc": queue.now(),
                }
                if part_store:
                    part_store.save(receipt)
                return receipt

            def fake_invoke(item, node, ws, timeout):
                clock[0] = max(clock[0], 25000.0)
                return {"status": "download"}

            with patch("time.monotonic", side_effect=fake_time), \
                 patch.object(adaptive, "invoke_selection", side_effect=fake_invoke), \
                 patch.object(adaptive, "persist_download", side_effect=fake_persist):
                result = adaptive.run(workspace, max_seconds=None)

            self.assertGreater(clock[0], 18600.0)
            self.assertEqual("load_complete", result["status"])
            self.assertTrue(result["pass_complete"])
            self.assertTrue(result["load_complete"])
            self.assertEqual(3, result["new_files_this_run"])
            self.assertEqual(0, result["unvisited_subgroups_this_pass"])

    def test_explicit_max_seconds_interrupts_cleanly(self):
        """When max_seconds is explicitly provided and exceeded, runner reports interrupted."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            candidates = {f"P{i}": make_candidate(i) for i in range(1, 5)}
            self.setup_environment(workspace, candidates)

            clock = [0.0]
            def fake_time():
                clock[0] += 200.0
                return clock[0]

            def fake_persist(storage, session, landing_root, control, plan, node, result, ws, part_store):
                subgroup = plan["subgroup_id"]
                raw = f"archive-{subgroup}".encode()
                return {
                    "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                    "record_type": "native_partition_receipt", "subgroup_id": subgroup,
                    "selection_id": node["id"], "selection": node["scope"],
                    "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                    "archive_object": {"id": f"drive-{subgroup}", "size": len(raw), "sha256": "a" * 64, "md5": "b" * 32},
                    "completed_at_utc": queue.now(),
                }

            with patch("time.monotonic", side_effect=fake_time), \
                 patch.object(adaptive, "invoke_selection", return_value={"status": "download"}), \
                 patch.object(adaptive, "persist_download", side_effect=fake_persist):
                result = adaptive.run(workspace, max_seconds=300)

            self.assertEqual("interrupted", result["status"])
            self.assertFalse(result["load_complete"])
            self.assertEqual("interrupted", result["run_stop_reason"])

    def test_writer_lock_prevents_concurrent_active_execution(self):
        """Active writer lock with recent heartbeat prevents second runner startup."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            self.setup_environment(workspace, {"P1": make_candidate(1)})

            # Simulate another active writer with heartbeat 60s ago
            active_lock = {
                "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                "record_type": "bdl_writer_lock", "host": "other-host", "pid": 99999,
                "started_at_utc": queue.now(), "heartbeat_utc": queue.now(), "status": "active",
            }
            FakeDriveControl.records["control/bdl-writer-lock.json"] = queue.rendered(active_lock)

            with self.assertRaisesRegex(RuntimeError, "Another active writer owns the BDL campaign"):
                adaptive.run(workspace, max_seconds=60)

    def test_writer_lock_overrides_stale_lock(self):
        """Stale writer lock (> 600s old) is overridden safely."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            self.setup_environment(workspace, {"P1": make_candidate(1)})

            # Stale heartbeat (15 minutes ago)
            stale_time = "2026-09-17T00:00:00Z"
            stale_lock = {
                "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                "record_type": "bdl_writer_lock", "host": "dead-host", "pid": 11111,
                "started_at_utc": stale_time, "heartbeat_utc": stale_time, "status": "active",
            }
            FakeDriveControl.records["control/bdl-writer-lock.json"] = queue.rendered(stale_lock)

            def fake_persist(storage, session, landing_root, control, p, node, result, ws, part_store):
                raw = b"test-raw"
                return {
                    "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                    "record_type": "native_partition_receipt", "subgroup_id": p["subgroup_id"],
                    "selection_id": node["id"], "selection": node["scope"],
                    "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                    "archive_object": {"id": "d1", "size": len(raw), "sha256": sha256(raw).hexdigest(), "md5": "b" * 32},
                    "completed_at_utc": queue.now(),
                }

            with patch.object(adaptive, "invoke_selection", return_value={"status": "download"}), \
                 patch.object(adaptive, "persist_download", side_effect=fake_persist):
                result = adaptive.run(workspace, max_seconds=60)

            self.assertEqual("load_complete", result["status"])
            # Verify lock was released on exit
            final_lock = json.loads(FakeDriveControl.records["control/bdl-writer-lock.json"].decode())
            self.assertEqual("released", final_lock["status"])

    def test_codespace_authorization_requires_opt_in_and_credentials(self):
        """Codespace execution requires explicit opt-in and valid env credentials."""
        mock_storage = Mock()
        with patch.dict(os.environ, {
            "GITHUB_ACTIONS": "false",
            "ZOHELO_ALLOW_CODESPACE_EXECUTION": "",
            "GUS_BDL_WEB_EMAIL": "",
            "GUS_BDL_WEB_PASSWORD": "",
        }, clear=True):
            # No opt-in
            with self.assertRaisesRegex(PermissionError, "explicit Codespace production authorization"):
                adaptive.authorize_production_runner(mock_storage, allow_codespace=False)

            # Opt-in but missing credentials
            with self.assertRaisesRegex(PermissionError, "Missing required Web credentials: GUS_BDL_WEB_EMAIL, GUS_BDL_WEB_PASSWORD"):
                adaptive.authorize_production_runner(mock_storage, allow_codespace=True)

            # Opt-in with credentials passes
            with patch.dict(os.environ, {
                "GUS_BDL_WEB_EMAIL": "test@zohelo.com",
                "GUS_BDL_WEB_PASSWORD": "secret-pass",
                "ZOHELO_ALLOW_UNCOMMITTED_DEV": "true",
            }):
                host = adaptive.authorize_production_runner(mock_storage, allow_codespace=True)
                self.assertEqual("codespace", host)
                mock_storage.authorize_writes.assert_called_once()

    def test_large_selection_falls_back_to_exact_partitions(self):
        """Subgroup with whole-export provider error falls back to partition plan without stopping."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            self.setup_environment(workspace, {"P1313": make_candidate(1313)})

            def fake_invoke(item, node, ws, timeout):
                if node["scope"]["kind"] == "whole":
                    raise adaptive.WorkerFailure("HTTP 500 Large selection", "provider_error")
                if node["scope"]["kind"] == "dimensions":
                    return {"status": "dimensions", "subgroup_id": "P1313", "selection_id": node["id"],
                            "dimensions": [{"id": "years", "year_axis": True, "options": [{"value": "2024", "label": "2024"}]}]}
                if node["scope"]["kind"] == "layouts":
                    return {"status": "layouts", "subgroup_id": "P1313", "selection_id": node["id"],
                            "layouts": [{"id": "l", "kind": "select", "value": "1", "label": "Standard"}]}
                if node["scope"]["kind"] == "territories":
                    return {"status": "territories", "subgroup_id": "P1313", "selection_id": node["id"],
                            "items": [{"value": "0", "label": "Polska"}], "advertised_count": 1}
                if node["scope"]["kind"] == "download":
                    return {"status": "download", "subgroup_id": "P1313", "selection_id": node["id"]}
                raise ValueError(f"Unexpected kind: {node['scope']['kind']}")

            def fake_persist(storage, session, landing_root, control, plan, node, result, ws, part_store):
                raw = b"partition-raw"
                receipt = {
                    "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                    "record_type": "native_partition_receipt", "subgroup_id": plan["subgroup_id"],
                    "selection_id": node["id"], "selection": node["scope"],
                    "dimension_inventory_sha256": "0" * 64,
                    "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                    "archive_object": {"id": "d-part", "size": len(raw), "sha256": sha256(raw).hexdigest(), "md5": "c" * 32},
                    "completed_at_utc": queue.now(),
                }
                if part_store:
                    part_store.save(receipt)
                return receipt

            with patch.object(adaptive, "invoke_selection", side_effect=fake_invoke), \
                 patch.object(adaptive, "persist_download", side_effect=fake_persist), \
                 patch.object(adaptive, "check_stored_receipt"):
                result = adaptive.run(workspace, max_seconds=60)

            self.assertEqual("load_complete", result["status"])
            self.assertEqual(1, result["new_files_this_run"])

    def test_resume_mode_verifies_receipts_and_avoids_redownload(self):
        """In resume mode, completed plans with valid receipts are reused without redownloading."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            self.setup_environment(workspace, {"P1": make_candidate(1)})

            # Populate completed plan and receipt in storage
            raw = b"existing-archive"
            h = sha256(raw).hexdigest()
            plan = parts.new_whole_plan("P1", "https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/1/2/1")
            receipt = {
                "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                "record_type": "native_partition_receipt", "subgroup_id": "P1",
                "selection_id": plan["root"], "selection": plan["nodes"][plan["root"]]["scope"],
                "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                "archive_object": {"id": "drive-P1", "size": len(raw), "sha256": h, "md5": "d" * 32},
                "completed_at_utc": queue.now(),
            }
            parts.accept_download(plan, plan["nodes"][plan["root"]], receipt)
            FakeDriveControl.records["control/web-parts-P1-v1.json"] = queue.rendered(plan)

            with patch.object(adaptive, "check_stored_receipt") as mock_check, \
                 patch.object(adaptive, "invoke_selection") as mock_invoke:
                result = adaptive.run(workspace, mode="resume")

            mock_check.assert_called_once()
            mock_invoke.assert_not_called()
            self.assertEqual("load_complete", result["status"])
            self.assertEqual(0, result["new_files_this_run"])

    def test_reload_mode_ignores_existing_plan_and_redownloads(self):
        """In reload mode, existing plans are ignored and fresh downloads take place."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            self.setup_environment(workspace, {"P1": make_candidate(1)})

            # Populate completed plan from previous run
            plan = parts.new_whole_plan("P1", "https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/1/2/1")
            parts.accept_download(plan, plan["nodes"][plan["root"]], {
                "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                "record_type": "native_partition_receipt", "subgroup_id": "P1",
                "selection_id": plan["root"], "selection": plan["nodes"][plan["root"]]["scope"],
                "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                "archive_object": {"id": "d-old", "size": 10, "sha256": "a" * 64, "md5": "b" * 32},
                "completed_at_utc": queue.now(),
            })
            FakeDriveControl.records["control/web-parts-P1-v1.json"] = queue.rendered(plan)

            def fake_persist(storage, session, landing_root, control, p, node, result, ws, part_store):
                raw = b"new-archive"
                return {
                    "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                    "record_type": "native_partition_receipt", "subgroup_id": "P1",
                    "selection_id": node["id"], "selection": node["scope"],
                    "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                    "archive_object": {"id": "d-new", "size": len(raw), "sha256": sha256(raw).hexdigest(), "md5": "b" * 32},
                    "completed_at_utc": queue.now(),
                }

            with patch.object(adaptive, "invoke_selection", return_value={"status": "download"}) as mock_invoke, \
                 patch.object(adaptive, "persist_download", side_effect=fake_persist):
                result = adaptive.run(workspace, mode="reload")

            mock_invoke.assert_called_once()
            self.assertEqual(1, result["new_files_this_run"])
            self.assertEqual("load_complete", result["status"])

    def test_corrupted_receipt_is_rejected_and_redownloaded(self):
        """Corrupted Drive object triggers re-download instead of silent acceptance."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            mock_storage = self.setup_environment(workspace, {"P1": make_candidate(1)})

            # Drive returns mismatched size (file corrupted or truncated)
            mock_storage.drive_service.files().get().execute.return_value = {
                "id": "drive-P1", "size": "9999", "md5Checksum": "wrong", "trashed": False,
            }

            plan = parts.new_whole_plan("P1", "https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/1/2/1")
            receipt = {
                "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                "record_type": "native_partition_receipt", "subgroup_id": "P1",
                "selection_id": plan["root"], "selection": plan["nodes"][plan["root"]]["scope"],
                "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                "archive_object": {"id": "drive-P1", "size": 10, "sha256": "a" * 64, "md5": "b" * 32},
                "completed_at_utc": queue.now(),
            }
            parts.accept_download(plan, plan["nodes"][plan["root"]], receipt)
            FakeDriveControl.records["control/web-parts-P1-v1.json"] = queue.rendered(plan)

            def fake_persist(storage, session, landing_root, control, p, node, result, ws, part_store):
                raw = b"fresh-download"
                return {
                    "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                    "record_type": "native_partition_receipt", "subgroup_id": "P1",
                    "selection_id": node["id"], "selection": node["scope"],
                    "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                    "archive_object": {"id": "drive-P1-new", "size": len(raw), "sha256": sha256(raw).hexdigest(), "md5": "b" * 32},
                    "completed_at_utc": queue.now(),
                }

            with patch.object(adaptive, "invoke_selection", return_value={"status": "download"}) as mock_invoke, \
                 patch.object(adaptive, "persist_download", side_effect=fake_persist):
                result = adaptive.run(workspace, mode="resume")

            # Must have invoked download because existing receipt failed check
            mock_invoke.assert_called_once()
            self.assertEqual(1, result["new_files_this_run"])

    def test_fair_traversal_slices_multi_partition_subgroup_and_yields_to_unvisited(self):
        """Multi-partition subgroup yields turns to other unvisited subgroups after a slice."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            candidates = {"P1": make_candidate(1), "P2": make_candidate(2)}
            self.setup_environment(workspace, candidates)

            order = []

            def fake_invoke(item, node, ws, timeout):
                order.append((item["subgroup_id"], node["scope"]["kind"]))
                if item["subgroup_id"] == "P1":
                    if node["scope"]["kind"] == "download" and node["scope"].get("territories") == ["all"]:
                        raise adaptive.WorkerFailure("Large selection HTTP 500", "provider_error")
                    if node["scope"]["kind"] == "dimensions":
                        return {"status": "dimensions", "subgroup_id": "P1", "selection_id": node["id"],
                                "dimensions": [{"id": "years", "year_axis": True, "options": [{"value": "2024", "label": "2024"}]}]}
                    if node["scope"]["kind"] == "layouts":
                        return {"status": "layouts", "subgroup_id": "P1", "selection_id": node["id"],
                                "layouts": [{"id": "l", "kind": "select", "value": "1", "label": "Standard"}]}
                    if node["scope"]["kind"] == "territories":
                        # Return 5000 territories -> will create >20 download tasks!
                        return {"status": "territories", "subgroup_id": "P1", "selection_id": node["id"],
                                "items": [{"value": str(i), "label": f"T{i}"} for i in range(5000)], "advertised_count": 5000}
                    if node["scope"]["kind"] == "download":
                        return {"status": "download", "subgroup_id": "P1", "selection_id": node["id"]}
                elif item["subgroup_id"] == "P2":
                    return {"status": "download", "subgroup_id": "P2", "selection_id": node["id"]}
                raise ValueError(f"Unexpected invocation: {item['subgroup_id']}, {node['scope']['kind']}")

            def fake_persist(storage, session, landing_root, control, p, node, result, ws, part_store):
                raw = b"data"
                receipt = {
                    "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                    "record_type": "native_partition_receipt", "subgroup_id": p["subgroup_id"],
                    "selection_id": node["id"], "selection": node["scope"],
                    "dimension_inventory_sha256": "0" * 64,
                    "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                    "archive_object": {"id": f"drive-{node['id'][:8]}", "size": len(raw), "sha256": sha256(raw).hexdigest(), "md5": "a" * 32},
                    "completed_at_utc": queue.now(),
                }
                if part_store:
                    part_store.save(receipt)
                return receipt

            with patch.object(adaptive, "invoke_selection", side_effect=fake_invoke), \
                 patch.object(adaptive, "persist_download", side_effect=fake_persist), \
                 patch.object(adaptive, "check_stored_receipt"):
                result = adaptive.run(workspace, max_seconds=60)

            self.assertEqual("load_complete", result["status"])
            self.assertTrue(result["load_complete"])
            # Verify that P2 (unvisited) ran before P1 finished all its slices!
            p2_indices = [idx for idx, (sub, kind) in enumerate(order) if sub == "P2"]
            p1_indices = [idx for idx, (sub, kind) in enumerate(order) if sub == "P1"]
            self.assertTrue(p2_indices, "P2 must have run")
            # P2 must appear before the final tasks of P1
            self.assertLess(p2_indices[0], p1_indices[-1], "P2 must run before P1 finishes all slices")

    def test_subgroup_repeated_failures_are_isolated_and_do_not_abort_entire_run(self):
        """When a single subgroup repeatedly fails, it is isolated after max_subgroup_failures without killing the run."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            candidates = {"P1": make_candidate(1), "P2": make_candidate(2)}
            self.setup_environment(workspace, candidates)

            order = []
            def fake_invoke(item, node, ws, timeout):
                order.append(item["subgroup_id"])
                if item["subgroup_id"] == "P1":
                    if node["scope"]["kind"] == "download" and node["scope"].get("territories") == ["all"]:
                        raise adaptive.WorkerFailure("Large selection", "provider_error")
                    if node["scope"]["kind"] == "dimensions":
                        return {"status": "dimensions", "subgroup_id": "P1", "selection_id": node["id"],
                                "dimensions": [{"id": "years", "year_axis": True, "options": [{"value": "2024", "label": "2024"}]}]}
                    if node["scope"]["kind"] == "layouts":
                        return {"status": "layouts", "subgroup_id": "P1", "selection_id": node["id"],
                                "layouts": [{"id": "l", "kind": "select", "value": "1", "label": "Standard"}]}
                    if node["scope"]["kind"] == "territories":
                        return {"status": "territories", "subgroup_id": "P1", "selection_id": node["id"],
                                "items": [{"value": str(i), "label": f"T{i}"} for i in range(100)], "advertised_count": 100}
                    if node["scope"]["kind"] == "download":
                        raise adaptive.WorkerFailure("Timeout on P1 chunk", "provider_timeout")
                elif item["subgroup_id"] == "P2":
                    return {"status": "download", "subgroup_id": "P2", "selection_id": node["id"]}
                raise ValueError(f"Unexpected invocation: {item['subgroup_id']}")

            def fake_persist(storage, session, landing_root, control, p, node, result, ws, part_store):
                raw = b"data"
                receipt = {
                    "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                    "record_type": "native_partition_receipt", "subgroup_id": p["subgroup_id"],
                    "selection_id": node["id"], "selection": node["scope"],
                    "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                    "archive_object": {"id": f"drive-{node['id'][:8]}", "size": len(raw), "sha256": sha256(raw).hexdigest(), "md5": "a" * 32},
                    "completed_at_utc": queue.now(),
                }
                if part_store:
                    part_store.save(receipt)
                return receipt

            with patch.object(adaptive, "invoke_selection", side_effect=fake_invoke), \
                 patch.object(adaptive, "persist_download", side_effect=fake_persist), \
                 patch.object(adaptive, "check_stored_receipt"):
                result = adaptive.run(workspace, max_seconds=60)

            self.assertEqual("pass_complete", result["status"])
            self.assertEqual(1, result["new_files_this_run"])
            p1_count = order.count("P1")
            # Whole attempt (1) + dimensions (1) + layouts (1) + territories (1) + 5 failed download chunks = 9
            self.assertLessEqual(p1_count, 12, "P1 must be capped and not run indefinitely")
            self.assertIn("P2", order, "P2 must still run despite P1 failures")

    def test_concurrent_workers_process_multiple_subgroups_in_parallel(self):
        """Multi-worker runner (concurrency=3) distributes subgroups across isolated worker workspaces."""
        with tempfile.TemporaryDirectory() as td:
            workspace = Path(td)
            candidates = {f"P{i}": make_candidate(i) for i in range(1, 7)}
            self.setup_environment(workspace, candidates)

            invoked_workspaces = []
            work_lock = threading.Lock()

            def fake_invoke(item, node, ws, timeout):
                with work_lock:
                    invoked_workspaces.append(str(ws))
                time.sleep(0.02)
                return {"status": "download"}

            def fake_persist(storage, session, landing_root, control, p, node, result, ws, part_store):
                raw = b"test-raw"
                receipt = {
                    "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
                    "record_type": "native_partition_receipt", "subgroup_id": p["subgroup_id"],
                    "selection_id": node["id"], "selection": node["scope"],
                    "landing_scope": "native_bytes_only", "content_validation": "not_performed",
                    "archive_object": {"id": f"d-{p['subgroup_id']}", "size": len(raw), "sha256": sha256(raw).hexdigest(), "md5": "b" * 32},
                    "completed_at_utc": queue.now(),
                }
                if part_store:
                    part_store.save(receipt)
                return receipt

            with patch.object(adaptive, "invoke_selection", side_effect=fake_invoke), \
                 patch.object(adaptive, "persist_download", side_effect=fake_persist):
                result = adaptive.run(workspace, concurrency=3)

            self.assertEqual("load_complete", result["status"])
            self.assertEqual(6, result["new_files_this_run"])
            worker_dirs = {Path(p).name for p in invoked_workspaces}
            self.assertTrue({"worker-0", "worker-1", "worker-2"} <= worker_dirs)

    def test_main_exit_codes(self):
        """main() returns 0 on complete load or graceful max_seconds interruption, and 1 on error."""
        with patch.object(adaptive, "run", return_value={"status": "load_complete", "run_stop_reason": "load_complete"}):
            with patch("sys.argv", ["bdl_web_adaptive.py", "--workspace", "fake_ws"]):
                self.assertEqual(0, adaptive.main())

        with patch.object(adaptive, "run", return_value={"status": "interrupted", "run_stop_reason": "interrupted"}):
            with patch("sys.argv", ["bdl_web_adaptive.py", "--workspace", "fake_ws", "--max-seconds", "18600"]):
                self.assertEqual(0, adaptive.main())

        with patch.object(adaptive, "run", return_value={"status": "incomplete", "run_stop_reason": "interrupted"}):
            with patch("sys.argv", ["bdl_web_adaptive.py", "--workspace", "fake_ws"]):
                self.assertEqual(1, adaptive.main())


if __name__ == "__main__":
    unittest.main()
