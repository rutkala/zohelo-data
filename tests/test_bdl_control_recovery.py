"""No-network recovery proofs: read-only resolution, no lost work or proxy fallback."""
from copy import deepcopy
from hashlib import md5, sha256
from pathlib import Path
import json
import re
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import httplib2
from googleapiclient.errors import HttpError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import bdl_web_queue as queue
import bdl_web_adaptive as adaptive
import bdl_web_partitions as parts


def value(n=0):
    return {"format_version": 1, "source_id": "gus_bdl", "transport": "web_ui", "candidates": {}, "generation": n}


class Request:
    def __init__(self, fn): self.fn = fn
    def execute(self, num_retries=0): return self.fn()


class Drive:
    def __init__(self):
        self.objects = {}
        self.writes = []
        self.error = None
        self.apply = True
        self.after_write = None
        self.readback_errors = 0
        self.readback_wrong = False
        self.metadata_errors = None
        self.change_during_read = False
        self.metadata_reads = 0
        self.close = Mock()
        self.storage = SimpleNamespace(drive_service=SimpleNamespace(files=lambda: self, _http=SimpleNamespace(close=self.close)))

    def put(self, identity, raw, name="web-queue-v1.json"):
        self.objects[identity] = {
            "id": identity, "name": name, "parents": ["control"], "trashed": False,
            "size": str(len(raw)), "version": "1", "modifiedTime": "2026-09-23T00:00:00Z",
            "md5Checksum": md5(raw).hexdigest(), "bytes": raw,
            "appProperties": {"source_id": "gus_bdl", "transport": "web_ui", "sha256": sha256(raw).hexdigest()},
        }

    def _mutation(self, kind, kwargs):
        def apply():
            self.writes.append(kind)
            media = kwargs["media_body"]; raw = media.getbytes(0, media.size())
            identity = kwargs.get("fileId") or "created-id"
            if self.apply:
                self.put(identity, raw, kwargs["body"].get("name", "web-queue-v1.json"))
                self.objects[identity]["appProperties"] = deepcopy(kwargs["body"]["appProperties"])
            if self.after_write: self.after_write(self, identity)
            if self.error: raise self.error
            return {"id": identity}
        return Request(apply)

    def create(self, **kwargs): return self._mutation("create", kwargs)
    def update(self, **kwargs): return self._mutation("update", kwargs)

    def list(self, **kwargs):
        def read():
            name_match = re.search(r"name\s*=\s*'([^']+)'", kwargs["q"])
            name = name_match.group(1) if name_match else None
            items = [deepcopy({k: v for k, v in x.items() if k != "bytes"}) for x in self.objects.values()
                     if not x["trashed"] and "control" in x["parents"] and (name is None or x["name"] == name)]
            return {"files": items}
        return Request(read)

    def get_media(self, **kwargs):
        def read():
            if self.writes and self.readback_errors:
                self.readback_errors -= 1
                raise TimeoutError("readback response lost")
            if self.writes and self.readback_wrong: return b"wrong"
            item = self.objects[kwargs["fileId"]]; raw = item["bytes"]
            if self.change_during_read and self.metadata_reads:
                item["version"] = str(int(item["version"]) + 1)
            return raw
        return Request(read)

    def get(self, **kwargs):
        def read():
            self.metadata_reads += 1
            if self.metadata_errors: raise self.metadata_errors
            return deepcopy({k: v for k, v in self.objects[kwargs["fileId"]].items() if k != "bytes"})
        return Request(read)


class ControlRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.sleep = patch.object(queue.time, "sleep").start()
        self.addCleanup(patch.stopall)
        self.drive = Drive()

    def control(self, existing=True):
        if existing: self.drive.put("original-id", queue.rendered(value()))
        return queue.DriveControl(self.drive.storage, "control", "web-queue-v1.json")

    def assert_failed(self, control):
        observed, identity = control.observed, control.file_id
        with self.assertRaises(Exception): control.save(value(1))
        self.assertEqual(control.observed, observed)
        self.assertEqual(control.file_id, identity)
        self.assertTrue(control.poisoned)
        self.assertEqual(len(self.drive.writes), 1)
        with self.assertRaisesRegex(RuntimeError, "explicit reconciliation"): control.save(value(2))
        self.assertEqual(len(self.drive.writes), 1)

    def test_update_applied_response_lost_recovers_without_any_other_write(self):
        c = self.control(); self.drive.error = TimeoutError("lost")
        with patch("builtins.print") as output: c.save(value(1))
        self.assertEqual(c.observed, queue.rendered(value(1)))
        self.assertEqual(self.drive.writes, ["update"])
        self.assertEqual(list(self.drive.objects), ["original-id"])
        self.assertFalse(c.poisoned); self.drive.close.assert_called()
        self.assertFalse(json.loads(output.call_args.args[0])["write_replayed"])
        self.drive.error = None; c.save(value(2)); self.assertEqual(c.load(), value(2))

    def test_create_response_lost_adopts_the_unique_created_id(self):
        c = self.control(False); self.drive.error = TimeoutError("lost")
        c.save(value(1))
        self.assertEqual(c.file_id, "created-id")
        self.assertEqual(self.drive.writes, ["create"])
        self.assertEqual(len(self.drive.objects), 1)

    def test_readback_timeout_after_successful_update_recovers(self):
        c = self.control(); self.drive.readback_errors = 1; c.save(value(1))
        self.assertEqual(c.observed, queue.rendered(value(1))); self.assertEqual(self.drive.writes, ["update"])

    def test_readback_timeout_after_successful_create_recovers(self):
        c = self.control(False); self.drive.readback_errors = 1; c.save(value(1))
        self.assertEqual(c.load(), value(1)); self.assertEqual(self.drive.writes, ["create"])

    def test_prior_state_only_does_not_replay_update(self):
        c = self.control(); self.drive.error = TimeoutError("lost"); self.drive.apply = False; self.assert_failed(c)

    def test_missing_create_does_not_replay_create(self):
        c = self.control(False); self.drive.error = TimeoutError("lost"); self.drive.apply = False; self.assert_failed(c)

    def test_duplicate_names_during_update_are_rejected(self):
        c = self.control(); self.drive.error = TimeoutError("lost")
        self.drive.after_write = lambda d, i: d.put("duplicate", queue.rendered(value(1)))
        self.assert_failed(c)

    def test_duplicate_names_during_create_are_rejected(self):
        c = self.control(False); self.drive.error = TimeoutError("lost")
        self.drive.after_write = lambda d, i: d.put("duplicate", queue.rendered(value(1)))
        self.assert_failed(c)

    def test_unexpected_remote_value_is_rejected(self):
        c = self.control(); self.drive.error = TimeoutError("lost")
        self.drive.after_write = lambda d, i: d.put(i, queue.rendered(value(99)))
        self.assert_failed(c)

    def test_corrupt_sha_is_rejected(self):
        c = self.control(); self.drive.error = TimeoutError("lost")
        self.drive.after_write = lambda d, i: d.objects[i]["appProperties"].update(sha256="0" * 64)
        self.assert_failed(c)

    def test_corrupt_md5_is_rejected(self):
        c = self.control(); self.drive.error = TimeoutError("lost")
        self.drive.after_write = lambda d, i: d.objects[i].update(md5Checksum="0" * 32)
        self.assert_failed(c)

    def test_metadata_change_during_read_is_rejected(self):
        c = self.control(); self.drive.error = TimeoutError("lost"); self.drive.change_during_read = True; self.assert_failed(c)

    def test_trashed_candidate_is_rejected(self):
        c = self.control(); self.drive.error = TimeoutError("lost")
        self.drive.after_write = lambda d, i: d.objects[i].update(trashed=True)
        self.assert_failed(c)

    def test_auth_denial_is_not_recovered_even_when_bytes_match(self):
        c = self.control(); self.drive.error = HttpError(httplib2.Response({"status": "403"}), b"denied")
        self.assert_failed(c); self.assertEqual(self.drive.metadata_reads, 0)

    def test_auth_denial_during_reconciliation_stops(self):
        c = self.control(); self.drive.error = TimeoutError("lost")
        self.drive.metadata_errors = HttpError(httplib2.Response({"status": "401"}), b"denied")
        self.assert_failed(c); self.assertEqual(self.drive.metadata_reads, 1)

    def test_transient_service_error_with_verified_candidate_recovers(self):
        c = self.control(); self.drive.error = HttpError(httplib2.Response({"status": "503"}), b"unavailable")
        c.save(value(1)); self.assertEqual(c.load(), value(1)); self.assertEqual(self.drive.writes, ["update"])

    def test_persistent_read_error_stops_with_bounded_attempts(self):
        c = self.control(); self.drive.error = TimeoutError("lost"); self.drive.metadata_errors = TimeoutError("read")
        self.assert_failed(c); self.assertEqual(self.drive.metadata_reads, 3)

    def test_wrong_normal_readback_does_not_replace_observed(self):
        c = self.control(); self.drive.readback_wrong = True; self.assert_failed(c)

    def test_oversized_candidate_does_not_write(self):
        c = self.control()
        with patch.object(queue, "MAX_CONTROL_BYTES", 10), self.assertRaisesRegex(RuntimeError, "bound"): c.save(value(1))
        self.assertEqual(self.drive.writes, [])

    def test_prewrite_drift_does_not_write(self):
        c = self.control(); self.drive.put("original-id", queue.rendered(value(99)))
        with self.assertRaisesRegex(RuntimeError, "outside"): c.save(value(1))
        self.assertEqual(self.drive.writes, [])

    def test_ordinary_create_and_update(self):
        c = self.control(False); c.save(value()); c.save(value(1))
        self.assertEqual(c.load(), value(1)); self.assertEqual(self.drive.writes, ["create", "update"])


class MonitorWorkspaceTests(unittest.TestCase):
    def test_bdl_overrides_do_not_change_dbw_monitoring(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        import wave0_supervisor as monitor
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp); root = base / "old-runtime"; bdl_root = base / "new-runtime"; workspace = base / "new-workspace"
            workspace.mkdir(); (workspace / "bootstrap-summary.json").write_text(json.dumps({"selection_complete_subgroups": 2075, "new_files_this_run": 3}))
            finder = Mock(return_value=[123])
            report = monitor.observe(root, finder, bdl_workspace=workspace, bdl_runtime_root=bdl_root)
            self.assertEqual(report["bdl"]["reported_selection_complete_subgroups"], 2075)
            self.assertEqual(report["bdl"]["reported_new_files_this_run"], 3)
            self.assertEqual(report["bdl"]["checkpoint_error"], None)
            self.assertEqual(report["dbw"]["checkpoint_error"], "checkpoint_missing")
            self.assertEqual(finder.call_args_list[0].args[0], bdl_root / "src/bdl_web_adaptive.py")
            self.assertEqual(finder.call_args_list[1].args[0], root / "src/dbw_bronze_loader.py")


class BrowserProxyTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Node is required for browser transport option checks")
    def test_explicit_playwright_routes_validate_all_ten_ports(self):
        root = Path(__file__).resolve().parents[1]
        script = """
import assert from 'node:assert/strict';
import { bdlBrowserOptions } from './portal/scripts/bdl-web-proxy.mjs';
assert.deepEqual(bdlBrowserOptions({}), {headless: true});
for (let port=8081; port<=8090; port++) {
  const server = `http://127.0.0.1:${port}`;
  assert.deepEqual(bdlBrowserOptions({BDL_WEB_PROXY:server}), {headless:true, proxy:{server}});
}
for (const server of ['', 'http://remote:8081', 'http://127.0.0.1:0', 'http://127.0.0.1:65536', 'http://user:secret@127.0.0.1:8081']) {
  assert.throws(() => bdlBrowserOptions({BDL_WEB_PROXY:server}));
}
"""
        subprocess.run(["node", "--input-type=module", "-e", script], cwd=root, check=True, timeout=20)
        for filename in ("bdl-web-selection-worker.mjs", "bdl-web-catalogue.mjs"):
            source = (root / "portal/scripts" / filename).read_text()
            self.assertIn("chromium.launch(bdlBrowserOptions())", source)
        source = (root / "src/bdl_web_adaptive.py").read_text()
        self.assertIn('env["BDL_WEB_PROXY"] = proxy', source)
        self.assertIn('env["BDL_WEB_PROXY"] = worker_proxy', source)


class ResumeAndProxyTests(unittest.TestCase):
    def test_landed_receipt_can_be_verified_without_changing_its_plan(self):
        node = parts.task("download", dimensions={}, layout=None, territories=["all"])
        plan = parts.new_whole_plan("P1", queue.WEB + "/dane/podgrup/wymiary/1/2/1", root_task=node)
        obj = {"id": "native", "size": 3, "sha256": sha256(b"abc").hexdigest(), "md5": md5(b"abc").hexdigest()}
        receipt = {"subgroup_id": "P1", "selection_id": node["id"], "selection": node["scope"],
                   "landing_scope": "native_bytes_only", "archive_object": obj}
        target = plan["nodes"][plan["root"]]; parts.accept_download(plan, target, receipt); before = deepcopy(plan)
        request = Request(lambda: {"id": "native", "size": "3", "md5Checksum": obj["md5"], "appProperties": {"sha256": obj["sha256"]}, "trashed": False})
        files = SimpleNamespace(get=lambda **kw: request)
        storage = SimpleNamespace(drive_service=SimpleNamespace(files=lambda: files))
        adaptive.check_stored_receipt(storage, plan, target, receipt)
        self.assertEqual(plan, before)
        failing_files = SimpleNamespace(get=Mock(side_effect=TimeoutError("transport")))
        failing_storage = SimpleNamespace(drive_service=SimpleNamespace(files=lambda: failing_files))
        with self.assertRaises(adaptive.CampaignControlFailure):
            adaptive.check_stored_receipt(failing_storage, plan, target, receipt)
        wrong = deepcopy(receipt); wrong["selection_id"] = "wrong"
        with self.assertRaises(ValueError): adaptive.check_stored_receipt(storage, plan, target, wrong)

    def test_ten_distinct_proxies_map_one_per_worker(self):
        proxies = [f"http://127.0.0.1:{p}" for p in range(8081, 8091)]
        actual = adaptive.validate_cluster_proxies(proxies, require_proxy_count=10, concurrency=10)
        self.assertEqual([actual[i % len(actual)] for i in range(10)], proxies)

    def test_missing_repeated_invalid_and_mismatched_proxies_fail(self):
        for proxies, workers in [([], 10), (["http://127.0.0.1:8081"] * 10, 10), (["http://127.0.0.1:99999"] * 10, 10), (["http://example.com:8081"] * 10, 10), ([f"http://127.0.0.1:{p}" for p in range(8081, 8091)], 9)]:
            with self.subTest(proxies=proxies, workers=workers), self.assertRaises(ValueError):
                adaptive.validate_cluster_proxies(proxies, require_proxy_count=10, concurrency=workers)

    def test_missing_required_proxies_fail_before_storage_initialization(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(adaptive, "BDL_PROXY_CLUSTER_FILE", Path(tmp) / "missing"), patch("storage_manager.StorageManager") as storage:
            with self.assertRaises(ValueError):
                adaptive.run(Path(tmp), concurrency=10, require_proxy_count=10)
            storage.assert_not_called()


if __name__ == "__main__":
    unittest.main()
