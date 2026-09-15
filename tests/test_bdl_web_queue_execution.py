"""Exercise the actual queue loop with isolated browser and Drive boundaries."""
from contextlib import ExitStack, redirect_stdout
from copy import deepcopy
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import bdl_web_queue as queue
import bdl_bulk_ingest
import bdl_bulk_plan
import storage_manager


def candidate(number):
    return {"category_id": "K1", "group_id": "G2", "subgroup_id": f"P{number}",
            "subgroup_name": f"Group {number}", "web_catalogue_verified": True,
            "url": f"{queue.WEB}/dane/podgrup/wymiary/1/2/{number}"}


class QueueExecutionTests(unittest.TestCase):
    def environment(self, directory, *, landed=()):
        state = queue.new_state()
        state["candidates"] = {f"P{i}": candidate(i) for i in (1, 2)}
        state["discovery_pending"] = []
        state["discovery_completed"] = {"test-web-table": {"row_count": 2}}
        store = Mock()
        store.load.return_value = state
        saves = []
        store.save.side_effect = lambda value: saves.append(deepcopy(value))
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.dict(os.environ, {
            "GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/main",
            "GITHUB_STEP_SUMMARY": str(Path(directory) / "summary.md"),
        }))
        stack.enter_context(patch.object(storage_manager, "StorageManager"))
        stack.enter_context(patch.object(bdl_bulk_plan, "_bulk_roots", return_value=("bulk", "control")))
        stack.enter_context(patch.object(bdl_bulk_plan, "_durable_status", return_value=(set(landed), set(landed))))
        stack.enter_context(patch.object(queue, "DriveControl", return_value=store))
        stack.enter_context(redirect_stdout(io.StringIO()))
        return state, saves

    @staticmethod
    def downloaded(script, env, timeout, result_path):
        subgroup = env["BDL_BULK_SUBGROUP_ID"]
        raw = b"PK-test-fixture-" + subgroup.encode()
        (Path(env["BDL_BULK_OUT_DIR"]) / f"download-{subgroup}.zip").write_bytes(raw)
        return {"subgroupId": subgroup, "status": "downloaded_relational_export",
                "archive": {"sha256": sha256(raw).hexdigest()}}

    def test_provider_error_continues_to_next_subgroup_and_retains_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            _, saves = self.environment(directory)
            attempted = []
            def worker(script, env, timeout, result_path):
                attempted.append(env["BDL_BULK_SUBGROUP_ID"])
                if attempted[-1] == "P1":
                    raise RuntimeError("BDL result-table HTTP 500")
                return self.downloaded(script, env, timeout, result_path)
            with patch.object(queue, "invoke", side_effect=worker), patch.object(
                bdl_bulk_ingest, "ingest_archive",
                return_value={"status": "bdl_web_bulk_landed", "row_count": 12},
            ) as publish:
                result = queue.run(Path(directory), 600)
            self.assertEqual(["P1", "P2"], attempted)
            publish.assert_called_once()
            self.assertEqual("P2", publish.call_args.args[1])
            self.assertEqual(1, result["completed_this_run"])
            self.assertEqual(12, result["rows_landed_this_run"])
            self.assertEqual("incomplete", result["status"])
            self.assertEqual(["P1"], result["failed_subgroups"])
            self.assertEqual(1, saves[-1]["failures"]["P1"]["attempts"])
            self.assertTrue((Path(directory) / "landed-P2.json").exists())
            self.assertTrue((Path(directory) / "failure-P1.json").exists())

    def test_ambiguous_drive_write_stops_instead_of_continuing_provider_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            self.environment(directory)
            with patch.object(queue, "invoke", side_effect=self.downloaded) as worker, patch.object(
                bdl_bulk_ingest, "ingest_archive", side_effect=RuntimeError("ambiguous Drive write"),
            ):
                with self.assertRaisesRegex(RuntimeError, "ambiguous Drive write"):
                    queue.run(Path(directory), 600)
            self.assertEqual(1, worker.call_count)
            report = json.loads((Path(directory) / "bootstrap-summary.json").read_text())
            self.assertEqual(0, report["completed_this_run"])
            self.assertEqual("incomplete", report["status"])

    def test_restart_does_not_download_completed_subgroup(self):
        with tempfile.TemporaryDirectory() as directory:
            self.environment(directory, landed={"P1"})
            with patch.object(queue, "invoke", side_effect=self.downloaded) as worker, patch.object(
                bdl_bulk_ingest, "ingest_archive",
                return_value={"status": "bdl_web_bulk_landed", "row_count": 12},
            ):
                result = queue.run(Path(directory), 600)
            self.assertEqual(1, worker.call_count)
            self.assertEqual("P2", worker.call_args.args[1]["BDL_BULK_SUBGROUP_ID"])
            self.assertEqual("complete", result["status"])
            self.assertEqual(1, result["completed_this_run"])

    def test_wrong_archive_hash_never_publishes_or_counts_as_landed(self):
        with tempfile.TemporaryDirectory() as directory:
            self.environment(directory)
            def worker(*args):
                result = self.downloaded(*args)
                result["archive"]["sha256"] = "0" * 64
                return result
            with patch.object(queue, "invoke", side_effect=worker), patch.object(
                bdl_bulk_ingest, "ingest_archive",
            ) as publish:
                result = queue.run(Path(directory), 600)
            publish.assert_not_called()
            self.assertEqual(0, result["completed_this_run"])
            self.assertEqual(["P1", "P2"], result["failed_subgroups"])
            self.assertEqual("incomplete", result["status"])

    def test_fully_landed_campaign_is_noop_without_browser_or_publisher(self):
        with tempfile.TemporaryDirectory() as directory:
            self.environment(directory, landed={"P1", "P2"})
            with patch.object(queue, "invoke") as worker, patch.object(
                bdl_bulk_ingest, "ingest_archive",
            ) as publish:
                result = queue.run(Path(directory), 600)
            worker.assert_not_called()
            publish.assert_not_called()
            self.assertEqual("complete", result["status"])
            self.assertEqual(0, result["completed_this_run"])


if __name__ == "__main__":
    unittest.main()
