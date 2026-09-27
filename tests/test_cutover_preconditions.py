import importlib.util
from pathlib import Path
import urllib.error
import unittest
from unittest import mock
from urllib.parse import parse_qs, urlparse

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_cutover_preconditions.py"
SPEC = importlib.util.spec_from_file_location("cutover_guard", MODULE_PATH)
guard = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(guard)


def run_item(run_id: int, status: str = "queued", path: str = ".github/workflows/deploy.yml"):
    return {
        "id": run_id,
        "status": status,
        "path": path,
        "head_sha": "a" * 40,
        "html_url": "https://example.invalid/run",
    }


class CutoverGuardTests(unittest.TestCase):
    def test_actions_pagination_accepts_more_than_100_and_exactly_100_terminal(self):
        for total in (100, 101):
            with self.subTest(total=total):
                def request(url, _headers):
                    query = parse_qs(urlparse(url).query)
                    status = query["status"][0]
                    page = int(query["page"][0])
                    if status != "queued":
                        return {"total_count": 0, "workflow_runs": []}, {}
                    if page == 1:
                        count = min(total, 100)
                        batch = [run_item(i + 1) for i in range(count)]
                        if total > 100:
                            next_url = url.replace("&page=1", "&page=2")
                            return {"total_count": total, "workflow_runs": batch}, {
                                "lInK": "<" + next_url + '>; rel="next"'
                            }
                        return {"total_count": total, "workflow_runs": batch}, {}
                    return {
                        "total_count": total,
                        "workflow_runs": [run_item(101)],
                    }, {}
                with mock.patch.object(guard, "_json_request", side_effect=request):
                    runs, error = guard._list_all_runs("owner/repo", None)
                self.assertIsNone(error)
                self.assertEqual(len(runs), total)

    def test_actions_pagination_rejects_partial_repeated_and_malformed_pages(self):
        scenarios = {
            "missing_total": ({"workflow_runs": []}, {}),
            "partial": (
                {"total_count": 101, "workflow_runs": [run_item(i) for i in range(100)]},
                {},
            ),
            "wrong_next": (
                {"total_count": 101, "workflow_runs": [run_item(i) for i in range(100)]},
                {"Link": '<https://api.github.com/other?page=2>; rel="next"'},
            ),
            "repeated": (
                {"total_count": 2, "workflow_runs": [run_item(1), run_item(1)]},
                {},
            ),
        }
        for name, response in scenarios.items():
            with self.subTest(name=name), mock.patch.object(
                guard, "_json_request", return_value=response
            ):
                runs, error = guard._list_all_runs("owner/repo", None)
                self.assertEqual(runs, [])
                self.assertIsNotNone(error)

    def test_actions_api_http_and_network_fail_closed(self):
        for exc, marker in (
            (urllib.error.HTTPError("u", 403, "forbidden", {}, None), "http_403"),
            (urllib.error.HTTPError("u", 404, "missing", {}, None), "http_404"),
            (OSError("network down"), "request_failed"),
        ):
            with self.subTest(marker=marker), mock.patch.object(
                guard, "_json_request", side_effect=exc
            ):
                runs, error = guard._list_all_runs("owner/repo", None)
                self.assertEqual(runs, [])
                self.assertIn(marker, error)

    def test_active_run_metadata_is_validated_before_filtering(self):
        valid = run_item(1, path=".github/workflows/unrelated.yml")
        defects = (
            {**valid, "path": ""},
            {**valid, "status": "completed"},
            {**valid, "head_sha": "bad"},
        )
        for run in defects:
            with self.subTest(run=run), mock.patch.object(
                guard, "_list_all_runs", return_value=([run], None)
            ):
                report = guard.check_active_workflows("owner/repo", None, "b" * 40)
                self.assertFalse(report["clean"])
                self.assertEqual(report["status"], "malformed_active_run")

    def test_old_deploy_is_blocked_but_compatible_queued_job_is_allowed(self):
        run = run_item(1)
        with mock.patch.object(guard, "_list_all_runs", return_value=([run], None)):
            with mock.patch.object(
                guard, "commit_is_same_or_descendant", return_value=(False, "behind")
            ):
                self.assertFalse(
                    guard.check_active_workflows("owner/repo", None, "b" * 40)["clean"]
                )
            with mock.patch.object(
                guard, "commit_is_same_or_descendant", return_value=(True, "compatible")
            ):
                self.assertTrue(
                    guard.check_active_workflows("owner/repo", None, "b" * 40)["clean"]
                )


if __name__ == "__main__":
    unittest.main()
