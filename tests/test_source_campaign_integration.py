import json
import sys
import tempfile
import unittest
from copy import deepcopy
from datetime import date
from hashlib import sha256
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.source_campaign import run_campaign  # noqa: E402
from ingestion.source_campaign_store import LocalCampaignStore  # noqa: E402


TODAY = date(2026, 9, 10)
RECENT_BODY = b'{"fixture":"recent","rows":[1,2]}'
HISTORY_BODY = b'{"fixture":"history","rows":[1,2,3]}'


def recent_task(today):
    return {
        "id": f"fixture:recent:{today.isoformat()}",
        "lane": "recent",
        "kind": "fixture_page",
        "cursor": {"period": today.isoformat()},
        "recurrence_key": "fixture-series",
    }


def history_task():
    return {
        "id": "fixture:history:1",
        "lane": "history",
        "kind": "fixture_page",
        "cursor": {"page": 1},
    }


def campaign_settings():
    return {
        "max_pending_tasks": 20,
        "max_recent_roots": 200,
        "discovery_pause_threshold": 10,
        "quota_windows": [{"seconds": 60, "requests": 1}],
        "min_request_interval_seconds": 0,
        "max_requests": 1,
        "max_run_seconds": 300,
        "max_completed_tasks": 20,
        "max_response_bytes": 1_000,
        "max_retained_raw_bytes": 10_000,
        "max_inline_wait_seconds": 0,
        "http_timeout_seconds": 10,
    }


class FixtureAdapter:
    SOURCE_ID = "integration_fixture"
    ALLOWED_HOSTS = ("fixture.example.test",)

    def __init__(self):
        self.requested = []

    def initial_tasks(self, today):
        return [recent_task(today), history_task()]

    def recent_tasks(self, today):
        return [recent_task(today)]

    def refresh_task(self, task, today):
        if task.get("recurrence_key") != "fixture-series":
            return None
        return recent_task(today)

    def request_for(self, task):
        self.requested.append(task["id"])
        return {
            "url": "https://fixture.example.test/page",
            "params": {"task_id": task["id"]},
        }

    def interpret(self, task, body, today):
        expected = RECENT_BODY if task["lane"] == "recent" else HISTORY_BODY
        if body != expected:
            raise ValueError("fixture body does not match its task")
        return {
            "record_count": 2 if task["lane"] == "recent" else 3,
            "next_tasks": [],
            "metadata": {"fixture_lane": task["lane"]},
        }


class FixtureFetcher:
    def __init__(self):
        self.requests = []

    def __call__(self, request_spec, allowed_hosts, *, max_bytes, timeout):
        self.requests.append(deepcopy(request_spec))
        task_id = request_spec["params"]["task_id"]
        body = RECENT_BODY if ":recent:" in task_id else HISTORY_BODY
        return 200, body, {"content-type": "application/json", "etag": task_id}


class FakeClock:
    def __init__(self, now=1_000.0):
        self.now = float(now)

    def __call__(self):
        return self.now


class SourceCampaignLocalIntegrationTests(unittest.TestCase):
    def test_fresh_runner_restores_raw_receipt_queue_and_quota_then_resumes(self):
        clock = FakeClock()
        settings = campaign_settings()

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first_store = LocalCampaignStore(root, FixtureAdapter.SOURCE_ID)
            first_adapter = FixtureAdapter()
            first_fetcher = FixtureFetcher()
            first = run_campaign(
                first_store,
                first_adapter,
                TODAY,
                settings,
                fetcher=first_fetcher,
                clock=clock,
                code_sha="first-run",
            )
            self.assertEqual(first["by_lane"], {"recent": 1})
            self.assertEqual(first_adapter.requested, [recent_task(TODAY)["id"]])

            restored_store = LocalCampaignStore(root, FixtureAdapter.SOURCE_ID)
            restored = restored_store.load()
            self.assertEqual(restored["quota_attempts"], [1_000.0])
            self.assertEqual(list(restored["completed"]), [recent_task(TODAY)["id"]])
            self.assertEqual([item["id"] for item in restored["pending"]], [history_task()["id"]])

            receipt_descriptor = restored["receipts"][0]
            receipt_bytes = (root / receipt_descriptor["id"]).read_bytes()
            self.assertEqual(len(receipt_bytes), receipt_descriptor["size_bytes"])
            self.assertEqual(sha256(receipt_bytes).hexdigest(), receipt_descriptor["sha256"])
            receipt = json.loads(receipt_bytes)
            self.assertTrue(receipt["accepted"])
            self.assertEqual(receipt["task"]["lane"], "recent")
            self.assertEqual(restored_store.read_raw(receipt["raw"]), RECENT_BODY)

            waiting_adapter = FixtureAdapter()
            waiting = run_campaign(
                restored_store,
                waiting_adapter,
                TODAY,
                settings,
                fetcher=FixtureFetcher(),
                clock=clock,
            )
            self.assertEqual(waiting["reason"], "quota_wait")
            self.assertEqual(waiting["retry_after_seconds"], 60.0)
            self.assertEqual(waiting_adapter.requested, [])

            clock.now = 1_061
            resumed_store = LocalCampaignStore(root, FixtureAdapter.SOURCE_ID)
            resumed_adapter = FixtureAdapter()
            resumed = run_campaign(
                resumed_store,
                resumed_adapter,
                TODAY,
                settings,
                fetcher=FixtureFetcher(),
                clock=clock,
                code_sha="resumed-run",
            )
            self.assertEqual(resumed["by_lane"], {"history": 1})
            self.assertEqual(resumed_adapter.requested, [history_task()["id"]])

            final_store = LocalCampaignStore(root, FixtureAdapter.SOURCE_ID)
            final_state = final_store.load()
            self.assertEqual(
                set(final_state["completed"]),
                {recent_task(TODAY)["id"], history_task()["id"]},
            )
            self.assertEqual(final_state["pending"], [])
            self.assertEqual(final_state["quota_attempts"], [1_061])
            self.assertEqual(final_state["accepted_responses"], 2)
            self.assertEqual(final_state["record_count"], 5)
            self.assertEqual(final_state["raw_bytes"], len(RECENT_BODY) + len(HISTORY_BODY))
            self.assertEqual(len(final_state["receipts"]), 2)
            for descriptor, expected_body in zip(
                final_state["receipts"], (RECENT_BODY, HISTORY_BODY), strict=True
            ):
                receipt = json.loads((root / descriptor["id"]).read_bytes())
                self.assertEqual(final_store.read_raw(receipt["raw"]), expected_body)


if __name__ == "__main__":
    unittest.main()
