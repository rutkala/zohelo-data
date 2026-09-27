from copy import deepcopy
from datetime import date
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ingestion.campaign_session import run_collection_session


class Adapter:
    SOURCE_ID = "example"


class CampaignSessionTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.events = []
        self.store = object()
        self.settings = {"max_run_seconds": 240, "quota_windows": [{"seconds": 60, "requests": 5}]}

    def run_session(self, reasons, **kwargs):
        remaining = iter(reasons)

        def collect(store, adapter, today, settings, **options):
            self.assertIs(store, self.store)
            self.assertEqual(today, date(2026, 9, 8))
            self.events.append(("collect", deepcopy(settings), options))
            self.now += min(240, settings["max_run_seconds"])
            reason, failed, accepted = next(remaining)
            return {"reason": reason, "requests": 2, "accepted_responses": accepted,
                    "failed_requests": failed, "total_accepted_responses": accepted + 10}

        def publish(store, adapter, sha):
            self.events.append(("publish", sha))
            return {"published_response_count": 12}

        return run_collection_session(
            self.store, Adapter(), self.settings, today_factory=lambda: date(2026, 9, 8),
            publish=kwargs.pop("publish", publish), collector=collect, clock=lambda: self.now,
            code_sha="revision", **kwargs)

    def test_publishes_every_batch_before_continuing(self):
        report = self.run_session([("request_budget", 0, 2)] * 3)
        self.assertEqual([e[0] for e in self.events], ["collect", "publish"] * 3)
        self.assertEqual(report["cycles_completed"], 3)
        self.assertEqual(report["accepted_responses"], 6)
        self.assertEqual(report["reason"], "cycle_budget")
        self.assertEqual(self.settings["max_run_seconds"], 240)

    def test_stops_without_spinning_on_provider_or_capacity(self):
        for reason in ("provider_retry_after", "quota_wait", "storage_capacity_pause", "no_due_tasks"):
            with self.subTest(reason=reason):
                self.events.clear()
                report = self.run_session([(reason, 0, 0)])
                self.assertEqual(report["cycles_completed"], 1)
                self.assertEqual(report["reason"], reason)
                self.assertEqual([e[0] for e in self.events], ["collect", "publish"])

    def test_partial_failure_publishes_prior_success_then_stops(self):
        report = self.run_session([("request_budget", 1, 1)])
        self.assertEqual(report["reason"], "source_error")
        self.assertEqual(report["failed_requests"], 1)
        self.assertEqual([e[0] for e in self.events], ["collect", "publish"])

    def test_session_budget_bounds_the_next_batch(self):
        report = self.run_session([("time_budget", 0, 2)] * 3, max_seconds=300)
        batches = [e[1]["max_run_seconds"] for e in self.events if e[0] == "collect"]
        self.assertEqual(batches, [240, 60])
        self.assertEqual(report["reason"], "session_time_budget")

    def test_publication_failure_propagates_without_next_collection(self):
        def fail(*args):
            raise RuntimeError("pointer drift")
        with self.assertRaisesRegex(RuntimeError, "pointer drift"):
            self.run_session([("request_budget", 0, 2)] * 3, publish=fail)
        self.assertEqual(len(self.events), 1)

    def test_reuses_transport_and_history_policy_without_resetting_quota(self):
        fetcher = object()
        self.run_session([("request_budget", 0, 2)] * 2, max_cycles=2,
                         history_enabled=False, fetcher=fetcher)
        for event in self.events:
            if event[0] == "collect":
                self.assertIs(event[2]["fetcher"], fetcher)
                self.assertFalse(event[2]["history_enabled"])
                self.assertEqual(event[1]["quota_windows"], self.settings["quota_windows"])

    def test_rejects_unbounded_session_inputs(self):
        for overrides in ({"max_cycles": 0}, {"max_cycles": 7}, {"max_cycles": True},
                          {"max_seconds": 1801}, {"max_seconds": 1}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.run_session([], **overrides)


if __name__ == "__main__":
    unittest.main()
