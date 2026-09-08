import io
import json
import sys
import unittest
from copy import deepcopy
from datetime import date
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion import source_campaign as campaign  # noqa: E402
from ingestion.sources import eurostat  # noqa: E402


TODAY = date(2026, 9, 10)


def task(task_id, lane="recent", *, recurrence_key=None):
    value = {"id": task_id, "lane": lane, "kind": f"{lane}_page", "cursor": {"page": 1}}
    if recurrence_key is not None:
        value["recurrence_key"] = recurrence_key
    return value


def settings(**overrides):
    value = {
        "max_pending_tasks": 100,
        "max_recent_roots": 200,
        "discovery_pause_threshold": 90,
        "quota_windows": [{"seconds": 60, "requests": 100}],
        "min_request_interval_seconds": 0,
        "max_requests": 20,
        "max_run_seconds": 600,
        "max_completed_tasks": 1_000,
        "max_response_bytes": 1_000,
        "max_retained_raw_bytes": 1_000_000,
        "max_inline_wait_seconds": 0,
        "http_timeout_seconds": 10,
    }
    value.update(overrides)
    return value


class MemoryStore:
    """A copy-on-write state pointer plus immutable, content-addressed objects."""

    def __init__(self, state=None):
        self.state = deepcopy(state)
        self.objects = {}
        self.object_metadata = {}
        self.saves = 0
        self.fail_save_number = None

    def load(self):
        return deepcopy(self.state)

    def save(self, state):
        self.saves += 1
        if self.saves == self.fail_save_number:
            raise OSError("injected pointer save failure")
        self.state = deepcopy(state)

    def put_raw(self, data, metadata):
        object_id = f"object-{len(self.objects) + 1}"
        payload = bytes(data)
        self.objects[object_id] = payload
        self.object_metadata[object_id] = deepcopy(metadata)
        return {"object_id": object_id, "size": len(payload)}

    def put_receipt(self, receipt):
        object_id = f"object-{len(self.objects) + 1}"
        payload = campaign.canonical(receipt)
        self.objects[object_id] = payload
        self.object_metadata[object_id] = {"kind": "accepted_receipt"}
        return {"object_id": object_id, "size": len(payload)}


class Adapter:
    SOURCE_ID = "example"
    ALLOWED_HOSTS = ("api.example.test",)

    def __init__(self, initial=(), recent_factory=None, interpreter=None):
        self._initial = list(initial)
        self._recent_factory = recent_factory or (lambda today: [])
        self._interpreter = interpreter or (
            lambda requested, body, today: {"record_count": 1, "next_tasks": []}
        )
        self.requested = []
        self.interpreted = []

    def initial_tasks(self, today):
        return deepcopy(self._initial)

    def recent_tasks(self, today):
        return deepcopy(self._recent_factory(today))

    def refresh_task(self, requested, today):
        if requested.get("recurrence_key") is None or requested["cursor"].get("page") != 1:
            return None
        return task(
            f"{requested['recurrence_key']}:{today.isoformat()}",
            "recent",
            recurrence_key=requested["recurrence_key"],
        )

    def request_for(self, requested):
        self.requested.append(requested["id"])
        return {"url": "https://api.example.test/data", "params": {"task": requested["id"]}}

    def interpret(self, requested, body, today):
        self.interpreted.append(requested["id"])
        return self._interpreter(requested, body, today)


class FakeClock:
    def __init__(self, now=1_000.0):
        self.now = float(now)

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def ok_fetch(body=b'{"ok":true}', headers=None):
    def fetcher(request_spec, allowed_hosts, *, max_bytes, timeout):
        return 200, body, dict(headers or {"content-type": "application/json"})

    return fetcher


class CampaignSchedulingTests(unittest.TestCase):
    def test_recent_and_history_keep_allocations_under_full_backlog(self):
        initial = []
        for index in range(4):
            initial.extend(
                [task(f"recent-{index}", "recent"), task(f"history-{index}", "history")]
            )
        initial.extend([task("discover", "discovery"), task("reconcile", "reconcile")])
        adapter = Adapter(initial)
        result = campaign.run_campaign(
            MemoryStore(), adapter, TODAY, settings(max_requests=6), fetcher=ok_fetch()
        )

        self.assertEqual(
            adapter.requested,
            ["recent-0", "history-0", "recent-1", "history-1", "discover", "reconcile"],
        )
        self.assertEqual(result["by_lane"], {"recent": 2, "history": 2, "discovery": 1, "reconcile": 1})

    def test_history_can_be_disabled_without_blocking_other_lanes(self):
        adapter = Adapter([task("history", "history"), task("recent", "recent")])
        store = MemoryStore()
        result = campaign.run_campaign(
            store, adapter, TODAY, settings(), history_enabled=False, fetcher=ok_fetch()
        )
        self.assertEqual(adapter.requested, ["recent"])
        self.assertEqual([item["id"] for item in store.state["pending"]], ["history"])
        self.assertEqual(result["coverage_status"], "incomplete")

    def test_duplicate_followups_and_completed_ids_are_admitted_once(self):
        root = task("root")
        child = task("child", "history")

        def interpret(requested, body, today):
            return {"record_count": 1, "next_tasks": [child, deepcopy(child), deepcopy(root)]}

        store = MemoryStore()
        campaign.run_campaign(
            store, Adapter([root], interpreter=interpret), TODAY, settings(max_requests=1), fetcher=ok_fetch()
        )
        self.assertEqual(list(store.state["completed"]), ["root"])
        self.assertEqual([item["id"] for item in store.state["pending"]], ["child"])

    def test_pending_recent_generation_blocks_new_daily_generation(self):
        def recent(current_day):
            return [task(f"series:{current_day.isoformat()}", recurrence_key="series")]

        adapter = Adapter(recent_factory=recent)
        store = MemoryStore()
        campaign.prepare_state(store, adapter, TODAY, settings())
        campaign.prepare_state(store, adapter, date(2026, 9, 11), settings())
        campaign.prepare_state(store, adapter, date(2026, 9, 12), settings())

        pending = [item for item in store.state["pending"] if item.get("recurrence_key") == "series"]
        self.assertEqual([item["id"] for item in pending], ["series:2026-09-10"])
        self.assertEqual(list(store.state["recent_roots"]), ["series"])

    def test_pending_and_completed_capacities_pause_before_more_work(self):
        with self.assertRaisesRegex(campaign.CapacityPause, "Pending task capacity"):
            campaign.prepare_state(
                MemoryStore(), Adapter([task("one"), task("two")]), TODAY, settings(max_pending_tasks=1)
            )

        state = campaign.new_state("example", TODAY)
        state["completed"]["old"] = "2026-09-10T00:00:00+00:00"
        state["pending"] = [task("new")]
        adapter = Adapter()
        result = campaign.run_campaign(
            MemoryStore(state), adapter, TODAY, settings(max_completed_tasks=1), fetcher=ok_fetch()
        )
        self.assertEqual(result["reason"], "state_capacity_pause")
        self.assertEqual(adapter.requested, [])

    def test_retained_byte_capacity_reserves_the_largest_response(self):
        state = campaign.new_state("example", TODAY)
        state["pending"] = [task("one")]
        state["raw_bytes"] = 6
        adapter = Adapter()
        result = campaign.run_campaign(
            MemoryStore(state),
            adapter,
            TODAY,
            settings(max_response_bytes=5, max_retained_raw_bytes=10),
            fetcher=ok_fetch(b"x"),
        )
        self.assertEqual(result["reason"], "storage_capacity_pause")
        self.assertEqual(adapter.requested, [])


class CampaignDurabilityTests(unittest.TestCase):
    def test_invalid_series_is_deferred_while_healthy_history_advances(self):
        bad_body = b'{"series":"retired","shape":"invalid"}'
        healthy_body = b'{"series":"healthy"}'

        def interpret(requested, body, today):
            if requested["id"] == "bad-series":
                raise ValueError("series payload violates its contract")
            self.assertEqual(body, healthy_body)
            return {"record_count": 4, "next_tasks": []}

        def fetcher(request_spec, allowed_hosts, *, max_bytes, timeout):
            body = bad_body if request_spec["params"]["task"] == "bad-series" else healthy_body
            return 200, body, {"content-type": "application/json"}

        clock = FakeClock()
        store = MemoryStore()
        adapter = Adapter(
            [task("bad-series", "recent"), task("healthy-history", "history")],
            interpreter=interpret,
        )
        result = campaign.run_campaign(
            store, adapter, TODAY, settings(max_requests=2), fetcher=fetcher, clock=clock
        )

        self.assertEqual(adapter.requested, ["bad-series", "healthy-history"])
        self.assertEqual(result["failed_requests"], 1)
        self.assertEqual(result["accepted_responses"], 1)
        self.assertEqual(result["records_received"], 4)
        self.assertEqual(result["errors"][0]["task_id"], "bad-series")
        self.assertNotIn("raw", result["errors"][0])
        self.assertEqual(list(store.state["completed"]), ["healthy-history"])
        self.assertEqual([item["id"] for item in store.state["pending"]], ["bad-series"])
        self.assertGreater(store.state["pending"][0]["retry_at"], clock.now)

        rejected_pointer = store.state["rejected_receipts"][0]
        rejected = json.loads(store.objects[rejected_pointer["object_id"]])
        self.assertFalse(rejected["accepted"])
        self.assertEqual(rejected["task"]["id"], "bad-series")
        self.assertEqual(rejected["http_status"], 200)
        self.assertEqual(store.objects[rejected["raw"]["object_id"]], bad_body)

    def test_planning_migration_cannot_erase_provider_quota(self):
        state = campaign.new_state(Adapter.SOURCE_ID, TODAY)
        state["quota_attempts"] = [1000.0]
        store = MemoryStore(state)
        adapter = Adapter()
        adapter.migrate_state = lambda value, today: {**value, "quota_attempts": []}
        with self.assertRaisesRegex(campaign.CampaignError, "protected campaign evidence"):
            campaign.prepare_state(store, adapter, TODAY, settings())
        self.assertEqual(store.state["quota_attempts"], [1000.0])

    def test_429_retry_after_stops_provider_before_other_tasks_advance(self):
        rate_body = b'{"error":"rate limited"}'
        calls = []

        def rate_limited(request_spec, allowed_hosts, *, max_bytes, timeout):
            calls.append(request_spec["params"]["task"])
            return 429, rate_body, {"retry-after": "120", "content-type": "application/json"}

        clock = FakeClock()
        store = MemoryStore()
        adapter = Adapter([task("limited", "recent"), task("untouched", "history")])
        result = campaign.run_campaign(
            store, adapter, TODAY, settings(max_requests=2), fetcher=rate_limited, clock=clock
        )

        self.assertEqual(calls, ["limited"])
        self.assertEqual(result["reason"], "source_error")
        self.assertEqual(result["failed_requests"], 1)
        self.assertEqual(result["accepted_responses"], 0)
        self.assertEqual(store.state["completed"], {})
        self.assertEqual([item["id"] for item in store.state["pending"]], ["limited", "untouched"])
        self.assertEqual(store.state["pending"][0]["retry_at"], 1_120.0)
        self.assertNotIn("failures", store.state["pending"][1])

        rejected_pointer = store.state["rejected_receipts"][0]
        rejected = json.loads(store.objects[rejected_pointer["object_id"]])
        self.assertEqual(rejected["http_status"], 429)
        self.assertEqual(store.objects[rejected["raw"]["object_id"]], rate_body)

        clock.now = 1_050
        restarted_store = MemoryStore(store.state)
        restarted_adapter = Adapter()
        waiting = campaign.run_campaign(
            restarted_store,
            restarted_adapter,
            TODAY,
            settings(max_requests=2),
            fetcher=ok_fetch(),
            clock=clock,
        )
        self.assertEqual(waiting["reason"], "provider_retry_after")
        self.assertEqual(waiting["retry_after_seconds"], 70.0)
        self.assertEqual(restarted_adapter.requested, [])
        self.assertEqual(restarted_store.state["completed"], {})

        clock.now = 1_121
        resumed_store = MemoryStore(restarted_store.state)
        resumed_adapter = Adapter()
        resumed = campaign.run_campaign(
            resumed_store,
            resumed_adapter,
            TODAY,
            settings(max_requests=2),
            fetcher=ok_fetch(),
            clock=clock,
        )
        self.assertEqual(resumed_adapter.requested, ["untouched", "limited"])
        self.assertEqual(resumed["accepted_responses"], 2)
        self.assertEqual(set(resumed_store.state["completed"]), {"limited", "untouched"})

    def test_quota_reservation_survives_success_and_restart(self):
        clock = FakeClock()
        store = MemoryStore()
        adapter = Adapter([task("one"), task("two")])
        first_settings = settings(
            max_requests=1, quota_windows=[{"seconds": 60, "requests": 1}]
        )
        campaign.run_campaign(store, adapter, TODAY, first_settings, fetcher=ok_fetch(), clock=clock)
        self.assertEqual(store.state["quota_attempts"], [1_000.0])

        restarted = Adapter()
        result = campaign.run_campaign(
            store, restarted, TODAY, first_settings, fetcher=ok_fetch(), clock=clock
        )
        self.assertEqual(result["reason"], "quota_wait")
        self.assertEqual(result["retry_after_seconds"], 60.0)
        self.assertEqual(restarted.requested, [])

    def test_failed_attempt_keeps_rolling_window_reservation(self):
        clock = FakeClock()
        store = MemoryStore()

        def failing_fetch(*args, **kwargs):
            return 503, b"temporarily unavailable", {"retry-after": "60"}

        quota_settings = settings(
            quota_windows=[{"seconds": 100, "requests": 1}], max_inline_wait_seconds=0
        )
        failed = campaign.run_campaign(
            store, Adapter([task("one")]), TODAY, quota_settings, fetcher=failing_fetch, clock=clock
        )
        self.assertEqual(failed["reason"], "source_error")
        self.assertEqual(store.state["quota_attempts"], [1_000.0])

        clock.now = 1_061
        restarted = Adapter()
        waiting = campaign.run_campaign(
            store, restarted, TODAY, quota_settings, fetcher=ok_fetch(), clock=clock
        )
        self.assertEqual(waiting["reason"], "quota_wait")
        self.assertEqual(waiting["retry_after_seconds"], 39.0)
        self.assertEqual(restarted.requested, [])

    def test_failed_eurostat_task_remains_valid_when_retry_becomes_due(self):
        clock = FakeClock()
        state = campaign.new_state(eurostat.SOURCE_ID, TODAY)
        state["pending"] = [eurostat.recent_tasks(TODAY)[0]]
        store = MemoryStore(state)
        calls = []

        def failing_fetch(request_spec, allowed_hosts, *, max_bytes, timeout):
            calls.append(request_spec["url"])
            return 503, b"temporarily unavailable", {"retry-after": "60"}

        retry_settings = settings()
        first = campaign.run_campaign(
            store, eurostat, TODAY, retry_settings, fetcher=failing_fetch, clock=clock
        )
        self.assertEqual(first["reason"], "source_error")
        self.assertEqual(store.state["pending"][0]["failures"], 1)

        clock.now += 61
        second = campaign.run_campaign(
            store, eurostat, TODAY, retry_settings, fetcher=failing_fetch, clock=clock
        )
        self.assertEqual(second["reason"], "source_error")
        self.assertEqual(store.state["pending"][0]["failures"], 2)
        self.assertEqual(len(calls), 2)

    def test_rolling_windows_and_minimum_interval_use_strict_expiry(self):
        state = campaign.new_state("example", TODAY)
        state["quota_attempts"] = [800, 920, 950, 999]
        configured = settings(
            quota_windows=[{"seconds": 100, "requests": 3}, {"seconds": 300, "requests": 4}],
            min_request_interval_seconds=5,
        )
        self.assertEqual(campaign.quota_wait(state, configured, 1_000), 100)
        self.assertEqual(state["quota_attempts"], [800, 920, 950, 999])
        self.assertEqual(campaign.quota_wait(state, configured, 1_100), 0)
        self.assertEqual(state["quota_attempts"], [920, 950, 999])

    def test_invalid_200_retains_exact_bytes_without_advancing_task(self):
        body = b'{"valid_json":false,"source_contract":"wrong"}'

        def reject(requested, payload, today):
            self.assertEqual(payload, body)
            raise ValueError("invalid source payload")

        store = MemoryStore()
        adapter = Adapter([task("cursor-7", "history")], interpreter=reject)
        result = campaign.run_campaign(store, adapter, TODAY, settings(), fetcher=ok_fetch(body))

        self.assertEqual(result["reason"], "source_error")
        self.assertEqual(store.state["completed"], {})
        self.assertEqual([item["id"] for item in store.state["pending"]], ["cursor-7"])
        raw_id = next(
            object_id
            for object_id, metadata in store.object_metadata.items()
            if metadata.get("task_id") == "cursor-7"
        )
        self.assertEqual(store.objects[raw_id], body)
        self.assertEqual(store.state["raw_bytes"], len(body))
        self.assertEqual(store.state["receipts"], [])

    def test_accepted_receipt_is_immutable_exact_and_points_to_exact_response(self):
        body = b" exact response bytes\n"
        headers = {
            "etag": '"abc"',
            "last-modified": "Wed, 10 Sep 2026 12:00:00 GMT",
            "content-type": "application/json",
            "ignored": "secret-ish transport detail",
        }
        store = MemoryStore()
        adapter = Adapter([task("accepted", "history")])
        campaign.run_campaign(
            store,
            adapter,
            TODAY,
            settings(max_requests=1),
            fetcher=ok_fetch(body, headers),
            clock=FakeClock(),
            code_sha="a" * 40,
        )

        raw_id = next(
            object_id for object_id, metadata in store.object_metadata.items() if metadata.get("task_id")
        )
        receipt_pointer = store.state["receipts"][0]
        receipt_bytes = store.objects[receipt_pointer["object_id"]]
        receipt = json.loads(receipt_bytes)
        self.assertEqual(store.objects[raw_id], body)
        self.assertEqual(receipt["raw"]["object_id"], raw_id)
        self.assertEqual(receipt["task"], task("accepted", "history"))
        self.assertEqual(receipt["request"]["params"], {"task": "accepted"})
        self.assertEqual(receipt["headers"], {key: headers[key] for key in ("etag", "last-modified", "content-type")})
        self.assertTrue(receipt["accepted"])
        self.assertEqual(receipt["code_sha"], "a" * 40)
        self.assertEqual(receipt_bytes, campaign.canonical(receipt))
        self.assertEqual(len(store.objects), 2)
        self.assertEqual(
            store.object_metadata[receipt_pointer["object_id"]],
            {"kind": "accepted_receipt"},
        )

    def test_final_pointer_save_error_propagates_without_marking_complete(self):
        store = MemoryStore()
        store.fail_save_number = 3  # prepare, durable quota reservation, accepted candidate
        with self.assertRaisesRegex(OSError, "pointer save failure"):
            campaign.run_campaign(
                store, Adapter([task("one")]), TODAY, settings(), fetcher=ok_fetch()
            )
        self.assertEqual(store.state["completed"], {})
        self.assertEqual([item["id"] for item in store.state["pending"]], ["one"])
        self.assertEqual(len(store.state["quota_attempts"]), 1)


class FakeResponse:
    def __init__(self, status, body, headers=None):
        self.status = status
        self._body = io.BytesIO(body)
        self.headers = dict(headers or {})

    def read(self, amount=-1):
        return self._body.read(amount)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class FakeOpener:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return self.response


class CampaignHttpTests(unittest.TestCase):
    @patch("ingestion.source_campaign.build_opener")
    def test_redirect_response_is_returned_without_following_it(self, build_opener):
        opener = FakeOpener(FakeResponse(302, b"redirect body", {"Location": "https://evil.test"}))
        build_opener.return_value = opener
        status, body, headers = campaign.fetch(
            {"url": "https://api.example.test/data"},
            ("api.example.test",),
            max_bytes=100,
            timeout=7,
        )
        self.assertEqual((status, body), (302, b"redirect body"))
        self.assertEqual(headers["location"], "https://evil.test")
        self.assertIsInstance(build_opener.call_args.args[0], campaign.NoRedirect)
        self.assertEqual(len(opener.requests), 1)

    @patch("ingestion.source_campaign.build_opener")
    def test_host_scope_is_checked_before_any_network_access(self, build_opener):
        invalid_urls = (
            "http://api.example.test/data",
            "https://evil.test/data",
            "https://api.example.test:444/data",
            "https://user@api.example.test/data",
            "https://api.example.test/data#fragment",
        )
        for url in invalid_urls:
            with self.subTest(url=url), self.assertRaisesRegex(campaign.CampaignError, "HTTPS host scope"):
                campaign.fetch({"url": url}, ("api.example.test",), max_bytes=100, timeout=7)
        build_opener.assert_not_called()

    @patch("ingestion.source_campaign.build_opener")
    def test_declared_and_streamed_oversize_responses_are_rejected(self, build_opener):
        build_opener.return_value = FakeOpener(FakeResponse(200, b"small", {"Content-Length": "101"}))
        with self.assertRaisesRegex(campaign.CampaignError, "byte limit"):
            campaign.fetch(
                {"url": "https://api.example.test/data"},
                ("api.example.test",),
                max_bytes=100,
                timeout=7,
            )

        build_opener.return_value = FakeOpener(FakeResponse(200, b"x" * 101))
        with self.assertRaisesRegex(campaign.CampaignError, "byte limit"):
            campaign.fetch(
                {"url": "https://api.example.test/data"},
                ("api.example.test",),
                max_bytes=100,
                timeout=7,
            )


if __name__ == "__main__":
    unittest.main()
