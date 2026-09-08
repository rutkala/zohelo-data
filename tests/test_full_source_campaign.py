import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ingestion.full_source_campaign import (
    _durable_catalogue_distribution,
    coverage,
    offer,
    run_full_campaign,
    task_for,
)
from ingestion.source_campaign import new_state
from tests.test_source_campaign import FakeClock, MemoryStore, TODAY, settings
from tests.test_eurostat_bulk_recovery import (
    CODELISTS,
    CONSTRAINT_ALL_KEYS,
    DATAFLOW,
    DSD,
    envelope,
)


def distribution(dataset="all", kind="wdi_zip", version="2026-09-08"):
    return {"dataset_id": dataset, "kind": kind, "version": version,
            "url": "https://databank.worldbank.org/data/download/WDI_CSV.zip", "params": {}}


class RawStore:
    def __init__(self):
        self.files = []
        self.objects = {}

    def put_file(self, path, metadata):
        body = path.read_bytes()
        self.files.append(body)
        object_id = f"raw-{len(self.files)}"
        self.objects[object_id] = body
        return {"id": object_id, "sha256": "a" * 64, "md5": "b" * 32,
                "size_bytes": len(body), "name": "raw.bin"}

    def read_to_file(self, descriptor, path):
        path.write_bytes(self.objects[descriptor["id"]])
        return dict(descriptor)


def eurostat_distribution(dataset="demo_cube", version="2026-09-08"):
    return {
        "dataset_id": dataset,
        "kind": "eurostat_tsv_gzip",
        "version": version,
        "url": (
            "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/"
            f"data/{dataset}"
        ),
        "params": {"format": "TSV", "compressed": "true"},
    }


class FullSourceCampaignTests(unittest.TestCase):
    def test_index_batches_keep_per_object_checkpoints_and_flush_on_request_budget(self):
        store = MemoryStore()
        quota = MemoryStore(new_state("world_bank_wdi", TODAY))
        clock = FakeClock(1788825600)
        published = []
        observed_checkpoints = []

        def fetcher(request, path, hosts, **kwargs):
            observed_checkpoints.append(store.state["accepted_responses"])
            clock.now += 3
            path.write_bytes(b"complete archive")
            return {"status_code": 200}

        def publish(current_store, sha):
            published.append(current_store.state["accepted_responses"])

        with tempfile.TemporaryDirectory() as directory:
            report = run_full_campaign(
                store, RawStore(), quota, "world_bank_wdi", settings(), directory,
                max_requests=11, clock=clock, fetcher=fetcher,
                inspector=lambda *a: {"status": "complete"},
                planner=lambda source: [distribution(f"part_{i}") for i in range(12)],
                publish=publish,
            )
        self.assertEqual(observed_checkpoints, list(range(11)))
        self.assertEqual(published, [1, 9, 11])
        self.assertEqual(report["accepted"], 11)
        self.assertEqual(report["pending_tasks"], 1)

    def test_index_flushes_elapsed_batch_and_prior_successes_after_source_failure(self):
        store = MemoryStore()
        clock = FakeClock(1788825600)
        published = []
        attempts = 0

        def fetcher(request, path, hosts, **kwargs):
            nonlocal attempts
            attempts += 1
            clock.now += 70
            if attempts > 4:
                raise OSError("provider connection failed")
            path.write_bytes(b"complete archive")
            return {"status_code": 200}

        with tempfile.TemporaryDirectory() as directory:
            report = run_full_campaign(
                store, RawStore(), MemoryStore(new_state("world_bank_wdi", TODAY)),
                "world_bank_wdi", settings(), directory,
                max_requests=7, clock=clock, fetcher=fetcher,
                inspector=lambda *a: {"status": "complete"},
                planner=lambda source: [distribution(f"part_{i}") for i in range(7)],
                publish=lambda current, sha: published.append(current.state["accepted_responses"]),
            )
        self.assertEqual(published, [1, 3, 4])
        self.assertEqual(report["accepted"], 4)
        self.assertEqual(len(report["failures"]), 3)

    def test_index_failure_stops_requests_and_restart_publishes_retained_progress_first(self):
        store = MemoryStore()
        quota = MemoryStore(new_state("world_bank_wdi", TODAY))
        clock = FakeClock(1788825600)
        events = []

        def fetcher(request, path, hosts, **kwargs):
            clock.now += 3
            events.append("fetch")
            path.write_bytes(b"complete archive")
            return {"status_code": 200}

        def fail_publish(*args):
            raise RuntimeError("ambiguous index promotion")

        with tempfile.TemporaryDirectory() as directory:
            kwargs = dict(clock=clock, fetcher=fetcher,
                          inspector=lambda *a: {"status": "complete"},
                          planner=lambda source: [distribution("one"), distribution("two")])
            with self.assertRaisesRegex(RuntimeError, "ambiguous index promotion"):
                run_full_campaign(store, RawStore(), quota, "world_bank_wdi", settings(),
                                  directory, publish=fail_publish, **kwargs)
            self.assertEqual(events, ["fetch"])
            self.assertEqual(store.state["accepted_responses"], 1)
            events.clear()
            run_full_campaign(store, RawStore(), quota, "world_bank_wdi", settings(),
                              directory, publish=lambda *a: events.append("publish"), **kwargs)
        self.assertEqual(events, ["publish", "fetch", "publish"])

    def test_full_archive_reserves_existing_provider_quota_and_restores_checkpoint(self):
        store = MemoryStore()
        quota = MemoryStore(new_state("world_bank_wdi", TODAY))
        raw = RawStore()
        clock = FakeClock(1788825600)
        seen = []

        def fetcher(request, path, hosts, **kwargs):
            self.assertEqual(len(quota.state["quota_attempts"]), 1)
            self.assertEqual(store.state["completed"], {})
            seen.append(request)
            path.write_bytes(b"exact archive bytes")
            return {"status_code": 200, "size_bytes": 19}

        with tempfile.TemporaryDirectory() as directory:
            result = run_full_campaign(store, raw, quota, "world_bank_wdi", settings(), directory,
                                       clock=clock, fetcher=fetcher,
                                       inspector=lambda path, kind: {"status": "complete", "members": ["WDIData.csv"]},
                                       planner=lambda source: [distribution()])
            self.assertEqual(result["validated_current_distributions"], 1)
            self.assertEqual(result["coverage_status"], "complete_current_catalogue")
            self.assertEqual(result["modeling_status"], "raw_distributions_only")
            self.assertEqual(raw.files, [b"exact archive bytes"])
            restarted = run_full_campaign(store, raw, quota, "world_bank_wdi", settings(), directory,
                                          clock=clock, fetcher=fetcher,
                                          inspector=lambda *args: {"status": "complete"},
                                          planner=lambda source: [distribution()])
            self.assertEqual(restarted["requests"], 0)
            self.assertEqual(len(seen), 1)

    def test_complete_inventory_admits_every_dataset_and_only_requeues_changed_versions(self):
        state = new_state("eurostat_bulk", TODAY)
        state["provider_id"] = "eurostat"
        datasets = [distribution(f"dataset_{i}", "eurostat_tsv_gzip") for i in range(1200)]
        offer(state, datasets)
        self.assertEqual(len(state["pending"]), 1200)
        self.assertEqual(len(state["recent_roots"]), 1200)
        completed = state["pending"].pop(0)
        state["completed"][completed["id"]] = {"raw": "verified"}
        offer(state, datasets)
        self.assertEqual(len(state["pending"]), 1199)
        self.assertEqual(coverage(state)["validated_current_distributions"], 1)
        changed = distribution("dataset_0", "eurostat_tsv_gzip", version="2026-09-09")
        offer(state, [changed])
        self.assertEqual(len(state["pending"]), 1200)
        self.assertEqual(state["pending"][-1]["lane"], "recent")
        self.assertEqual(coverage(state)["validated_current_distributions"], 0)

    def test_validated_inventory_retires_only_its_kind_and_keeps_queue_metadata_small(self):
        state = new_state("eurostat_bulk", TODAY)
        state["provider_id"] = "eurostat"
        old_data = eurostat_distribution("retired")
        codelist = distribution("GEO", "eurostat_codelist_tsv")
        offer(state, [old_data, codelist])
        replacement = eurostat_distribution("current")
        replacement["catalogue_metadata"] = {
            "title": "Current dataset",
            "inventory_fields": {f"column-{index}": "large" for index in range(100)},
        }
        inventory_receipt = {"id": "inventory", "sha256": "a" * 64, "size_bytes": 99}
        durable = _durable_catalogue_distribution(replacement, inventory_receipt)
        offer(state, [durable], replace_kinds={"eurostat_tsv_gzip"})

        self.assertNotIn("eurostat_tsv_gzip:retired", state["recent_roots"])
        self.assertIn("eurostat_codelist_tsv:GEO", state["recent_roots"])
        saved = state["recent_roots"]["eurostat_tsv_gzip:current"]
        self.assertEqual(saved["catalogue_metadata"], {
            "title": "Current dataset",
            "source_inventory_receipt": inventory_receipt,
        })
        self.assertFalse(any(
            item["cursor"].get("dataset_id") == "retired" for item in state["pending"]
        ))

    def test_provider_cooldown_prevents_bulk_fetch_without_resetting_quota(self):
        clock = FakeClock(1788825600)
        state = new_state("world_bank_wdi", TODAY)
        state["quota_attempts"] = [clock() - 1]
        state["provider_retry_at"] = clock() + 900
        quota = MemoryStore(state)
        with tempfile.TemporaryDirectory() as directory:
            result = run_full_campaign(MemoryStore(), RawStore(), quota, "world_bank_wdi", settings(), directory,
                                       clock=clock, fetcher=lambda *a, **k: self.fail("must not fetch"),
                                       inspector=lambda *a: {}, planner=lambda source: [distribution()])
        self.assertEqual(result["reason"], "provider_retry_after")
        self.assertEqual(quota.state["quota_attempts"], [clock() - 1])

    def test_invalid_distribution_retains_task_and_never_counts_as_full_coverage(self):
        clock = FakeClock(1788825600)
        store = MemoryStore()
        raw = RawStore()

        def fetcher(request, path, hosts, **kwargs):
            path.write_bytes(b"not a zip")
            return {"status_code": 200}

        with tempfile.TemporaryDirectory() as directory:
            result = run_full_campaign(store, raw, MemoryStore(new_state("world_bank_wdi", TODAY)),
                                       "world_bank_wdi", settings(), directory, clock=clock, fetcher=fetcher,
                                       inspector=lambda *a: {"status": "unsupported"},
                                       planner=lambda source: [distribution()])
        self.assertEqual(result["validated_current_distributions"], 0)
        self.assertEqual(result["coverage_status"], "incomplete")
        self.assertEqual(result["failed_pending_tasks"], 1)
        self.assertEqual(raw.files, [])

    def test_eurostat_coverage_requires_all_three_current_inventories(self):
        state = new_state("eurostat_bulk", TODAY)
        state["provider_id"] = "eurostat"
        state["last_catalogue_date"] = TODAY.isoformat()
        current = eurostat_distribution()
        offer(state, [current])
        state["completed"][task_for(current)["id"]] = {"partitioned": True}
        state["inventory_success"] = {"eurostat_inventory": TODAY.isoformat()}
        state["last_catalogue_success"] = TODAY.isoformat()

        partial = coverage(state)
        self.assertEqual(partial["validated_current_distributions"], 1)
        self.assertFalse(partial["inventories_current"])
        self.assertEqual(partial["coverage_status"], "incomplete")

        for kind in (
            "eurostat_inventory_codelist", "eurostat_inventory_metadata"
        ):
            state["inventory_success"][kind] = TODAY.isoformat()
        self.assertEqual(coverage(state)["coverage_status"], "complete_current_catalogue")

    def test_wdi_prior_day_archive_is_not_current_daily_coverage(self):
        state = new_state("world_bank_wdi_bulk", TODAY)
        state["provider_id"] = "world_bank_wdi"
        state["last_catalogue_date"] = TODAY.isoformat()
        state["last_catalogue_success"] = TODAY.isoformat()
        state["latest_wdi"] = {"catalogue_date": "2026-09-09", "raw": "accepted"}
        self.assertEqual(coverage(state)["validated_current_distributions"], 0)
        self.assertEqual(coverage(state)["coverage_status"], "incomplete")

    def test_413_recovery_survives_partition_async_restarts_and_aggregates_all_leaves(self):
        campaign_state = new_state("eurostat_bulk", TODAY)
        campaign_state["provider_id"] = "eurostat"
        campaign_state["last_catalogue_date"] = TODAY.isoformat()
        offer(campaign_state, [eurostat_distribution()])
        store = MemoryStore(campaign_state)
        quota_state = new_state("eurostat", TODAY)
        clock = FakeClock(1788998400)
        quota_state["quota_attempts"] = [clock() - 1]
        quota = MemoryStore(quota_state)
        raw = RawStore()
        calls = []
        submitted_partitions = {}
        configured = settings(quota_windows=[{"seconds": 10_000, "requests": 100}])

        def fetcher(request, path, hosts, **kwargs):
            # The quota checkpoint must precede every parent, structure,
            # partition, polling, and download request.
            self.assertEqual(len(quota.state["quota_attempts"]), len(calls) + 2)
            calls.append(request)
            url = request["url"]
            status = 200
            if url == eurostat_distribution()["url"]:
                body = (
                    b"<Fault><faultcode>413</faultcode><faultstring>"
                    b"EXTRACTION_TOO_BIG: request entity too large"
                    b"</faultstring></Fault>"
                )
            elif "/dataflow/" in url:
                body = DATAFLOW.encode()
            elif "/datastructure/" in url:
                body = DSD.encode()
            elif "/codelist/" in url:
                body = CODELISTS.encode()
            elif "/contentconstraint/" in url:
                body = CONSTRAINT_ALL_KEYS.encode()
            elif "/async/status/" in url:
                request_id = url.rsplit("/", 1)[-1]
                body = envelope(request_id, "AVAILABLE").encode()
                status = 202
            elif "/async/data/" in url:
                body = b"accepted async partition data"
            elif url not in submitted_partitions:
                if not submitted_partitions:
                    request_id = "partition-job-1"
                    submitted_partitions[url] = request_id
                    body = envelope(request_id, "PROCESSING").encode()
                    status = 202
                else:
                    submitted_partitions[url] = "empty"
                    body = (
                        b"<Fault><faultcode>100</faultcode><faultstring>"
                        b"NO_RESULTS: no observations</faultstring></Fault>"
                    )
            else:
                self.fail("partition must resume through its durable async request")
            path.write_bytes(body)
            return {"status_code": status, "size_bytes": len(body)}

        def inspector(path, kind):
            body = path.read_bytes()
            if b"<queued>" in body:
                return {"status": "async"}
            if b"EXTRACTION_TOO_BIG" in body:
                return {"status": "unsupported", "http_status": 413}
            if b"NO_RESULTS" in body:
                return {"status": "no_results", "fault_type": "NO_RESULTS"}
            return {"status": "complete", "rows": 1}

        with tempfile.TemporaryDirectory() as directory:
            first = run_full_campaign(
                store, raw, quota, "eurostat", configured, directory,
                max_requests=30, clock=clock, fetcher=fetcher,
                inspector=inspector,
                planner=lambda source: [eurostat_distribution()],
            )
            self.assertEqual(first["reason"], "asynchronous_preparation", first)
            durable_task = store.state["pending"][0]
            self.assertIn("partition_recovery", durable_task)
            self.assertIn("partition_async", durable_task)
            self.assertNotIn("partition_inputs", durable_task)

            clock.now += 61
            second = run_full_campaign(
                store, raw, quota, "eurostat", configured, directory,
                max_requests=30, clock=clock, fetcher=fetcher,
                inspector=inspector,
                planner=lambda source: [eurostat_distribution()],
            )
            self.assertEqual(second["reason"], "asynchronous_preparation")
            self.assertEqual(
                store.state["pending"][0]["partition_async"]["plan"]["phase"],
                "download",
            )

            clock.now += 61
            final = run_full_campaign(
                store, raw, quota, "eurostat", configured, directory,
                max_requests=30, clock=clock, fetcher=fetcher,
                inspector=inspector,
                planner=lambda source: [eurostat_distribution()],
            )

        parent_id = task_for(eurostat_distribution())["id"]
        self.assertEqual(final["coverage_status"], "incomplete")  # inventories remain required
        self.assertEqual(final["validated_current_distributions"], 1)
        self.assertEqual(final["accepted"], 2)
        self.assertEqual(store.state["pending"], [])
        self.assertTrue(store.state["completed"][parent_id]["partitioned"])
        leaf_entries = [
            value for key, value in store.state["completed"].items()
            if key.startswith(parent_id + "::partition:")
        ]
        self.assertEqual(len(leaf_entries), 2)
        self.assertEqual(len(store.state["receipts"]), 2)
        self.assertEqual(store.state["accepted_responses"], 2)
        # One pre-existing attempt plus 10 requests: 413, five structure
        # documents, async submission/poll/download, and one direct empty leaf.
        self.assertEqual(len(quota.state["quota_attempts"]), 11)

        accepted = [
            json.loads(payload)
            for payload in store.objects.values()
            if json.loads(payload).get("kind") == "full_distribution"
        ]
        self.assertEqual(len(accepted), 2)
        self.assertEqual(
            {item["distribution"]["original_dataset_id"] for item in accepted},
            {"demo_cube"},
        )
        self.assertEqual(len({item["distribution"]["dataset_id"] for item in accepted}), 2)
        self.assertTrue(all("partition_selection" in item["distribution"] for item in accepted))
        self.assertEqual(
            sum(item["inspection"].get("empty_partition", False) for item in accepted),
            1,
        )

    def test_disabled_source_performs_no_store_quota_or_transport_work(self):
        class Forbidden:
            def __getattr__(self, name):
                raise AssertionError(f"disabled campaign accessed {name}")

        result = run_full_campaign(
            Forbidden(), Forbidden(), Forbidden(), "world_bank_wdi",
            {**settings(), "enabled": False}, "/must/not/be/created",
            fetcher=lambda *args, **kwargs: self.fail("disabled campaign fetched"),
            planner=lambda source: self.fail("disabled campaign planned"),
        )
        self.assertEqual(result["reason"], "source_disabled")
        self.assertEqual(result["requests"], 0)
        self.assertEqual(result["coverage_status"], "disabled")


if __name__ == "__main__":
    unittest.main()
