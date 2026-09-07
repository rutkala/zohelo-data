import json
from hashlib import sha256
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion.nbp_state import (  # noqa: E402
    NBPStateError,
    UncertainStatePointerError,
    LoadedState,
    commit_response,
    list_successful_response_descriptors,
    load_state,
    plan_requests,
    source_specs_from_config,
    new_state,
    SourceSpec,
    store_raw_response,
    validate_nbp_response,
)


class MemoryStore:
    def __init__(self):
        self.files, self.folders = {}, {}
        self.number = 1
        self.fail_create = None
        self.raise_after_replace = False
        self.drift = None

    def _id(self):
        value = f"f{self.number}"
        self.number += 1
        return value

    def find(self, name, parent_id):
        return [key for key, item in self.files.items() if item["name"] == name and item["parent"] == parent_id] + [key for key, item in self.folders.items() if item["name"] == name and item["parent"] == parent_id]

    def create(self, name, data, parent_id):
        if name == self.fail_create:
            raise OSError("injected create failure")
        file_id = self._id()
        self.files[file_id] = {"name": name, "data": data, "parent": parent_id}
        return file_id

    def read(self, file_id):
        return self.files[file_id]["data"]

    def replace(self, file_id, data):
        if self.drift is not None:
            self.files[file_id]["data"] = self.drift
            return
        self.files[file_id]["data"] = data
        if self.raise_after_replace:
            raise OSError("lost reply")

    def mkdir(self, name, parent_id):
        folder_id = self._id()
        self.folders[folder_id] = {"name": name, "parent": parent_id}
        return folder_id


def configured_specs():
    return source_specs_from_config(Path(__file__).resolve().parents[1] / "config" / "sources.yaml", ["nbp_exchange_rates_table_a"])


def a_body(day="2020-01-01", value=4.0):
    return json.dumps([{
        "table": "A", "no": "001/A/NBP/2020", "effectiveDate": day,
        "rates": [{"currency": "dolar amerykański", "code": "USD", "mid": value}],
    }], separators=(",", ":")).encode()


class NBPStateTests(unittest.TestCase):
    def setUp(self):
        self.specs = configured_specs()
        self.store = MemoryStore()
        self.root = "controlroot"
        self.landing = "landingA"
        self.now = datetime(2026, 9, 7, tzinfo=timezone.utc)

    def _loaded(self):
        return load_state(self.store, self.root, self.specs, now_utc=self.now)

    def _commit(self, loaded, plan, status=200, body=None, **kwargs):
        return commit_response(self.store, self.root, loaded, self.specs, plan, http_status=status, body=a_body(plan.requested_start_date.isoformat()) if body is None else body, retrieved_at_utc=self.now, landing_source_folder_id=self.landing, **kwargs)

    def test_93_day_chunks_are_inclusive_and_resume_same_hole(self):
        loaded = self._loaded()
        plans = plan_requests(loaded.state, self.specs, date(2002, 7, 1), recent_recheck_days=1, max_catch_up_chunks=2)
        self.assertEqual([(p.requested_start_date, p.requested_end_date) for p in plans], [(date(2002, 1, 2), date(2002, 4, 4)), (date(2002, 4, 5), date(2002, 6, 30))])
        first = self._commit(loaded, plans[0], body=a_body("2002-01-02"))
        resumed = plan_requests(first.state, self.specs, date(2002, 7, 1), recent_recheck_days=1, max_catch_up_chunks=2)
        self.assertEqual(resumed[0].requested_start_date, date(2002, 4, 5))

    def test_gap_blocks_recent_recheck_until_catch_up_and_404_advances_coverage_only(self):
        loaded = self._loaded()
        first = plan_requests(loaded.state, self.specs, date(2002, 4, 5), recent_recheck_days=10)[0]
        completed = self._commit(loaded, first, status=404, body=b"")
        source = completed.state["sources"][first.source_id]
        self.assertEqual(source["last_checked_through_date"], "2002-03-26")
        self.assertIsNone(source["latest_observation_date"])
        self.assertIsNone(source["last_successful_ingestion_at_utc"])
        self.assertEqual(list_successful_response_descriptors(completed.state), [])
        # A nonempty valid 404 page is still exact retained request evidence.
        nonempty = self._commit(load_state(self.store, self.root, self.specs), first, status=404, body=b"not-found")
        attempt = json.loads(self.store.read(nonempty.attempt_file_id))
        self.assertIsNotNone(attempt["raw_file_id"])
        next_plan = plan_requests(completed.state, self.specs, date(2002, 4, 5), recent_recheck_days=10)[0]
        self.assertEqual(next_plan.requested_start_date, date(2002, 3, 27))

    def test_content_addressed_reuse_unchanged_replay_and_reversion_keep_later_sequence(self):
        loaded = self._loaded()
        plan = plan_requests(loaded.state, self.specs, date(2002, 1, 2), recent_recheck_days=1)[0]
        one = self._commit(loaded, plan, body=a_body("2002-01-02", 4.0), run_id="run-1")
        # State is complete, so the normal next plan is the same recent recheck.
        recheck = plan_requests(one.state, self.specs, date(2002, 1, 2), recent_recheck_days=1)[0]
        two = self._commit(load_state(self.store, self.root, self.specs), recheck, body=a_body("2002-01-02", 4.0), run_id="run-2")
        three = self._commit(load_state(self.store, self.root, self.specs), recheck, body=a_body("2002-01-02", 4.5), run_id="run-3")
        four = self._commit(load_state(self.store, self.root, self.specs), recheck, body=a_body("2002-01-02", 4.0), run_id="run-4")
        descriptors = list_successful_response_descriptors(four.state)
        self.assertEqual([item["ingestion_sequence"] for item in descriptors], [1, 2, 3, 4])
        self.assertEqual(descriptors[0]["raw_file_id"], descriptors[1]["raw_file_id"])
        self.assertEqual(descriptors[0]["raw_file_id"], descriptors[3]["raw_file_id"])
        self.assertNotEqual(descriptors[1]["raw_file_id"], descriptors[2]["raw_file_id"])

    def test_rotating_history_cursor_moves_only_after_successful_history_chunk(self):
        spec = SourceSpec("nbp_exchange_rates_table_a", date(2020, 1, 1), "https://api.nbp.pl/api/exchangerates/tables/A/{start_date}/{end_date}", {"format": "json"}, 93)
        specs = {spec.source_id: spec}
        state = new_state(specs, self.now)
        source = state[spec.source_id] if False else state["sources"][spec.source_id]
        # All old dates are covered, so planning can add a recent recheck and one
        # bounded rotation pass without confusing a maximum observed date for coverage.
        source["completed_intervals"] = [{"start_date": "2020-01-01", "end_date": "2020-04-26", "outcome": "no_observations", "ingestion_sequence": 1, "attempt_file_id": "prior"}]
        state["global_sequence"] = 1
        loaded = LoadedState(state, None, None, None)
        plans = plan_requests(state, specs, date(2020, 5, 1), recent_recheck_days=5, max_historical_recheck_chunks=1)
        history = next(plan for plan in plans if plan.mode == "historical_recheck")
        self.assertEqual((history.requested_start_date, history.requested_end_date), (date(2020, 1, 1), date(2020, 4, 2)))
        result = commit_response(self.store, self.root, loaded, specs, history, http_status=404, body=b"", retrieved_at_utc=self.now)
        self.assertEqual(result.state["sources"][spec.source_id]["historical_cursor"], "2020-04-03")

    def test_malformed_and_bad_status_do_not_checkpoint(self):
        loaded = self._loaded()
        plan = plan_requests(loaded.state, self.specs, date(2002, 1, 2), recent_recheck_days=1)[0]
        bad_json = self._commit(loaded, plan, body=b"not json", code_sha="a" * 40)
        self.assertFalse(bad_json.advanced)
        self.assertEqual(bad_json.state["global_sequence"], 0)
        failed_attempt = json.loads(self.store.read(bad_json.attempt_file_id))
        self.assertEqual(failed_attempt["response_size_bytes"], len(b"not json"))
        self.assertEqual(failed_attempt["code_sha"], "a" * 40)
        bad_status = self._commit(load_state(self.store, self.root, self.specs), plan, status=500, body=b"oops")
        self.assertFalse(bad_status.advanced)
        self.assertEqual(bad_status.state["sources"][plan.source_id]["last_attempt_file_id"], bad_status.attempt_file_id)
        with self.assertRaisesRegex(NBPStateError, "size limit"):
            validate_nbp_response("nbp_exchange_rates_table_a", 200, b"x" * 9, max_response_bytes=8)

    def test_out_of_interval_rows_and_raw_write_failures_do_not_advance_coverage(self):
        loaded = self._loaded()
        plan = plan_requests(loaded.state, self.specs, date(2002, 1, 2), recent_recheck_days=1)[0]
        out_of_range = self._commit(loaded, plan, body=a_body("2002-01-03"))
        self.assertFalse(out_of_range.advanced)
        self.assertEqual(out_of_range.state["global_sequence"], 0)
        self.assertIsNone(out_of_range.state["sources"][plan.source_id]["last_checked_through_date"])

        isolated = MemoryStore()
        body = a_body("2002-01-02")
        isolated.fail_create = sha256(body).hexdigest() + ".json"
        with self.assertRaisesRegex(OSError, "create failure"):
            commit_response(isolated, self.root, load_state(isolated, self.root, self.specs, now_utc=self.now), self.specs, plan, http_status=200, body=body, retrieved_at_utc=self.now, landing_source_folder_id=self.landing)
        self.assertEqual(isolated.find("current-ingestion-state.json", self.root), [])

    def test_storage_failure_before_snapshot_never_creates_pointer(self):
        loaded = self._loaded()
        plan = plan_requests(loaded.state, self.specs, date(2002, 1, 2), recent_recheck_days=1)[0]
        self.store.fail_create = None  # attempt succeeds; force the immutable snapshot write.
        original_create = self.store.create
        def fail_snapshot(name, data, parent):
            if name.endswith(".json") and parent in self.store.find("states", self.root):
                raise OSError("snapshot failed")
            return original_create(name, data, parent)
        self.store.create = fail_snapshot
        with self.assertRaisesRegex(OSError, "snapshot failed"):
            self._commit(loaded, plan)
        self.assertEqual(self.store.find("current-ingestion-state.json", self.root), [])

    def test_multi_source_stale_pointer_cannot_hide_a_second_source_commit(self):
        specs = source_specs_from_config(Path(__file__).resolve().parents[1] / "config" / "sources.yaml", ["nbp_exchange_rates_table_a", "nbp_exchange_rates_table_b"])
        base = load_state(self.store, self.root, specs, now_utc=self.now)
        plans = plan_requests(base.state, specs, date(2002, 1, 2), recent_recheck_days=1)
        plan_a = next(plan for plan in plans if plan.source_id.endswith("_a"))
        plan_b = next(plan for plan in plans if plan.source_id.endswith("_b"))
        commit_response(self.store, self.root, base, specs, plan_a, http_status=200, body=a_body("2002-01-02"), retrieved_at_utc=self.now, landing_source_folder_id=self.landing)
        b_body = json.dumps([{"table": "B", "no": "001/B/NBP/2002", "effectiveDate": "2002-01-02", "rates": [{"currency": "x", "code": "USD", "mid": 4.0}]}]).encode()
        with self.assertRaisesRegex(NBPStateError, "pointer changed"):
            commit_response(self.store, self.root, base, specs, plan_b, http_status=200, body=b_body, retrieved_at_utc=self.now, landing_source_folder_id=self.landing)
        restored = load_state(self.store, self.root, specs)
        self.assertEqual(restored.state["sources"][plan_a.source_id]["last_checked_through_date"], "2002-01-02")
        self.assertIsNone(restored.state["sources"][plan_b.source_id]["last_checked_through_date"])

    def test_verified_pointer_bytes_reused_in_process_still_detect_next_commit_drift(self):
        loaded = self._loaded()
        plan = plan_requests(loaded.state, self.specs, date(2002, 1, 2), recent_recheck_days=1)[0]
        first = self._commit(loaded, plan)
        self.assertEqual(first.pointer_raw, self.store.files[first.pointer_file_id]["data"])
        reused = LoadedState(first.state, first.pointer_file_id, first.pointer_raw, first.snapshot_file_id)

        competing = json.loads(first.pointer_raw)
        competing["updated_at_utc"] = "2026-09-07T00:00:01Z"
        self.store.files[first.pointer_file_id]["data"] = json.dumps(
            competing, sort_keys=True, separators=(",", ":")
        ).encode()
        with self.assertRaisesRegex(NBPStateError, "pointer changed"):
            self._commit(reused, plan, body=a_body("2002-01-02", 4.1))

    def test_uncertain_pointer_readback_does_not_claim_commit_and_fresh_restore_works(self):
        loaded = self._loaded()
        plan = plan_requests(loaded.state, self.specs, date(2002, 1, 2), recent_recheck_days=1)[0]
        one = self._commit(loaded, plan)
        pointer_id = one.pointer_file_id
        prior_pointer = self.store.files[pointer_id]["data"]
        self.store.drift = b'{"format_version":1,"state_file_id":"other","state_sha256":"' + b"a" * 64 + b'"}'
        with self.assertRaises(UncertainStatePointerError):
            self._commit(load_state(self.store, self.root, self.specs), plan, body=a_body("2002-01-02", 4.1))
        self.assertEqual(pointer_id, one.pointer_file_id)
        # An operator can retain the previous stable pointer; fresh restoration
        # validates its immutable snapshot rather than relying on process memory.
        self.store.files[pointer_id]["data"] = prior_pointer
        self.store.drift = None
        restored = load_state(self.store, self.root, self.specs)
        self.assertEqual(restored.state["global_sequence"], 1)

    def test_table_b_table_c_and_gold_payload_contracts(self):
        b = json.dumps([{"table": "B", "no": "001/B/NBP/2020", "effectiveDate": "2020-01-01", "rates": [{"currency": "x", "code": "USD", "mid": 4.0}]}]).encode()
        c = json.dumps([{"table": "C", "no": "001/C/NBP/2020", "tradingDate": "2019-12-31", "effectiveDate": "2020-01-01", "rates": [{"currency": "x", "code": "USD", "bid": 3.9, "ask": 4.1}]}]).encode()
        gold = b'[{"data":"2020-01-01","cena":220.5}]'
        self.assertEqual(validate_nbp_response("nbp_exchange_rates_table_b", 200, b).observation_count, 1)
        self.assertEqual(validate_nbp_response("nbp_exchange_rates_table_c", 200, c).observation_max_date, date(2020, 1, 1))
        self.assertEqual(validate_nbp_response("nbp_gold_prices", 200, gold).observation_min_date, date(2020, 1, 1))

    def test_historical_nullable_provenance_and_ambiguous_keys(self):
        legacy_c = json.dumps([{
            "table": "C", "no": None, "effectiveDate": "2002-01-02",
            "rates": [{"currency": None, "code": "USD", "bid": 3.9, "ask": 4.1}],
        }]).encode()
        self.assertEqual(validate_nbp_response("nbp_exchange_rates_table_c", 200, legacy_c).observation_count, 1)
        # Different source publication numbers may not silently represent two
        # current analytical values for the same table/date/currency key.
        repeated_identical = json.dumps([{
            "table": "A", "no": "one", "effectiveDate": "2002-01-02",
            "rates": [{"country": "RFN", "code": "EUR", "mid": 4.0}, {"country": "UGW", "code": "EUR", "mid": 4.0}],
        }]).encode()
        self.assertEqual(validate_nbp_response("nbp_exchange_rates_table_a", 200, repeated_identical).observation_count, 1)
        conflicting = json.dumps([
            {"table": "A", "no": "one", "effectiveDate": "2002-01-02", "rates": [{"code": "USD", "mid": 4.0}]},
            {"table": "A", "no": "two", "effectiveDate": "2002-01-02", "rates": [{"code": "USD", "mid": 4.1}]},
        ]).encode()
        with self.assertRaisesRegex(NBPStateError, "conflicting table"):
            validate_nbp_response("nbp_exchange_rates_table_a", 200, conflicting)
        with self.assertRaisesRegex(NBPStateError, "duplicate gold"):
            validate_nbp_response("nbp_gold_prices", 200, b'[{"data":"2002-01-02","cena":1},{"data":"2002-01-02","cena":2}]')

    def test_unsafe_identifiers_and_invalid_bid_ask_are_rejected(self):
        with self.assertRaises(NBPStateError):
            store_raw_response(self.store, "../bad", b"x", self.landing)
        c = json.dumps([{"table": "C", "no": "1", "tradingDate": "2020-01-01", "effectiveDate": "2020-01-02", "rates": [{"currency": "x", "code": "USD", "bid": 5, "ask": 4}]}]).encode()
        with self.assertRaisesRegex(NBPStateError, "bid"):
            validate_nbp_response("nbp_exchange_rates_table_c", 200, c)


if __name__ == "__main__":
    unittest.main()
