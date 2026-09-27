"""No-network BDL discovery, failure fairness and completion regressions."""
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import bdl_web_queue as queue


def candidate(number, verified=True):
    return {"category_id": "K1", "group_id": "G2", "subgroup_id": f"P{number}",
            "subgroup_name": f"Group {number}", "web_catalogue_verified": verified,
            "url": f"{queue.WEB}/dane/podgrup/wymiary/1/2/{number}"}


class BdlWebQueueTests(unittest.TestCase):
    def test_failed_first_subgroup_does_not_starve_unattempted_work(self):
        state = queue.new_state()
        state["candidates"] = {"P1": candidate(1), "P2": candidate(2)}
        state["failures"]["P1"] = {"attempts": 3}
        self.assertEqual("P2", queue.next_candidate(state, set(), set())["subgroup_id"])
        self.assertEqual("P1", queue.next_candidate(state, {"P2"}, set())["subgroup_id"])
        self.assertIsNone(queue.next_candidate(state, {"P2"}, {"P1"}))

    def test_partial_catalogue_is_never_complete_even_when_all_known_data_landed(self):
        state = queue.new_state()
        state["candidates"] = {"P1": candidate(1)}
        result = queue.progress(state, {"P1"}, 1, 4)
        self.assertEqual("incomplete", result["status"])
        self.assertFalse(result["catalogue_exhausted"])

    def test_unverified_recovery_index_blocks_completion(self):
        state = queue.new_state()
        state["candidates"] = {"P1": candidate(1, False)}
        state["discovery_pending"] = []
        state["discovery_completed"] = {"table": {}}
        self.assertEqual("incomplete", queue.progress(state, {"P1"}, 1, 4)["status"])

    def test_missing_or_failed_subgroup_remains_pending(self):
        state = queue.new_state()
        state["candidates"] = {"P1": candidate(1), "P2": candidate(2)}
        state["discovery_pending"] = []
        state["discovery_completed"] = {"table": {}}
        state["failures"]["P1"] = {"attempts": 1}
        result = queue.progress(state, {"P2"}, 1, 4)
        self.assertEqual("incomplete", result["status"])
        self.assertEqual(["P1"], result["failed_subgroups"])
        self.assertEqual(1, result["remaining_known_subgroups"])

    def test_complete_requires_exhausted_web_catalogue_and_every_landed_subgroup(self):
        state = queue.new_state()
        state["candidates"] = {"P1": candidate(1), "P2": candidate(2)}
        state["discovery_pending"] = []
        state["discovery_completed"] = {"table": {}}
        result = queue.progress(state, {"P1", "P2", "P99"}, 2, 30)
        self.assertEqual("complete", result["status"])
        self.assertEqual(2, result["landed_known_subgroups"])
        self.assertEqual(3, result["landed_total_subgroups"])

    def test_empty_campaign_is_not_success(self):
        state = queue.new_state()
        state["discovery_pending"] = []
        self.assertEqual("incomplete", queue.progress(state, set(), 0, 0)["status"])

    def test_discovery_walks_entire_hierarchy_and_preserves_metadata(self):
        state = queue.new_state()
        for identifier, name in [("K1", "Category"), ("G2", "Group"), ("P3", "Subgroup")]:
            task = state["discovery_pending"][0]
            result = {"url": task["url"], "complete": True, "expected_count": 1,
                      "records": [{"id": identifier, "name": name, "cells": [name, identifier]}],
                      "pages": [{"page": 1, "first": 1, "last": 1, "count": 1}]}
            state = queue.apply_discovery(state, task, result)
        self.assertEqual([], state["discovery_pending"])
        self.assertEqual(3, len(state["discovery_completed"]))
        self.assertTrue(queue.candidate_valid(state["candidates"]["P3"]))
        self.assertEqual(["Subgroup", "P3"], state["candidates"]["P3"]["web_metadata"]["cells"])

    def test_incomplete_or_duplicate_discovery_leaves_parent_pending(self):
        state = queue.new_state()
        task = state["discovery_pending"][0]
        baseline = deepcopy(state)
        for result in [
            {"url": task["url"], "complete": False},
            {"url": task["url"], "complete": True, "expected_count": 2,
             "records": [{"id": "K1", "name": "One"}]},
            {"url": task["url"], "complete": True, "expected_count": 2,
             "records": [{"id": "K1", "name": "One"}, {"id": "K1", "name": "One"}]},
        ]:
            with self.assertRaises(ValueError):
                queue.apply_discovery(state, task, result)
            self.assertEqual(baseline, state)

    def test_candidate_url_cannot_redirect_or_switch_subgroups(self):
        value = candidate(1)
        self.assertTrue(queue.candidate_valid(value))
        for url in ["https://example.org/x", value["url"] + "?api=true", value["url"] + "1"]:
            self.assertFalse(queue.candidate_valid({**value, "url": url}))

    def test_recovery_seed_requires_exact_inspected_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.json"
            path.write_text('{"remaining_candidates": []}')
            with self.assertRaisesRegex(ValueError, "inspected"):
                queue.new_state(path)


if __name__ == "__main__":
    unittest.main()
