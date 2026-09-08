import sys
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.campaign_recovery import (  # noqa: E402
    CampaignRecoveryError,
    retry_validation_failures,
)
from ingestion.source_campaign_store import LocalCampaignStore  # noqa: E402


OLD_SHA = "a" * 40
NEW_SHA = "b" * 40


def pending_task(task_id="series-1", retry_at=9_999):
    return {
        "id": task_id,
        "lane": "history",
        "kind": "series_page",
        "cursor": {"series": task_id, "page": 1},
        "failures": 2,
        "retry_at": retry_at,
    }


def rejected_receipt(
    task,
    *,
    source_id="recovery_source",
    status=200,
    error_type="ValueError",
    code_sha=OLD_SHA,
):
    receipt_task = {
        key: deepcopy(value)
        for key, value in task.items()
        if key not in {"failures", "retry_at"}
    }
    return {
        "schema_version": 1,
        "accepted": False,
        "source_id": source_id,
        "task": receipt_task,
        "request": {"url": "https://source.example.test/page"},
        "http_status": status,
        "error_type": error_type,
        "code_sha": code_sha,
    }


def descriptor(task_id, receipt_id):
    return {
        "task_id": task_id,
        "id": receipt_id,
        "sha256": "0" * 64,
        "size_bytes": 1,
    }


def campaign_state(task=None, rejected=None):
    return {
        "schema_version": 1,
        "source_id": "recovery_source",
        "onboarding_date": "2026-09-08",
        "pending": [deepcopy(task)] if task is not None else [],
        "completed": {"already-complete": "2026-09-08T00:00:00+00:00"},
        "recent_roots": {},
        "quota_attempts": [100.0, 200.0],
        "receipts": [],
        "rejected_receipts": list(rejected or []),
        "raw_bytes": 42,
        "accepted_responses": 3,
        "record_count": 7,
        "lane_position": 4,
        "coverage_status": "incomplete",
        "catalogue_totals": {},
        "provider_retry_at": 12_345,
        "historical_cursor": "unchanged-history",
        "last_error": {"task_id": "series-1"},
    }


class UncertainSaveError(RuntimeError):
    uncertain = True


class MemoryRecoveryStore:
    source_id = "recovery_source"

    def __init__(self, state, receipts=None):
        self.state = deepcopy(state)
        self.receipts = deepcopy(receipts or {})
        self.read_ids = []
        self.saved_states = []
        self.save_error = None

    def load(self):
        return deepcopy(self.state)

    def read_receipt(self, receipt_descriptor):
        self.read_ids.append(receipt_descriptor["id"])
        return deepcopy(self.receipts[receipt_descriptor["id"]])

    def save(self, state):
        if self.save_error is not None:
            raise self.save_error
        self.state = deepcopy(state)
        self.saved_states.append(deepcopy(state))


class CampaignRecoveryTests(unittest.TestCase):
    def test_only_retry_time_changes_and_bounded_audit_is_saved(self):
        task = pending_task()
        item = descriptor(task["id"], "eligible")
        state = campaign_state(task, [item])
        state["validation_retry_audits"] = [
            {
                "schema_version": 1,
                "kind": "validation_failure_retry",
                "corrected_code_sha": f"prior-{index}",
                "task_ids": [f"old-{index}"],
            }
            for index in range(25)
        ]
        store = MemoryRecoveryStore(
            state, {"eligible": rejected_receipt(task)}
        )

        result = retry_validation_failures(store, NEW_SHA)

        self.assertEqual(
            result,
            {
                "source_id": "recovery_source",
                "task_ids": ["series-1"],
                "reset_count": 1,
            },
        )
        self.assertEqual(store.state["pending"][0]["retry_at"], 0)
        self.assertEqual(store.state["pending"][0]["failures"], 2)
        for field in (
            "provider_retry_at",
            "quota_attempts",
            "historical_cursor",
            "completed",
            "last_error",
            "lane_position",
        ):
            self.assertEqual(store.state[field], state[field])
        self.assertEqual(len(store.state["validation_retry_audits"]), 20)
        self.assertEqual(
            store.state["validation_retry_audits"][-1],
            {
                "schema_version": 1,
                "kind": "validation_failure_retry",
                "corrected_code_sha": NEW_SHA,
                "task_ids": ["series-1"],
            },
        )
        self.assertEqual(len(store.saved_states), 1)

    def test_429_and_503_receipts_cannot_bypass_provider_backoff(self):
        for status in (429, 503):
            with self.subTest(status=status):
                task = pending_task()
                item = descriptor(task["id"], f"status-{status}")
                store = MemoryRecoveryStore(
                    campaign_state(task, [item]),
                    {
                        item["id"]: rejected_receipt(
                            task, status=status, error_type="CampaignError"
                        )
                    },
                )
                result = retry_validation_failures(store, NEW_SHA)
                self.assertEqual(result["task_ids"], [])
                self.assertEqual(store.state["pending"][0]["retry_at"], 9_999)
                self.assertEqual(store.state["provider_retry_at"], 12_345)
                self.assertEqual(store.saved_states, [])

    def test_same_code_and_repeated_control_are_no_ops(self):
        task = pending_task()
        same_item = descriptor(task["id"], "same-code")
        same_store = MemoryRecoveryStore(
            campaign_state(task, [same_item]),
            {"same-code": rejected_receipt(task, code_sha=NEW_SHA)},
        )
        self.assertEqual(retry_validation_failures(same_store, NEW_SHA)["reset_count"], 0)
        self.assertEqual(same_store.saved_states, [])

        eligible_item = descriptor(task["id"], "old-code")
        store = MemoryRecoveryStore(
            campaign_state(task, [eligible_item]),
            {"old-code": rejected_receipt(task)},
        )
        self.assertEqual(retry_validation_failures(store, NEW_SHA)["task_ids"], ["series-1"])
        first_saved = deepcopy(store.state)
        self.assertEqual(retry_validation_failures(store, NEW_SHA)["reset_count"], 0)
        self.assertEqual(store.state, first_saved)
        self.assertEqual(len(store.saved_states), 1)

    def test_newest_receipt_supersedes_an_older_validation_failure(self):
        task = pending_task()
        old = descriptor(task["id"], "older-validation")
        latest = descriptor(task["id"], "newer-service-error")
        store = MemoryRecoveryStore(
            campaign_state(task, [old, latest]),
            {
                "older-validation": rejected_receipt(task),
                "newer-service-error": rejected_receipt(
                    task, status=503, error_type="CampaignError"
                ),
            },
        )

        result = retry_validation_failures(store, NEW_SHA)
        self.assertEqual(result["task_ids"], [])
        self.assertEqual(store.state["pending"][0]["retry_at"], 9_999)
        self.assertEqual(store.saved_states, [])

    def test_source_identity_tampering_fails_closed_and_task_mismatch_is_ineligible(self):
        task = pending_task()
        item = descriptor(task["id"], "tampered-source")
        store = MemoryRecoveryStore(
            campaign_state(task, [item]),
            {"tampered-source": rejected_receipt(task, source_id="other_source")},
        )
        with self.assertRaisesRegex(CampaignRecoveryError, "source identity"):
            retry_validation_failures(store, NEW_SHA)
        self.assertEqual(store.saved_states, [])
        self.assertEqual(store.state["pending"][0]["retry_at"], 9_999)

        mismatched = rejected_receipt(task)
        mismatched["task"]["cursor"]["page"] = 2
        mismatch_store = MemoryRecoveryStore(
            campaign_state(task, [descriptor(task["id"], "wrong-cursor")]),
            {"wrong-cursor": mismatched},
        )
        self.assertEqual(
            retry_validation_failures(mismatch_store, NEW_SHA)["task_ids"], []
        )
        self.assertEqual(mismatch_store.state["pending"][0]["retry_at"], 9_999)
        self.assertEqual(mismatch_store.saved_states, [])

    def test_missing_pending_task_and_missing_state_are_no_ops(self):
        absent = pending_task("absent")
        item = descriptor(absent["id"], "no-pending")
        store = MemoryRecoveryStore(
            campaign_state(None, [item]),
            {"no-pending": rejected_receipt(absent)},
        )
        self.assertEqual(retry_validation_failures(store, NEW_SHA)["task_ids"], [])
        self.assertEqual(store.saved_states, [])

        empty = MemoryRecoveryStore(None)
        self.assertEqual(
            retry_validation_failures(empty, NEW_SHA),
            {
                "source_id": "recovery_source",
                "task_ids": [],
                "reset_count": 0,
            },
        )
        self.assertEqual(empty.read_ids, [])

    def test_only_the_newest_twenty_rejected_receipts_are_read(self):
        task = pending_task("too-old")
        descriptors = [descriptor(task["id"], "eligible-but-too-old")]
        receipts = {"eligible-but-too-old": rejected_receipt(task)}
        for index in range(20):
            ghost = pending_task(f"ghost-{index}")
            receipt_id = f"newer-{index}"
            descriptors.append(descriptor(ghost["id"], receipt_id))
            receipts[receipt_id] = rejected_receipt(ghost)
        store = MemoryRecoveryStore(campaign_state(task, descriptors), receipts)

        result = retry_validation_failures(store, NEW_SHA)
        self.assertEqual(result["task_ids"], [])
        self.assertEqual(len(store.read_ids), 20)
        self.assertNotIn("eligible-but-too-old", store.read_ids)
        self.assertEqual(store.state["pending"][0]["retry_at"], 9_999)

    def test_uncertain_state_save_propagates_without_mutating_loaded_state(self):
        task = pending_task()
        item = descriptor(task["id"], "eligible")
        state = campaign_state(task, [item])
        store = MemoryRecoveryStore(state, {"eligible": rejected_receipt(task)})
        store.save_error = UncertainSaveError("lost pointer reply")

        with self.assertRaisesRegex(UncertainSaveError, "lost pointer reply"):
            retry_validation_failures(store, NEW_SHA)
        self.assertEqual(store.state, state)

    def test_local_store_recovery_is_durable_in_a_fresh_instance(self):
        task = pending_task()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = LocalCampaignStore(root, "recovery_source")
            receipt = store.put_receipt(rejected_receipt(task))
            state = campaign_state(task, [{"task_id": task["id"], **receipt}])
            store.save(state)

            result = retry_validation_failures(
                LocalCampaignStore(root, "recovery_source"), NEW_SHA
            )
            self.assertEqual(result["task_ids"], ["series-1"])

            restored = LocalCampaignStore(root, "recovery_source").load()
            self.assertEqual(restored["pending"][0]["retry_at"], 0)
            self.assertEqual(restored["pending"][0]["failures"], 2)
            self.assertEqual(restored["provider_retry_at"], 12_345)
            self.assertEqual(restored["quota_attempts"], [100.0, 200.0])
            self.assertEqual(restored["validation_retry_audits"][-1]["task_ids"], ["series-1"])


if __name__ == "__main__":
    unittest.main()
