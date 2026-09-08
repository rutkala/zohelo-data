"""Tests for metadata-only publication of accepted full distributions."""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from hashlib import md5, sha256
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.bulk_publication import (  # noqa: E402
    BULK_INDEX_COLUMNS,
    BulkPublicationError,
    publish_bulk_index,
    verify_bulk_index,
)
from ingestion.source_campaign import new_state  # noqa: E402
from ingestion.source_campaign_store import LocalCampaignStore  # noqa: E402


def accepted(store, provider_id: str, number: int, *, params=None):
    dataset_id = "__inventory__" if number == 0 else f"dataset_{number}"
    payload_hash = sha256(f"archive-{number}".encode()).hexdigest()
    raw = {
        "id": f"driveRaw{number}",
        "name": f"raw-{payload_hash}.bin",
        "sha256": payload_hash,
        "md5": md5(f"archive-{number}".encode(), usedforsecurity=False).hexdigest(),
        "size_bytes": 270_000_000 + number,
        "metadata": {"dataset_id": dataset_id, "source_id": provider_id},
    }
    timestamp = (
        datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=number)
    ).isoformat()
    distribution = {
        "dataset_id": dataset_id,
        "url": f"https://example.test/data/{dataset_id}",
        "params": {"compressed": "true"} if params is None else params,
        "version": timestamp,
        "kind": "eurostat_inventory" if number == 0 else "eurostat_tsv_gzip",
    }
    receipt = {
        "schema_version": 1,
        "source_id": provider_id,
        "accepted": True,
        "kind": "full_distribution",
        "distribution": distribution,
        "retrieved_at_utc": timestamp,
        "raw": raw,
        "transport": {"final_url": distribution["url"], "status_code": 200},
        "inspection": {
            "status": "complete",
            "row_count": number + 10,
            "sha256": payload_hash,
        },
        "code_sha": "a" * 40,
    }
    receipt_descriptor = store.put_receipt(receipt)
    task_id = f"distribution:{number}"
    return (
        {"task_id": task_id, **receipt_descriptor},
        {
            "dataset_id": dataset_id,
            "raw": raw,
            "receipt": receipt_descriptor,
        },
        receipt,
    )


def save_state(store, provider_id: str, accepted_items, *, pending=()):
    state = new_state(f"{provider_id}_bulk", date(2026, 9, 8))
    state["provider_id"] = provider_id
    state["receipts"] = [deepcopy(item[0]) for item in accepted_items]
    state["accepted_responses"] = len(accepted_items)
    state["completed"] = {
        item[0]["task_id"]: deepcopy(item[1]) for item in accepted_items
    }
    state["pending"] = list(pending)
    store.save(state)


class BulkPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.provider_id = "eurostat"

    def tearDown(self):
        self.temporary.cleanup()

    def store(self):
        return LocalCampaignStore(self.root, f"{self.provider_id}_bulk")

    def test_publishes_queryable_metadata_without_archive_payload(self):
        store = self.store()
        item = accepted(store, self.provider_id, 1)
        save_state(store, self.provider_id, [item])

        manifest = publish_bulk_index(store, "b" * 40)

        self.assertEqual(manifest["format_version"], 2)
        self.assertEqual(manifest["kind"], "full_distribution_index")
        self.assertEqual(manifest["source_id"], "eurostat_bulk")
        self.assertEqual(manifest["table_name"], "eurostat_distributions")
        # An empty queue alone is not a completeness claim: all current
        # inventories and their admitted catalogue membership still govern it.
        self.assertEqual(manifest["coverage_status"], "incomplete")
        self.assertEqual(
            manifest["columns"],
            [{"name": name, "type": kind} for name, kind in BULK_INDEX_COLUMNS],
        )
        pointer_path = (
            self.root
            / "06_control/source_campaigns/eurostat_bulk/current-landing.json"
        )
        self.assertEqual(json.loads(pointer_path.read_text())["format_version"], 1)

        parquet_path = self.root / manifest["files"][0]["id"]
        connection = duckdb.connect(":memory:")
        try:
            columns = connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet_path)]
            ).fetchall()
            row = connection.execute(
                "SELECT dataset_id, source_id, version, kind, raw_file_id, "
                "raw_file_name, raw_size_bytes, raw_sha256, request_json, "
                "inspection_json FROM read_parquet(?)",
                [str(parquet_path)],
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual([(value[0], value[1]) for value in columns], list(BULK_INDEX_COLUMNS))
        self.assertEqual(row[0:2], ("dataset_1", "eurostat"))
        self.assertEqual(row[4], "driveRaw1")
        self.assertEqual(row[6], 270_000_001)
        self.assertNotIn("payload", [value[0] for value in columns])
        self.assertEqual(json.loads(row[8])["params"], {"compressed": "true"})
        self.assertEqual(json.loads(row[9])["status"], "complete")
        self.assertLess(parquet_path.stat().st_size, 100_000)

    def test_restart_appends_only_new_receipt_and_verifies_all_on_demand(self):
        first_store = self.store()
        first = accepted(first_store, self.provider_id, 0)
        save_state(first_store, self.provider_id, [first])
        first_manifest = publish_bulk_index(first_store, "a" * 40)

        second_store = self.store()
        second = accepted(second_store, self.provider_id, 1)
        save_state(second_store, self.provider_id, [first, second])
        read_receipt = second_store.read_receipt
        reads = []

        def observed(descriptor):
            reads.append(descriptor["id"])
            return read_receipt(descriptor)

        second_store.read_receipt = observed
        second_manifest = publish_bulk_index(second_store, "b" * 40)

        self.assertEqual(len(reads), 1)
        self.assertEqual(second_manifest["row_count"], 2)
        self.assertEqual(len(second_manifest["files"]), 1)
        self.assertNotEqual(second_manifest["files"], first_manifest["files"])
        self.assertTrue((self.root / first_manifest["files"][0]["id"]).is_file())
        verified = verify_bulk_index(self.store())
        self.assertEqual(verified["snapshot_id"], second_manifest["snapshot_id"])

    def test_publication_is_bounded_and_noop_preserves_pointer(self):
        store = self.store()
        items = [accepted(store, self.provider_id, number) for number in range(3)]
        save_state(store, self.provider_id, items)
        with patch("ingestion.bulk_publication.MAX_NEW_DISTRIBUTIONS", 2):
            first = publish_bulk_index(store, "a" * 40)
        self.assertEqual(first["published_distribution_count"], 2)
        self.assertEqual(first["pending_publication_count"], 1)
        second = publish_bulk_index(self.store(), "b" * 40)
        pointer = (
            self.root
            / "06_control/source_campaigns/eurostat_bulk/current-landing.json"
        )
        before = pointer.read_bytes()
        same = publish_bulk_index(self.store(), "c" * 40)
        self.assertEqual(same, second)
        self.assertEqual(pointer.read_bytes(), before)

    def test_many_single_record_publishes_compact_only_the_small_tail(self):
        items = []
        manifest = None
        # A small test target exercises the same lifetime behavior as the
        # production 256-row tail without thousands of DuckDB process starts.
        with patch("ingestion.bulk_publication.MAX_TAIL_DISTRIBUTIONS", 4):
            for number in range(33):
                store = self.store()  # every append starts from a fresh restore
                items.append(accepted(store, self.provider_id, number))
                save_state(store, self.provider_id, items)
                manifest = publish_bulk_index(store, "a" * 40)

            self.assertIsNotNone(manifest)
            self.assertEqual(manifest["row_count"], 33)
            self.assertEqual(manifest["published_distribution_count"], 33)
            self.assertEqual(manifest["pending_publication_count"], 0)
            self.assertEqual(len(manifest["files"]), 9)

            pointer_path = (
                self.root
                / "06_control/source_campaigns/eurostat_bulk/current-landing.json"
            )
            pointer = json.loads(pointer_path.read_text())
            self.assertLess(pointer["manifest_size_bytes"], 32_000)

            dataset_ids = []
            connection = duckdb.connect(":memory:")
            try:
                for descriptor in manifest["files"]:
                    dataset_ids.extend(
                        row[0]
                        for row in connection.execute(
                            "SELECT dataset_id FROM read_parquet(?)",
                            [str(self.root / descriptor["id"])],
                        ).fetchall()
                    )
            finally:
                connection.close()
            self.assertEqual(
                dataset_ids,
                ["__inventory__"] + [f"dataset_{number}" for number in range(1, 33)],
            )

            old_ids = {item["id"] for item in manifest["files"]}
            old_tail = manifest["files"][-1]
            store = self.store()
            items.append(accepted(store, self.provider_id, 33))
            save_state(store, self.provider_id, items)
            original_read = store.read_landing_object
            prior_fragment_reads = []

            def observed_read(descriptor, *args, **kwargs):
                if descriptor.get("id") in old_ids:
                    prior_fragment_reads.append(descriptor["id"])
                return original_read(descriptor, *args, **kwargs)

            store.read_landing_object = observed_read
            next_manifest = publish_bulk_index(store, "b" * 40)

        self.assertEqual(set(prior_fragment_reads), {old_tail["id"]})
        self.assertEqual(next_manifest["row_count"], 34)
        self.assertEqual(len(next_manifest["files"]), 9)
        self.assertNotIn(old_tail, next_manifest["files"])
        self.assertTrue((self.root / old_tail["id"]).is_file())
        self.assertEqual(verify_bulk_index(self.store())["row_count"], 34)

    def test_rejects_state_drift_and_authentication_in_request(self):
        drift_root = self.root / "drift"
        drift_store = LocalCampaignStore(drift_root, "eurostat_bulk")
        item = accepted(drift_store, self.provider_id, 1)
        save_state(drift_store, self.provider_id, [item])
        state = drift_store.load()
        state["completed"][item[0]["task_id"]]["dataset_id"] = "changed"
        drift_store.save(state)
        with self.assertRaisesRegex(BulkPublicationError, "completed campaign state"):
            publish_bulk_index(LocalCampaignStore(drift_root, "eurostat_bulk"), "d" * 40)

        auth_root = self.root / "auth"
        auth_store = LocalCampaignStore(auth_root, "eurostat_bulk")
        item = accepted(
            auth_store, self.provider_id, 1, params={"api_key": "must-not-publish"}
        )
        save_state(auth_store, self.provider_id, [item])
        with self.assertRaisesRegex(BulkPublicationError, "authentication field"):
            publish_bulk_index(LocalCampaignStore(auth_root, "eurostat_bulk"), "e" * 40)

    def test_requires_bulk_store_and_strict_git_sha(self):
        ordinary = LocalCampaignStore(self.root / "ordinary", "eurostat")
        with self.assertRaisesRegex(BulkPublicationError, "end with _bulk"):
            publish_bulk_index(ordinary, "a" * 40)
        with self.assertRaisesRegex(BulkPublicationError, "40-character"):
            publish_bulk_index(self.store(), "not-a-git-sha")


if __name__ == "__main__":
    unittest.main()
