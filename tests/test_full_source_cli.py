"""CLI reporting tests for fresh full-distribution verification."""
from __future__ import annotations

import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import full_source_campaign as cli  # noqa: E402


class FullSourceCliTests(unittest.TestCase):
    def test_verify_current_requires_current_index_and_labels_raw_sample(self):
        storage = MagicMock()
        storage.resolve_root.return_value = "root"
        storage.get_or_create_nested_folder.side_effect = ["control", "raw-root"]
        storage.resolve_zone.return_value = "landing"
        state = {
            "provider_id": "world_bank_wdi",
            "receipts": [{"id": "receipt"}],
            "accepted_responses": 1,
            "completed": {},
            "pending": [],
            "raw_bytes": 123,
        }
        store = MagicMock()
        store.load.return_value = state
        store.read_receipt.return_value = {"raw": {"id": "raw"}}
        raw_store = MagicMock()
        index = {"snapshot_id": "snapshot-current"}

        with (
            patch.object(
                sys,
                "argv",
                [
                    "full_source_campaign.py",
                    "--source",
                    "world_bank_wdi",
                    "--verify-current",
                ],
            ),
            patch.object(cli, "load_settings", return_value={"enabled": True}),
            patch.object(cli, "production_storage", return_value=storage),
            patch("ingestion.source_campaign_store._CampaignStore", return_value=store),
            patch("ingestion.drive_state_store.DriveStateStore"),
            patch("ingestion.bulk_transport.BulkDriveRawStore", return_value=raw_store),
            patch("ingestion.bulk_publication.verify_bulk_index", return_value=index) as verify,
            patch("sys.stdout", new_callable=io.StringIO) as output,
        ):
            self.assertEqual(cli.main(), 0)

        verify.assert_called_once_with(store, require_current=True)
        report = json.loads(output.getvalue())
        self.assertEqual(report["raw_verification_scope"], "sampled_latest_accepted_object")
        self.assertIs(report["raw_history_audit"], False)
        self.assertEqual(report["index_snapshot_id"], "snapshot-current")


if __name__ == "__main__":
    unittest.main()
