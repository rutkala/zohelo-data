"""End-to-end local durability for one official full-distribution archive."""
from __future__ import annotations

from datetime import date
from hashlib import md5, sha256
import io
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.bulk_publication import publish_bulk_index, verify_bulk_index  # noqa: E402
from ingestion.full_source_campaign import run_full_campaign  # noqa: E402
from ingestion.source_campaign import new_state  # noqa: E402
from ingestion.source_campaign_store import LocalCampaignStore  # noqa: E402


TODAY = date(2026, 9, 10)
NOW = 1788998400.0
CODE_SHA = "a" * 40


def wdi_archive() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "WDIData.csv",
            "Country Name,Country Code,Indicator Name,Indicator Code,2024\n"
            "Poland,POL,Population,SP.POP,1\n",
        )
        archive.writestr("WDICountry.csv", "Country Code,Region\nPOL,Europe\n")
        archive.writestr("WDISeries.csv", "Series Code,Topic\nSP.POP,People\n")
    return output.getvalue()


class MockBulkRawStore:
    """The upload boundary is mocked; bytes and provenance remain exact."""

    def __init__(self):
        self.objects = {}

    def put_file(self, path, metadata):
        body = Path(path).read_bytes()
        digest = sha256(body).hexdigest()
        self.objects[digest] = {"body": body, "metadata": dict(metadata)}
        return {
            "id": "mockRaw" + digest[:16],
            "name": f"raw-{digest}.bin",
            "sha256": digest,
            "md5": md5(body, usedforsecurity=False).hexdigest(),
            "size_bytes": len(body),
        }


class FullDistributionIntegrationTests(unittest.TestCase):
    def test_wdi_archive_survives_fresh_state_reload_and_bulk_index_verification(self):
        archive = wdi_archive()
        raw_store = MockBulkRawStore()
        publisher_checkpoints = []

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            campaign = LocalCampaignStore(root, "world_bank_wdi_bulk")
            quota = LocalCampaignStore(root, "world_bank_wdi")
            quota.save(new_state("world_bank_wdi", TODAY))

            def fetcher(request, path, hosts, **kwargs):
                self.assertEqual(
                    request["url"],
                    "https://databank.worldbank.org/data/download/WDI_CSV.zip",
                )
                self.assertEqual(
                    len(LocalCampaignStore(root, "world_bank_wdi").load()["quota_attempts"]),
                    1,
                )
                Path(path).write_bytes(archive)
                return {"status_code": 200, "size_bytes": len(archive)}

            def publish_after_archive(store, code_sha):
                checkpoint = LocalCampaignStore(root, "world_bank_wdi_bulk").load()
                publisher_checkpoints.append({
                    "receipts": len(checkpoint["receipts"]),
                    "accepted": checkpoint["accepted_responses"],
                })
                return publish_bulk_index(store, code_sha)

            result = run_full_campaign(
                campaign,
                raw_store,
                quota,
                "world_bank_wdi",
                {
                    "enabled": True,
                    "quota_windows": [{"seconds": 3600, "requests": 10}],
                    "min_request_interval_seconds": 0,
                },
                root / "work",
                clock=lambda: NOW,
                max_requests=2,
                code_sha=CODE_SHA,
                fetcher=fetcher,
                publish=publish_after_archive,
            )

            fresh_campaign = LocalCampaignStore(root, "world_bank_wdi_bulk")
            state = fresh_campaign.load()
            verified = verify_bulk_index(
                LocalCampaignStore(root, "world_bank_wdi_bulk")
            )
            fresh_quota = LocalCampaignStore(root, "world_bank_wdi").load()

        self.assertEqual(result["requests"], 1)
        self.assertEqual(result["coverage_status"], "complete_current_catalogue")
        self.assertEqual(publisher_checkpoints, [{"receipts": 1, "accepted": 1}])
        self.assertEqual(verified["published_distribution_count"], 1)
        self.assertEqual(verified["pending_publication_count"], 0)
        self.assertEqual(len(state["receipts"]), 1)
        completed = next(iter(state["completed"].values()))
        raw = completed["raw"]
        self.assertEqual(raw_store.objects[raw["sha256"]]["body"], archive)
        self.assertEqual(
            raw_store.objects[raw["sha256"]]["metadata"],
            {"source_id": "world_bank_wdi", "dataset_id": "WDI"},
        )
        self.assertEqual(fresh_quota["quota_attempts"], [NOW])


if __name__ == "__main__":
    unittest.main()
