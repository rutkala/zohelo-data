"""Safety and recovery tests for the authenticated GUS BDL Web bulk path."""

from __future__ import annotations

from copy import deepcopy
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import bdl_bulk_ingest
import bdl_bulk_plan


ROOT = Path(__file__).resolve().parents[1]


class _StateStore:
    def __init__(self, state):
        self.state = deepcopy(state)
        self.saved = []

    def load(self):
        return deepcopy(self.state)

    def save(self, state):
        self.state = deepcopy(state)
        self.saved.append(deepcopy(state))


class _Response:
    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


class _Session:
    def __init__(self, response=None):
        self.response = response or _Response()
        self.calls = []

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.response


def _settings(**overrides):
    settings = {
        "max_requests": 3,
        "max_inline_wait_seconds": 10,
        "min_request_interval_seconds": 1,
        "quota_history_seconds": 900,
        "quota_windows": [{"seconds": 900, "requests": 400}],
    }
    settings.update(overrides)
    return settings


class BdlBulkPlanTests(unittest.TestCase):
    def test_catalogue_request_is_reserved_before_transport(self):
        store = _StateStore({"quota_attempts": [], "provider_retry_at": 0})
        transport = _Session()
        session = bdl_bulk_plan._QuotaAwareSession(transport, store, _settings())

        with patch.object(bdl_bulk_plan.time, "time", return_value=1_000):
            response = session.get("https://bdl.stat.gov.pl/api/v1/subjects")

        self.assertEqual(200, response.status_code)
        self.assertEqual([1_000], store.saved[0]["quota_attempts"])
        self.assertEqual(1, len(transport.calls))

    def test_provider_retry_after_is_persisted(self):
        store = _StateStore({"quota_attempts": [], "provider_retry_at": 0})
        transport = _Session(_Response(429, {"Retry-After": "120"}))
        session = bdl_bulk_plan._QuotaAwareSession(transport, store, _settings())

        with patch.object(bdl_bulk_plan.time, "time", return_value=1_000):
            session.get("https://bdl.stat.gov.pl/api/v1/subjects")

        self.assertEqual(1_120, store.state["provider_retry_at"])
        self.assertGreaterEqual(len(store.saved), 2)

    def test_partial_snapshot_folder_is_not_a_completion_signal(self):
        storage = Mock()
        storage.FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
        incomplete = {"id": "folder", "name": "P10", "mimeType": storage.FOLDER_MIME_TYPE}
        complete = {
            "id": "marker",
            "name": "P11.json",
            "appProperties": {
                "source_id": "gus_bdl",
                "transport": "web_bulk",
                "subgroup_id": "P11",
                "status": "landed",
            },
        }
        nonbulk = {
            "id": "marker-2",
            "name": "P12.json",
            "appProperties": {
                "source_id": "gus_bdl",
                "transport": "web_bulk",
                "subgroup_id": "P12",
                "status": "below_bulk_threshold",
            },
        }
        with patch.object(
            bdl_bulk_plan,
            "_list_named",
            return_value=[incomplete, complete, nonbulk],
        ):
            landed, processed = bdl_bulk_plan._durable_status(storage, "bulk", "control")

        self.assertEqual({"P11"}, landed)
        self.assertEqual({"P11", "P12"}, processed)
        self.assertNotIn("P10", processed)


class BdlBulkIngestTests(unittest.TestCase):
    def test_production_write_requires_main_ref(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "source.zip"
            archive.write_bytes(b"PK\x03\x04")
            with patch.dict(
                os.environ,
                {"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/feature"},
                clear=False,
            ):
                with self.assertRaisesRegex(PermissionError, "main-branch"):
                    bdl_bulk_ingest.ingest_archive(archive, "P10", True)

    def test_completion_marker_carries_manifest_identity(self):
        storage = Mock()
        storage.drive_service.files.return_value.list.return_value.execute.return_value = {
            "files": []
        }
        created = {
            "id": "marker-id",
            "name": "P10.json",
            "size": 400,
            "appProperties": {
                "source_id": "gus_bdl",
                "transport": "web_bulk",
                "subgroup_id": "P10",
                "status": "landed",
                "manifest_sha256": "a" * 64,
            },
        }
        request = Mock()
        request.execute.return_value = created
        storage.drive_service.files.return_value.create.return_value = request
        with patch.object(
            bdl_bulk_ingest,
            "MediaInMemoryUpload",
            side_effect=lambda raw, **_: SimpleNamespace(raw=raw),
        ):
            # Size is generated dynamically; mirror it from the media payload by
            # allowing the verifier to read the response size through a tiny shim.
            request.execute.side_effect = lambda **_: {
                **created,
                "size": len(
                    storage.drive_service.files.return_value.create.call_args.kwargs[
                        "media_body"
                    ].raw
                ),
            }
            marker = bdl_bulk_ingest._upload_completion_marker(
                storage,
                control_id="control",
                subgroup_id="P10",
                archive_sha="b" * 64,
                row_count=12,
                manifest_object={"id": "manifest-id", "sha256": "a" * 64},
            )

        self.assertEqual("marker-id", marker["id"])
        body = storage.drive_service.files.return_value.create.call_args.kwargs["body"]
        self.assertEqual("landed", body["appProperties"]["status"])
        self.assertEqual("a" * 64, body["appProperties"]["manifest_sha256"])


class BdlBulkWorkflowTests(unittest.TestCase):
    def test_workflow_shares_provider_lock_and_excludes_archives_from_evidence(self):
        path = ROOT / ".github" / "workflows" / "source-gus-bdl.yml"
        workflow = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        self.assertEqual("zohelo-pipeline-gus_bdl", workflow["concurrency"]["group"])
        job = workflow["jobs"]["web_bulk_backfill"]
        self.assertIn("refs/heads/main", job["if"])
        self.assertEqual(["platform_transform_and_release"], job["needs"])
        upload = next(
            step for step in job["steps"] if step.get("name") == "Upload sanitized backfill evidence"
        )
        evidence_paths = upload["with"]["path"]
        self.assertNotIn("download-", evidence_paths)
        self.assertNotIn("bdl-web-bulk/\n", evidence_paths)

    def test_worker_hashes_download_as_stream(self):
        worker = (ROOT / "portal" / "scripts" / "bdl-web-bulk-worker.mjs").read_text(
            encoding="utf-8"
        )
        self.assertIn("createReadStream(target)", worker)
        self.assertNotIn("fs.readFile(target)", worker)


if __name__ == "__main__":
    unittest.main()
