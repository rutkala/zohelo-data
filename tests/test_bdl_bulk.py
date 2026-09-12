"""Safety and recovery tests for the authenticated GUS BDL Web bulk path."""

from __future__ import annotations

from hashlib import sha256
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


class BdlBulkPlanTests(unittest.TestCase):
    def test_catalogue_candidates_use_durable_hierarchy_and_numeric_order(self):
        subjects = [
            {"subject_id": "K1", "parent_subject_id": None, "subject_name": "K", "has_variables": False},
            {"subject_id": "G3", "parent_subject_id": "K1", "subject_name": "G", "has_variables": False},
            {"subject_id": "P10", "parent_subject_id": "G3", "subject_name": "Ten", "has_variables": True},
            {"subject_id": "P2", "parent_subject_id": "G3", "subject_name": "Two", "has_variables": True},
        ]

        candidates, invalid = bdl_bulk_plan._catalogue_candidates(subjects)

        self.assertEqual([], invalid)
        self.assertEqual(["P2", "P10"], [item["subgroup_id"] for item in candidates])
        self.assertEqual(["K1", "G3", "P2"], candidates[0]["path"])
        self.assertEqual(
            "https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/1/3/2",
            candidates[0]["url"],
        )

    def test_invalid_or_cyclic_subgroups_are_explicit_completion_blockers(self):
        subjects = [
            {"subject_id": "K1", "parent_subject_id": None},
            {"subject_id": "G3", "parent_subject_id": "P2"},
            {"subject_id": "P2", "parent_subject_id": "G3"},
            {"subject_id": "P10", "parent_subject_id": "G999"},
        ]

        candidates, invalid = bdl_bulk_plan._catalogue_candidates(subjects)

        self.assertEqual([], candidates)
        self.assertEqual(["P2", "P10"], invalid)

    def test_subject_catalogue_validates_selected_release_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "subjects.parquet"
            import duckdb

            with duckdb.connect() as connection:
                escaped = str(path).replace("'", "''")
                connection.execute(
                    """
                    create table subjects(
                        subject_key varchar,
                        parent_subject_id varchar,
                        subject_name varchar,
                        has_variables boolean
                    )
                    """
                )
                connection.execute("insert into subjects values ('K1', null, 'Category', false)")
                connection.execute(f"copy subjects to '{escaped}' (format parquet)")
            raw = path.read_bytes()
            store = Mock()
            store.read.return_value = raw
            manifest = {
                "release_id": "release-1",
                "release_scope": "bdl_platform",
                "datasets": [{
                    "dataset_id": "dim_bdl_subject",
                    "files": [{"id": "subjects-file", "size": len(raw), "sha256": sha256(raw).hexdigest()}],
                }],
                "artifacts": [{
                    "id": "state-file",
                    "name": "ingestion-state.json",
                    "size": len(b'{"source_id":"gus_bdl"}'),
                    "sha256": sha256(b'{"source_id":"gus_bdl"}').hexdigest(),
                }],
            }
            store.read.side_effect = [raw, b'{"source_id":"gus_bdl"}']
            with (
                patch.object(bdl_bulk_plan, "DriveReleaseStore", return_value=store),
                patch.object(bdl_bulk_plan, "read_current_release_manifest", return_value=manifest),
            ):
                storage = Mock()
                storage.get_or_create_nested_folder.return_value = "release-root"
                release_id, subjects, release_state = bdl_bulk_plan._subject_catalogue(storage, "root")

        self.assertEqual("release-1", release_id)
        self.assertEqual("K1", subjects[0]["subject_id"])
        self.assertEqual("gus_bdl", release_state["source_id"])
        self.assertEqual(["subjects-file", "state-file"], [call.args[0] for call in store.read.call_args_list])

    def test_complete_requires_both_subject_catalogue_roots_and_no_pending_pages(self):
        base = {
            "pending": [{"id": "child", "lane": "discovery", "kind": "subjects", "cursor": {}}],
            "completed": {
                "discovery:subjects:pl:root:p000000": "now",
                "discovery:subjects:en:root:p000000": "now",
            },
        }

        pending = bdl_bulk_plan._subject_discovery_status(base)
        exhausted = bdl_bulk_plan._subject_discovery_status({**base, "pending": []})

        self.assertFalse(pending["exhausted"])
        self.assertEqual(1, pending["pending_tasks"])
        self.assertTrue(exhausted["exhausted"])

    def test_complete_requires_release_landing_to_match_current_campaign(self):
        release = {
            "accepted_response_count": 10,
            "published_response_count": 10,
            "pending_publication_count": 0,
            "raw_response_count": 10,
        }
        current = bdl_bulk_plan._release_landing_status(release, {"accepted_responses": 10})
        ahead = bdl_bulk_plan._release_landing_status(release, {"accepted_responses": 11})
        backlog = bdl_bulk_plan._release_landing_status(
            {**release, "published_response_count": 9, "pending_publication_count": 1},
            {"accepted_responses": 10},
        )

        self.assertTrue(current["current"])
        self.assertFalse(ahead["current"])
        self.assertFalse(backlog["current"])

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
        plan_step = next(
            step for step in job["steps"] if step.get("name") == "Plan next unprocessed BDL subgroup"
        )
        self.assertNotIn("GUS_BDL_API_KEY", plan_step.get("env", {}))

    def test_worker_hashes_download_as_stream(self):
        worker = (ROOT / "portal" / "scripts" / "bdl-web-bulk-worker.mjs").read_text(
            encoding="utf-8"
        )
        self.assertIn("createReadStream(target)", worker)
        self.assertNotIn("fs.readFile(target)", worker)
        self.assertIn("findNewReadyExport(page, baselineFingerprints)", worker)
        self.assertNotIn("downloaded_existing_export", worker)
        self.assertIn("filename does not identify the selected subgroup", worker)
        self.assertIn("filename predates the current generation request", worker)
        self.assertIn("emitted > providerStartedAt", worker)
        self.assertNotIn("emitted >= providerStartedAt", worker)
        self.assertNotIn("start - 300000", worker)
        self.assertNotIn("const starts = [", worker)
        self.assertIn("settlePreexistingExports(page)", worker)
        self.assertIn("entries.some((entry) => !isReadyExport(entry))", worker)
        self.assertIn("did not settle before the generation request", worker)
        self.assertIn("await requestPromise", worker)
        self.assertIn("generationRequestObservedAt", worker)
        self.assertNotIn("const generationStartedAt = new Date()", worker)
        workflow = (ROOT / ".github" / "workflows" / "source-gus-bdl.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("downloaded_existing_export", workflow)


if __name__ == "__main__":
    unittest.main()
