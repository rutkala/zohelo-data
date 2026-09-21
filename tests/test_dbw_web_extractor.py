"""Unit tests for the GUS DBW Web bulk extractor."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from dbw_web_extractor import (
    DbwWebExtractor,
    _discover_bulk_filenames,
    _hash_bytes,
    _hash_file,
    _membership_sha256,
    _require_production_context,
    _snapshot_sha256,
    _upload_bytes,
)


class TestDbwWebExtractor(unittest.TestCase):
    SNAPSHOT_ID = "123e4567-e89b-42d3-a456-426614174000"

    def test_production_context_guard(self):
        with patch.dict("os.environ", {"GITHUB_ACTIONS": "false", "ZOHELO_ALLOW_CODESPACE_EXECUTION": "false"}, clear=True):
            with self.assertRaises(PermissionError):
                _require_production_context(allow_codespace=False)

        with patch.dict("os.environ", {"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/main"}, clear=True):
            _require_production_context(allow_codespace=False)

        with patch.dict("os.environ", {"ZOHELO_ALLOW_CODESPACE_EXECUTION": "true"}, clear=True):
            _require_production_context(allow_codespace=False)

        with patch.dict("os.environ", {}, clear=True):
            _require_production_context(allow_codespace=True)

    def test_hash_helpers(self):
        data = b"Hello DBW World"
        sha, md5 = _hash_bytes(data)
        self.assertEqual(len(sha), 64)
        self.assertEqual(len(md5), 32)

        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(data)
            tmp.flush()
            f_sha, f_md5 = _hash_file(Path(tmp.name))
            self.assertEqual(f_sha, sha)
            self.assertEqual(f_md5, md5)
        memberships = {7: "a" * 64}
        self.assertNotEqual(
            _snapshot_sha256(self.SNAPSHOT_ID, memberships),
            _snapshot_sha256(
                "223e4567-e89b-42d3-a456-426614174000", memberships
            ),
        )

    def test_extract_indicators_tree_traversal(self):
        sample_tree = [
            {
                "id": "369",
                "type": "AREA",
                "name": "Gospodarka",
                "children": [
                    {
                        "id": "369-161",
                        "type": "GROUP",
                        "name": "Budownictwo",
                        "children": [
                            {
                                "id": "369-161-162-163",
                                "type": "INDICATOR",
                                "indicator_id": 378,
                                "name": "Izby oddane do użytkowania",
                                "name_en": "Rooms completed",
                            },
                            {
                                "id": "369-161-162-165",
                                "type": "INDICATOR",
                                "indicator_id": 380,
                                "name": "Kubatura budynków oddanych",
                                "name_en": "Cubic volume of buildings",
                            },
                        ],
                    }
                ],
            }
        ]

        extracted = DbwWebExtractor.extract_indicators(sample_tree)
        self.assertEqual(len(extracted), 2)
        self.assertEqual(extracted[0]["id"], 378)
        self.assertEqual(extracted[0]["name"], "Izby oddane do użytkowania")
        self.assertEqual(extracted[0]["path"], "Gospodarka > Budownictwo > Izby oddane do użytkowania")
        self.assertEqual(extracted[1]["id"], 380)

    def test_bulk_discovery_requires_recognized_nonempty_unique_zip_inventory(self):
        document = {
            "data": {"table": {"rows": [[{
                "files": [
                    {"filename": "7_history.zip"},
                    {"filename": "7_recent.zip"},
                ]
            }]]}}
        }
        raw = json.dumps(document).encode()
        self.assertEqual(
            _discover_bulk_filenames(raw),
            ["7_history.zip", "7_recent.zip"],
        )
        for invalid in (
            b"{}",
            b'{"success": false, "error": "provider failure"}',
            b'{"data": {"table": {"rows": []}}}',
            b'{"data": {"table": {"rows": [[{"files": [{"filename": "../7.zip"}]}]]}}}',
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(RuntimeError):
                    _discover_bulk_filenames(invalid)

    @patch("dbw_web_extractor._upload_bytes")
    @patch("dbw_web_extractor._http_get")
    def test_invalid_aggregate_envelope_cannot_publish_completed_receipt(
        self, mock_get, mock_upload
    ):
        mock_get.side_effect = [b"{}", b"id_zmienna;nazwa\n7;Test\n"]

        def upload_result(_storage, data, **kwargs):
            sha, md5 = _hash_bytes(data)
            return {
                "id": f"id-{kwargs['name']}", "name": kwargs["name"],
                "size": len(data), "sha256": sha, "md5": md5, "reused": False,
            }

        mock_upload.side_effect = upload_result
        extractor = object.__new__(DbwWebExtractor)
        extractor.storage = MagicMock()
        extractor.proxy = None
        extractor.workspace = Path("unused")
        extractor.metadata_dir = "metadata"
        extractor.bulk_dir = "bulk"
        extractor.checkpoints_dir = "checkpoints"
        extractor.catalogue_sha256 = "a" * 64
        extractor.native_snapshot_id = self.SNAPSHOT_ID
        with self.assertRaisesRegex(RuntimeError, "recognized data.table.rows"):
            extractor.process_indicator({"id": 7, "name": "Test indicator"})
        uploaded_names = [call.kwargs["name"] for call in mock_upload.call_args_list]
        self.assertNotIn(f"completed-v3-{self.SNAPSHOT_ID}-7.json", uploaded_names)

    @patch("dbw_web_extractor.StorageManager")
    def test_extractor_initialization(self, mock_storage_cls):
        mock_storage = MagicMock()
        mock_storage.resolve_zone.side_effect = lambda z, **kw: f"zone_{z}"
        mock_storage.get_or_create_nested_folder.side_effect = lambda segments, **kw: f"folder_{'_'.join(segments)}"
        mock_storage_cls.return_value = mock_storage

        with tempfile.TemporaryDirectory() as tmp_dir:
            extractor = DbwWebExtractor(
                workspace=Path(tmp_dir),
                storage=mock_storage,
                allow_codespace=True,
            )
            mock_storage.get_or_create_nested_folder.assert_not_called()
            extractor.native_snapshot_lease_claim = "claim"
            extractor.prepare_write_paths()
            self.assertEqual(extractor.dbw_landing, "folder_gus_dbw")
            self.assertEqual(extractor.bulk_dir, "folder_bulk")
            self.assertEqual(extractor.checkpoints_dir, "folder_checkpoints")

    def test_only_v3_identity_bound_bulk_receipts_resume_an_indicator(self):
        extractor = object.__new__(DbwWebExtractor)
        extractor.checkpoints_dir = "checkpoints"
        extractor.metadata_dir = "metadata"
        extractor.bulk_dir = "bulk"
        extractor.native_snapshot_id = self.SNAPSHOT_ID
        extractor.storage = MagicMock()
        descriptors = []
        metadata = []
        bulk = []
        for role, object_id, name, source_name, target in (
            ("aggregates", "agg", "agg.json", "aggregates_14_pl.json", metadata),
            ("metryka", "met", "met.csv", "metryka_14.csv", metadata),
            ("bulk_zip", "zip", "history.zip", "history.zip", bulk),
        ):
            descriptor = {
                "id": object_id, "name": name, "size": 10,
                "sha256": "b" * 64, "md5": "c" * 32,
                "role": role, "source_name": source_name,
            }
            descriptors.append(descriptor)
            target.append({
                "id": object_id, "name": name, "size": "10",
                "md5Checksum": "c" * 32,
                "appProperties": {"sha256": "b" * 64},
            })
        membership = _membership_sha256(descriptors)
        receipt = {
            "schema_version": 3,
            "record_type": "gus_dbw_indicator_completion",
            "indicator_id": 14,
            "status": "completed",
            "bulk_complete": True,
            "metadata_complete": True,
            "catalogue_sha256": "a" * 64,
            "native_snapshot_id": self.SNAPSHOT_ID,
            "native_membership_sha256": membership,
            "expected_bulk_files": ["history.zip"],
            "landed_objects": descriptors,
        }
        raw = json.dumps(receipt).encode()
        receipt_sha, receipt_md5 = _hash_bytes(raw)
        checkpoint = {
            "id": "receipt", "name": f"completed-v3-{self.SNAPSHOT_ID}-14.json",
            "size": str(len(raw)), "md5Checksum": receipt_md5,
            "appProperties": {
                "sha256": receipt_sha,
                "checkpoint_schema": "3", "checkpoint_status": "completed",
                "bulk_complete": "true", "metadata_complete": "true",
                "catalogue_sha256": "a" * 64,
                "native_snapshot_id": self.SNAPSHOT_ID,
                "native_membership_sha256": membership,
            },
        }

        def list_response(**kwargs):
            query = kwargs["q"]
            if "'metadata' in parents" in query:
                result = metadata
            elif "'bulk' in parents" in query:
                result = bulk
            else:
                result = [checkpoint]
            return MagicMock(execute=MagicMock(return_value={"files": result}))

        extractor.storage.drive_service.files.return_value.list.side_effect = list_response
        media = extractor.storage.drive_service.files.return_value.get_media
        media.return_value.execute.return_value = raw
        self.assertEqual(
            extractor.load_completed_checkpoints("a" * 64, self.SNAPSHOT_ID),
            {14: membership},
        )

        duplicate = {**checkpoint, "id": "receipt-retry"}

        def duplicate_list_response(**kwargs):
            query = kwargs["q"]
            if "'metadata' in parents" in query:
                result = metadata
            elif "'bulk' in parents" in query:
                result = bulk
            else:
                result = [checkpoint, duplicate]
            return MagicMock(execute=MagicMock(return_value={"files": result}))

        extractor.storage.drive_service.files.return_value.list.side_effect = (
            duplicate_list_response
        )
        extractor.storage.drive_service.files.return_value.get_media.return_value.execute.return_value = raw
        with self.assertRaisesRegex(RuntimeError, "Duplicate DBW completed receipts"):
            extractor.load_completed_checkpoints("a" * 64, self.SNAPSHOT_ID)

    @patch("dbw_web_extractor._upload_bytes")
    @patch("dbw_web_extractor.uuid.uuid4")
    def test_native_snapshot_resumes_until_completed_then_starts_refresh(
        self, mock_uuid, mock_upload
    ):
        extractor = object.__new__(DbwWebExtractor)
        extractor.control_landing = "control"
        extractor.native_snapshot_lease_claim = "claim"
        extractor.storage = MagicMock()
        start = {
            "name": f"native-snapshot-v1-{'a' * 64}-{self.SNAPSHOT_ID}.json",
            "appProperties": {
                "record_type": "gus_dbw_native_snapshot_start",
                "catalogue_sha256": "a" * 64,
                "native_snapshot_id": self.SNAPSHOT_ID,
            },
        }
        listing = extractor.storage.drive_service.files.return_value.list.return_value.execute
        listing.return_value = {"files": [start]}
        self.assertEqual(
            extractor.start_or_resume_native_snapshot("a" * 64), self.SNAPSHOT_ID
        )
        mock_upload.assert_not_called()

        listing.return_value = {"files": [
            start,
            {"name": "landing-complete-v2-x.json", "appProperties": {
                "completion_schema": "2",
                "native_snapshot_id": self.SNAPSHOT_ID,
            }},
        ]}
        next_snapshot = "223e4567-e89b-42d3-a456-426614174000"
        mock_uuid.return_value = next_snapshot
        self.assertEqual(
            extractor.start_or_resume_native_snapshot("a" * 64), next_snapshot
        )
        self.assertEqual(
            mock_upload.call_args.kwargs["extra_properties"]["native_snapshot_id"],
            next_snapshot,
        )

    @patch("dbw_web_extractor.time.sleep")
    @patch("dbw_web_extractor._upload_bytes")
    @patch("dbw_web_extractor.uuid.uuid4")
    def test_snapshot_lease_elects_one_cross_runner_writer(
        self, mock_uuid, mock_upload, mock_sleep
    ):
        extractor = object.__new__(DbwWebExtractor)
        extractor.control_root = "control"
        extractor.storage = MagicMock()
        extractor.native_snapshot_lease_claim = None
        extractor.native_snapshot_lease_owner = None
        extractor.native_snapshot_lease_claims = set()
        own = "123e4567-e89b-42d3-a456-426614174000"
        other = "023e4567-e89b-42d3-a456-426614174000"
        mock_uuid.return_value = own
        future = "2999-01-01T00:00:00+00:00"
        extractor.storage.drive_service.files.return_value.list.return_value.execute.return_value = {
            "files": [
                {"appProperties": {
                    "record_type": "gus_dbw_native_snapshot_lease",
                    "catalogue_sha256": "a" * 64,
                    "claim_id": own,
                    "expires_at_utc": future,
                }, "createdTime": "2026-09-20T20:00:01Z"},
                {"appProperties": {
                    "record_type": "gus_dbw_native_snapshot_lease",
                    "catalogue_sha256": "b" * 64,
                    "claim_id": other,
                    "expires_at_utc": future,
                }, "createdTime": "2026-09-20T20:00:00Z"},
            ]
        }
        with self.assertRaisesRegex(RuntimeError, "durable native-snapshot lease"):
            extractor.acquire_native_snapshot_lease("a" * 64)
        mock_sleep.assert_called_once()
        self.assertEqual(mock_upload.call_count, 2)
        self.assertEqual(
            mock_upload.call_args_list[-1].kwargs["extra_properties"]["released_claim_id"],
            own,
        )

    @patch("dbw_web_extractor.time.sleep")
    @patch("dbw_web_extractor._upload_bytes")
    def test_snapshot_lease_tombstones_claim_when_election_listing_fails(
        self, mock_upload, _mock_sleep
    ):
        extractor = object.__new__(DbwWebExtractor)
        extractor.control_root = "control"
        extractor.storage = MagicMock()
        extractor.native_snapshot_lease_claim = None
        extractor.native_snapshot_lease_owner = None
        extractor.native_snapshot_lease_claims = set()
        extractor.storage.drive_service.files.return_value.list.return_value.execute.side_effect = RuntimeError("transient")
        with self.assertRaisesRegex(RuntimeError, "transient"):
            extractor.acquire_native_snapshot_lease("a" * 64)
        self.assertEqual(mock_upload.call_count, 2)
        self.assertIsNone(extractor.native_snapshot_lease_claim)

    def test_snapshot_lease_renewal_retains_claims_until_cleanup(self):
        extractor = object.__new__(DbwWebExtractor)
        extractor.catalogue_sha256 = "a" * 64
        extractor.native_snapshot_lease_owner = "owner"
        extractor.native_snapshot_lease_claim = "old"
        extractor.native_snapshot_lease_claims = {"old"}
        with patch.object(
            extractor, "_create_native_snapshot_lease_claim", return_value="new"
        ), patch.object(extractor, "_verify_native_snapshot_lease"), patch.object(
            extractor,
            "_release_native_snapshot_claim",
            side_effect=[RuntimeError("transient"), None, None],
        ) as release:
            with self.assertRaisesRegex(RuntimeError, "transient"):
                extractor.renew_native_snapshot_lease()
            self.assertEqual(extractor.native_snapshot_lease_claims, {"old", "new"})
            extractor.release_native_snapshot_lease()
        self.assertEqual(release.call_count, 3)
        self.assertEqual(extractor.native_snapshot_lease_claims, set())
        self.assertIsNone(extractor.native_snapshot_lease_claim)

    def test_bulk_zip_cache_is_namespaced_by_native_snapshot(self):
        source = (Path(__file__).resolve().parents[1] / "src/dbw_web_extractor.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            'self.workspace / "snapshots" / self.native_snapshot_id', source
        )
        self.assertNotIn('self.workspace / f"worker_{ind_id}_{filename}"', source)

    @patch("dbw_web_extractor.time.monotonic", return_value=10.0)
    def test_catalogue_verification_deadline_stops_before_drive_reads(self, _clock):
        extractor = object.__new__(DbwWebExtractor)
        extractor.storage = MagicMock()
        extractor.metadata_dir = "metadata"
        extractor.bulk_dir = "bulk"
        extractor.checkpoints_dir = "checkpoints"
        with self.assertRaisesRegex(RuntimeError, "bounded finalization deadline"):
            extractor.load_completed_checkpoints(
                "a" * 64, self.SNAPSHOT_ID, deadline_monotonic=10.0
            )
        extractor.storage.drive_service.files.return_value.list.assert_not_called()

    def test_landing_finalization_renews_and_fits_actions_timeout(self):
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "src/dbw_web_extractor.py").read_text(encoding="utf-8")
        renewal = source.index("extractor.renew_native_snapshot_lease()")
        final_scan = source.index("verified_memberships = extractor.load_completed_checkpoints", renewal)
        post_scan_deadline = source.index(
            "if time.monotonic() >= finalization_deadline:", final_scan
        )
        publication = source.index("extractor.publish_catalogue_completion(", post_scan_deadline)
        self.assertLess(renewal, final_scan)
        self.assertLess(final_scan, post_scan_deadline)
        self.assertLess(post_scan_deadline, publication)
        workflow = (repo / ".github/workflows/dbw-web-bootstrap.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("timeout-minutes: 360", workflow)
        self.assertIn("--max-seconds 12600", workflow)

    @patch("dbw_web_extractor._find_exact_file")
    def test_changed_native_response_uses_content_addressed_revision(self, mock_find):
        data = b"new provider bytes"
        sha, md5 = _hash_bytes(data)
        mock_find.side_effect = [
            [{"id": "old", "name": "indicators_tree.json", "size": "3",
              "md5Checksum": "old", "appProperties": {"sha256": "old"}}],
            [],
        ]
        storage = MagicMock()
        storage.drive_service.files.return_value.create.return_value.execute.return_value = {
            "id": "new", "name": f"indicators_tree--sha256-{sha}.json",
            "size": str(len(data)), "md5Checksum": md5,
            "appProperties": {"sha256": sha},
        }
        result = _upload_bytes(
            storage, data, name="indicators_tree.json", parent_id="taxonomy",
            kind="taxonomy", mime_type="application/json",
        )
        self.assertEqual(result["name"], f"indicators_tree--sha256-{sha}.json")
        storage.drive_service.files.return_value.create.assert_called_once()

    @patch("dbw_web_extractor._upload_bytes")
    @patch("dbw_web_extractor._http_get")
    def test_metadata_only_run_cannot_create_full_completion_receipt(self, mock_get, mock_upload):
        mock_get.side_effect = [b'{}', b'id_zmienna;nazwa\n7;Test\n']
        def upload_result(_storage, data, **kwargs):
            sha, md5 = _hash_bytes(data)
            return {
                "id": f"id-{kwargs['name']}", "name": kwargs["name"],
                "size": len(data), "sha256": sha, "md5": md5, "reused": False,
            }

        mock_upload.side_effect = upload_result
        extractor = object.__new__(DbwWebExtractor)
        extractor.storage = MagicMock()
        extractor.proxy = None
        extractor.metadata_dir = "metadata"
        extractor.checkpoints_dir = "checkpoints"
        extractor.catalogue_sha256 = "a" * 64
        extractor.native_snapshot_id = self.SNAPSHOT_ID
        result = extractor.process_indicator(
            {"id": 7, "name": "Test indicator"}, skip_bulk_zips=True
        )
        self.assertEqual(result["status"], "metadata_only")
        self.assertEqual(
            mock_upload.call_args.kwargs["name"],
            f"partial-v3-{self.SNAPSHOT_ID}-7.json",
        )
        self.assertEqual(mock_upload.call_args.kwargs["extra_properties"]["bulk_complete"], "false")
        self.assertEqual(mock_upload.call_args.kwargs["extra_properties"]["catalogue_sha256"], "a" * 64)

    def test_catalogue_completion_rejects_partial_counts(self):
        extractor = object.__new__(DbwWebExtractor)
        with self.assertRaisesRegex(RuntimeError, "catalogue exhaustion"):
            extractor.publish_catalogue_completion(
                catalogue_indicators=1550,
                completed_indicators=1549,
                catalogue_sha256="a" * 64,
                native_snapshot_id=self.SNAPSHOT_ID,
                native_snapshot_sha256=_snapshot_sha256(
                    self.SNAPSHOT_ID, {7: "b" * 64}
                ),
            )


if __name__ == "__main__":
    unittest.main()
