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
            self.assertEqual(extractor.dbw_landing, "folder_gus_dbw")
            self.assertEqual(extractor.bulk_dir, "folder_bulk")
            self.assertEqual(extractor.checkpoints_dir, "folder_checkpoints")

    def test_only_v3_identity_bound_bulk_receipts_resume_an_indicator(self):
        extractor = object.__new__(DbwWebExtractor)
        extractor.checkpoints_dir = "checkpoints"
        extractor.native_snapshot_id = self.SNAPSHOT_ID
        extractor.storage = MagicMock()
        extractor.storage.drive_service.files.return_value.list.return_value.execute.return_value = {
            "files": [
                {"name": "12.json", "appProperties": {}},
                {"name": f"partial-v3-{self.SNAPSHOT_ID}-13.json", "appProperties": {
                    "checkpoint_schema": "3", "checkpoint_status": "metadata_only",
                    "bulk_complete": "false", "metadata_complete": "true",
                    "native_snapshot_id": self.SNAPSHOT_ID,
                }},
                {"name": f"completed-v3-{self.SNAPSHOT_ID}-14.json", "appProperties": {
                    "checkpoint_schema": "3", "checkpoint_status": "completed",
                    "bulk_complete": "true", "metadata_complete": "true",
                    "catalogue_sha256": "a" * 64,
                    "native_snapshot_id": self.SNAPSHOT_ID,
                    "native_membership_sha256": "c" * 64,
                }},
                {"name": f"completed-v3-{self.SNAPSHOT_ID}-15.json", "appProperties": {
                    "checkpoint_schema": "3", "checkpoint_status": "completed",
                    "bulk_complete": "true", "metadata_complete": "true",
                    "catalogue_sha256": "b" * 64,
                    "native_snapshot_id": self.SNAPSHOT_ID,
                    "native_membership_sha256": "d" * 64,
                }},
            ]
        }
        self.assertEqual(
            extractor.load_completed_checkpoints("a" * 64, self.SNAPSHOT_ID),
            {14: "c" * 64},
        )

    @patch("dbw_web_extractor._upload_bytes")
    @patch("dbw_web_extractor.uuid.uuid4")
    def test_native_snapshot_resumes_until_completed_then_starts_refresh(
        self, mock_uuid, mock_upload
    ):
        extractor = object.__new__(DbwWebExtractor)
        extractor.control_landing = "control"
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
