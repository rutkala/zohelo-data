"""Unit tests for the GUS DBW Web bulk extractor."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from dbw_web_extractor import (
    DbwWebExtractor,
    _hash_bytes,
    _hash_file,
    _require_production_context,
    _upload_bytes,
)


class TestDbwWebExtractor(unittest.TestCase):
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
        extractor.storage = MagicMock()
        extractor.storage.drive_service.files.return_value.list.return_value.execute.return_value = {
            "files": [
                {"name": "12.json", "appProperties": {}},
                {"name": "partial-v3-13.json", "appProperties": {
                    "checkpoint_schema": "3", "checkpoint_status": "metadata_only",
                    "bulk_complete": "false", "metadata_complete": "true",
                }},
                {"name": "completed-v3-14.json", "appProperties": {
                    "checkpoint_schema": "3", "checkpoint_status": "completed",
                    "bulk_complete": "true", "metadata_complete": "true",
                    "catalogue_sha256": "a" * 64,
                }},
                {"name": "completed-v3-15.json", "appProperties": {
                    "checkpoint_schema": "3", "checkpoint_status": "completed",
                    "bulk_complete": "true", "metadata_complete": "true",
                    "catalogue_sha256": "b" * 64,
                }},
            ]
        }
        self.assertEqual(extractor.load_completed_checkpoints("a" * 64), {14})

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
        result = extractor.process_indicator(
            {"id": 7, "name": "Test indicator"}, skip_bulk_zips=True
        )
        self.assertEqual(result["status"], "metadata_only")
        self.assertEqual(mock_upload.call_args.kwargs["name"], "partial-v3-7.json")
        self.assertEqual(mock_upload.call_args.kwargs["extra_properties"]["bulk_complete"], "false")
        self.assertEqual(mock_upload.call_args.kwargs["extra_properties"]["catalogue_sha256"], "a" * 64)

    def test_catalogue_completion_rejects_partial_counts(self):
        extractor = object.__new__(DbwWebExtractor)
        with self.assertRaisesRegex(RuntimeError, "catalogue exhaustion"):
            extractor.publish_catalogue_completion(
                catalogue_indicators=1550,
                completed_indicators=1549,
                catalogue_sha256="a" * 64,
            )


if __name__ == "__main__":
    unittest.main()
