"""Unit tests for the GUS DBW Web bulk extractor."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from dbw_web_extractor import DbwWebExtractor, _hash_bytes, _hash_file, _require_production_context


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


if __name__ == "__main__":
    unittest.main()
