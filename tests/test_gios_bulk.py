"""Unit tests for GIOŚ bulk extractor (PL-ENV-010)."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from ingestion.sources.gios_bulk import (
    _hash_file,
    run_gios_bulk_ingestion,
    GIOS_FILE_CATALOG,
)


class TestGiosBulkExtractor(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_hash_file(self):
        test_file = self.workspace / "sample.bin"
        test_file.write_bytes(b"hello gios test data")
        sha, md5 = _hash_file(test_file)
        self.assertEqual(len(sha), 64)
        self.assertEqual(len(md5), 32)

    @patch("urllib.request.urlopen")
    def test_run_gios_bulk_ingestion_local(self, mock_urlopen):
        mock_urlopen.side_effect = lambda req, timeout=None: io.BytesIO(b"mock binary content of xlsx or zip")

        result = run_gios_bulk_ingestion(
            workspace=self.workspace,
            years=[2023],
            include_metadata=True,
            skip_upload=True,
        )

        self.assertEqual(result["status"], "downloaded_locally")
        self.assertEqual(result["total_files"], 2)  # metadata + 2023 archive
        self.assertTrue((self.workspace / "Metadane oraz kody stacji i stanowisk pomiarowych.xlsx").exists())
        self.assertTrue((self.workspace / "2023.zip").exists())
        self.assertTrue((self.workspace / "gios_receipt.json").exists())

        receipt = json.loads((self.workspace / "gios_receipt.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["source_id"], "gios_pjp_bulk")
        self.assertEqual(len(receipt["files"]), 2)
        self.assertEqual(receipt["files"][0]["category"], "metadata")
        self.assertEqual(receipt["files"][1]["category"], "measurements")
        self.assertEqual(receipt["files"][1]["year"], 2023)


if __name__ == "__main__":
    unittest.main()
