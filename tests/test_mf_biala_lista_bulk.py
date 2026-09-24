from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
SOURCES_DIR = SRC_DIR / "ingestion" / "sources"
if str(SOURCES_DIR) not in sys.path:
    sys.path.insert(0, str(SOURCES_DIR))

from mf_biala_lista_bulk import (
    _hash_file,
    resolve_target_date,
    run_biala_lista_ingestion,
)


class TestMfBialaListaBulk(unittest.TestCase):
    def test_hash_file(self):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b"sample_7z_content_for_testing")
            tmp.flush()
            sha, md5 = _hash_file(Path(tmp.name))
            self.assertEqual(len(sha), 64)
            self.assertEqual(len(md5), 32)

    @patch("mf_biala_lista_bulk._check_url_exists")
    def test_resolve_target_date_explicit(self, mock_check):
        mock_check.return_value = True
        date_str, url = resolve_target_date("20260920")
        self.assertEqual(date_str, "20260920")
        self.assertIn("20260920.7z", url)

    @patch("mf_biala_lista_bulk._check_url_exists")
    def test_resolve_target_date_fallback(self, mock_check):
        mock_check.side_effect = [False, True]
        date_str, url = resolve_target_date()
        self.assertTrue(len(date_str) == 8)
        self.assertTrue(url.endswith(f"{date_str}.7z"))

    @patch("mf_biala_lista_bulk._download_stream")
    @patch("mf_biala_lista_bulk._check_url_exists")
    def test_run_biala_lista_ingestion_local(self, mock_check, mock_download):
        mock_check.return_value = True

        def fake_download(url, target_path):
            target_path.write_bytes(b"7z_fake_archive_bytes")

        mock_download.side_effect = fake_download

        with tempfile.TemporaryDirectory() as tmp_dir:
            res = run_biala_lista_ingestion(
                workspace=Path(tmp_dir),
                target_date="20260920",
                skip_upload=True,
            )
            self.assertEqual(res["status"], "downloaded_locally")
            self.assertEqual(res["file_name"], "20260920.7z")
            self.assertEqual(res["snapshot_date"], "20260920")
            self.assertEqual(res["size_bytes"], len(b"7z_fake_archive_bytes"))


if __name__ == "__main__":
    unittest.main()
