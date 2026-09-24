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

from imgw_bulk import (
    _hash_file,
    list_remote_directory,
    run_imgw_bulk_ingestion,
)


class TestImgwBulk(unittest.TestCase):
    def test_hash_file(self):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b"sample_imgw_weather_data_for_testing")
            tmp.flush()
            sha, md5 = _hash_file(Path(tmp.name))
            self.assertEqual(len(sha), 64)
            self.assertEqual(len(md5), 32)

    @patch("urllib.request.urlopen")
    def test_list_remote_directory(self, mock_urlopen):
        html = """
        <html><body>
        <a href="/parent">Parent Directory</a>
        <a href="2022_100_s.zip">2022_100_s.zip</a>
        <a href="2022_105_s.zip">2022_105_s.zip</a>
        <a href="?C=N;O=D">Name</a>
        </body></html>
        """
        mock_resp = MagicMock()
        mock_resp.read.return_value = html.encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        links = list_remote_directory("https://fake.imgw.pl/")
        self.assertEqual(links, ["2022_100_s.zip", "2022_105_s.zip"])

    @patch("imgw_bulk.list_remote_directory")
    @patch("imgw_bulk.download_stream")
    def test_run_imgw_bulk_ingestion_local(self, mock_download, mock_list):
        mock_list.return_value = ["2022_100_s.zip"]

        def fake_download(url, target_path):
            target_path.write_bytes(b"PK_fake_zip_bytes")

        mock_download.side_effect = fake_download

        with tempfile.TemporaryDirectory() as tmp_dir:
            res = run_imgw_bulk_ingestion(
                workspace=Path(tmp_dir),
                years=[2022],
                skip_upload=True,
            )
            self.assertEqual(res["status"], "downloaded_locally")
            self.assertEqual(res["years"], [2022])
            self.assertEqual(res["total_files"], 2)  # stations catalog + 1 zip archive
            self.assertTrue(Path(res["receipt"]).exists())


if __name__ == "__main__":
    unittest.main()
