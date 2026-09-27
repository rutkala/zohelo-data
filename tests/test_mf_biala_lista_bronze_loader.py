import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mf_biala_lista_bronze_loader import transform_biala_lista_bronze


class TestMfBialaListaBronzeLoader(unittest.TestCase):
    def test_transform_biala_lista_bronze(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            json_data = {
                "naglowek": {
                    "dataGenerowaniaDanych": "20260920",
                    "liczbaTransformacji": "5000",
                    "schemat": "RRRRMMDDNNNNNNNNNNBBBBBBBBBBBBBBBBBBBBBBBBBB",
                },
                "skrotyPodatnikowCzynnych": [
                    "000000cb3170b93faae31644153812e8be137f49159fa682b1a96a497dee9b84695e96ef3c4bb0238546f3e4115ab5a0674e166f5903b976279a2a28b71ee8da",
                    "000001637d476631c3e8655005847afac6c0e991bea7198f7f8b4e215ead87168137b6b3e4765793ebb32e1372b851841bf017b95cd97ee9b2bcc9112c8d75a7",
                ],
                "skrotyPodatnikowZwolnionych": [
                    "0000020bfcc19ea5dcfa6b12be66ec6c5d8f1ed3ed79144f1753eeb021dc87eec67190f5a1f3aab4d9153e1b9c997f6f09bfc9d65926d1878c53d3354d41eeab",
                ],
                "maski": [
                    "XX10100055YYYXXXXXXXXXXXXX",
                    "XX10200032YYYYXXXXXXXXXXXX",
                ],
            }
            json_file = tmp_path / "20260920.json"
            json_file.write_text(json.dumps(json_data, indent=2), encoding="utf-8")

            # Create 7z archive
            archive_path = tmp_path / "20260920.7z"
            subprocess.run(
                ["7z", "a", str(archive_path), str(json_file)],
                check=True,
                stdout=subprocess.DEVNULL,
            )

            workspace = tmp_path / "bronze_out"
            result = transform_biala_lista_bronze(
                archive_path=archive_path,
                workspace=workspace,
                skip_upload=True,
            )

            self.assertEqual(result["status"], "completed_locally")
            self.assertEqual(result["snapshot_date"], "2026-09-20")
            self.assertEqual(result["active_hashes"], 2)
            self.assertEqual(result["exempt_hashes"], 1)
            self.assertEqual(result["masks"], 2)

            con = duckdb.connect()
            header_df = con.execute(f"SELECT * FROM read_parquet('{workspace}/br_biala_lista_header.parquet')").fetchdf()
            self.assertEqual(len(header_df), 1)
            self.assertEqual(header_df["transformation_count"].iloc[0], 5000)

            masks_df = con.execute(f"SELECT * FROM read_parquet('{workspace}/br_biala_lista_masks.parquet')").fetchdf()
            self.assertEqual(len(masks_df), 2)
            self.assertEqual(masks_df["bank_prefix"].iloc[0], "10100055")

            tax_df = con.execute(f"SELECT * FROM read_parquet('{workspace}/br_biala_lista_taxpayers.parquet')").fetchdf()
            self.assertEqual(len(tax_df), 3)
            active_cnt = (tax_df["status"] == "active").sum()
            exempt_cnt = (tax_df["status"] == "exempt").sum()
            self.assertEqual(active_cnt, 2)
            self.assertEqual(exempt_cnt, 1)
            con.close()


if __name__ == "__main__":
    unittest.main()
