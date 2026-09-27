from pathlib import Path
import sys
import tempfile
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import duckdb

from prg_bronze_loader import transform_prg_bronze


class TestPrgBronzeLoader(unittest.TestCase):
    def test_transform_prg_bronze_local(self):
        landing_dir = REPO_ROOT / "portal/test-results/prg"
        if not (landing_dir / "00_jednostki_administracyjne.zip").exists():
            self.skipTest("Local PRG archive not available for offline test")

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            result = transform_prg_bronze(
                landing_dir=landing_dir,
                output_dir=output_dir,
                skip_upload=True,
            )
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["tables"]["br_prg_country.parquet"], 1)
            self.assertEqual(result["tables"]["br_prg_voivodeships.parquet"], 16)
            self.assertEqual(result["tables"]["br_prg_counties.parquet"], 380)
            self.assertEqual(result["tables"]["br_prg_municipalities.parquet"], 2479)

            con = duckdb.connect()
            # Verify voivodeship data integrity
            woj_rows = con.execute(f"""
                SELECT count(*), count(distinct teryt_code), count(distinct voivodeship_name)
                FROM '{output_dir / "br_prg_voivodeships.parquet"}'
            """).fetchone()
            self.assertEqual(woj_rows[0], 16)
            self.assertEqual(woj_rows[1], 16)
            self.assertEqual(woj_rows[2], 16)

            # Verify geometry presence
            wkt_sample = con.execute(f"""
                SELECT country_code, country_name, starts_with(geometry_wkt, 'MULTIPOLYGON') or starts_with(geometry_wkt, 'POLYGON')
                FROM '{output_dir / "br_prg_country.parquet"}'
            """).fetchone()
            self.assertEqual(wkt_sample[0], "PL")
            self.assertEqual(wkt_sample[1], "POLSKA")
            self.assertTrue(wkt_sample[2])

            con.close()


if __name__ == "__main__":
    unittest.main()
