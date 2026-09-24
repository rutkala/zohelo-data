"""Unit tests for IMGW-PIB Bronze loader (PL-ENV-008 / PL-ENV-009)."""
import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

import duckdb

from imgw_bronze_loader import (
    transform_stations_catalog,
    transform_synoptic_archives,
    transform_imgw_bronze,
)


class TestIMGWBronzeLoader(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp_dir.name)
        self.landing_dir = self.workspace / "landing"
        self.bronze_dir = self.workspace / "bronze"
        self.landing_dir.mkdir(parents=True)
        self.bronze_dir.mkdir(parents=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_transform_stations_catalog(self):
        csv_content = (
            "352200375,WARSZAWA-OKĘCIE,352200375\n"
            "350190566,KRAKÓW-BALICE,350190566\n"
            "349190625,ZAKOPANE,349190625\n"
        )
        catalog_path = self.landing_dir / "wykaz_stacji.csv"
        catalog_path.write_text(csv_content, encoding="utf-8")

        output_parquet = self.bronze_dir / "br_imgw_stations.parquet"
        count = transform_stations_catalog(catalog_path, output_parquet)

        self.assertEqual(count, 3)
        self.assertTrue(output_parquet.exists())

        con = duckdb.connect(":memory:")
        df = con.execute(f"SELECT * FROM '{output_parquet}' ORDER BY station_code").fetchdf()
        con.close()

        self.assertEqual(len(df), 3)
        self.assertEqual(df["station_code"].tolist(), ["349190625", "350190566", "352200375"])
        self.assertEqual(df["station_name"].tolist(), ["ZAKOPANE", "KRAKÓW-BALICE", "WARSZAWA-OKĘCIE"])

    def test_transform_synoptic_archives(self):
        # Create mock zip archive with s_d_ and s_d_t_ files encoded in latin2
        zip_path = self.landing_dir / "2023_01_s.zip"

        sd_csv = (
            "352200375,WARSZAWA-OKĘCIE,2023,01,01,15.2,,8.4,,11.5,,6.2,,0.5,,W,0,,0.0,,1.2,,,,,,,,,\n"
            "352200375,WARSZAWA-OKĘCIE,2023,01,02,10.0,,4.1,,7.2,,2.0,,3.2,,D,0,,1.5,,2.4,,,,,,,,,\n"
        )
        sdt_csv = (
            "352200375,WARSZAWA-OKĘCIE,2023,01,01,6.5,,5.2,,11.8,,9.0,,78.5,,1015.2,,1022.4,,,,,,,\n"
            "352200375,WARSZAWA-OKĘCIE,2023,01,02,7.0,,4.0,,7.5,,8.0,,85.0,,1010.0,,1018.1,,,,,,,\n"
        )

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("s_d_352200375_2023.csv", sd_csv.encode("latin2"))
            zf.writestr("s_d_t_352200375_2023.csv", sdt_csv.encode("latin2"))

        output_parquet = self.bronze_dir / "br_imgw_synoptic_daily.parquet"
        count = transform_synoptic_archives([zip_path], output_parquet, self.bronze_dir)

        self.assertEqual(count, 2)
        self.assertTrue(output_parquet.exists())

        con = duckdb.connect(":memory:")
        df = con.execute(f"SELECT * FROM '{output_parquet}' ORDER BY observation_date").fetchdf()
        con.close()

        self.assertEqual(len(df), 2)
        self.assertEqual(str(df["observation_date"][0])[:10], "2023-01-01")
        self.assertEqual(df["tmax_celsius"][0], 15.2)
        self.assertEqual(df["tmin_celsius"][0], 8.4)
        self.assertEqual(df["precipitation_mm"][0], 0.5)
        self.assertEqual(df["wind_speed_ms"][0], 5.2)
        self.assertEqual(df["pressure_sea_level_hpa"][0], 1022.4)
        self.assertEqual(df["raw_archive_file"][0], "2023_01_s.zip")

    def test_transform_imgw_bronze_end_to_end(self):
        catalog_path = self.landing_dir / "wykaz_stacji.csv"
        catalog_path.write_text("352200375,WARSZAWA-OKĘCIE,352200375\n", encoding="utf-8")

        zip_path = self.landing_dir / "2023_01_s.zip"
        sd_csv = "352200375,WARSZAWA-OKĘCIE,2023,01,01,15.2,,8.4,,11.5,,6.2,,0.5,,W,0,,0.0,,1.2,,,,,,,,,\n"
        sdt_csv = "352200375,WARSZAWA-OKĘCIE,2023,01,01,6.5,,5.2,,11.8,,9.0,,78.5,,1015.2,,1022.4,,,,,,,\n"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("s_d_352200375_2023.csv", sd_csv.encode("latin2"))
            zf.writestr("s_d_t_352200375_2023.csv", sdt_csv.encode("latin2"))

        result = transform_imgw_bronze(
            landing_workspace=self.landing_dir,
            output_workspace=self.bronze_dir,
            skip_upload=True,
        )

        self.assertEqual(result["status"], "transformed_locally")
        self.assertEqual(result["stations_count"], 1)
        self.assertEqual(result["observations_count"], 1)
        self.assertTrue((self.bronze_dir / "br_imgw_stations.parquet").exists())
        self.assertTrue((self.bronze_dir / "br_imgw_synoptic_daily.parquet").exists())
        self.assertTrue((self.bronze_dir / "summary.json").exists())


if __name__ == "__main__":
    unittest.main()
