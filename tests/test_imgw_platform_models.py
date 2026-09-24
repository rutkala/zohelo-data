"""Exercise IMGW-PIB platform dbt models and dimensional marts offline with DuckDB."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]


class TestIMGWPlatformModels(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="zohelo-imgw-test-")
        cls.root = Path(cls.temp_dir.name)
        cls.data_root = cls.root / "data"
        cls.imgw_bronze = cls.data_root / "02_bronze" / "imgw_pib"
        cls.imgw_bronze.mkdir(parents=True, exist_ok=True)
        cls.database = cls.root / "imgw.duckdb"
        cls.target = cls.root / "target"

        # Create synthetic br_imgw_stations.parquet
        con = duckdb.connect()
        con.execute(f"""
            CREATE TABLE stations AS
            SELECT
                '352200375' AS station_code,
                'WARSZAWA-OKĘCIE' AS station_name,
                '352200375' AS station_num,
                '2026-09-20 12:00:00'::TIMESTAMP AS processed_at_utc
            UNION ALL
            SELECT
                '350190566' AS station_code,
                'KRAKÓW-BALICE' AS station_name,
                '350190566' AS station_num,
                '2026-09-20 12:00:00'::TIMESTAMP AS processed_at_utc
        """)
        con.execute(f"COPY stations TO '{cls.imgw_bronze / 'br_imgw_stations.parquet'}' (FORMAT PARQUET)")

        # Create synthetic br_imgw_synoptic_daily.parquet
        con.execute(f"""
            CREATE TABLE synoptic AS
            SELECT
                '352200375' AS station_code,
                'WARSZAWA-OKĘCIE' AS station_name,
                '2023-01-01'::DATE AS observation_date,
                15.2::DOUBLE AS tmax_celsius,
                8.4::DOUBLE AS tmin_celsius,
                11.5::DOUBLE AS tmean_celsius,
                0.5::DOUBLE AS precipitation_mm,
                'W' AS precipitation_type,
                0.0::DOUBLE AS snow_depth_cm,
                1.2::DOUBLE AS sunshine_hours,
                5.2::DOUBLE AS wind_speed_ms,
                78.5::DOUBLE AS relative_humidity_pct,
                1022.4::DOUBLE AS pressure_sea_level_hpa,
                '2023_01_s.zip' AS raw_archive_file,
                '2026-09-20 12:00:00'::TIMESTAMP AS processed_at_utc
            UNION ALL
            SELECT
                '352200375' AS station_code,
                'WARSZAWA-OKĘCIE' AS station_name,
                '2023-01-02'::DATE AS observation_date,
                -2.0::DOUBLE AS tmax_celsius,
                -8.5::DOUBLE AS tmin_celsius,
                -5.1::DOUBLE AS tmean_celsius,
                12.5::DOUBLE AS precipitation_mm,
                'S' AS precipitation_type,
                5.0::DOUBLE AS snow_depth_cm,
                0.0::DOUBLE AS sunshine_hours,
                8.1::DOUBLE AS wind_speed_ms,
                85.0::DOUBLE AS relative_humidity_pct,
                1015.0::DOUBLE AS pressure_sea_level_hpa,
                '2023_01_s.zip' AS raw_archive_file,
                '2026-09-20 12:00:00'::TIMESTAMP AS processed_at_utc
        """)
        con.execute(f"COPY synoptic TO '{cls.imgw_bronze / 'br_imgw_synoptic_daily.parquet'}' (FORMAT PARQUET)")
        con.close()

        # Run dbt build
        env = dict(os.environ)
        env.update(
            ZOHELO_DATA_ROOT=str(cls.data_root),
            ZOHELO_DUCKDB_PATH=str(cls.database),
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            DO_NOT_TRACK="1",
        )
        cmd = [
            "dbt", "build",
            "--profiles-dir", str(REPO_ROOT),
            "--target-path", str(cls.target),
            "--select",
            "+stg_imgw_stations",
            "+stg_imgw_synoptic_daily",
            "+dim_weather_station",
            "+fact_daily_weather",
            "+mart_weather_daily_summary",
            "--vars", '{"enable_imgw_pib": true}',
            "--threads", "1",
            "--no-partial-parse",
        ]
        res = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
        if res.returncode != 0:
            raise AssertionError(f"dbt build failed:\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}")

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test_dim_weather_station(self):
        con = duckdb.connect(str(self.database))
        res = con.execute('SELECT station_code, station_name, source_provider FROM "04_gold"."dim_weather_station" ORDER BY station_code').fetchall()
        con.close()
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0], ("350190566", "KRAKÓW-BALICE", "IMGW-PIB"))
        self.assertEqual(res[1], ("352200375", "WARSZAWA-OKĘCIE", "IMGW-PIB"))

    def test_fact_daily_weather(self):
        con = duckdb.connect(str(self.database))
        res = con.execute("""
            SELECT observation_date, tmax_celsius, tmin_celsius, precipitation_mm, precipitation_type_standardized
            FROM "04_gold"."fact_daily_weather"
            ORDER BY observation_date
        """).fetchall()
        con.close()
        self.assertEqual(len(res), 2)
        self.assertEqual(str(res[0][0]), "2023-01-01")
        self.assertEqual(res[0][1], 15.2)
        self.assertEqual(res[0][2], 8.4)
        self.assertEqual(res[0][3], 0.5)
        self.assertEqual(res[0][4], "liquid")

        self.assertEqual(str(res[1][0]), "2023-01-02")
        self.assertEqual(res[1][1], -2.0)
        self.assertEqual(res[1][2], -8.5)
        self.assertEqual(res[1][3], 12.5)
        self.assertEqual(res[1][4], "solid")

    def test_mart_weather_daily_summary(self):
        con = duckdb.connect(str(self.database))
        res = con.execute("""
            SELECT
                observation_date,
                diurnal_temperature_range_celsius,
                has_precipitation,
                is_heavy_rain_day,
                is_frost_day
            FROM "04_gold"."mart_weather_daily_summary"
            ORDER BY observation_date
        """).fetchall()
        con.close()
        self.assertEqual(len(res), 2)
        # Day 1: 15.2 - 8.4 = 6.8 range, has_precip=1, heavy_rain=0, frost=0
        self.assertAlmostEqual(res[0][1], 6.8, places=4)
        self.assertEqual(res[0][2], 1)
        self.assertEqual(res[0][3], 0)
        self.assertEqual(res[0][4], 0)

        # Day 2: -2.0 - (-8.5) = 6.5 range, has_precip=1, heavy_rain=1 (12.5 >= 10.0), frost=1 (tmin < 0)
        self.assertAlmostEqual(res[1][1], 6.5, places=4)
        self.assertEqual(res[1][2], 1)
        self.assertEqual(res[1][3], 1)
        self.assertEqual(res[1][4], 1)


if __name__ == "__main__":
    unittest.main()
