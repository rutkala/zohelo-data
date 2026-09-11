import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.sources.opendata_bronze_loader import (
    DriveZipStream,
    ORGANIZATION_COLUMNS,
    LOCATION_COLUMNS,
    PEOPLE_COLUMNS,
    parse_senzing_record,
    process_member_stream,
)


class OpenDataBronzeLoaderTests(unittest.TestCase):
    def test_parse_senzing_organization_record(self):
        raw = {
            "DATA_SOURCE": "BRIGHTQUERY",
            "RECORD_ID": "100035667861",
            "bq_dataset": "COMPANY",
            "FEATURES": [
                {"NAME_ORG": "INTERSECTION INVESTMENT ADVISORS LLC", "NAME_TYPE": "PRIMARY"},
                {"RECORD_TYPE": "ORGANIZATION"},
                {
                    "ADDR_CITY": "Geneva",
                    "ADDR_COUNTRY": "USA",
                    "ADDR_LINE1": "1374 Averill Cir",
                    "ADDR_POSTAL_CODE": "60134",
                    "ADDR_STATE": "IL",
                    "ADDR_TYPE": "BUSINESS",
                },
                {"GEO_LATITUDE": "41.89005", "GEO_LONGITUDE": "-88.27914"},
                {"PLACEKEY": "1e5n6ebvi2@5sb-823-835"},
                {"BQ_ID": "100035667861"},
                {"REL_ANCHOR_DOMAIN": "BQ", "REL_ANCHOR_KEY": 100035667861},
            ],
        }
        parsed = parse_senzing_record(raw, ORGANIZATION_COLUMNS)
        self.assertEqual(len(parsed), len(ORGANIZATION_COLUMNS))
        # record_id
        self.assertEqual(parsed[0], "100035667861")
        # data_source
        self.assertEqual(parsed[1], "BRIGHTQUERY")
        # bq_dataset
        self.assertEqual(parsed[2], "COMPANY")
        # name_org
        self.assertEqual(parsed[3], "INTERSECTION INVESTMENT ADVISORS LLC")
        # name_type
        self.assertEqual(parsed[4], "PRIMARY")
        # record_type
        self.assertEqual(parsed[5], "ORGANIZATION")
        # addr_city (index 7)
        self.assertEqual(parsed[7], "Geneva")
        # geo_latitude (index 12)
        self.assertAlmostEqual(parsed[12], 41.89005)
        # geo_longitude (index 13)
        self.assertAlmostEqual(parsed[13], -88.27914)
        # placekey (index 14)
        self.assertEqual(parsed[14], "1e5n6ebvi2@5sb-823-835")

    def test_parse_senzing_location_record(self):
        raw = {
            "DATA_SOURCE": "BRIGHTQUERY",
            "RECORD_ID": "396997940",
            "bq_dataset": "LOCATION",
            "FEATURES": [
                {"NAME_FULL": "MAURICE SMITH"},
                {"RECORD_TYPE": "PERSON"},
                {"ADDR_CITY": "Jonesboro", "ADDR_COUNTRY": "USA", "ADDR_LINE1": "2509 Creekside"},
                {"PLACEKEY": "@5z6-3vw-8sz"},
            ],
        }
        parsed = parse_senzing_record(raw, LOCATION_COLUMNS)
        self.assertEqual(len(parsed), len(LOCATION_COLUMNS))
        self.assertEqual(parsed[0], "396997940")
        self.assertEqual(parsed[3], "MAURICE SMITH")
        self.assertEqual(parsed[4], "PERSON")
        self.assertEqual(parsed[6], "Jonesboro")

    def test_parse_senzing_people_record(self):
        raw = {
            "DATA_SOURCE": "BRIGHTQUERY",
            "RECORD_ID": "8810944360",
            "bq_dataset": "PEOPLE_BUSINESS",
            "FEATURES": [
                {"NAME_FULL": "WENGMENG CHOW", "NAME_FIRST": "WENGMENG", "NAME_LAST": "CHOW"},
                {"RECORD_TYPE": "PERSON"},
                {"ADDR_COUNTRY": "SGP"},
                {"GROUP_ASSN_ID_NUMBER": "100067924135", "GROUP_ASSN_ID_TYPE": "BQ_ID"},
                {"LINKEDIN": "https://www.linkedin.com/in/wengmeng-chow-5628a9142"},
            ],
        }
        parsed = parse_senzing_record(raw, PEOPLE_COLUMNS)
        self.assertEqual(len(parsed), len(PEOPLE_COLUMNS))
        self.assertEqual(parsed[0], "8810944360")
        self.assertEqual(parsed[3], "WENGMENG CHOW")
        self.assertEqual(parsed[4], "WENGMENG")
        self.assertEqual(parsed[5], "CHOW")
        self.assertEqual(parsed[13], "https://www.linkedin.com/in/wengmeng-chow-5628a9142")

    def test_process_member_stream_produces_valid_parquet(self):
        records = [
            {
                "DATA_SOURCE": "BRIGHTQUERY",
                "RECORD_ID": f"ORG_{i}",
                "bq_dataset": "COMPANY",
                "FEATURES": [
                    {"NAME_ORG": f"Company {i}", "NAME_TYPE": "PRIMARY"},
                    {"ADDR_CITY": "Warsaw", "ADDR_COUNTRY": "PL"},
                    {"GEO_LATITUDE": "52.23", "GEO_LONGITUDE": "21.01"},
                ],
            }
            for i in range(100)
        ]
        jsonl_data = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        stream = io.BytesIO(jsonl_data)

        with tempfile.TemporaryDirectory() as td:
            parquet_path = Path(td) / "test_output.parquet"
            total = process_member_stream(stream, ORGANIZATION_COLUMNS, parquet_path)
            self.assertEqual(total, 100)
            self.assertTrue(parquet_path.exists())
            self.assertGreater(parquet_path.stat().st_size, 0)

            # Validate via DuckDB read
            con = duckdb.connect(":memory:")
            res = con.execute(f"SELECT count(*), min(record_id), max(geo_latitude) FROM '{parquet_path}'").fetchall()
            self.assertEqual(res[0][0], 100)
            self.assertAlmostEqual(res[0][2], 52.23)

    def test_drive_zip_stream_with_mock_drive_service(self):
        # Create a synthetic ZIP in memory
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("Organization/part_001.json", b'{"RECORD_ID":"1","FEATURES":[]}\n')
            zf.writestr("Locations/part_001.json", b'{"RECORD_ID":"2","FEATURES":[]}\n')
        zip_bytes = zip_buf.getvalue()

        # Mock Drive service get_media with Range header support
        class MockDriveFiles:
            def __init__(self, data):
                self.data = data
                self.headers = {}

            def get_media(self, fileId):
                self.headers = {}
                return self

            def execute(self):
                range_header = self.headers.get("Range", "")
                if range_header.startswith("bytes="):
                    parts = range_header[6:].split("-")
                    start = int(parts[0])
                    end = int(parts[1]) if parts[1] else len(self.data) - 1
                    return self.data[start : end + 1]
                return self.data

        class MockDriveService:
            def __init__(self, data):
                self._files = MockDriveFiles(data)

            def files(self):
                return self._files

        mock_service = MockDriveService(zip_bytes)
        stream = DriveZipStream(mock_service, "fake-file-id", len(zip_bytes), chunk_size=1024)
        zf = zipfile.ZipFile(stream)
        names = zf.namelist()
        self.assertEqual(names, ["Organization/part_001.json", "Locations/part_001.json"])

        content = zf.read("Organization/part_001.json")
        self.assertEqual(content, b'{"RECORD_ID":"1","FEATURES":[]}\n')


if __name__ == "__main__":
    unittest.main()
