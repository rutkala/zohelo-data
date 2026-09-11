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

    def test_process_member_stream_with_pipe_and_special_chars(self):
        records = [
            {
                "DATA_SOURCE": "BRIGHTQUERY",
                "RECORD_ID": "PIPE_01",
                "bq_dataset": "COMPANY",
                "FEATURES": [
                    {"NAME_ORG": "ACME | CONSULTING | GROUP, LLC", "NAME_TYPE": "PRIMARY"},
                    {"ADDR_CITY": "New York, NY", "ADDR_COUNTRY": "USA"},
                ],
            }
        ]
        jsonl_data = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        stream = io.BytesIO(jsonl_data)

        with tempfile.TemporaryDirectory() as td:
            parquet_path = Path(td) / "test_pipe.parquet"
            total = process_member_stream(stream, ORGANIZATION_COLUMNS, parquet_path)
            self.assertEqual(total, 1)

            con = duckdb.connect(":memory:")
            res = con.execute(f"SELECT name_org, addr_city FROM '{parquet_path}'").fetchall()
            self.assertEqual(res[0][0], "ACME | CONSULTING | GROUP, LLC")
            self.assertEqual(res[0][1], "New York, NY")

    def test_process_member_stream_locations_and_people(self):
        loc_record = {
            "DATA_SOURCE": "BRIGHTQUERY",
            "RECORD_ID": "LOC_1",
            "bq_dataset": "LOCATION",
            "FEATURES": [
                {"NAME_FULL": "BRANCH 101"},
                {"ADDR_CITY": "Krakow", "ADDR_COUNTRY": "PL"},
                {"GEO_LATITUDE": "50.06", "GEO_LONGITUDE": "19.94"},
            ],
        }
        loc_stream = io.BytesIO(json.dumps(loc_record).encode("utf-8"))

        people_record = {
            "DATA_SOURCE": "BRIGHTQUERY",
            "RECORD_ID": "P_1",
            "bq_dataset": "PEOPLE_BUSINESS",
            "FEATURES": [
                {"NAME_FULL": "Jan Kowalski", "NAME_FIRST": "Jan", "NAME_LAST": "Kowalski"},
                {"LINKEDIN": "https://linkedin.com/in/jankowalski"},
            ],
        }
        people_stream = io.BytesIO(json.dumps(people_record).encode("utf-8"))

        with tempfile.TemporaryDirectory() as td:
            loc_parquet = Path(td) / "test_loc.parquet"
            total_loc = process_member_stream(loc_stream, LOCATION_COLUMNS, loc_parquet)
            self.assertEqual(total_loc, 1)

            people_parquet = Path(td) / "test_people.parquet"
            total_p = process_member_stream(people_stream, PEOPLE_COLUMNS, people_parquet)
            self.assertEqual(total_p, 1)

            con = duckdb.connect(":memory:")
            res_loc = con.execute(f"SELECT name_full, addr_city, geo_latitude FROM '{loc_parquet}'").fetchall()
            self.assertEqual(res_loc[0][0], "BRANCH 101")
            self.assertEqual(res_loc[0][1], "Krakow")
            self.assertAlmostEqual(res_loc[0][2], 50.06)

            res_p = con.execute(f"SELECT name_full, linkedin FROM '{people_parquet}'").fetchall()
            self.assertEqual(res_p[0][0], "Jan Kowalski")
            self.assertEqual(res_p[0][1], "https://linkedin.com/in/jankowalski")

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

    def test_checkpoint_sharding_fallback_and_save(self):
        from unittest.mock import MagicMock
        from ingestion.sources.opendata_bronze_loader import OpenDataBronzeRunner

        mock_storage = MagicMock()
        mock_storage.resolve_root.return_value = "mock-root-id"
        mock_storage.get_or_create_nested_folder.return_value = "mock-control-id"

        # Mock files().list and get_media
        mock_files = MagicMock()
        mock_storage.drive_service.files.return_value = mock_files

        # Base checkpoint in Drive
        base_checkpoint = {
            "schema_version": 1,
            "processed_members": ["Organization/part_001.json", "Locations/part_001.json"],
            "bronze_tables": {
                "organizations": [{"member": "Organization/part_001.json", "rows": 100}],
                "locations": [{"member": "Locations/part_001.json", "rows": 50}],
                "people": [],
            },
            "total_rows": 150,
            "last_updated": "2026-09-11T12:00:00Z",
        }
        base_raw = json.dumps(base_checkpoint).encode("utf-8")

        # Simulate: checkpoint-organizations.json does NOT exist (files=[]), but checkpoint.json DOES exist
        def mock_list(q="", **kwargs):
            mock_res = MagicMock()
            if "checkpoint-organizations.json" in q:
                mock_res.execute.return_value = {"files": []}
            elif "checkpoint.json" in q:
                mock_res.execute.return_value = {"files": [{"id": "base-cp-id"}]}
            else:
                mock_res.execute.return_value = {"files": []}
            return mock_res

        mock_files.list.side_effect = mock_list
        mock_files.get_media.return_value.execute.return_value = base_raw

        runner = OpenDataBronzeRunner(mock_storage, allow_production_write=True)
        # Mock _resolve_folder to return mock-control-id
        runner._resolve_folder = MagicMock(return_value="mock-control-id")

        # Loading category shard when file doesn't exist falls back to seeding from base checkpoint
        cp_org = runner._load_checkpoint(category="organizations")
        self.assertEqual(cp_org["category"], "organizations")
        self.assertEqual(cp_org["processed_members"], ["Organization/part_001.json"])
        self.assertEqual(cp_org["total_rows"], 100)
        self.assertFalse(cp_org["complete"])

        # Test saving category shard
        mock_files.create.return_value.execute.return_value = {"id": "new-shard-id"}
        saved_id = runner._save_checkpoint(cp_org, category="organizations")
        self.assertEqual(saved_id, "new-shard-id")

        # Verify create was called with target name checkpoint-organizations.json
        create_args = mock_files.create.call_args[1]
        self.assertEqual(create_args["body"]["name"], "checkpoint-organizations.json")


if __name__ == "__main__":
    unittest.main()
