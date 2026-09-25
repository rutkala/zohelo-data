"""Unit tests for GIOŚ Bronze loader (PL-ENV-010)."""
import csv
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from gios_bronze_loader import (
    transform_gios_metadata,
    transform_gios_measurements,
    transform_gios_bronze,
)


def create_synthetic_xlsx(path: Path, sheets: dict[str, list[list[str]]]) -> None:
    """Create a minimal valid XLSX file with specified sheets using openxml standard."""
    with zipfile.ZipFile(path, "w") as z:
        sheet_entries = []
        rel_entries = []
        override_entries = []

        sheet_idx = 1
        for s_name, rows in sheets.items():
            r_id = f"rId{sheet_idx}"
            sheet_target = f"worksheets/sheet{sheet_idx}.xml"
            sheet_entries.append(f'<sheet name="{s_name}" sheetId="{sheet_idx}" r:id="{r_id}"/>')
            rel_entries.append(f'<Relationship Id="{r_id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="{sheet_target}"/>')
            override_entries.append(f'<Override PartName="/xl/{sheet_target}" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>')

            sheet_xml_rows = []
            for r_i, r in enumerate(rows, start=1):
                c_xml = []
                for c_i, val in enumerate(r):
                    col_letter = chr(ord("A") + c_i)
                    cell_ref = f"{col_letter}{r_i}"
                    if val is not None:
                        c_xml.append(f'<c r="{cell_ref}" t="inlineStr"><is><t>{val}</t></is></c>')
                sheet_xml_rows.append(f'<row r="{r_i}">{"".join(c_xml)}</row>')

            sheet_content = (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">\n'
                f'<sheetData>{"".join(sheet_xml_rows)}</sheetData>\n'
                '</worksheet>'
            )
            z.writestr(f"xl/{sheet_target}", sheet_content.encode("utf-8"))
            sheet_idx += 1

        wb_content = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">\n'
            f'<sheets>{"".join(sheet_entries)}</sheets>\n'
            '</workbook>'
        )
        z.writestr("xl/workbook.xml", wb_content.encode("utf-8"))

        wb_rels = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">\n'
            f'{"".join(rel_entries)}\n'
            '</Relationships>'
        )
        z.writestr("xl/_rels/workbook.xml.rels", wb_rels.encode("utf-8"))

        content_types = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">\n'
            '<Default Extension="xml" ContentType="application/xml"/>\n'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>\n'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>\n'
            f'{"".join(override_entries)}\n'
            '</Types>'
        )
        z.writestr("[Content_Types].xml", content_types.encode("utf-8"))


class TestGiosBronzeLoader(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp_dir.name)
        self.landing_dir = self.workspace / "landing"
        self.bronze_dir = self.workspace / "bronze"
        self.landing_dir.mkdir(parents=True)
        self.bronze_dir.mkdir(parents=True)

        # 1. Create synthetic GIOŚ metadata Excel
        self.metadata_xlsx = self.landing_dir / "Metadane oraz kody stacji i stanowisk pomiarowych.xlsx"
        sheets = {
            "Stacje": [
                ["Kod stacji", "Stary Kod stacji", "Nazwa stacji", "Typ stacji", "Typ obszaru", "WGS84 φ N", "WGS84 λ E", "Województwo", "Miejscowość", "Adres"],
                ["DsWrocAlWisn", "DsWrocAlWisn_old", "Wrocław - Wiśniowa", "komunikacyjna", "miejski", "51.08639", "17.01250", "DOLNOŚLĄSKIE", "Wrocław", "Al. Wiśniowa"],
                ["MzWarszKrasin", "MzWarszKrasin_old", "Warszawa - Krasińskiego", "tło", "miejski", "52.26861", "20.97639", "MAZOWIECKIE", "Warszawa", "ul. Krasińskiego"],
            ],
            "Stanowiska": [
                ["Kod stanowiska", "Kod stacji", "Wskaźnik", "Czas uśredniania", "Typ pomiaru"],
                ["DsWrocAlWisn-PM10-1g", "DsWrocAlWisn", "PM10", "1g", "automatyczny"],
                ["DsWrocAlWisn-NO2-1g", "DsWrocAlWisn", "NO2", "1g", "automatyczny"],
                ["MzWarszKrasin-PM2.5-1g", "MzWarszKrasin", "PM2.5", "1g", "automatyczny"],
            ],
        }
        create_synthetic_xlsx(self.metadata_xlsx, sheets)

        # 2. Create synthetic measurement zip archive
        self.meas_zip = self.landing_dir / "2023.zip"
        meas_csv = (
            "Kod stacji;Kod stanowiska;Wskaźnik;Data;Wartość\n"
            "DsWrocAlWisn;DsWrocAlWisn-PM10-1g;PM10;2023-01-01 01:00:00;25.4\n"
            "DsWrocAlWisn;DsWrocAlWisn-PM10-1g;PM10;2023-01-01 02:00:00;28.1\n"
            "MzWarszKrasin;MzWarszKrasin-PM2.5-1g;PM2.5;2023-01-01 01:00:00;14.2\n"
        )
        with zipfile.ZipFile(self.meas_zip, "w") as z:
            z.writestr("2023_PM10_1g.csv", meas_csv.encode("utf-8"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_transform_gios_metadata(self):
        out_stations = self.bronze_dir / "br_gios_stations.parquet"
        out_sensors = self.bronze_dir / "br_gios_sensors.parquet"

        st_cnt, sens_cnt = transform_gios_metadata(self.metadata_xlsx, out_stations, out_sensors)
        self.assertEqual(st_cnt, 2)
        self.assertEqual(sens_cnt, 3)
        self.assertTrue(out_stations.exists())
        self.assertTrue(out_sensors.exists())

        con = duckdb.connect()
        stations = con.execute(f"SELECT station_code, station_name, city, latitude, longitude FROM '{out_stations}' ORDER BY station_code").fetchall()
        self.assertEqual(len(stations), 2)
        self.assertEqual(stations[0][0], "DsWrocAlWisn")
        self.assertEqual(stations[0][1], "Wrocław - Wiśniowa")
        self.assertEqual(stations[0][2], "Wrocław")
        self.assertAlmostEqual(stations[0][3], 51.08639)
        self.assertAlmostEqual(stations[0][4], 17.01250)

        sensors = con.execute(f"SELECT sensor_code, station_code, pollutant, averaging_interval FROM '{out_sensors}' ORDER BY sensor_code").fetchall()
        self.assertEqual(len(sensors), 3)
        self.assertEqual(sensors[0][0], "DsWrocAlWisn-NO2-1g")
        self.assertEqual(sensors[0][2], "NO2")
        con.close()

    def test_transform_gios_measurements(self):
        out_meas = self.bronze_dir / "br_gios_measurements.parquet"
        cnt = transform_gios_measurements([self.meas_zip], out_meas, self.workspace)
        self.assertEqual(cnt, 3)
        self.assertTrue(out_meas.exists())

        con = duckdb.connect()
        rows = con.execute(f"SELECT station_code, sensor_code, pollutant, value, observation_date FROM '{out_meas}' ORDER BY measurement_timestamp, station_code").fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][0], "DsWrocAlWisn")
        self.assertEqual(rows[0][2], "PM10")
        self.assertAlmostEqual(rows[0][3], 25.4)
        con.close()

    def test_transform_gios_bronze_full_pipeline(self):
        result = transform_gios_bronze(
            landing_workspace=self.landing_dir,
            output_workspace=self.bronze_dir,
            skip_upload=True,
        )
        self.assertEqual(result["status"], "transformed_locally")
        self.assertEqual(result["stations_count"], 2)
        self.assertEqual(result["sensors_count"], 3)
        self.assertEqual(result["measurements_count"], 3)
        self.assertTrue((self.bronze_dir / "br_gios_stations.parquet").exists())
        self.assertTrue((self.bronze_dir / "br_gios_sensors.parquet").exists())
        self.assertTrue((self.bronze_dir / "br_gios_measurements.parquet").exists())
        self.assertTrue((self.bronze_dir / "summary.json").exists())


if __name__ == "__main__":
    unittest.main()
