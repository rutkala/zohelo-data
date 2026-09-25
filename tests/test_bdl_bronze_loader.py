import io
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import duckdb

from bdl_bronze_loader import generate_partition_filename, process_bdl_zip_to_parquet


class TestBdlBronzeLoader(unittest.TestCase):
    def test_process_bdl_zip_to_parquet(self):
        csv_content = (
            '"Kod";"Nazwa";"Ogółem";"Rok";"Wartosc";"Jednostka miary";"Atrybut";\n'
            '000000000000;"POLSKA";"ogółem";"2005";17;"%";"";\n'
            '000000000000;"POLSKA";"ogółem";"2006";20,3;"%";"";\n'
            '020100000000;"DOLNOŚLĄSKIE";"ogółem";"2006";15,75;"%";"";\n'
        )
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("test_subgroup.csv", csv_content.encode("utf-8"))

        con = duckdb.connect()
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_pq:
            tmp_pq_path = Path(tmp_pq.name)

        try:
            count = process_bdl_zip_to_parquet(zip_buffer.getvalue(), "P3176", tmp_pq_path, con)
            self.assertEqual(count, 3)

            # Query the generated parquet
            rows = con.execute(
                f"select subgroup_id, unit_id, unit_name, period_year, val_numeric, measure_unit from '{tmp_pq_path}'"
            ).fetchall()
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0][0], "P3176")
            self.assertEqual(rows[0][1], "000000000000")
            self.assertEqual(rows[0][4], 17.0)
            self.assertEqual(rows[1][4], 20.3)
            self.assertEqual(rows[2][1], "020100000000")
            self.assertEqual(rows[2][4], 15.75)
        finally:
            tmp_pq_path.unlink(missing_ok=True)
            con.close()

    def test_thousand_separators_with_spaces_and_non_breaking_spaces(self):
        """Verify Polish space, non-breaking space, narrow no-break space don't become NULL."""
        # Using regular space (' '), non-breaking space ('\xa0'), narrow no-break space ('\u202f')
        csv_content = (
            '"Kod";"Nazwa";"Rok";"Wartosc";"Jednostka miary";"Atrybut";\n'
            '000000000000;"PL";"2020";"1 250,5";"tys. zł";"";\n'
            '000000000000;"PL";"2021";"2\xa0500,75";"tys. zł";"";\n'
            '000000000000;"PL";"2022";"10\u202f000,0";"tys. zł";"";\n'
            '000000000000;"PL";"2023";"1 234 567,89";"tys. zł";"";\n'
            '000000000000;"PL";"2024";"-";"tys. zł";"";\n'
        )
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("test_spaces.csv", csv_content.encode("utf-8"))

        con = duckdb.connect()
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_pq:
            tmp_pq_path = Path(tmp_pq.name)

        try:
            count = process_bdl_zip_to_parquet(zip_buffer.getvalue(), "P2000", tmp_pq_path, con)
            self.assertEqual(count, 5)

            rows = con.execute(
                f"select period_year, val_raw, val_numeric from '{tmp_pq_path}' order by period_year"
            ).fetchall()
            self.assertEqual(len(rows), 5)
            # Regular space
            self.assertEqual(rows[0][0], 2020)
            self.assertEqual(rows[0][1], "1 250,5")
            self.assertEqual(rows[0][2], 1250.5)
            # Non-breaking space
            self.assertEqual(rows[1][0], 2021)
            self.assertEqual(rows[1][1], "2\xa0500,75")
            self.assertEqual(rows[1][2], 2500.75)
            # Narrow no-break space
            self.assertEqual(rows[2][0], 2022)
            self.assertEqual(rows[2][1], "10\u202f000,0")
            self.assertEqual(rows[2][2], 10000.0)
            # Multi-thousands space
            self.assertEqual(rows[3][0], 2023)
            self.assertEqual(rows[3][2], 1234567.89)
            # Dash evaluates to NULL in val_numeric, but preserves "-" in val_raw
            self.assertEqual(rows[4][0], 2024)
            self.assertEqual(rows[4][1], "-")
            self.assertIsNone(rows[4][2])
        finally:
            tmp_pq_path.unlink(missing_ok=True)
            con.close()

    def test_multi_csv_zip_archive(self):
        """Verify all CSV files in a ZIP archive are extracted and unioned into the Parquet output."""
        csv1 = (
            '"Kod";"Nazwa";"Rok";"Wartosc";"Wymiar 1";"Jednostka miary";"Atrybut";\n'
            '000000000000;"PL";"2020";"100";"kobiety";"osoba";"";\n'
            '000000000000;"PL";"2021";"110";"kobiety";"osoba";"";\n'
        )
        csv2 = (
            '"Kod";"Nazwa";"Rok";"Wartosc";"Wymiar 1";"Jednostka miary";"Atrybut";\n'
            '000000000000;"PL";"2020";"90";"mężczyźni";"osoba";"";\n'
            '000000000000;"PL";"2021";"95";"mężczyźni";"osoba";"";\n'
            '000000000000;"PL";"2022";"98";"mężczyźni";"osoba";"";\n'
        )
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("part_female.csv", csv1.encode("utf-8"))
            z.writestr("part_male.csv", csv2.encode("utf-8"))

        con = duckdb.connect()
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_pq:
            tmp_pq_path = Path(tmp_pq.name)

        try:
            count = process_bdl_zip_to_parquet(zip_buffer.getvalue(), "P1500", tmp_pq_path, con)
            # Total rows across both CSVs: 2 + 3 = 5
            self.assertEqual(count, 5)

            rows = con.execute(
                f"select period_year, val_numeric, dimensions_json from '{tmp_pq_path}' order by period_year, val_numeric"
            ).fetchall()
            self.assertEqual(len(rows), 5)
            values = [r[1] for r in rows]
            self.assertIn(100.0, values)
            self.assertIn(110.0, values)
            self.assertIn(90.0, values)
            self.assertIn(95.0, values)
            self.assertIn(98.0, values)
        finally:
            tmp_pq_path.unlink(missing_ok=True)
            con.close()

    def test_preserve_7_digit_terc_codes(self):
        """Verify 7-digit municipal TERC codes are not unconditionally lpad'd to 12 digits."""
        csv_content = (
            '"Kod";"Nazwa";"Rok";"Wartosc";"Jednostka miary";"Atrybut";\n'
            '0201011;"Bolesławiec";"2021";"40000";"osoba";"";\n'
            '201011;"Bolesławiec stripped";"2021";"40000";"osoba";"";\n'
            '010201011000;"Bolesławiec 12digit";"2021";"40000";"osoba";"";\n'
            '0201;"powiat bolesławiecki";"2021";"90000";"osoba";"";\n'
            '02;"dolnośląskie";"2021";"2900000";"osoba";"";\n'
        )
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("terc_test.csv", csv_content.encode("utf-8"))

        con = duckdb.connect()
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_pq:
            tmp_pq_path = Path(tmp_pq.name)

        try:
            count = process_bdl_zip_to_parquet(zip_buffer.getvalue(), "P2100", tmp_pq_path, con)
            self.assertEqual(count, 5)

            rows = con.execute(
                f"select unit_name, unit_id, terc_code from '{tmp_pq_path}' order by unit_name"
            ).fetchall()
            row_dict = {r[0]: (r[1], r[2]) for r in rows}

            # 7-digit TERC municipal code: must be preserved as 7 digits, NOT 12 digits!
            boleslawiec_id, boleslawiec_terc = row_dict["Bolesławiec"]
            self.assertEqual(boleslawiec_id, "0201011")
            self.assertEqual(boleslawiec_terc, "0201011")
            self.assertNotEqual(boleslawiec_id, "0000000201011")

            # 6-digit code with stripped leading zero: padded to 7 digits
            stripped_id, stripped_terc = row_dict["Bolesławiec stripped"]
            self.assertEqual(stripped_id, "0201011")
            self.assertEqual(stripped_terc, "0201011")

            # 12-digit BDL unit code: preserved as 12 digits, terc extracted from positions 3-9
            full_id, full_terc = row_dict["Bolesławiec 12digit"]
            self.assertEqual(full_id, "010201011000")
            self.assertEqual(full_terc, "0201011")

            # 4-digit powiat code: preserved as 4 digits
            powiat_id, powiat_terc = row_dict["powiat bolesławiecki"]
            self.assertEqual(powiat_id, "0201")
            self.assertIsNone(powiat_terc)

            # 2-digit voivodeship code: preserved as 2 digits
            voiv_id, voiv_terc = row_dict["dolnośląskie"]
            self.assertEqual(voiv_id, "02")
            self.assertIsNone(voiv_terc)
        finally:
            tmp_pq_path.unlink(missing_ok=True)
            con.close()

    def test_unique_slice_naming(self):
        """Verify partition filenames include subgroup_id and selection_id/hash so slices don't overwrite."""
        # Function unit tests
        fn_default = generate_partition_filename("P1313")
        self.assertEqual(fn_default, "part_P1313.parquet")

        fn_sel1 = generate_partition_filename("P1313", selection_id="sel-1")
        self.assertEqual(fn_sel1, "part_P1313_sel-1.parquet")

        fn_sel2 = generate_partition_filename("P1313", selection_id="sel-2")
        self.assertEqual(fn_sel2, "part_P1313_sel-2.parquet")
        self.assertNotEqual(fn_sel1, fn_sel2)

        fn_hash = generate_partition_filename("P1313", part_hash="14ee2a59a50a153ac83125a00875c271")
        self.assertEqual(fn_hash, "part_P1313_14ee2a59a50a153a.parquet")

        # Test writing into a directory with different selection_ids to ensure no overwrite
        csv_slice_1 = (
            '"Kod";"Nazwa";"Rok";"Wartosc";"Jednostka miary";"Atrybut";\n'
            '000000000000;"PL";"2020";"10";"osoba";"";\n'
        )
        csv_slice_2 = (
            '"Kod";"Nazwa";"Rok";"Wartosc";"Jednostka miary";"Atrybut";\n'
            '000000000000;"PL";"2021";"20";"osoba";"";\n'
        )
        zip1 = io.BytesIO()
        with zipfile.ZipFile(zip1, "w") as z:
            z.writestr("slice1.csv", csv_slice_1.encode("utf-8"))

        zip2 = io.BytesIO()
        with zipfile.ZipFile(zip2, "w") as z:
            z.writestr("slice2.csv", csv_slice_2.encode("utf-8"))

        con = duckdb.connect()
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir)
            count1 = process_bdl_zip_to_parquet(
                zip1.getvalue(), "P1313", out_dir, con, selection_id="slice_1", raw_archive_file="s1.zip"
            )
            count2 = process_bdl_zip_to_parquet(
                zip2.getvalue(), "P1313", out_dir, con, selection_id="slice_2", raw_archive_file="s2.zip"
            )
            self.assertEqual(count1, 1)
            self.assertEqual(count2, 1)

            file1 = out_dir / "part_P1313_slice_1.parquet"
            file2 = out_dir / "part_P1313_slice_2.parquet"
            self.assertTrue(file1.exists(), "First partition slice must exist!")
            self.assertTrue(file2.exists(), "Second partition slice must exist without overwriting the first!")

            rows1 = con.execute(f"select selection_id, raw_archive_file from '{file1}'").fetchall()
            self.assertEqual(rows1[0][0], "slice_1")
            self.assertEqual(rows1[0][1], "s1.zip")

            rows2 = con.execute(f"select selection_id, raw_archive_file from '{file2}'").fetchall()
            self.assertEqual(rows2[0][0], "slice_2")
            self.assertEqual(rows2[0][1], "s2.zip")
        con.close()

    def test_sub_annual_period_and_okres_column(self):
        """Verify sub-annual period identifiers (monthly/quarterly) and 'Okres' column are preserved."""
        csv_content = (
            '"Kod";"Nazwa";"Okres";"Wartosc";"Jednostka miary";"Atrybut";\n'
            '000000000000;"PL";"2020-M01";"105,2";"pkt";"";\n'
            '000000000000;"PL";"2020-M02";"106,1";"pkt";"";\n'
            '000000000000;"PL";"2020-Q1";"105,6";"pkt";"";\n'
            '000000000000;"PL";"2020";"107,0";"pkt";"";\n'
        )
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("subannual.csv", csv_content.encode("utf-8"))

        con = duckdb.connect()
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_pq:
            tmp_pq_path = Path(tmp_pq.name)

        try:
            count = process_bdl_zip_to_parquet(zip_buffer.getvalue(), "P4000", tmp_pq_path, con)
            self.assertEqual(count, 4)

            rows = con.execute(
                f"select period_raw, period_year, val_numeric from '{tmp_pq_path}' order by period_raw"
            ).fetchall()
            self.assertEqual(len(rows), 4)

            raw_to_year = {r[0]: (r[1], r[2]) for r in rows}
            self.assertEqual(raw_to_year["2020-M01"], (2020, 105.2))
            self.assertEqual(raw_to_year["2020-M02"], (2020, 106.1))
            self.assertEqual(raw_to_year["2020-Q1"], (2020, 105.6))
            self.assertEqual(raw_to_year["2020"], (2020, 107.0))
        finally:
            tmp_pq_path.unlink(missing_ok=True)
            con.close()


if __name__ == "__main__":
    unittest.main()
