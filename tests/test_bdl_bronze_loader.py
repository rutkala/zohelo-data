import io
from pathlib import Path
import tempfile
import unittest
import zipfile

import duckdb

from bdl_bronze_loader import process_bdl_zip_to_parquet


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
            rows = con.execute(f"select subgroup_id, unit_id, unit_name, period_year, val_numeric, measure_unit from '{tmp_pq_path}'").fetchall()
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


if __name__ == "__main__":
    unittest.main()
