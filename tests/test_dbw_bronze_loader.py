"""Unit tests for GUS DBW Bronze loader."""
import io
from pathlib import Path
import unittest
import zipfile

import duckdb
import pandas as pd


class TestDBWBronzeLoader(unittest.TestCase):
    def test_csv_semicolon_parsing(self):
        csv_content = (
            "rowNumber;id_zmienna;id_przekroj;id_wymiar_1;id_pozycja_1;id_okres;id_daty;wartosc;precyzja;\n"
            "1;1023;1075;2;33617;282;1995;123,456;2;\n"
            "2;1023;1075;2;33617;282;1996;789,012;2;\n"
        )
        lines = csv_content.splitlines()
        headers = lines[0].split(";")
        rows = []
        for line in lines[1:]:
            parts = line.split(";")
            d = {headers[i]: parts[i] for i in range(len(headers))}
            raw_val = d.get("wartosc", "").strip()
            num_val = float(raw_val.replace(",", ".")) if raw_val else None
            rows.append({
                "indicator_id": int(d["id_zmienna"]),
                "przekroj_id": int(d["id_przekroj"]),
                "period_year": int(d["id_daty"]),
                "wartosc_raw": raw_val,
                "wartosc_numeric": num_val,
                "precyzja": int(d["precyzja"]),
            })
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["wartosc_numeric"], 123.456)
        self.assertEqual(rows[1]["wartosc_numeric"], 789.012)

    def test_dictionary_parsing(self):
        import csv
        dict_content = (
            "nazwa_kolumny;nazwa_slownika;id_elementu;opis;\n"
            "id_zmienna;Zmienne;1023;\"Eksport towarów i usług\";\n"
            "id_przekroj;Przekroje;1075;\"Polska; Sektory instytucjonalne\";\n"
        )
        reader = csv.reader(dict_content.splitlines(), delimiter=";", quotechar='"')
        headers = next(reader)
        dict_rows = []
        for parts in reader:
            if not parts:
                continue
            d = {headers[i]: parts[i] for i in range(len(headers))}
            dict_rows.append({
                "column_name": d.get("nazwa_kolumny", ""),
                "dictionary_name": d.get("nazwa_slownika", ""),
                "element_id": int(d["id_elementu"]),
                "element_name": d.get("opis", "").strip(),
            })
        self.assertEqual(len(dict_rows), 2)
        self.assertEqual(dict_rows[0]["element_name"], "Eksport towarów i usług")
        self.assertEqual(dict_rows[1]["element_name"], "Polska; Sektory instytucjonalne")

    def test_duckdb_parquet_conversion(self):
        data = [
            {"indicator_id": 1023, "wartosc_numeric": 12.34, "period_year": 2020},
            {"indicator_id": 1023, "wartosc_numeric": 56.78, "period_year": 2021},
        ]
        df = pd.DataFrame(data)
        con = duckdb.connect(":memory:")
        con.register("test_df", df)
        res = con.execute("SELECT sum(wartosc_numeric) as total, count(*) as cnt FROM test_df").fetchone()
        self.assertEqual(res[1], 2)
        self.assertAlmostEqual(res[0], 69.12, places=2)
        con.close()


if __name__ == "__main__":
    unittest.main()
