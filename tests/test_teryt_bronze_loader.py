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

from teryt_bronze_loader import transform_teryt_bronze


class TestTerytBronzeLoader(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.landing_dir = Path(self.temp_dir.name) / "landing"
        self.output_dir = Path(self.temp_dir.name) / "bronze"
        self.landing_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)

        # 1. Synthetic TERC
        terc_csv = (
            "WOJ;POW;GMI;RODZ;NAZWA;NAZWA_DOD;STAN_NA\n"
            "02;;;;DOLNOŚLĄSKIE;województwo;2026-01-01\n"
            "02;01;;;bolesławiecki;powiat;2026-01-01\n"
            "02;01;01;1;Bolesławiec;gmina miejska;2026-01-01\n"
        )
        with zipfile.ZipFile(self.landing_dir / "TERC_Urzedowy_test.zip", "w") as z:
            z.writestr("TERC_Urzedowy_test.csv", terc_csv.encode("utf-8"))

        # 2. Synthetic SIMC
        simc_csv = (
            "WOJ;POW;GMI;RODZ_GMI;RM;MZ;NAZWA;SYM;SYMPOD;STAN_NA\n"
            "02;01;01;1;96;1;Bolesławiec;0935967;0935967;2026-01-01\n"
            "02;01;02;2;01;1;Bolesławice;0189397;0189397;2026-01-01\n"
            "02;01;02;2;00;1;Kolonia;0189405;0189397;2026-01-01\n"
        )
        with zipfile.ZipFile(self.landing_dir / "SIMC_Urzedowy_test.zip", "w") as z:
            z.writestr("SIMC_Urzedowy_test.csv", simc_csv.encode("utf-8"))

        # 3. Synthetic ULIC (including quotes in street name)
        ulic_csv = (
            "WOJ;POW;GMI;RODZ_GMI;SYM;SYM_UL;CECHA;NAZWA_1;NAZWA_2;STAN_NA\n"
            "02;01;01;1;0935967;00100;ul.;Mickiewicza;Adama;2026-09-18\n"
            "02;01;01;1;0935967;00200;pl.;Zamkowy;;2026-09-18\n"
            '02;01;01;1;0935967;03975;ul.;Dobrzańskiego;"Hubala" Henryka;2026-09-18\n'
        )
        with zipfile.ZipFile(self.landing_dir / "ULIC_Urzedowy_test.zip", "w") as z:
            z.writestr("ULIC_Urzedowy_test.csv", ulic_csv.encode("utf-8"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_transform_teryt_bronze(self):
        result = transform_teryt_bronze(
            landing_dir=self.landing_dir,
            output_dir=self.output_dir,
            skip_upload=True,
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["tables"]["br_teryt_terc.parquet"], 3)
        self.assertEqual(result["tables"]["br_teryt_simc.parquet"], 3)
        self.assertEqual(result["tables"]["br_teryt_ulic.parquet"], 3)

        con = duckdb.connect()

        # Check TERC parquet
        terc_rows = con.execute(f"""
            SELECT woj, pow, gmi, level, teryt_code, parent_teryt_code, nazwa
            FROM '{self.output_dir / "br_teryt_terc.parquet"}'
            ORDER BY teryt_code
        """).fetchall()
        self.assertEqual(terc_rows[0], ("02", None, None, "wojewodztwo", "02", None, "DOLNOŚLĄSKIE"))
        self.assertEqual(terc_rows[1], ("02", "01", None, "powiat", "0201", "02", "bolesławiecki"))
        self.assertEqual(terc_rows[2], ("02", "01", "01", "gmina", "0201011", "0201", "Bolesławiec"))

        # Check SIMC parquet
        simc_rows = con.execute(f"""
            SELECT sym, sympod, is_parent_locality, nazwa, teryt_gmina_code
            FROM '{self.output_dir / "br_teryt_simc.parquet"}'
            ORDER BY sym
        """).fetchall()
        self.assertEqual(simc_rows[0], ("0189397", "0189397", True, "Bolesławice", "0201022"))
        self.assertEqual(simc_rows[1], ("0189405", "0189397", False, "Kolonia", "0201022"))
        self.assertEqual(simc_rows[2], ("0935967", "0935967", True, "Bolesławiec", "0201011"))

        # Check ULIC parquet
        ulic_rows = con.execute(f"""
            SELECT sym_ul, cecha, nazwa_1, nazwa_2, full_street_name
            FROM '{self.output_dir / "br_teryt_ulic.parquet"}'
            ORDER BY sym_ul
        """).fetchall()
        self.assertEqual(ulic_rows[0], ("00100", "ul.", "Mickiewicza", "Adama", "ul. Adama Mickiewicza"))
        self.assertEqual(ulic_rows[1], ("00200", "pl.", "Zamkowy", None, "pl. Zamkowy"))
        self.assertEqual(ulic_rows[2], ("03975", "ul.", "Dobrzańskiego", '"Hubala" Henryka', 'ul. "Hubala" Henryka Dobrzańskiego'))

        con.close()


if __name__ == "__main__":
    unittest.main()
