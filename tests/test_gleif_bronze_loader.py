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

from gleif_bronze_loader import (
    transform_gleif_rr_to_bronze,
    transform_gleif_repex_to_bronze,
    transform_gleif_lei2_to_bronze,
    run_gleif_bronze_transformation,
)


class TestGleifBronzeLoader(unittest.TestCase):
    def test_gleif_bronze_transformation_pipeline(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            landing_dir = tmp_path / "landing"
            landing_dir.mkdir()
            bronze_dir = tmp_path / "bronze"
            bronze_dir.mkdir()

            # 1. Mock Relationship Records CSV & Zip
            rr_csv_content = (
                '"Relationship.StartNode.NodeID","Relationship.StartNode.NodeIDType","Relationship.EndNode.NodeID","Relationship.EndNode.NodeIDType","Relationship.RelationshipType","Relationship.RelationshipStatus","Relationship.Period.1.startDate","Relationship.Period.1.endDate","Registration.RegistrationStatus","Registration.ManagingLOU"\n'
                '"001GPB6A9XPE8XJICC14","LEI","5493001Z012YSB2A0K51","LEI","IS_DIRECTLY_CONSOLIDATED_BY","ACTIVE","2012-11-29T00:00:00Z","","PUBLISHED","5493001KJTIIGC8Y1R12"\n'
                '"004L5FPTUREIWK9T2N63","LEI","5493001Z012YSB2A0K51","LEI","IS_ULTIMATELY_CONSOLIDATED_BY","ACTIVE","2015-01-01T00:00:00Z","","PUBLISHED","5493001KJTIIGC8Y1R12"\n'
            )
            rr_zip = landing_dir / "gleif_goldencopy_rr.csv.zip"
            with zipfile.ZipFile(rr_zip, "w") as z:
                z.writestr("test_rr.csv", rr_csv_content)

            # 2. Mock Reporting Exceptions CSV & Zip
            repex_csv_content = (
                '"LEI","Exception.Category","Exception.Reason.1","Exception.Reference.1"\n'
                '"001GPB6A9XPE8XJICC14","DIRECT_ACCOUNTING_CONSOLIDATION_PARENT","NON_CONSOLIDATING",""\n'
            )
            repex_zip = landing_dir / "gleif_goldencopy_repex.csv.zip"
            with zipfile.ZipFile(repex_zip, "w") as z:
                z.writestr("test_repex.csv", repex_csv_content)

            # 3. Mock LEI2 CSV & Zip
            lei2_csv_content = (
                '"LEI","Entity.LegalName","Entity.LegalAddress.Country","Entity.LegalAddress.City","Entity.LegalAddress.PostalCode","Entity.HeadquartersAddress.Country","Entity.HeadquartersAddress.City","Entity.EntityStatus","Entity.EntityCategory","Entity.LegalForm.EntityLegalFormCode","Registration.ValidationAuthority.ValidationAuthorityID","Registration.ValidationAuthority.ValidationAuthorityEntityID","Registration.InitialRegistrationDate","Registration.LastUpdateDate","Registration.RegistrationStatus","Registration.ManagingLOU"\n'
                '"001GPB6A9XPE8XJICC14","Test Polish Subsidiary Sp. z o.o.","PL","Warszawa","00-001","PL","Warszawa","ACTIVE","GENERAL","HZEH","RA000484","0000123456","2012-11-29T16:33:00Z","2026-05-27T17:40:22Z","ISSUED","5493001KJTIIGC8Y1R12"\n'
                '"5493001Z012YSB2A0K51","Global Parent Corp","US","New York","10001","US","New York","ACTIVE","GENERAL","T91T","RA000602","4386463","2012-06-06T15:56:00Z","2026-05-27T17:40:22Z","ISSUED","5493001KJTIIGC8Y1R12"\n'
            )
            lei2_zip = landing_dir / "gleif_goldencopy_lei2.csv.zip"
            with zipfile.ZipFile(lei2_zip, "w") as z:
                z.writestr("test_lei2.csv", lei2_csv_content)

            # Run transformation
            res = run_gleif_bronze_transformation(
                landing_workspace=landing_dir,
                bronze_workspace=bronze_dir,
                skip_upload=True,
            )

            self.assertEqual(res["status"], "completed_locally")
            self.assertEqual(res["results"]["relationship_records"]["rows"], 2)
            self.assertEqual(res["results"]["reporting_exceptions"]["rows"], 1)
            self.assertEqual(res["results"]["lei_records"]["rows"], 2)

            # Query Parquet tables with DuckDB
            con = duckdb.connect()
            rr_df = con.execute(f"SELECT * FROM read_parquet('{bronze_dir}/br_gleif_relationship_records.parquet')").fetchdf()
            self.assertEqual(len(rr_df), 2)
            self.assertEqual(rr_df["child_lei"].iloc[0], "001GPB6A9XPE8XJICC14")
            self.assertEqual(rr_df["relationship_type"].iloc[0], "IS_DIRECTLY_CONSOLIDATED_BY")

            repex_df = con.execute(f"SELECT * FROM read_parquet('{bronze_dir}/br_gleif_reporting_exceptions.parquet')").fetchdf()
            self.assertEqual(len(repex_df), 1)
            self.assertEqual(repex_df["exception_category"].iloc[0], "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT")

            lei_df = con.execute(f"SELECT * FROM read_parquet('{bronze_dir}/br_gleif_lei2.parquet')").fetchdf()
            self.assertEqual(len(lei_df), 2)
            pl_entity = lei_df[lei_df["legal_country"] == "PL"].iloc[0]
            self.assertEqual(pl_entity["legal_city"], "Warszawa")
            self.assertEqual(pl_entity["validation_authority_id"], "RA000484")
            self.assertEqual(pl_entity["validation_authority_entity_id"], "0000123456")
            con.close()


if __name__ == "__main__":
    unittest.main()
