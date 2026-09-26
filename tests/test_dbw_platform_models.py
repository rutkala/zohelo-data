"""Exercise GUS DBW (Dziedzinowe Bazy Wiedzy) platform dbt models and marts offline with DuckDB."""
import os
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import dbw_platform  # noqa: E402


class TestDBWPlatformModels(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="zohelo-dbw-test-")
        cls.root = Path(cls.temp_dir.name)
        cls.data_root = cls.root / "data"
        cls.release_id = "a" * 64
        cls.bronze_dir = cls.data_root / "02_bronze" / "gus_dbw" / "releases" / cls.release_id
        (cls.bronze_dir / "taxonomy").mkdir(parents=True, exist_ok=True)
        (cls.bronze_dir / "metadata").mkdir(parents=True, exist_ok=True)
        (cls.bronze_dir / "dictionaries").mkdir(parents=True, exist_ok=True)
        (cls.bronze_dir / "observations").mkdir(parents=True, exist_ok=True)
        cls.database = cls.root / "dbw.duckdb"
        cls.target = cls.root / "target"

        con = duckdb.connect()

        # 1. Indicators taxonomy
        con.execute(f"""
            CREATE TABLE ind AS
            SELECT
                1023::BIGINT AS indicator_id,
                'Eksport towarów i usług' AS indicator_name,
                'Exports of goods and services' AS indicator_name_en,
                'Gospodarka' AS thematic_area,
                'Handel zagraniczny' AS domain,
                'Gospodarka > Handel zagraniczny > Eksport' AS taxonomy_path,
                'dbw-1023' AS node_id,
                'dbw-10' AS parent_id,
                '2026-09-20 12:00:00'::VARCHAR AS processed_at_utc
        """)
        con.execute(f"COPY ind TO '{cls.bronze_dir / 'taxonomy' / 'br_dbw_indicators.parquet'}' (FORMAT PARQUET)")

        # 2. Indicators metadata
        con.execute(f"""
            CREATE TABLE meta AS
            SELECT
                1023::BIGINT AS indicator_id,
                'Eksport' AS metric_name,
                'Exports' AS metric_name_en,
                'Wartość eksportu towarów i usług w cenach bieżących' AS description,
                'Roczna' AS frequency,
                'mln zł' AS measure_unit,
                'GUS' AS data_source,
                'Ustawa o statystyce publicznej' AS legal_basis,
                '2026-01-15' AS last_update,
                '2026-09-20 12:00:00'::VARCHAR AS processed_at_utc
        """)
        con.execute(f"COPY meta TO '{cls.bronze_dir / 'metadata' / 'br_dbw_metadata.parquet'}' (FORMAT PARQUET)")

        # 3. Dictionaries
        con.execute(f"""
            CREATE TABLE dicts AS
            SELECT
                1023::BIGINT AS indicator_id,
                'id_wymiar_1' AS column_name,
                'Rodzaje towarów' AS dictionary_name,
                33617::BIGINT AS element_id,
                'Ogółem' AS element_name,
                '2026-09-20 12:00:00'::VARCHAR AS processed_at_utc
        """)
        con.execute(f"COPY dicts TO '{cls.bronze_dir / 'dictionaries' / 'br_dbw_dictionaries.parquet'}' (FORMAT PARQUET)")

        # 4. Observations
        con.execute(f"""
            CREATE TABLE obs AS
            SELECT
                1023::BIGINT AS indicator_id,
                1075::BIGINT AS przekroj_id,
                2::BIGINT AS wymiar_1,
                33617::BIGINT AS pozycja_1,
                CAST(NULL AS BIGINT) AS wymiar_2,
                CAST(NULL AS BIGINT) AS pozycja_2,
                CAST(NULL AS BIGINT) AS wymiar_3,
                CAST(NULL AS BIGINT) AS pozycja_3,
                CAST(NULL AS BIGINT) AS wymiar_4,
                CAST(NULL AS BIGINT) AS pozycja_4,
                CAST(NULL AS BIGINT) AS wymiar_5,
                CAST(NULL AS BIGINT) AS pozycja_5,
                CAST(NULL AS BIGINT) AS wymiar_6,
                CAST(NULL AS BIGINT) AS pozycja_6,
                CAST(NULL AS BIGINT) AS wymiar_7,
                CAST(NULL AS BIGINT) AS pozycja_7,
                CAST(NULL AS BIGINT) AS wymiar_8,
                CAST(NULL AS BIGINT) AS pozycja_8,
                CAST(NULL AS BIGINT) AS wymiar_9,
                CAST(NULL AS BIGINT) AS pozycja_9,
                282::INTEGER AS okres_id,
                1::INTEGER AS sposob_prezentacji_miara_id,
                2022::INTEGER AS period_year,
                '123456.78' AS wartosc_raw,
                123456.78::DOUBLE AS wartosc_numeric,
                2::INTEGER AS precyzja,
                CAST(NULL AS INTEGER) AS brak_wartosci_id,
                CAST(NULL AS INTEGER) AS tajnosci_id,
                CAST(NULL AS INTEGER) AS flaga_id,
                'Baza_1023.zip' AS raw_archive_file,
                1::BIGINT AS source_row_number,
                '2026-09-20 12:00:00'::VARCHAR AS processed_at_utc
        """)
        con.execute(f"COPY obs TO '{cls.bronze_dir / 'observations' / 'part_1023.parquet'}' (FORMAT PARQUET)")
        con.close()
        observation = cls.bronze_dir / "observations" / "part_1023.parquet"
        dictionary = cls.bronze_dir / "dictionaries" / "br_dbw_dictionaries.parquet"
        dictionary_part = cls.bronze_dir / "dictionaries" / "dict_1023.parquet"
        dictionary_part.write_bytes(dictionary.read_bytes())
        cls.marker = cls.bronze_dir / "_control" / f"bronze-complete-v1-{cls.release_id}.json"
        cls.marker.parent.mkdir(parents=True, exist_ok=True)
        content_inventory = lambda paths: hashlib.sha256(
            "\n".join(
                f"{path.name}\0{hashlib.sha256(path.read_bytes()).hexdigest()}"
                for path in sorted(paths)
            ).encode()
        ).hexdigest()
        marker = {
            "schema_version": 1,
            "record_type": "gus_dbw_bronze_completion",
            "source_id": "gus_dbw",
            "status": "complete_native_snapshot",
            "release_id": cls.release_id,
            "completed_indicators": 1,
            "observation_partitions": 1,
            "dictionary_partitions": 1,
            "observation_inventory_sha256": hashlib.sha256(b"part_1023.parquet").hexdigest(),
            "dictionary_inventory_sha256": hashlib.sha256(b"dict_1023.parquet").hexdigest(),
            "observation_content_inventory_sha256": content_inventory([observation]),
            "dictionary_content_inventory_sha256": content_inventory([dictionary_part]),
            "taxonomy_sha256": hashlib.sha256((cls.bronze_dir / "taxonomy" / "br_dbw_indicators.parquet").read_bytes()).hexdigest(),
            "metadata_sha256": hashlib.sha256((cls.bronze_dir / "metadata" / "br_dbw_metadata.parquet").read_bytes()).hexdigest(),
            "consolidated_dictionary_sha256": hashlib.sha256(dictionary.read_bytes()).hexdigest(),
        }
        cls.marker.write_text(json.dumps(marker), encoding="utf-8")

        # Run dbt build
        env = dict(os.environ)
        env.update(
            ZOHELO_DATA_ROOT=str(cls.data_root),
            ZOHELO_DBW_BRONZE_RELEASE_ID=cls.release_id,
            ZOHELO_DUCKDB_PATH=str(cls.database),
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            DO_NOT_TRACK="1",
        )
        cmd = [
            sys.executable, "-c", "from dbt.cli.main import cli; cli()", "build",
            "--profiles-dir", str(REPO_ROOT),
            "--target-path", str(cls.target),
            "--select",
            "+stg_dbw_indicators",
            "+stg_dbw_metadata",
            "+stg_dbw_dictionaries",
            "+stg_dbw_observations",
            "+dim_dbw_indicator",
            "+fact_dbw_observations",
            "+mart_dbw_coverage",
            "--vars", '{"enable_gus_dbw": true}',
            "--threads", "1",
            "--no-partial-parse",
        ]
        res = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
        if res.returncode != 0:
            raise AssertionError(f"DBW dbt build failed:\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}")

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test_dim_dbw_indicator(self):
        con = duckdb.connect(str(self.database))
        res = con.execute('SELECT indicator_key, indicator_name, measure_unit, domain_name FROM "04_gold"."dim_dbw_indicator"').fetchall()
        con.close()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], (1023, "Eksport towarów i usług", "mln zł", "Handel zagraniczny"))

    def test_fact_dbw_observations(self):
        con = duckdb.connect(str(self.database))
        res = con.execute('SELECT indicator_key, period_year, wartosc_numeric, raw_archive_file FROM "04_gold"."fact_dbw_observations"').fetchall()
        con.close()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], (1023, 2022, 123456.78, "Baza_1023.zip"))

    def test_mart_dbw_coverage(self):
        con = duckdb.connect(str(self.database))
        res = con.execute('SELECT total_indicators, total_observations, earliest_year, latest_year FROM "04_gold"."mart_dbw_coverage"').fetchall()
        con.close()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], (1, 1, 2022, 2022))


class TestDBWPlatformExports(unittest.TestCase):
    def test_observation_exports_are_bounded_unique_and_row_complete(self):
        with tempfile.TemporaryDirectory(prefix="zohelo-dbw-export-") as temporary:
            root = Path(temporary)
            with duckdb.connect() as connection:
                connection.execute("""
                    create table observations as
                    select
                        case when i % 2 = 0 then 101 else 202 end::bigint as indicator_id,
                        i::bigint as source_row_number,
                        md5(i::varchar) || md5((i * 17)::varchar) as payload
                    from range(4000) source(i)
                """)
                with patch.object(
                    dbw_platform, "MAX_RELEASE_PART_BYTES", 16 * 1024
                ):
                    paths = dbw_platform._copy_relation(
                        connection,
                        "observations",
                        root / "bronze_dbw_observations",
                        "bronze_dbw_observations",
                        4000,
                    )
                self.assertGreater(len(paths), 2)
                self.assertEqual(len(paths), len({path.name for path in paths}))
                self.assertTrue(all(path.stat().st_size <= 16 * 1024 for path in paths))
                rendered = ",".join(dbw_platform._quoted_path(path) for path in paths)
                rows, distinct_rows, indicators = connection.execute(
                    f"select count(*), count(distinct source_row_number), "
                    f"count(distinct indicator_id) from read_parquet([{rendered}])"
                ).fetchone()
                self.assertEqual((rows, distinct_rows, indicators), (4000, 4000, 2))
                columns = [
                    row[0] for row in connection.execute(
                        f"describe select * from read_parquet([{rendered}])"
                    ).fetchall()
                ]
                self.assertEqual(
                    columns, ["indicator_id", "source_row_number", "payload"]
                )

    def test_unpartitioned_oversize_fails_closed(self):
        with tempfile.TemporaryDirectory(prefix="zohelo-dbw-export-") as temporary:
            root = Path(temporary)
            with duckdb.connect() as connection:
                connection.execute("""
                    create table metadata as
                    select i::bigint as indicator_id, md5(i::varchar) as value
                    from range(1000) source(i)
                """)
                with patch.object(dbw_platform, "MAX_RELEASE_PART_BYTES", 512):
                    with self.assertRaisesRegex(
                        RuntimeError, "needs a reviewed partition key"
                    ):
                        dbw_platform._copy_relation(
                            connection,
                            "metadata",
                            root / "bronze_dbw_metadata",
                            "bronze_dbw_metadata",
                            1000,
                        )


if __name__ == "__main__":
    unittest.main()
