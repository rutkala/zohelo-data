from __future__ import annotations

import gzip
from hashlib import sha256
import json
from pathlib import Path
import sys
import tempfile
import unittest

import duckdb


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from eurostat_bulk_decode import (  # noqa: E402
    EurostatBulkDecodeError,
    decode_full_distribution,
)


class EurostatBulkDecodeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def _source(self, body: bytes) -> tuple[Path, str]:
        source = self.root / "distribution.tsv.gz"
        source.write_bytes(gzip.compress(body, mtime=0))
        return source, sha256(source.read_bytes()).hexdigest()

    def _decode(self, source: Path, digest: str, output: Path | None = None):
        return decode_full_distribution(
            source,
            output or self.root / "bronze.parquet",
            dataset_id="demo_test",
            retrieved_at_utc="2026-09-27T03:30:00Z",
            raw_sha256=digest,
            raw_file_id="drive-file-1",
            source_version="2026-09-26T23:00:00+0200",
        )

    def test_decodes_every_cell_with_complete_key_and_lossless_value_text(self):
        source, digest = self._source(
            (
                "freq,unit,geo\\TIME_PERIOD\t2020 \t2021-01 \t2022-Q1 \n"
                "A,NR,PL\t123.00 p\t:\t\n"
                "A,NR,DE\t-4.5E+2 \t: z\t789 e\n"
            ).encode("utf-8")
        )
        report = self._decode(source, digest)

        self.assertEqual(report["source_rows"], 2)
        self.assertEqual(report["observation_cells"], 6)
        self.assertEqual(report["missing_cells"], 3)
        self.assertEqual(report["empty_cells"], 1)
        self.assertEqual(report["colon_cells"], 2)
        self.assertEqual(report["flagged_cells"], 3)
        self.assertRegex(report["output_sha256"], r"^[0-9a-f]{64}$")

        with duckdb.connect() as connection:
            rows = connection.execute(
                """
                SELECT period_key, dimension_key_json, value_text, value_numeric,
                       status_code, is_missing, missing_reason, raw_sha256,
                       source_row_number, source_period_position
                FROM read_parquet(?)
                ORDER BY geo_code DESC, period_key
                """,
                [str(self.root / "bronze.parquet")],
            ).fetchall()
        self.assertEqual(rows[0][0], "2020")
        self.assertEqual(
            json.loads(rows[0][1]),
            {"freq": "A", "geo": "PL", "time": "2020", "unit": "NR"},
        )
        self.assertEqual(rows[0][2:7], ("123.00", 123.0, "p", False, None))
        self.assertEqual(rows[0][7], digest)
        self.assertEqual(rows[0][8:], (2, 1))
        self.assertEqual(rows[1][2], ":")
        self.assertEqual(rows[1][5:7], (True, "colon"))
        self.assertEqual(rows[2][2], "")
        self.assertEqual(rows[2][5:7], (True, "empty"))

    def test_header_only_distribution_produces_typed_empty_parquet(self):
        source, digest = self._source(b"freq,geo\\TIME_PERIOD\t2025 \n")
        report = self._decode(source, digest)
        self.assertEqual(report["observation_cells"], 0)
        with duckdb.connect() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM read_parquet(?)",
                    [str(self.root / "bronze.parquet")],
                ).fetchone()[0],
                0,
            )

    def test_rejects_receipt_drift_duplicate_keys_and_malformed_cells(self):
        source, digest = self._source(
            b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\t1\nA,PL\t2\n"
        )
        with self.assertRaisesRegex(EurostatBulkDecodeError, "SHA-256"):
            self._decode(source, "0" * 64)
        with self.assertRaisesRegex(EurostatBulkDecodeError, "repeats"):
            self._decode(source, digest)

        malformed, malformed_digest = self._source(
            b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\tunknown e\n"
        )
        with self.assertRaisesRegex(EurostatBulkDecodeError, "non-numeric"):
            self._decode(malformed, malformed_digest, self.root / "other.parquet")

    def test_rejects_stale_output_and_invalid_series_arity(self):
        source, digest = self._source(
            b"freq,unit,geo\\TIME_PERIOD\t2025 \nA,PL\t1\n"
        )
        with self.assertRaisesRegex(EurostatBulkDecodeError, "series key"):
            self._decode(source, digest)
        output = self.root / "existing.parquet"
        output.write_bytes(b"preserve")
        with self.assertRaisesRegex(EurostatBulkDecodeError, "fresh"):
            self._decode(source, digest, output)
        self.assertEqual(output.read_bytes(), b"preserve")


if __name__ == "__main__":
    unittest.main()
