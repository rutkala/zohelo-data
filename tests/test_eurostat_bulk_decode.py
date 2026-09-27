from __future__ import annotations

import gzip
from hashlib import md5, sha256
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

    def _source(self, body: bytes) -> tuple[Path, dict[str, object]]:
        source = self.root / "distribution.tsv.gz"
        source.write_bytes(gzip.compress(body, mtime=0))
        raw = source.read_bytes()
        digest = sha256(raw).hexdigest()
        receipt = {
            "schema_version": 1,
            "source_id": "eurostat",
            "accepted": True,
            "kind": "full_distribution",
            "distribution": {
                "dataset_id": "demo_test",
                "kind": "eurostat_tsv_gzip",
                "version": "2026-09-26T23:00:00+0200",
                "url": "https://ec.europa.eu/eurostat/demo_test.tsv.gz",
                "params": {},
            },
            "retrieved_at_utc": "2026-09-27T03:30:00+00:00",
            "raw": {
                "id": "drive-file-1",
                "name": f"raw-{digest}.bin",
                "sha256": digest,
                "md5": md5(raw, usedforsecurity=False).hexdigest(),
                "size_bytes": len(raw),
            },
            "inspection": {"status": "complete"},
        }
        receipt_path = self.root / "receipt.json"
        receipt_path.write_text(json.dumps(receipt, sort_keys=True), encoding="utf-8")
        receipt_raw = receipt_path.read_bytes()
        return source, {
            "path": receipt_path,
            "id": "receipt-drive-id",
            "sha256": sha256(receipt_raw).hexdigest(),
            "size_bytes": len(receipt_raw),
        }

    def _decode(self, source: Path, receipt: dict[str, object], output: Path | None = None):
        return decode_full_distribution(
            source,
            receipt["path"],
            output or self.root / "bronze.parquet",
            receipt_descriptor={
                key: receipt[key] for key in ("id", "sha256", "size_bytes")
            },
        )

    def test_decodes_every_cell_with_complete_key_and_lossless_value_text(self):
        source, receipt = self._source(
            (
                "freq,unit,geo\\TIME_PERIOD\t2020 \t2021-01 \t2022-Q1 \n"
                "A,NR,PL\t123.00 p\t:\t\n"
                "A,NR,DE\t-4.5E+2 \t: z\t789 e\n"
            ).encode("utf-8")
        )
        report = self._decode(source, receipt)

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
        self.assertEqual(rows[0][7], json.loads(receipt["path"].read_text())["raw"]["sha256"])
        self.assertEqual(rows[0][8:], (2, 1))
        self.assertEqual(rows[1][2], ":")
        self.assertEqual(rows[1][5:7], (True, "colon"))
        self.assertEqual(rows[2][2], "")
        self.assertEqual(rows[2][5:7], (True, "empty"))

    def test_header_only_distribution_produces_typed_empty_parquet(self):
        source, receipt = self._source(b"freq,geo\\TIME_PERIOD\t2025 \n")
        report = self._decode(source, receipt)
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
        source, receipt = self._source(
            b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\t1\nA,PL\t2\n"
        )
        drifted = {**receipt, "sha256": "0" * 64}
        with self.assertRaisesRegex(EurostatBulkDecodeError, "immutable descriptor"):
            self._decode(source, drifted)
        with self.assertRaisesRegex(EurostatBulkDecodeError, "repeats"):
            self._decode(source, receipt)

        malformed, malformed_receipt = self._source(
            b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\tunknown e\n"
        )
        with self.assertRaisesRegex(EurostatBulkDecodeError, "non-numeric"):
            self._decode(malformed, malformed_receipt, self.root / "other.parquet")

    def test_rejects_stale_output_and_invalid_series_arity(self):
        source, receipt = self._source(
            b"freq,unit,geo\\TIME_PERIOD\t2025 \nA,PL\t1\n"
        )
        with self.assertRaisesRegex(EurostatBulkDecodeError, "series key"):
            self._decode(source, receipt)
        output = self.root / "existing.parquet"
        output.write_bytes(b"preserve")
        with self.assertRaisesRegex(EurostatBulkDecodeError, "fresh"):
            self._decode(source, receipt, output)
        self.assertEqual(output.read_bytes(), b"preserve")

    def test_derives_provenance_from_exact_accepted_receipt(self):
        source, receipt = self._source(b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\t7\n")
        report = self._decode(source, receipt)
        self.assertEqual(report["dataset_id"], "demo_test")
        with duckdb.connect() as connection:
            row = connection.execute(
                """
                SELECT dataset_id, source_version, retrieved_at_utc, raw_file_id,
                       receipt_file_id, receipt_sha256, receipt_size_bytes
                FROM read_parquet(?)
                """,
                [str(self.root / "bronze.parquet")],
            ).fetchone()
        self.assertEqual(row[0], "demo_test")
        self.assertEqual(row[1], "2026-09-26T23:00:00+0200")
        self.assertEqual(row[3:], (
            "drive-file-1", "receipt-drive-id", receipt["sha256"], receipt["size_bytes"]
        ))

        accepted = json.loads(receipt["path"].read_text())
        accepted["distribution"]["dataset_id"] = "wrong_dataset"
        receipt["path"].write_text(json.dumps(accepted, sort_keys=True), encoding="utf-8")
        with self.assertRaisesRegex(EurostatBulkDecodeError, "immutable descriptor"):
            self._decode(source, receipt, self.root / "drifted.parquet")


if __name__ == "__main__":
    unittest.main()
