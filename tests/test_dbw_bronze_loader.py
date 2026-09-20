"""Unit tests for GUS DBW Bronze loader."""
import io
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock
import zipfile

import duckdb
import pandas as pd

from dbw_bronze_loader import (
    DBWBronzeLoader,
    DBWLandingIncompleteError,
    _require_production_context,
    _restore_verified_drive_file,
    _upload_file_to_drive,
    validate_landing_completion,
)


class TestDBWBronzeLoader(unittest.TestCase):
    def test_production_context_requires_main_actions_or_explicit_codespace(self):
        from unittest.mock import patch

        with patch.dict("os.environ", {"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/feature"}, clear=True):
            with self.assertRaises(PermissionError):
                _require_production_context()
        with patch.dict("os.environ", {"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/main"}, clear=True):
            _require_production_context()
        with patch.dict("os.environ", {}, clear=True):
            _require_production_context(allow_codespace=True)

    def _completion(self):
        return {
            "schema_version": 1,
            "record_type": "gus_dbw_landing_completion",
            "source_id": "gus_dbw",
            "status": "complete_current_catalogue",
            "landing_scope": "native_bytes_only",
            "catalogue_indicators": 1550,
            "completed_indicators": 1550,
            "pending_indicators": 0,
            "failed_indicators": 0,
            "bulk_complete": True,
            "metadata_complete": True,
            "catalogue_sha256": "a" * 64,
        }

    def test_landing_completion_gate_accepts_only_reconciled_full_scope(self):
        document = self._completion()
        self.assertIs(validate_landing_completion(document), document)

        document = self._completion()
        document["completed_indicators"] -= 1
        with self.assertRaises(DBWLandingIncompleteError):
            validate_landing_completion(document)

        document = self._completion()
        document["bulk_complete"] = False
        with self.assertRaises(DBWLandingIncompleteError):
            validate_landing_completion(document)

    def test_bronze_upload_preserves_mismatched_existing_object(self):
        storage = MagicMock()
        storage.drive_service.files.return_value.list.return_value.execute.return_value = {
            "files": [{
                "id": "old-id",
                "name": "part_7.parquet",
                "size": "4",
                "md5Checksum": "wrong",
                "appProperties": {"sha256": "wrong"},
            }]
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "part.parquet"
            path.write_bytes(b"new bytes")
            with self.assertRaisesRegex(RuntimeError, "prior object was preserved"):
                _upload_file_to_drive(storage, path, "part_7.parquet", "parent")
        storage.drive_service.files.return_value.delete.assert_not_called()
        storage.drive_service.files.return_value.create.assert_not_called()

    def test_verified_remote_output_is_restored_on_fresh_runner(self):
        raw = b"verified parquet bytes"
        storage = MagicMock()
        storage.drive_service.files.return_value.list.return_value.execute.return_value = {
            "files": [{
                "id": "remote", "name": "br_dbw_indicators.parquet",
                "size": str(len(raw)), "md5Checksum": hashlib.md5(raw).hexdigest(),
                "appProperties": {"sha256": hashlib.sha256(raw).hexdigest()},
            }]
        }
        storage.drive_service.files.return_value.get_media.return_value.execute.return_value = raw
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "br_dbw_indicators.parquet"
            self.assertTrue(_restore_verified_drive_file(
                storage, name=path.name, parent_id="taxonomy", local_path=path
            ))
            self.assertEqual(path.read_bytes(), raw)

    def test_release_receipts_bind_exact_metadata_and_bulk_membership(self):
        loader = object.__new__(DBWBronzeLoader)
        loader.landing_checkpoints = "checkpoints"
        loader.storage = MagicMock()
        catalogue_sha = "a" * 64
        receipts = []
        files = []
        for indicator_id in (7, 8):
            document = {
                "schema_version": 2,
                "record_type": "gus_dbw_indicator_completion",
                "indicator_id": indicator_id,
                "status": "completed",
                "bulk_complete": True,
                "metadata_complete": True,
                "catalogue_sha256": catalogue_sha,
                "files_landed": [
                    f"aggregates_{indicator_id}_pl--sha256-{'d' * 64}.json",
                    f"metryka_{indicator_id}--sha256-{'b' * 64}.csv",
                    f"{indicator_id}_history--sha256-{'c' * 64}.zip",
                ],
            }
            raw = json.dumps(document).encode()
            receipts.append(raw)
            files.append({
                "id": f"receipt-{indicator_id}",
                "name": f"completed-v2-{indicator_id}.json",
                "size": str(len(raw)),
                "md5Checksum": hashlib.md5(raw).hexdigest(),
                "appProperties": {
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "catalogue_sha256": catalogue_sha,
                    "checkpoint_schema": "2",
                    "checkpoint_status": "completed",
                    "bulk_complete": "true",
                    "metadata_complete": "true",
                },
            })
        loader.storage.drive_service.files.return_value.list.return_value.execute.return_value = {
            "files": files
        }
        media = loader.storage.drive_service.files.return_value.get_media
        media.side_effect = [
            MagicMock(execute=MagicMock(return_value=receipts[0])),
            MagicMock(execute=MagicMock(return_value=receipts[1])),
        ]
        completion = self._completion()
        completion["catalogue_indicators"] = 2
        completion["completed_indicators"] = 2
        bound = loader.load_release_receipts(completion)
        self.assertEqual(bound["indicator_ids"], {7, 8})
        self.assertEqual(len(bound["metadata_names"]), 2)
        self.assertEqual(len(bound["bulk_names"]), 2)

    def test_bronze_release_is_namespaced_by_catalogue_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            loader = object.__new__(DBWBronzeLoader)
            loader.base_workspace = Path(tmp)
            loader.storage = MagicMock()
            loader.session = "session"
            loader.releases_root = "releases"
            loader.storage.get_or_create_nested_folder.side_effect = lambda parts, **kwargs: (
                f"{kwargs['root_id']}/{'/'.join(parts)}"
            )
            completion = self._completion()
            completion["_completion_created_at_utc"] = "2026-09-20T18:00:00Z"
            loader.bind_release(completion)
            self.assertEqual(loader.release_id, "a" * 64)
            self.assertIn("a" * 64, loader.bronze_obs)
            self.assertEqual(loader.processed_at_utc, "2026-09-20T18:00:00Z")

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
