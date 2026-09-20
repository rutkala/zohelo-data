"""Unit tests for GUS DBW Bronze loader."""
import io
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import zipfile

import duckdb
import pandas as pd

from dbw_bronze_loader import (
    DBWBronzeLoader,
    DBWLandingIncompleteError,
    _require_production_context,
    _restore_verified_drive_file,
    _has_integrity_metadata,
    _membership_sha256,
    _snapshot_sha256,
    _upload_file_to_drive,
    _verify_native_bytes,
    validate_landing_completion,
    validate_full_release_selection,
)


class TestDBWBronzeLoader(unittest.TestCase):
    SNAPSHOT_ID = "123e4567-e89b-42d3-a456-426614174000"

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
            "schema_version": 2,
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
            "native_snapshot_id": self.SNAPSHOT_ID,
            "native_snapshot_sha256": "b" * 64,
        }

    def test_landing_completion_gate_accepts_only_reconciled_full_scope(self):
        document = self._completion()
        self.assertIs(validate_landing_completion(document), document)

        document = self._completion()
        document["completed_indicators"] -= 1
        with self.assertRaises(DBWLandingIncompleteError):
            validate_landing_completion(document)

    def test_partial_release_selection_is_rejected_before_writes(self):
        validate_full_release_selection(None, None)
        with self.assertRaisesRegex(ValueError, "complete production release"):
            validate_full_release_selection(10, None)
        with self.assertRaisesRegex(ValueError, "complete production release"):
            validate_full_release_selection(None, [7, 8])

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

    @patch("dbw_bronze_loader.time.sleep")
    @patch("dbw_bronze_loader._upload_control_bytes")
    @patch("dbw_bronze_loader.uuid.uuid4")
    def test_bronze_writer_lease_tombstones_losing_claim(
        self, mock_uuid, mock_upload, mock_sleep
    ):
        loader = object.__new__(DBWBronzeLoader)
        loader.storage = MagicMock()
        loader.landing_control = "control"
        loader.writer_lease_claim = None
        own = "123e4567-e89b-42d3-a456-426614174000"
        other = "023e4567-e89b-42d3-a456-426614174000"
        mock_uuid.return_value = own
        future = "2999-01-01T00:00:00+00:00"
        loader.storage.drive_service.files.return_value.list.return_value.execute.return_value = {
            "files": [
                {"createdTime": "2026-09-20T20:00:01Z", "appProperties": {
                    "record_type": "gus_dbw_bronze_writer_lease",
                    "claim_id": own, "expires_at_utc": future,
                }},
                {"createdTime": "2026-09-20T20:00:00Z", "appProperties": {
                    "record_type": "gus_dbw_bronze_writer_lease",
                    "claim_id": other, "expires_at_utc": future,
                }},
            ]
        }
        with self.assertRaisesRegex(RuntimeError, "Another host"):
            loader.acquire_writer_lease()
        mock_sleep.assert_called_once()
        self.assertEqual(mock_upload.call_count, 2)
        self.assertEqual(
            mock_upload.call_args_list[-1].kwargs["properties"]["released_claim_id"], own
        )

    def test_bronze_writer_lease_renews_same_owner_before_finalization(self):
        loader = object.__new__(DBWBronzeLoader)
        loader.writer_lease_owner = "owner"
        loader.writer_lease_claim = "old"
        with patch.object(loader, "_create_writer_lease_claim", return_value="new") as create, \
             patch.object(loader, "_verify_writer_lease") as verify, \
             patch.object(loader, "_release_writer_claim") as release:
            self.assertEqual(loader.renew_writer_lease(), "new")
        create.assert_called_once_with("owner")
        verify.assert_called_once_with("owner")
        release.assert_called_once_with("old")
        self.assertEqual(loader.writer_lease_claim, "new")

    def test_incomplete_session_stops_before_lease_renewal_and_finalization(self):
        source = (
            Path(__file__).resolve().parents[1] / "src" / "dbw_bronze_loader.py"
        ).read_text(encoding="utf-8")
        incomplete_gate = source.index("if completed_indicators != total_targets:")
        renewal = source.index("loader.renew_writer_lease()", incomplete_gate)
        consolidation = source.index("# Restore every persisted dictionary partition", renewal)
        self.assertLess(incomplete_gate, renewal)
        self.assertLess(renewal, consolidation)

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

    def test_resume_requires_both_verified_per_indicator_partitions(self):
        valid = {
            "size": "1001",
            "md5Checksum": "a" * 32,
            "appProperties": {"sha256": "b" * 64},
        }
        self.assertTrue(_has_integrity_metadata(valid, min_size=1000))
        self.assertFalse(_has_integrity_metadata({**valid, "md5Checksum": ""}, min_size=1000))
        self.assertFalse(_has_integrity_metadata(None, min_size=1000))

    def _release_receipt_fixture(self, *, corrupt_native_size: bool = False):
        loader = object.__new__(DBWBronzeLoader)
        loader.landing_checkpoints = "checkpoints"
        loader.landing_metadata = "metadata"
        loader.landing_bulk = "bulk"
        loader.storage = MagicMock()
        catalogue_sha = "a" * 64
        receipts = []
        receipt_files = []
        metadata_files = []
        bulk_files = []
        memberships = {}
        for indicator_id in (7, 8):
            landed_objects = []
            for role, name, folder_files in (
                ("aggregates", f"aggregates_{indicator_id}_pl--sha256-{'d' * 64}.json", metadata_files),
                ("metryka", f"metryka_{indicator_id}--sha256-{'b' * 64}.csv", metadata_files),
                ("bulk_zip", f"{indicator_id}_history--sha256-{'c' * 64}.zip", bulk_files),
            ):
                raw_native = f"{role}-{indicator_id}".encode()
                descriptor = {
                    "id": f"{role}-{indicator_id}",
                    "name": name,
                    "size": len(raw_native),
                    "sha256": hashlib.sha256(raw_native).hexdigest(),
                    "md5": hashlib.md5(raw_native).hexdigest(),
                    "role": role,
                    "source_name": (
                        f"aggregates_{indicator_id}_pl.json" if role == "aggregates"
                        else f"metryka_{indicator_id}.csv" if role == "metryka"
                        else f"{indicator_id}_history.zip"
                    ),
                }
                landed_objects.append(descriptor)
                folder_files.append({
                    "id": descriptor["id"],
                    "name": name,
                    "size": str(
                        len(raw_native) + (1 if corrupt_native_size and role == "bulk_zip" and indicator_id == 8 else 0)
                    ),
                    "md5Checksum": descriptor["md5"],
                    "appProperties": {"sha256": descriptor["sha256"]},
                })
            document = {
                "schema_version": 3,
                "record_type": "gus_dbw_indicator_completion",
                "indicator_id": indicator_id,
                "status": "completed",
                "bulk_complete": True,
                "metadata_complete": True,
                "catalogue_sha256": catalogue_sha,
                "native_snapshot_id": self.SNAPSHOT_ID,
                "files_landed": [item["name"] for item in landed_objects],
                "landed_objects": landed_objects,
                "expected_bulk_files": [f"{indicator_id}_history.zip"],
            }
            membership = _membership_sha256(landed_objects)
            memberships[indicator_id] = membership
            document["native_membership_sha256"] = membership
            raw = json.dumps(document).encode()
            receipts.append(raw)
            receipt_files.append({
                "id": f"receipt-{indicator_id}",
                "name": f"completed-v3-{self.SNAPSHOT_ID}-{indicator_id}.json",
                "size": str(len(raw)),
                "md5Checksum": hashlib.md5(raw).hexdigest(),
                "appProperties": {
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "catalogue_sha256": catalogue_sha,
                    "native_snapshot_id": self.SNAPSHOT_ID,
                    "native_membership_sha256": membership,
                    "checkpoint_schema": "3",
                    "checkpoint_status": "completed",
                    "bulk_complete": "true",
                    "metadata_complete": "true",
                },
            })
        def list_response(**kwargs):
            query = kwargs["q"]
            if "'metadata' in parents" in query:
                files = metadata_files
            elif "'bulk' in parents" in query:
                files = bulk_files
            else:
                files = receipt_files
            return MagicMock(execute=MagicMock(return_value={"files": files}))

        loader.storage.drive_service.files.return_value.list.side_effect = list_response
        media = loader.storage.drive_service.files.return_value.get_media
        media.side_effect = [
            MagicMock(execute=MagicMock(return_value=receipts[0])),
            MagicMock(execute=MagicMock(return_value=receipts[1])),
        ]
        completion = self._completion()
        completion["catalogue_indicators"] = 2
        completion["completed_indicators"] = 2
        completion["native_snapshot_sha256"] = _snapshot_sha256(
            self.SNAPSHOT_ID, memberships
        )
        return loader, completion

    def test_release_receipts_bind_exact_native_object_identities(self):
        loader, completion = self._release_receipt_fixture()
        bound = loader.load_release_receipts(completion)
        self.assertEqual(bound["indicator_ids"], {7, 8})
        self.assertEqual(set(bound["metadata_members"]), {"metryka-7", "metryka-8"})
        self.assertEqual(bound["metadata_owners"], {"metryka-7": 7, "metryka-8": 8})
        self.assertEqual(set(bound["bulk_members"]), {"bulk_zip-7", "bulk_zip-8"})
        self.assertEqual(
            {key: [item["id"] for item in value] for key, value in bound["bulk_by_indicator"].items()},
            {7: ["bulk_zip-7"], 8: ["bulk_zip-8"]},
        )

    def test_native_bytes_are_verified_before_parsing(self):
        raw = b"native bytes"
        descriptor = {
            "name": "7_history.zip",
            "size": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "md5": hashlib.md5(raw).hexdigest(),
        }
        _verify_native_bytes(raw, descriptor)
        with self.assertRaisesRegex(DBWLandingIncompleteError, "byte verification"):
            _verify_native_bytes(raw + b" changed", descriptor)

    def test_metadata_indicator_must_match_receipt_owner(self):
        raw = b"id_zmienna;nazwa;\n7;Wrong indicator;\n"
        loader = object.__new__(DBWBronzeLoader)
        loader.workspace = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(loader.workspace))
        loader.processed_at_utc = "2026-09-20T00:00:00Z"
        loader.landing_metadata = "metadata"
        loader.storage = MagicMock()
        loader.storage.drive_service.files.return_value.list.return_value.execute.return_value = {
            "files": [{"id": "metryka-8", "name": "metryka_8.csv"}]
        }
        loader.storage.drive_service.files.return_value.get_media.return_value.execute.return_value = raw
        descriptor = {
            "name": "metryka_8.csv", "size": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "md5": hashlib.md5(raw).hexdigest(),
        }
        with self.assertRaisesRegex(DBWLandingIncompleteError, "receipt ownership"):
            loader.build_metadata_table(
                allowed_objects={"metryka-8": descriptor},
                object_owners={"metryka-8": 8},
            )

    def test_release_receipt_rejects_native_identity_mismatch(self):
        loader, completion = self._release_receipt_fixture(corrupt_native_size=True)
        with self.assertRaisesRegex(DBWLandingIncompleteError, "receipt/native object mismatch"):
            loader.load_release_receipts(completion)

    def test_local_launchers_verify_background_lock_acquisition(self):
        repo_root = Path(__file__).resolve().parents[1]
        for relative in (
            "scripts/run_dbw_bronze.sh",
            "scripts/run_bdl_web.sh",
            "scripts/run_full_gus_parallel.sh",
        ):
            script = (repo_root / relative).read_text(encoding="utf-8")
            self.assertIn("kill -0", script, relative)
            self.assertNotIn("pkill", script, relative)
        combined = (repo_root / "scripts/run_full_gus_parallel.sh").read_text(encoding="utf-8")
        self.assertIn("--max-seconds 18000", combined)
        loader_source = (repo_root / "src/dbw_bronze_loader.py").read_text(encoding="utf-8")
        self.assertIn('default=18000, help="Bound one resumable writer lease session"', loader_source)

    def test_dbt_sources_require_explicit_snapshot_release(self):
        repo_root = Path(__file__).resolve().parents[1]
        sources = (repo_root / "models/bronze/sources.yml").read_text(encoding="utf-8")
        self.assertEqual(sources.count("ZOHELO_DBW_BRONZE_RELEASE_ID"), 4)
        self.assertNotIn("gus_dbw/observations/*.parquet", sources)
        self.assertIn("gus_dbw/releases/", sources)
        guard = (repo_root / "macros/assert_dbw_bronze_release.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("bronze-complete-v1-", guard)
        self.assertIn("observation_partitions = completed_indicators", guard)
        self.assertIn("dictionary_partitions = completed_indicators", guard)
        self.assertIn("observation_inventory_sha256", guard)
        self.assertIn("from glob(", guard)
        for model_path in (repo_root / "models/bronze").glob("br_dbw_*.sql"):
            self.assertIn("assert_dbw_bronze_release()", model_path.read_text(encoding="utf-8"))
        indicators = (repo_root / "models/bronze/br_dbw_indicators.sql").read_text(
            encoding="utf-8"
        )
        for column in ("thematic_area", "domain", "taxonomy_path", "node_id", "parent_id"):
            self.assertIn(column, indicators)
        self.assertNotIn("domain_id", indicators)

    def test_selected_complete_snapshot_is_consumable_by_dbw_dbt_models(self):
        repo_root = Path(__file__).resolve().parents[1]
        release_id = "b" * 64
        with tempfile.TemporaryDirectory() as tmp:
            data_root = Path(tmp) / "data"
            release_root = data_root / "02_bronze/gus_dbw/releases" / release_id
            for folder in ("observations", "taxonomy", "metadata", "dictionaries", "_control"):
                (release_root / folder).mkdir(parents=True, exist_ok=True)
            con = duckdb.connect()
            con.execute(f"""
                COPY (SELECT
                    7::BIGINT indicator_id, 1::BIGINT przekroj_id,
                    NULL::BIGINT wymiar_1, NULL::BIGINT pozycja_1,
                    NULL::BIGINT wymiar_2, NULL::BIGINT pozycja_2,
                    NULL::BIGINT wymiar_3, NULL::BIGINT pozycja_3,
                    NULL::BIGINT wymiar_4, NULL::BIGINT pozycja_4,
                    NULL::BIGINT wymiar_5, NULL::BIGINT pozycja_5,
                    NULL::BIGINT wymiar_6, NULL::BIGINT pozycja_6,
                    NULL::BIGINT wymiar_7, NULL::BIGINT pozycja_7,
                    NULL::BIGINT wymiar_8, NULL::BIGINT pozycja_8,
                    NULL::BIGINT wymiar_9, NULL::BIGINT pozycja_9,
                    1::INTEGER okres_id, 1::INTEGER sposob_prezentacji_miara_id,
                    2026::INTEGER period_year, '1'::VARCHAR wartosc_raw,
                    1.0::DOUBLE wartosc_numeric, 0::INTEGER precyzja,
                    NULL::INTEGER brak_wartosci_id, NULL::INTEGER tajnosci_id,
                    NULL::INTEGER flaga_id, '7.zip'::VARCHAR raw_archive_file,
                    1::BIGINT source_row_number, '2026-09-20T00:00:00Z'::VARCHAR processed_at_utc
                ) TO '{release_root / 'observations/part_7.parquet'}' (FORMAT PARQUET)
            """)
            con.execute(f"""
                COPY (SELECT 7::BIGINT indicator_id, 'Indicator'::VARCHAR indicator_name,
                    ''::VARCHAR indicator_name_en, 'Area'::VARCHAR thematic_area,
                    'Domain'::VARCHAR "domain", 'Area > Domain > Indicator'::VARCHAR taxonomy_path,
                    'node'::VARCHAR node_id, 'parent'::VARCHAR parent_id,
                    '2026-09-20T00:00:00Z'::VARCHAR processed_at_utc
                ) TO '{release_root / 'taxonomy/br_dbw_indicators.parquet'}' (FORMAT PARQUET)
            """)
            con.execute(f"""
                COPY (SELECT 7::BIGINT indicator_id, 'Metric'::VARCHAR metric_name,
                    ''::VARCHAR metric_name_en, ''::VARCHAR description,
                    'annual'::VARCHAR frequency, 'unit'::VARCHAR measure_unit,
                    'GUS'::VARCHAR data_source, ''::VARCHAR legal_basis,
                    '2026-09-20'::VARCHAR last_update,
                    '2026-09-20T00:00:00Z'::VARCHAR processed_at_utc
                ) TO '{release_root / 'metadata/br_dbw_metadata.parquet'}' (FORMAT PARQUET)
            """)
            con.execute(f"""
                COPY (SELECT 7::BIGINT indicator_id, 'column'::VARCHAR column_name,
                    'dictionary'::VARCHAR dictionary_name, 1::BIGINT element_id,
                    'element'::VARCHAR element_name,
                    '2026-09-20T00:00:00Z'::VARCHAR processed_at_utc
                ) TO '{release_root / 'dictionaries/br_dbw_dictionaries.parquet'}' (FORMAT PARQUET)
            """)
            con.close()
            __import__("shutil").copyfile(
                release_root / "dictionaries/br_dbw_dictionaries.parquet",
                release_root / "dictionaries/dict_7.parquet",
            )
            marker = {
                "schema_version": 1,
                "record_type": "gus_dbw_bronze_completion",
                "source_id": "gus_dbw",
                "status": "complete_native_snapshot",
                "release_id": release_id,
                "completed_indicators": 1,
                "observation_partitions": 1,
                "dictionary_partitions": 1,
                "observation_inventory_sha256": hashlib.sha256(b"part_7.parquet").hexdigest(),
                "dictionary_inventory_sha256": hashlib.sha256(b"dict_7.parquet").hexdigest(),
            }
            (release_root / "_control" / f"bronze-complete-v1-{release_id}.json").write_text(
                json.dumps(marker), encoding="utf-8"
            )
            env = os.environ.copy()
            env.update({
                "ZOHELO_DATA_ROOT": str(data_root),
                "ZOHELO_DBW_BRONZE_RELEASE_ID": release_id,
                "ZOHELO_DUCKDB_PATH": str(Path(tmp) / "dbw.duckdb"),
            })
            result = subprocess.run(
                [
                    str(repo_root / ".venv/bin/dbt"), "build", "--profiles-dir", str(repo_root),
                    "--project-dir", str(repo_root), "--select", "br_dbw_observations",
                    "br_dbw_indicators", "br_dbw_metadata", "br_dbw_dictionaries",
                    "--vars", '{"enable_gus_dbw": true}',
                ],
                env=env,
                text=True,
                capture_output=True,
                timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_bronze_release_is_namespaced_by_native_snapshot_hash(self):
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
            self.assertEqual(loader.release_id, "b" * 64)
            self.assertEqual(loader.catalogue_sha256, "a" * 64)
            self.assertIn("b" * 64, loader.bronze_obs)
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
