"""Regression tests for audited retained-DBW Bronze publication."""
from __future__ import annotations
import hashlib, json, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import Mock, patch
import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from ingestion.source_campaign_store import CampaignStoreError, DriveCampaignStore, LocalCampaignStore
from retained_dbw_publication import (
    EXPECTED_SCHEMAS, _numeric_fragment_order, _verify_repack, canonical,
    publish_retained_bronze, publish_retained_bronze_until_complete, validate_audit,
)


def parquet(path: Path, sql: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try: con.execute(f"COPY ({sql}) TO ? (FORMAT PARQUET)", [str(path)])
    finally: con.close()


def descriptor(path: str, file: Path | None = None) -> dict:
    raw = file.read_bytes() if file else b"x"
    return {"path": path, "id": f"id-{path}", "name": Path(path).name, "size": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(), "md5": hashlib.md5(raw).hexdigest()}


class RetainedPublicationTests(unittest.TestCase):
    def fixture(self, root: Path):
        audit = root / "audit"; cache = audit / "verified-cache"
        taxonomy = cache / "taxonomy/br_dbw_indicators.parquet"
        parquet(taxonomy, "SELECT i::BIGINT indicator_id, 'Wskaźnik '||i::VARCHAR indicator_name, "
                "'Indicator '||i::VARCHAR indicator_name_en, 'Area' thematic_area, 'Domain' \"domain\", "
                "'Area > Domain' taxonomy_path, 'n'||i::VARCHAR node_id, 'p' parent_id, "
                "'2026-09-21T00:00:00Z' processed_at_utc FROM range(1,1551) t(i)")
        metadata = cache / "metadata/br_dbw_metadata.parquet"
        parquet(metadata, "SELECT 1::BIGINT indicator_id, 'Metric' metric_name, '' metric_name_en, '' description, "
                "'annual' frequency, 'unit' measure_unit, 'GUS' data_source, '' legal_basis, '2026-09-21' last_update, "
                "'2026-09-21T00:00:00Z' processed_at_utc")
        dictionaries = cache / "dictionaries/br_dbw_dictionaries.parquet"
        parquet(dictionaries, "SELECT 1::BIGINT indicator_id, 'c' column_name, 'd' dictionary_name, "
                "1::BIGINT element_id, 'e' element_name, '2026-09-21T00:00:00Z' processed_at_utc")
        observations = []
        for indicator in (1, 2, 3):
            path = cache / f"observations/part_{indicator}.parquet"
            nulls = ", ".join(f"NULL::BIGINT {name}" for i in range(1,10) for name in (f'wymiar_{i}',f'pozycja_{i}'))
            parquet(path, f"SELECT {indicator}::BIGINT indicator_id, 1::BIGINT przekroj_id, {nulls}, "
                "1::INTEGER okres_id, 1::INTEGER sposob_prezentacji_miara_id, 2026::INTEGER period_year, "
                "'1' wartosc_raw, 1.0::DOUBLE wartosc_numeric, 0::INTEGER precyzja, NULL::INTEGER brak_wartosci_id, "
                "NULL::INTEGER tajnosci_id, NULL::INTEGER flaga_id, 'x.zip' raw_archive_file, "
                "1::BIGINT source_row_number, '2026-09-21T00:00:00Z' processed_at_utc")
            observations.append(descriptor(f"observations/part_{indicator}.parquet", path))
        objects = [descriptor(f"receipts/{i}.json") for i in range(1,1551)]
        objects += observations + [descriptor(f"observations/part_{i}.parquet") for i in range(4,1551)]
        objects += [descriptor("dictionaries/br_dbw_dictionaries.parquet", dictionaries),
                    descriptor("metadata/br_dbw_metadata.parquet", metadata),
                    descriptor("taxonomy/br_dbw_indicators.parquet", taxonomy)]
        objects.sort(key=lambda item: item["path"])
        inventory_sha = hashlib.sha256(canonical(objects)).hexdigest()
        inventory = {"format_version":1,"source_id":"gus_dbw","scope":"retained_pre_release_bronze_diagnostic",
                     "inventory_sha256":inventory_sha,"object_count":3103,
                     "total_bytes":sum(x["size"] for x in objects),"objects":objects}
        audit.mkdir(parents=True, exist_ok=True)
        (audit/"descriptor-inventory.json").write_text(json.dumps(inventory))
        report = {"format_version":1,"source_id":"gus_dbw","status":"retained_outputs_audited",
                  "run_id":"audit-run","inventory_sha256":inventory_sha,"remote_inventory_stable_after_restore":True,
                  "measured_parquet_rows":{"observations":3,"dictionaries":1,"metadata":1,"taxonomy":1550}}
        (audit/"audit-report.json").write_text(json.dumps(report))
        (audit/"run-status.json").write_text(json.dumps({"status":"complete","run_id":"audit-run","inventory_sha256":inventory_sha}))
        return audit

    def test_resume_reuses_fragments_and_failure_preserves_pointer(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); audit=self.fixture(root); store=LocalCampaignStore(root/"store","gus_dbw_retained_bronze")
            first=publish_retained_bronze(store,audit,root/"work","a"*40,max_indicators=1)
            self.assertEqual((first["published_indicator_count"],first["pending_indicator_count"]),(1,1549))
            fixed_ids={f["id"] for d in first["datasets"] for f in d["files"]}
            second=publish_retained_bronze(store,audit,root/"work","a"*40,max_indicators=1)
            self.assertEqual(second["published_indicator_count"],2)
            self.assertEqual(fixed_ids,{f["id"] for d in second["datasets"] for f in d["files"]})
            pointer=store.load_landing_pointer()
            with patch.object(store,"promote_landing_pointer",side_effect=CampaignStoreError("promotion failed")):
                with self.assertRaisesRegex(CampaignStoreError,"promotion failed"):
                    publish_retained_bronze(store,audit,root/"work","a"*40,max_indicators=1)
            self.assertEqual(store.load_landing_pointer(),pointer)

    def test_competing_and_exact_stale_owner_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            one=LocalCampaignStore(Path(tmp),"gus_dbw_retained_bronze")
            two=LocalCampaignStore(Path(tmp),"gus_dbw_retained_bronze")
            one.acquire_publication_owner("owner-one")
            with self.assertRaisesRegex(CampaignStoreError,"another source"):
                two.acquire_publication_owner("owner-two")
            with self.assertRaisesRegex(CampaignStoreError,"does not match"):
                two.recover_publication_owner("wrong", "operator-run")
            two.recover_publication_owner("owner-one", "operator-run")
            two.acquire_publication_owner("owner-two")
            two.release_publication_owner()

    def test_stream_proof_preserves_order_nulls_duplicates_and_more_than_ten_fragments(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.parquet"
            parquet(source, "SELECT i::BIGINT id, CASE WHEN i%3=0 THEN NULL ELSE 'NULL' END val, "
                    "CASE WHEN i%4=0 THEN 'line'||chr(10)||'\"quoted\"' ELSE '' END payload, "
                    "CASE WHEN i%5=0 THEN 'NaN'::DOUBLE ELSE i::DOUBLE/10 END number "
                    "FROM (SELECT i%9 i FROM range(24) t(i))")
            con = duckdb.connect()
            try:
                for number in range(12):
                    output = root / f"data_{number}.parquet"
                    con.execute(f"COPY (SELECT * FROM read_parquet('{source}') LIMIT 2 OFFSET {number * 2}) "
                                f"TO '{output}' (FORMAT PARQUET)")
            finally:
                con.close()
            ordered = _numeric_fragment_order(root.glob("data_*.parquet"))
            self.assertEqual([path.name for path in ordered], [f"data_{i}.parquet" for i in range(12)])
            _verify_repack(source, ordered, 24)
            with self.assertRaisesRegex(CampaignStoreError, "values or order"):
                _verify_repack(source, list(reversed(ordered)), 24)

    def test_retry_reuses_exact_objects_after_mid_upload_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); audit = self.fixture(root)
            store = LocalCampaignStore(root / "store", "gus_dbw_retained_bronze")
            original = store.put_or_reuse_landing_object
            calls = 0

            def fail_after_verified_create(*args, **kwargs):
                nonlocal calls
                result = original(*args, **kwargs)
                calls += 1
                if calls == 4:
                    raise CampaignStoreError("lost after verified upload")
                return result

            with patch.object(store, "put_or_reuse_landing_object", side_effect=fail_after_verified_create):
                with self.assertRaisesRegex(CampaignStoreError, "lost after verified upload"):
                    publish_retained_bronze(store, audit, root / "work", "a" * 40, max_indicators=1)
            landing = root / "store/06_control/source_campaigns/gus_dbw_retained_bronze/landing_publications"
            before = {path.name for path in landing.glob("*.parquet")}
            self.assertEqual(len(before), 4)
            result = publish_retained_bronze(store, audit, root / "work", "a" * 40, max_indicators=1)
            after = {path.name for path in landing.glob("*.parquet")}
            self.assertEqual(before, after)
            self.assertEqual(result["published_indicator_count"], 1)

    def test_inconsistent_complete_claim_is_rejected_before_shortcut(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); audit = self.fixture(root)
            store = LocalCampaignStore(root / "store", "gus_dbw_retained_bronze")
            publish_retained_bronze(store, audit, root / "work", "a" * 40, max_indicators=1)
            pointer = store.load_landing_pointer()
            manifest_descriptor = {"id": pointer["manifest_file_id"], "name": pointer["manifest_file_name"],
                "size": pointer["manifest_size_bytes"], "sha256": pointer["manifest_sha256"]}
            manifest = json.loads(store.read_landing_object(manifest_descriptor))
            manifest["published_indicator_count"] = 1550
            manifest["pending_indicator_count"] = 0
            original = store.read_landing_object

            def inconsistent(descriptor, **kwargs):
                if descriptor.get("id") == manifest_descriptor["id"]:
                    return canonical(manifest)
                return original(descriptor, **kwargs)

            with patch.object(store, "read_landing_object", side_effect=inconsistent):
                with self.assertRaisesRegex(CampaignStoreError, "audit binding changed"):
                    publish_retained_bronze(store, audit, root / "work", "a" * 40, max_indicators=1)


    def test_drive_rejects_self_consistent_unreviewed_audit_before_owner_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); audit = self.fixture(root)
            validate_audit(audit)  # Valid local fixture, but not the reviewed production snapshot.
            for publish in (publish_retained_bronze, publish_retained_bronze_until_complete):
                store = Mock(spec=DriveCampaignStore)
                with self.assertRaisesRegex(CampaignStoreError, "exact reviewed audit hashes"):
                    publish(store, audit, root / "work", "a" * 40)
                store.acquire_publication_owner.assert_not_called()

    def test_both_reviewed_hashes_are_required_and_tampering_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            audit = self.fixture(Path(tmp))
            _, inventory, report_sha = validate_audit(audit)
            with patch("retained_dbw_publication.REVIEWED_INVENTORY_SHA256", inventory["inventory_sha256"]), \
                 patch("retained_dbw_publication.REVIEWED_AUDIT_REPORT_SHA256", report_sha):
                validate_audit(audit, require_reviewed_snapshot=True)
                report = json.loads((audit / "audit-report.json").read_text())
                report["unreviewed_change"] = True
                (audit / "audit-report.json").write_text(json.dumps(report))
                with self.assertRaisesRegex(CampaignStoreError, "exact reviewed audit hashes"):
                    validate_audit(audit, require_reviewed_snapshot=True)
            with patch("retained_dbw_publication.REVIEWED_AUDIT_REPORT_SHA256", report_sha):
                with self.assertRaisesRegex(CampaignStoreError, "exact reviewed audit hashes"):
                    validate_audit(audit, require_reviewed_snapshot=True)


if __name__ == "__main__": unittest.main()
