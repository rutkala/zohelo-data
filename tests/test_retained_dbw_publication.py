"""Regression tests for audited retained-DBW Bronze publication."""
from __future__ import annotations
import hashlib, json, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from ingestion.source_campaign_store import CampaignStoreError, LocalCampaignStore
from retained_dbw_publication import EXPECTED_SCHEMAS, canonical, publish_retained_bronze


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


if __name__ == "__main__": unittest.main()
