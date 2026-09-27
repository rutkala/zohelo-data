"""Exercise actual dbt outputs through publication and a fresh local consumer."""

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
from transformation.silver_builder import DATASET_MODELS, build_silver, _discover_staging_models
from release_protocol import publish_release, restore_current_release
from test_release_protocol import MemoryStore


class SilverPublicationTests(unittest.TestCase):
    def test_every_dataset_is_required_before_a_build(self):
        self.assertEqual(set(_discover_staging_models(list(DATASET_MODELS))), set(DATASET_MODELS.values()))
        with self.assertRaisesRegex(ValueError, "nbp_gold_prices"):
            _discover_staging_models(list(DATASET_MODELS)[:-1])

    def test_real_dbt_outputs_publish_and_restore_without_builder_files(self):
        store = MemoryStore()
        temporary_root = REPO_ROOT / ".local/test-tmp"
        temporary_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="zohelo-publish-fixture-", dir=temporary_root) as directory:
            workspace = Path(directory)
            fixture_names = {"nbp_exchange_rates_table_a": "table_a", "nbp_exchange_rates_table_b": "table_b",
                             "nbp_exchange_rates_table_c": "table_c", "nbp_gold_prices": "gold_prices"}
            con = duckdb.connect()
            try:
                for dataset_id, fixture in fixture_names.items():
                    destination = workspace / "02_bronze" / dataset_id / "input.parquet"
                    destination.parent.mkdir(parents=True)
                    con.read_json(str(REPO_ROOT / "tests" / "fixtures" / "nbp" / f"{fixture}.json")).write_parquet(str(destination))
            finally:
                con.close()
            datasets, artifacts = build_silver(workspace)
            first = publish_release(store, "root", datasets=datasets, artifacts=artifacts,
                                    inputs=[{"provenance": "synthetic_fixture"}], code_sha="a" * 40,
                                    measurements={"input_files": 4})
            self.assertEqual(first["manifest"]["release_scope"], "nbp_silver")
        # The builder directory/database and every source Parquet have now gone.
        manifest = restore_current_release(store, "root")
        with tempfile.TemporaryDirectory(prefix="zohelo-fresh-consumer-", dir=temporary_root) as directory:
            con = duckdb.connect()
            try:
                for dataset in manifest["datasets"]:
                    file = dataset["files"][0]
                    local = Path(directory) / file["name"]
                    local.write_bytes(store.read(file["id"]))
                    rows = con.read_parquet(str(local)).fetchall()
                    self.assertEqual(len(rows), dataset["row_count"])
                    if dataset["dataset_id"] == "nbp_gold_prices":
                        actual = con.execute("SELECT sum(price_pln_per_gram) FROM read_parquet(?)", [str(local)]).fetchone()[0]
                        self.assertEqual(str(actual), "331.95")
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
