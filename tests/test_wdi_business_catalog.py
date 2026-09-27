import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from wdi_business_catalog import build_wdi_business_catalog  # noqa: E402


class WdiBusinessCatalogTests(unittest.TestCase):
    def test_catalogue_keeps_wdi_source_metrics_release_data_and_lineage(self):
        manifest = {
            "sources": {"source.p.wdi_bulk.data": {
                "unique_id": "source.p.wdi_bulk.data", "resource_type": "source",
                "name": "data", "schema": "01_landing",
            }},
            "nodes": {
                "model.p.br_wdi_data": {
                    "unique_id": "model.p.br_wdi_data", "resource_type": "model",
                    "name": "br_wdi_data", "schema": "02_bronze",
                    "depends_on": {"nodes": ["source.p.wdi_bulk.data"]},
                },
                "model.p.mart_wdi_coverage": {
                    "unique_id": "model.p.mart_wdi_coverage", "resource_type": "model",
                    "name": "mart_wdi_coverage", "schema": "04_gold",
                    "depends_on": {"nodes": ["model.p.br_wdi_data"]},
                },
            },
            "semantic_models": {"semantic_model.p.wdi_coverage": {
                "unique_id": "semantic_model.p.wdi_coverage", "resource_type": "semantic_model",
                "name": "wdi_coverage", "depends_on": {"nodes": ["model.p.mart_wdi_coverage"]},
            }},
            "metrics": {"metric.p.wdi_source_value_total": {
                "unique_id": "metric.p.wdi_source_value_total", "resource_type": "metric",
                "name": "wdi_source_value_total", "config": {"meta": {"definition_status": "source_defined"}},
                "depends_on": {"nodes": ["semantic_model.p.wdi_coverage"]},
            }},
        }
        result = build_wdi_business_catalog(
            code_sha="a" * 40,
            source_config={"sources": {"world_bank_wdi": {"metadata": {
                "display_name": "World Development Indicators (WDI)", "description": "Official WDI",
            }}}},
            ingestion_state={"sources": {"world_bank_wdi": {
                "raw_response_count": 6, "coverage_complete": True,
            }}},
            dbt_manifest=manifest,
            dataset_metadata=[{
                "dataset_id": "mart_wdi_coverage", "table_name": "mart_wdi_coverage",
                "model_name": "mart_wdi_coverage", "model_id": "model.p.mart_wdi_coverage",
                "layer": "04_gold", "row_count": 1, "date_column": "snapshot_date",
                "min_date": "2026-09-13", "max_date": "2026-09-13", "columns": [],
                "path": "/tmp/private.parquet", "paths": ["/tmp/private.parquet"],
            }],
        )
        self.assertEqual(result["sources"][0]["source_id"], "world_bank_wdi")
        self.assertTrue(result["sources"][0]["coverage_complete"])
        self.assertEqual([item["name"] for item in result["metrics"]], ["wdi_source_value_total"])
        self.assertEqual(result["metrics_status"], "source_defined")
        self.assertNotIn("path", result["datasets"][0])
        self.assertEqual(
            {node["id"] for node in result["lineage"]["nodes"]},
            {"source.p.wdi_bulk.data", "model.p.br_wdi_data", "model.p.mart_wdi_coverage", "semantic_model.p.wdi_coverage", "metric.p.wdi_source_value_total"},
        )


if __name__ == "__main__":
    unittest.main()
