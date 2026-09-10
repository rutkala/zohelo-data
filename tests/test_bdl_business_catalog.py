import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bdl_business_catalog import build_bdl_business_catalog  # noqa: E402


SOURCE_CONFIG = {
    "sources": {
        "gus_bdl": {
            "metadata": {
                "display_name": "GUS BDL (Local Data Bank)",
                "description": "Official local statistics",
                "provider_url": "https://stat.gov.pl/",
                "documentation_url": "https://bdl.stat.gov.pl/api/v1",
            }
        }
    }
}


class BdlBusinessCatalogTests(unittest.TestCase):
    def test_catalogue_keeps_only_bdl_source_defined_metrics_and_lineage(self):
        manifest = {
            "sources": {
                "source.p.landing.gus_bdl_responses": {
                    "unique_id": "source.p.landing.gus_bdl_responses",
                    "resource_type": "source",
                    "name": "gus_bdl_responses",
                    "schema": "01_landing",
                }
            },
            "nodes": {
                "model.p.br_bdl_variables": {
                    "unique_id": "model.p.br_bdl_variables",
                    "resource_type": "model",
                    "name": "br_bdl_variables",
                    "schema": "02_bronze",
                    "depends_on": {"nodes": ["source.p.landing.gus_bdl_responses"]},
                },
                "model.p.mart_bdl_coverage": {
                    "unique_id": "model.p.mart_bdl_coverage",
                    "resource_type": "model",
                    "name": "mart_bdl_coverage",
                    "schema": "04_gold",
                    "depends_on": {"nodes": ["model.p.br_bdl_variables"]},
                },
            },
            "semantic_models": {
                "semantic_model.p.bdl_coverage": {
                    "unique_id": "semantic_model.p.bdl_coverage",
                    "resource_type": "semantic_model",
                    "name": "bdl_coverage",
                    "depends_on": {"nodes": ["model.p.mart_bdl_coverage"]},
                }
            },
            "metrics": {
                "metric.p.bdl_discovered_total": {
                    "unique_id": "metric.p.bdl_discovered_total",
                    "resource_type": "metric",
                    "name": "bdl_discovered_total",
                    "config": {"meta": {"definition_status": "source_defined", "unit": "variables"}},
                    "depends_on": {"nodes": ["semantic_model.p.bdl_coverage"]},
                },
                "metric.p.proposed_total": {
                    "unique_id": "metric.p.proposed_total",
                    "resource_type": "metric",
                    "name": "proposed_total",
                },
            },
        }
        result = build_bdl_business_catalog(
            code_sha="a" * 40,
            source_config=SOURCE_CONFIG,
            ingestion_state={"sources": {"gus_bdl": {"raw_response_count": 15, "coverage_complete": False}}},
            dbt_manifest=manifest,
            dataset_metadata=[{
                "dataset_id": "mart_bdl_coverage",
                "model_id": "model.p.mart_bdl_coverage",
                "model_name": "mart_bdl_coverage",
                "layer": "04_gold",
                "table_name": "mart_bdl_coverage",
                "row_count": 1,
                "date_column": "snapshot_date",
                "min_date": "2026-09-10",
                "max_date": "2026-09-10",
                "columns": [{"name": "snapshot_date", "type": "date"}],
                "path": "/tmp/not-exported.parquet",
            }],
        )
        self.assertEqual([item["source_id"] for item in result["sources"]], ["gus_bdl"])
        self.assertEqual(result["sources"][0]["name"], "GUS BDL (Local Data Bank)")
        self.assertEqual(result["sources"][0]["raw_response_count"], 15)
        self.assertFalse(result["sources"][0]["coverage_complete"])
        self.assertEqual([item["name"] for item in result["metrics"]], ["bdl_discovered_total"])
        self.assertEqual(result["metrics_status"], "source_defined")
        self.assertEqual(
            {node["id"] for node in result["lineage"]["nodes"]},
            {
                "source.p.landing.gus_bdl_responses",
                "model.p.br_bdl_variables",
                "model.p.mart_bdl_coverage",
                "semantic_model.p.bdl_coverage",
                "metric.p.bdl_discovered_total",
            },
        )
        self.assertNotIn("path", result["datasets"][0])
        self.assertNotIn("model_id", result["datasets"][0])


if __name__ == "__main__":
    unittest.main()
