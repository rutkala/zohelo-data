import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from business_catalog import SOURCE_IDS, build_business_catalog  # noqa: E402


SOURCE_CONFIG = {
    "sources": {
        source_id: {
            "metadata": {
                "source_name": "NBP",
                "description": f"Description for {source_id}",
            }
        }
        for source_id in SOURCE_IDS
    }
}


class BusinessCatalogTests(unittest.TestCase):
    def test_builds_four_business_labeled_sources_from_observed_config_shape(self):
        state = {
            "sources": {
                source_id: {
                    "checked_through": "2026-08-28",
                    "latest_observation_date": "2026-08-28",
                    "last_successful_ingestion_at": "2026-09-07T01:02:03Z",
                    "last_attempt_at": "2026-09-07T01:02:03Z",
                    "raw_response_count": index + 1,
                }
                for index, source_id in enumerate(SOURCE_IDS)
            }
        }
        metadata = [{"dataset_id": "nbp_exchange_rates_table_a", "model_name": "stg_nbp_table_a", "path": "/tmp/secret.parquet", "model_id": "model.zohelo_data.stg_nbp_table_a"}]
        manifest = {
            "nodes": {
                "model.zohelo_data.stg_nbp_table_a": {
                    "unique_id": "model.zohelo_data.stg_nbp_table_a",
                    "resource_type": "model",
                    "name": "stg_nbp_table_a",
                    "path": "models/staging/stg_nbp_table_a.sql",
                    "description": "Table A silver model.",
                    "depends_on": {"nodes": ["source.zohelo_data.bronze.nbp_exchange_rates_table_a"]},
                }
            },
            "sources": {
                "source.zohelo_data.bronze.nbp_exchange_rates_table_a": {
                    "unique_id": "source.zohelo_data.bronze.nbp_exchange_rates_table_a",
                    "resource_type": "source",
                    "name": "nbp_exchange_rates_table_a",
                    "description": "Bronze A.",
                }
            },
        }

        result = build_business_catalog(
            code_sha="a" * 40,
            source_config=SOURCE_CONFIG,
            ingestion_state=state,
            dbt_manifest=manifest,
            dataset_metadata=metadata,
        )

        self.assertEqual(result["format_version"], 1)
        self.assertEqual(result["code_sha"], "a" * 40)
        self.assertEqual([item["source_id"] for item in result["sources"]], list(SOURCE_IDS))
        self.assertEqual(result["sources"][0]["name"], "NBP Table A (Convertible FX)")
        self.assertEqual(result["sources"][0]["description"], "Description for nbp_exchange_rates_table_a")
        self.assertEqual(result["sources"][0]["raw_response_count"], 1)
        self.assertNotIn("path", result["datasets"][0])
        self.assertNotIn("model_id", result["datasets"][0])
        self.assertEqual(set(result["datasets"][0]), {"dataset_id", "model_name"})
        self.assertEqual(result["lineage"]["edges"], [{
            "from": "source.zohelo_data.bronze.nbp_exchange_rates_table_a",
            "to": "model.zohelo_data.stg_nbp_table_a",
        }])

    def test_unknown_coverage_remains_null_and_file_metadata_is_ignored(self):
        state = {
            "sources": {
                source_id: {
                    "checked_through": None,
                    "latest_observation_date": None,
                    "last_successful_ingestion_at": None,
                    "last_attempt_at": None,
                    "raw_response_count": 0,
                    "coverage_complete": None,
                    "filemtime": "2099-01-01T00:00:00Z",
                }
                for source_id in SOURCE_IDS
            }
        }
        result = build_business_catalog(
            code_sha="b" * 40,
            source_config=SOURCE_CONFIG,
            ingestion_state=state,
            dbt_manifest={},
            dataset_metadata=[],
        )
        for source in result["sources"]:
            self.assertIsNone(source["coverage_complete"])
            self.assertIsNone(source["latest_observation_date"])
            self.assertNotIn("filemtime", source)

    def test_lineage_contains_ancestors_only_and_excludes_tests_fixtures_disabled_and_unrelated(self):
        metadata = [{"dataset_id": "gold", "model_name": "fact_fx_quotes"}]
        manifest = {
            "sources": {
                "source.p.bronze.nbp_exchange_rates_table_a": {
                    "unique_id": "source.p.bronze.nbp_exchange_rates_table_a",
                    "resource_type": "source",
                    "name": "nbp_exchange_rates_table_a",
                }
            },
            "nodes": {
                "model.p.br_nbp_table_a": {
                    "unique_id": "model.p.br_nbp_table_a", "resource_type": "model", "name": "br_nbp_table_a",
                    "depends_on": {"nodes": ["source.p.bronze.nbp_exchange_rates_table_a"]},
                },
                "model.p.stg_nbp_table_a": {
                    "unique_id": "model.p.stg_nbp_table_a", "resource_type": "model", "name": "stg_nbp_table_a",
                    "path": "models/staging/stg_nbp_table_a.sql", "depends_on": {"nodes": ["model.p.br_nbp_table_a"]},
                },
                "model.p.fact_fx_quotes": {
                    "unique_id": "model.p.fact_fx_quotes", "resource_type": "model", "name": "fact_fx_quotes",
                    "path": "models/marts/fact_fx_quotes.sql", "depends_on": {"nodes": ["model.p.stg_nbp_table_a"]},
                },
                "test.p.not_null": {
                    "unique_id": "test.p.not_null", "resource_type": "test", "name": "not_null",
                    "depends_on": {"nodes": ["model.p.fact_fx_quotes"]},
                },
                "model.p.fixture_daily_values": {
                    "unique_id": "model.p.fixture_daily_values", "resource_type": "model", "name": "fixture_daily_values",
                    "depends_on": {"nodes": []},
                },
                "model.p.disabled": {
                    "unique_id": "model.p.disabled", "resource_type": "model", "name": "disabled",
                    "config": {"enabled": False}, "depends_on": {"nodes": []},
                },
                "model.p.unrelated": {
                    "unique_id": "model.p.unrelated", "resource_type": "model", "name": "unrelated",
                    "depends_on": {"nodes": []},
                },
            },
        }
        result = build_business_catalog(
            code_sha="c" * 40,
            source_config=SOURCE_CONFIG,
            ingestion_state={"sources": {source_id: {"raw_response_count": 0} for source_id in SOURCE_IDS}},
            dbt_manifest=manifest,
            dataset_metadata=metadata,
        )
        node_ids = {node["id"] for node in result["lineage"]["nodes"]}
        self.assertEqual(node_ids, {
            "source.p.bronze.nbp_exchange_rates_table_a",
            "model.p.br_nbp_table_a",
            "model.p.stg_nbp_table_a",
            "model.p.fact_fx_quotes",
        })
        self.assertTrue(all(edge["from"] in node_ids and edge["to"] in node_ids for edge in result["lineage"]["edges"]))
        self.assertEqual(result["metrics"], [])
        self.assertEqual(result["metrics_status"], "awaiting_business_approval")
        self.assertTrue(result["metrics_explanation"])

    def test_lineage_layers_follow_physical_landing_bronze_silver_and_gold_schemas(self):
        metadata = [{"dataset_id": "fact", "model_name": "fact_fx_quotes"}]
        manifest = {
            "sources": {
                "source.p.landing.nbp_batches": {
                    "unique_id": "source.p.landing.nbp_batches",
                    "resource_type": "source",
                    "name": "nbp_batches",
                    "schema": "01_landing",
                }
            },
            "nodes": {
                "model.p.br_nbp_table_a": {
                    "unique_id": "model.p.br_nbp_table_a",
                    "resource_type": "model",
                    "name": "br_nbp_table_a",
                    "schema": "02_bronze",
                    "depends_on": {"nodes": ["source.p.landing.nbp_batches"]},
                },
                "model.p.stg_nbp_table_a": {
                    "unique_id": "model.p.stg_nbp_table_a",
                    "resource_type": "model",
                    "name": "stg_nbp_table_a",
                    "schema": "03_silver",
                    "depends_on": {"nodes": ["model.p.br_nbp_table_a"]},
                },
                "model.p.fact_fx_quotes": {
                    "unique_id": "model.p.fact_fx_quotes",
                    "resource_type": "model",
                    "name": "fact_fx_quotes",
                    "schema": "04_gold",
                    "depends_on": {"nodes": ["model.p.stg_nbp_table_a"]},
                },
            },
        }

        result = build_business_catalog(
            code_sha="d" * 40,
            source_config=SOURCE_CONFIG,
            ingestion_state={"sources": {source_id: {"raw_response_count": 0} for source_id in SOURCE_IDS}},
            dbt_manifest=manifest,
            dataset_metadata=metadata,
        )

        nodes = {node["id"]: node for node in result["lineage"]["nodes"]}
        self.assertEqual(
            {node_id: node["layer"] for node_id, node in nodes.items()},
            {
                "source.p.landing.nbp_batches": "landing",
                "model.p.br_nbp_table_a": "bronze",
                "model.p.stg_nbp_table_a": "silver",
                "model.p.fact_fx_quotes": "gold",
            },
        )
        self.assertEqual(
            {(edge["from"], edge["to"]) for edge in result["lineage"]["edges"]},
            {
                ("source.p.landing.nbp_batches", "model.p.br_nbp_table_a"),
                ("model.p.br_nbp_table_a", "model.p.stg_nbp_table_a"),
                ("model.p.stg_nbp_table_a", "model.p.fact_fx_quotes"),
            },
        )


if __name__ == "__main__":
    unittest.main()
