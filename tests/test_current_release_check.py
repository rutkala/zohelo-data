import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.check_current_silver_release import _build_report  # noqa: E402


class CurrentReleaseCheckTests(unittest.TestCase):
    def test_report_includes_dataset_and_semantic_query_counts(self):
        manifest = {
            "release_scope": "nbp_platform",
            "release_id": "release-1",
            "code_sha": "a" * 40,
        }
        datasets = [
            {
                "dataset_id": "fact_fx_quotes",
                "rows": 4,
                "min_date": "2026-09-01",
                "max_date": "2026-09-02",
                "query_seconds": 0.01,
            }
        ]
        semantic = {
            "status": "verified",
            "metrics": [
                {"name": "nbp_table_a_mid", "row_count": 3},
                {"name": "nbp_gold_price_pln_per_gram_1000", "row_count": 2},
            ],
        }
        report = _build_report(manifest, datasets, 1.2345, 4096, semantic)
        self.assertEqual(report["status"], "fresh_consumer_verified")
        self.assertEqual(report["dataset_count"], 1)
        self.assertEqual(report["datasets"], datasets)
        self.assertEqual(report["query_metrics"], {"metric_count": 2, "row_count": 5})
        self.assertEqual(report["semantic_validation"], semantic)
        self.assertEqual(report["download_bytes"], 4096)

    def test_legacy_report_has_zero_semantic_queries_without_claiming_validation(self):
        manifest = {
            "release_scope": "nbp_silver",
            "release_id": "release-legacy",
            "code_sha": "b" * 40,
        }
        report = _build_report(manifest, [], 0.5, 100)
        self.assertEqual(report["query_metrics"], {"metric_count": 0, "row_count": 0})
        self.assertNotIn("semantic_validation", report)


if __name__ == "__main__":
    unittest.main()
