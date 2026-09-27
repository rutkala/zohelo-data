import copy
import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/check-source-coverage.py"
SPEC = importlib.util.spec_from_file_location("check_source_coverage", SCRIPT)
coverage_check = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(coverage_check)


class SourceCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.documents = coverage_check.load_documents(
            ROOT / "docs/source-research/source-inventory.json",
            ROOT / "config/domain-taxonomy.yaml",
            ROOT / "config/source-domain-coverage.json",
        )

    def documents_copy(self):
        return copy.deepcopy(self.documents)

    def test_repository_coverage_is_complete_and_reports_discovery_gaps(self):
        summary = coverage_check.validate_documents(*self.documents_copy())
        self.assertEqual(summary["inventory_sources"], 182)
        self.assertEqual(summary["source_mappings"], 182)
        self.assertEqual(summary["categories"], 231)
        self.assertEqual(summary["analytical_dimensions"], 17)
        self.assertEqual(summary["official_classifications"], 11)
        self.assertEqual(summary["inventory_candidate_assignments"], 442)
        self.assertEqual(summary["additional_candidates"], 7)
        self.assertEqual(summary["additional_candidate_assignments"], 30)
        self.assertEqual(summary["coverage_gap_records"], 15)
        self.assertEqual(summary["categories_with_inventory_candidates"], 160)
        self.assertEqual(summary["categories_without_inventory_candidates"], 71)
        self.assertEqual(summary["categories_with_any_candidates"], 177)
        self.assertEqual(summary["categories_without_any_candidates"], 54)
        self.assertTrue(summary["discovery_evidence_only"])

    def test_missing_inventory_mapping_is_rejected(self):
        inventory, taxonomy, coverage = self.documents_copy()
        coverage["sources"].pop()
        with self.assertRaisesRegex(
            coverage_check.CoverageValidationError,
            "exactly match inventory order",
        ):
            coverage_check.validate_documents(inventory, taxonomy, coverage)

    def test_unknown_category_and_stale_count_are_rejected(self):
        inventory, taxonomy, coverage = self.documents_copy()
        coverage["sources"][0]["candidate_category_ids"][0] = "unknown.domain.category"
        with self.assertRaisesRegex(coverage_check.CoverageValidationError, "unknown category"):
            coverage_check.validate_documents(inventory, taxonomy, coverage)

        inventory, taxonomy, coverage = self.documents_copy()
        first_category = next(iter(coverage["category_discovery_counts"]))
        coverage["category_discovery_counts"][first_category]["inventory_product_candidates"] += 1
        with self.assertRaisesRegex(
            coverage_check.CoverageValidationError,
            "stale category discovery count",
        ):
            coverage_check.validate_documents(inventory, taxonomy, coverage)

    def test_dimension_and_classification_identity_are_enforced(self):
        inventory, taxonomy, coverage = self.documents_copy()
        taxonomy["analytical_dimensions"][-1]["id"] = taxonomy["analytical_dimensions"][0]["id"]
        with self.assertRaisesRegex(coverage_check.CoverageValidationError, "duplicate analytical"):
            coverage_check.validate_documents(inventory, taxonomy, coverage)

        inventory, taxonomy, coverage = self.documents_copy()
        taxonomy["official_classifications"] = [
            value for value in taxonomy["official_classifications"] if value["id"] != "ICD"
        ]
        with self.assertRaisesRegex(
            coverage_check.CoverageValidationError,
            "unexpected official classification count",
        ):
            coverage_check.validate_documents(inventory, taxonomy, coverage)

    def test_candidate_only_disclaimer_is_enforced(self):
        inventory, taxonomy, coverage = self.documents_copy()
        coverage["interpretation"] = ["These mappings are complete production coverage."]
        with self.assertRaisesRegex(
            coverage_check.CoverageValidationError,
            "discovery evidence only",
        ):
            coverage_check.validate_documents(inventory, taxonomy, coverage)


if __name__ == "__main__":
    unittest.main()
