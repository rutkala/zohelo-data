#!/usr/bin/env python3
"""Validate the source discovery taxonomy and its complete inventory mapping."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_SOURCE_COUNT = 182
EXPECTED_DOMAIN_COUNT = 15
EXPECTED_SUBDOMAIN_COUNT = 45
EXPECTED_CATEGORY_COUNT = 231
EXPECTED_DIMENSION_COUNT = 17
EXPECTED_CLASSIFICATION_COUNT = 11
EXPECTED_INVENTORY_ASSIGNMENT_COUNT = 442
EXPECTED_ADDITIONAL_CANDIDATE_COUNT = 7
EXPECTED_ADDITIONAL_ASSIGNMENT_COUNT = 30
EXPECTED_GAP_COUNT = 15
REQUIRED_CLASSIFICATIONS = {
    "PKD",
    "NACE",
    "PKWiU",
    "CPA",
    "CPC",
    "HS",
    "CN",
    "COICOP",
    "COFOG",
    "ISCED",
    "ICD",
}
REQUIRED_DIMENSIONS = {"fmcg-product-family", "media-form-channel", "sport-discipline"}
TAXONOMY_ID = re.compile(r"^[a-z][a-z0-9-]*(?:\.[a-z][a-z0-9-]*)*$")
CONFIDENCE_VALUES = {"low", "medium", "high"}
PLANNED_LANES = {
    "existing-baseline",
    "phase-2-first-production-campaign",
    "phase-4-reference",
    "phase-7-public-wave",
    "phase-8-deferred-prerequisite",
}


class CoverageValidationError(ValueError):
    """The checked taxonomy or coverage ledger violates its contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CoverageValidationError(message)


def _object(value: Any, label: str) -> dict[str, Any]:
    _require(isinstance(value, dict), f"{label} must be an object")
    return value


def _list(value: Any, label: str) -> list[Any]:
    _require(isinstance(value, list), f"{label} must be a list")
    return value


def _unique(values: list[str], label: str) -> None:
    duplicates = sorted(value for value, count in Counter(values).items() if count > 1)
    _require(not duplicates, f"duplicate {label}: {duplicates}")


def load_documents(
    inventory_path: Path,
    taxonomy_path: Path,
    coverage_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    inventory_value = json.loads(inventory_path.read_text(encoding="utf-8"))
    inventory = {"sources": inventory_value} if isinstance(inventory_value, list) else inventory_value
    taxonomy = yaml.safe_load(taxonomy_path.read_text(encoding="utf-8"))
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    return (
        _object(inventory, "inventory"),
        _object(taxonomy, "taxonomy"),
        _object(coverage, "coverage ledger"),
    )


def validate_documents(
    inventory: dict[str, Any],
    taxonomy: dict[str, Any],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    """Validate documents and return discovery counts for operator/CI reporting."""
    _require(taxonomy.get("schema_version") == "1.0.0", "unexpected taxonomy schema_version")
    _require(taxonomy.get("language") == "en", "taxonomy language must be English")
    _require(coverage.get("schema_version") == "1.0.0", "unexpected coverage schema_version")
    _require(
        coverage.get("taxonomy_version") == taxonomy.get("taxonomy_version"),
        "coverage taxonomy_version does not match the taxonomy",
    )
    _require(
        coverage.get("status") == "candidate-coverage-ledger",
        "coverage ledger must retain candidate status",
    )

    inventory_sources = _list(inventory.get("sources"), "inventory sources")
    inventory_ids = [
        _object(source, f"inventory source {index}").get("id")
        for index, source in enumerate(inventory_sources)
    ]
    _require(all(isinstance(value, str) and value for value in inventory_ids), "invalid inventory ID")
    _unique(inventory_ids, "inventory source IDs")
    _require(
        len(inventory_ids) == EXPECTED_SOURCE_COUNT,
        f"expected {EXPECTED_SOURCE_COUNT} inventory sources, found {len(inventory_ids)}",
    )

    domain_ids: list[str] = []
    subdomain_ids: list[str] = []
    category_ids: list[str] = []
    for domain_index, domain_value in enumerate(
        _list(taxonomy.get("subject_domains"), "subject_domains")
    ):
        domain = _object(domain_value, f"subject domain {domain_index}")
        domain_id = domain.get("id")
        _require(isinstance(domain_id, str) and TAXONOMY_ID.fullmatch(domain_id), "invalid domain ID")
        _require(isinstance(domain.get("name"), str) and domain["name"], f"missing English name: {domain_id}")
        domain_ids.append(domain_id)
        for subdomain_value in _list(domain.get("subdomains"), f"subdomains for {domain_id}"):
            subdomain = _object(subdomain_value, f"subdomain under {domain_id}")
            subdomain_id = subdomain.get("id")
            _require(
                isinstance(subdomain_id, str)
                and TAXONOMY_ID.fullmatch(subdomain_id)
                and subdomain_id.startswith(f"{domain_id}."),
                f"invalid subdomain ID under {domain_id}",
            )
            _require(
                isinstance(subdomain.get("name"), str) and subdomain["name"],
                f"missing English name: {subdomain_id}",
            )
            subdomain_ids.append(subdomain_id)
            for category_value in _list(
                subdomain.get("categories"), f"categories for {subdomain_id}"
            ):
                category = _object(category_value, f"category under {subdomain_id}")
                category_id = category.get("id")
                _require(
                    isinstance(category_id, str)
                    and TAXONOMY_ID.fullmatch(category_id)
                    and category_id.startswith(f"{subdomain_id}."),
                    f"invalid category ID under {subdomain_id}",
                )
                _require(
                    isinstance(category.get("name"), str) and category["name"],
                    f"missing English name: {category_id}",
                )
                category_ids.append(category_id)
    _unique(domain_ids, "domain IDs")
    _unique(subdomain_ids, "subdomain IDs")
    _unique(category_ids, "category IDs")
    _require(len(domain_ids) == EXPECTED_DOMAIN_COUNT, "unexpected subject-domain count")
    _require(len(subdomain_ids) == EXPECTED_SUBDOMAIN_COUNT, "unexpected subdomain count")
    _require(len(category_ids) == EXPECTED_CATEGORY_COUNT, "unexpected category count")
    valid_domains = set(domain_ids)
    valid_categories = set(category_ids)

    dimensions = [
        _object(value, "analytical dimension")
        for value in _list(taxonomy.get("analytical_dimensions"), "analytical_dimensions")
    ]
    dimension_ids = [value.get("id") for value in dimensions]
    _require(
        all(isinstance(value, str) and TAXONOMY_ID.fullmatch(value) for value in dimension_ids),
        "invalid analytical dimension ID",
    )
    _unique(dimension_ids, "analytical dimension IDs")
    _require(len(dimension_ids) == EXPECTED_DIMENSION_COUNT, "unexpected analytical dimension count")
    _require(
        REQUIRED_DIMENSIONS <= set(dimension_ids),
        f"missing required analytical dimensions: {sorted(REQUIRED_DIMENSIONS - set(dimension_ids))}",
    )

    classifications = [
        _object(value, "official classification")
        for value in _list(taxonomy.get("official_classifications"), "official_classifications")
    ]
    classification_ids = [value.get("id") for value in classifications]
    _require(all(isinstance(value, str) and value for value in classification_ids), "invalid classification ID")
    _unique(classification_ids, "official classification IDs")
    _require(
        len(classification_ids) == EXPECTED_CLASSIFICATION_COUNT,
        "unexpected official classification count",
    )
    _require(
        REQUIRED_CLASSIFICATIONS <= set(classification_ids),
        f"missing required classifications: {sorted(REQUIRED_CLASSIFICATIONS - set(classification_ids))}",
    )
    for classification in classifications:
        _require(classification.get("version_required") is True, f"classification version must be required: {classification['id']}")
        _require(
            isinstance(classification.get("reference"), str)
            and classification["reference"].startswith("https://"),
            f"classification needs an official HTTPS reference: {classification['id']}",
        )

    mappings = [
        _object(value, "source mapping")
        for value in _list(coverage.get("sources"), "coverage sources")
    ]
    mapping_ids = [value.get("source_id") for value in mappings]
    _require(mapping_ids == inventory_ids, "coverage source IDs must exactly match inventory order")
    _unique(mapping_ids, "coverage source IDs")
    inventory_category_counter: Counter[str] = Counter()
    for mapping in mappings:
        source_id = mapping["source_id"]
        mapped_domains = _list(mapping.get("candidate_domain_ids"), f"candidate domains for {source_id}")
        mapped_categories = _list(
            mapping.get("candidate_category_ids"), f"candidate categories for {source_id}"
        )
        _require(mapped_domains and mapped_categories, f"source mapping is empty: {source_id}")
        _unique(mapped_domains, f"candidate domains for {source_id}")
        _unique(mapped_categories, f"candidate categories for {source_id}")
        _require(set(mapped_domains) <= valid_domains, f"unknown domain mapping for {source_id}")
        _require(set(mapped_categories) <= valid_categories, f"unknown category mapping for {source_id}")
        _require(
            all(category.split(".", 1)[0] in mapped_domains for category in mapped_categories),
            f"category root is absent from candidate domains for {source_id}",
        )
        _require(mapping.get("mapping_confidence") in CONFIDENCE_VALUES, f"invalid confidence: {source_id}")
        _require(mapping.get("planned_lane") in PLANNED_LANES, f"invalid planned lane: {source_id}")
        _require(isinstance(mapping.get("uncertainty"), str) and mapping["uncertainty"], f"missing uncertainty: {source_id}")
        inventory_category_counter.update(mapped_categories)

    inventory_assignment_count = sum(inventory_category_counter.values())
    _require(
        inventory_assignment_count == EXPECTED_INVENTORY_ASSIGNMENT_COUNT,
        f"expected {EXPECTED_INVENTORY_ASSIGNMENT_COUNT} inventory assignments, found {inventory_assignment_count}",
    )

    additional = [
        _object(value, "additional candidate")
        for value in _list(coverage.get("additional_candidates"), "additional_candidates")
    ]
    additional_ids = [value.get("candidate_id") for value in additional]
    _require(all(isinstance(value, str) and value for value in additional_ids), "invalid additional candidate ID")
    _unique(additional_ids, "additional candidate IDs")
    _require(
        len(additional) == EXPECTED_ADDITIONAL_CANDIDATE_COUNT,
        "unexpected additional candidate count",
    )
    additional_category_counter: Counter[str] = Counter()
    for candidate in additional:
        candidate_categories = _list(
            candidate.get("category_ids"), f"categories for {candidate['candidate_id']}"
        )
        _require(candidate_categories, f"additional candidate has no categories: {candidate['candidate_id']}")
        _unique(candidate_categories, f"categories for {candidate['candidate_id']}")
        _require(
            set(candidate_categories) <= valid_categories,
            f"unknown category for additional candidate {candidate['candidate_id']}",
        )
        _require(
            candidate.get("mapping_confidence") in CONFIDENCE_VALUES,
            f"invalid confidence for additional candidate {candidate['candidate_id']}",
        )
        additional_category_counter.update(candidate_categories)
    additional_assignment_count = sum(additional_category_counter.values())
    _require(
        additional_assignment_count == EXPECTED_ADDITIONAL_ASSIGNMENT_COUNT,
        f"expected {EXPECTED_ADDITIONAL_ASSIGNMENT_COUNT} additional assignments, found {additional_assignment_count}",
    )

    gaps = [
        _object(value, "coverage gap")
        for value in _list(coverage.get("coverage_gaps"), "coverage_gaps")
    ]
    gap_ids = [value.get("gap_id") for value in gaps]
    _require(all(isinstance(value, str) and value for value in gap_ids), "invalid coverage gap ID")
    _unique(gap_ids, "coverage gap IDs")
    _require(len(gaps) == EXPECTED_GAP_COUNT, "unexpected coverage gap count")
    for gap in gaps:
        _require(gap.get("domain_id") in valid_domains, f"unknown domain for gap {gap['gap_id']}")
        _require(isinstance(gap.get("gap"), str) and gap["gap"], f"missing gap text: {gap['gap_id']}")

    stated_counts = _object(
        coverage.get("category_discovery_counts"), "category_discovery_counts"
    )
    _require(set(stated_counts) == valid_categories, "category discovery count keys do not match taxonomy")
    for category_id in category_ids:
        stated = _object(stated_counts[category_id], f"discovery count for {category_id}")
        expected_inventory = inventory_category_counter[category_id]
        expected_additional = additional_category_counter[category_id]
        _require(
            stated.get("inventory_product_candidates") == expected_inventory
            and stated.get("additional_candidates") == expected_additional,
            f"stale category discovery count: {category_id}",
        )

    interpretation = " ".join(_list(coverage.get("interpretation"), "coverage interpretation")).lower()
    count_interpretation = str(coverage.get("category_discovery_interpretation", "")).lower()
    _require(
        "does not claim complete category coverage" in interpretation
        and "successful ingestion" in interpretation
        and "not ingested datasets" in count_interpretation,
        "coverage ledger must state that assignments are discovery evidence only",
    )

    categories_with_inventory = sum(inventory_category_counter[value] > 0 for value in category_ids)
    categories_with_any = sum(
        inventory_category_counter[value] > 0 or additional_category_counter[value] > 0
        for value in category_ids
    )
    return {
        "status": "ok",
        "discovery_evidence_only": True,
        "inventory_sources": len(inventory_ids),
        "source_mappings": len(mapping_ids),
        "subject_domains": len(domain_ids),
        "subdomains": len(subdomain_ids),
        "categories": len(category_ids),
        "analytical_dimensions": len(dimension_ids),
        "official_classifications": len(classification_ids),
        "inventory_candidate_assignments": inventory_assignment_count,
        "additional_candidates": len(additional),
        "additional_candidate_assignments": additional_assignment_count,
        "coverage_gap_records": len(gaps),
        "categories_with_inventory_candidates": categories_with_inventory,
        "categories_without_inventory_candidates": len(category_ids) - categories_with_inventory,
        "categories_with_any_candidates": categories_with_any,
        "categories_without_any_candidates": len(category_ids) - categories_with_any,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inventory",
        type=Path,
        default=ROOT / "docs/source-research/source-inventory.json",
    )
    parser.add_argument("--taxonomy", type=Path, default=ROOT / "config/domain-taxonomy.yaml")
    parser.add_argument(
        "--coverage", type=Path, default=ROOT / "config/source-domain-coverage.json"
    )
    args = parser.parse_args(argv)
    try:
        summary = validate_documents(*load_documents(args.inventory, args.taxonomy, args.coverage))
    except (CoverageValidationError, json.JSONDecodeError, yaml.YAMLError, OSError) as error:
        print(f"source coverage validation failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
