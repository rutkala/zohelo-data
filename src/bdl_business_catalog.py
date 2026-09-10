"""Build the release-bound business catalogue for the BDL modeled platform."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping


SOURCE_ID = "gus_bdl"
_SOURCE_SET = frozenset({SOURCE_ID})
_DEFAULT_NAME = "GUS BDL (Local Data Bank)"
_DATASET_FIELDS = (
    "dataset_id",
    "table_name",
    "layer",
    "model_name",
    "row_count",
    "min_date",
    "max_date",
    "date_column",
    "columns",
)


class BdlBusinessCatalogError(ValueError):
    """Raised when BDL catalogue inputs do not match the release contract."""


def build_bdl_business_catalog(
    *,
    code_sha: str,
    source_config: dict,
    ingestion_state: dict,
    dbt_manifest: dict,
    dataset_metadata: list[dict],
) -> dict:
    if not isinstance(code_sha, str) or not code_sha.strip():
        raise BdlBusinessCatalogError("code_sha must be a non-empty string")
    configured = _source_config(source_config)
    if set(configured) != _SOURCE_SET:
        raise BdlBusinessCatalogError("BDL source configuration must contain only gus_bdl")
    state = _source_state(ingestion_state)
    if any(source_id not in _SOURCE_SET for source_id in state):
        raise BdlBusinessCatalogError("BDL ingestion state contains unknown sources")
    if not isinstance(dataset_metadata, list):
        raise BdlBusinessCatalogError("dataset_metadata must be a list")

    source_entry = _build_source_entry(configured[SOURCE_ID], state.get(SOURCE_ID, {}))
    lineage = _build_lineage(dbt_manifest, dataset_metadata)
    datasets = [
        {field: deepcopy(item[field]) for field in _DATASET_FIELDS if field in item}
        for item in dataset_metadata
        if isinstance(item, Mapping)
    ]
    metrics = _source_metrics(dbt_manifest)
    return {
        "format_version": 1,
        "code_sha": code_sha,
        "sources": [source_entry],
        "datasets": datasets,
        "lineage": lineage,
        "metrics": metrics,
        "metrics_status": "source_defined" if metrics else "awaiting_business_approval",
        "metrics_explanation": (
            "Release-bound BDL coverage metrics report source-universe, discovered-variable, accepted-landing, and modeled totals at one snapshot date. Heterogeneous observation values remain queryable tables and are not exposed as additive semantic metrics."
            if metrics else "No governed BDL semantic metrics are present in this release's dbt manifest."
        ),
    }


def _source_config(source_config: Any) -> Mapping[str, Any]:
    if not isinstance(source_config, Mapping):
        raise BdlBusinessCatalogError("source_config must be an object")
    sources = source_config.get("sources", source_config)
    if not isinstance(sources, Mapping):
        raise BdlBusinessCatalogError("source_config.sources must be an object")
    return {key: value for key, value in sources.items() if key == SOURCE_ID}


def _source_state(ingestion_state: Any) -> Mapping[str, Any]:
    if not isinstance(ingestion_state, Mapping):
        raise BdlBusinessCatalogError("ingestion_state must be an object")
    sources = ingestion_state.get("sources", ingestion_state)
    if not isinstance(sources, Mapping):
        raise BdlBusinessCatalogError("ingestion_state.sources must be an object")
    return sources


def _metadata_block(configured: Any) -> Mapping[str, Any]:
    if not isinstance(configured, Mapping):
        return {}
    metadata = configured.get("metadata")
    return metadata if isinstance(metadata, Mapping) else configured


def _build_source_entry(configured: Any, state: Any) -> dict[str, Any]:
    metadata = _metadata_block(configured)
    state_map = state if isinstance(state, Mapping) else {}
    name = metadata.get("display_name") or metadata.get("name") or metadata.get("label") or _DEFAULT_NAME
    description = metadata.get("description") or ""
    result = {
        "source_id": SOURCE_ID,
        "name": name if isinstance(name, str) and name.strip() else _DEFAULT_NAME,
        "description": description if isinstance(description, str) else str(description),
        "status": "published_snapshot",
        "checked_through": state_map.get("checked_through"),
        "latest_observation_date": state_map.get("latest_observation_date"),
        "last_successful_ingestion_at": state_map.get("last_successful_ingestion_at"),
        "last_attempt_at": state_map.get("last_attempt_at"),
        "raw_response_count": state_map.get("raw_response_count", 0),
        "coverage_complete": state_map.get("coverage_complete"),
    }
    for field in (
        "provider_url",
        "documentation_url",
        "publication_schedule_url",
        "frequency",
        "coverage_start",
        "reuse_terms_url",
        "reuse_summary",
        "quote_unit",
        "methodology_notes",
    ):
        if field in metadata:
            result[field] = deepcopy(metadata[field])
    coverage = state_map.get("coverage")
    if isinstance(coverage, Mapping):
        result["coverage"] = deepcopy(coverage)
    raw_count = result["raw_response_count"]
    if not isinstance(raw_count, int) or isinstance(raw_count, bool) or raw_count < 0:
        raise BdlBusinessCatalogError("gus_bdl raw_response_count must be a nonnegative integer")
    return result


def _build_lineage(dbt_manifest: Any, dataset_metadata: list[dict]) -> dict[str, list[dict[str, str]]]:
    if not isinstance(dbt_manifest, Mapping):
        raise BdlBusinessCatalogError("dbt_manifest must be an object")
    records: dict[str, Mapping[str, Any]] = {}
    for section in ("sources", "nodes", "semantic_models", "metrics"):
        values = dbt_manifest.get(section, {})
        if not isinstance(values, Mapping):
            continue
        for key, raw in values.items():
            if isinstance(raw, Mapping):
                unique_id = raw.get("unique_id", key)
                if isinstance(unique_id, str) and unique_id:
                    records[unique_id] = raw

    exported_ids: set[str] = set()
    for metadata in dataset_metadata:
        if not isinstance(metadata, Mapping):
            continue
        model_name = metadata.get("model_name") or metadata.get("model_id")
        if not isinstance(model_name, str) or not model_name:
            continue
        for unique_id, record in records.items():
            if unique_id == model_name or record.get("name") == model_name or record.get("alias") == model_name:
                exported_ids.add(unique_id)

    included: set[str] = set()

    def visit(unique_id: str) -> None:
        if unique_id in included:
            return
        record = records.get(unique_id)
        if record is None or _excluded(record, unique_id):
            return
        included.add(unique_id)
        deps = record.get("depends_on", {})
        nodes = deps.get("nodes", []) if isinstance(deps, Mapping) else []
        if isinstance(nodes, list):
            for dependency_id in nodes:
                if isinstance(dependency_id, str):
                    visit(dependency_id)

    for unique_id in sorted(exported_ids):
        visit(unique_id)
    for metric in _source_metrics(dbt_manifest):
        visit(metric["unique_id"])

    nodes: list[dict[str, str]] = []
    for unique_id in sorted(included):
        record = records[unique_id]
        kind = record.get("resource_type") or (unique_id.split('.', 1)[0] if '.' in unique_id else 'dbt')
        label = record.get("name") or record.get("alias") or unique_id.rsplit('.', 1)[-1]
        description = record.get("description") or f"dbt {kind} {label}."
        nodes.append({
            "id": unique_id,
            "label": label,
            "kind": kind,
            "layer": _layer(kind, record),
            "description": description,
        })

    edges: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for unique_id in sorted(included):
        record = records[unique_id]
        deps = record.get("depends_on", {})
        dependencies = deps.get("nodes", []) if isinstance(deps, Mapping) else []
        if isinstance(dependencies, list):
            for dependency_id in dependencies:
                edge = (dependency_id, unique_id)
                if dependency_id in included and edge not in seen:
                    seen.add(edge)
                    edges.append({"from": dependency_id, "to": unique_id})
    return {"nodes": nodes, "edges": edges}


def _source_metrics(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    metrics = manifest.get("metrics", {})
    if not isinstance(metrics, Mapping):
        return []
    result = []
    for unique_id, record in sorted(metrics.items()):
        if not isinstance(record, Mapping) or _excluded(record, unique_id):
            continue
        config = record.get("config", {})
        meta = config.get("meta") if isinstance(config, Mapping) else None
        if not meta:
            meta = record.get("meta")
        if not isinstance(meta, Mapping) or meta.get("definition_status") != "source_defined":
            continue
        copied = {
            field: deepcopy(record[field])
            for field in ("name", "label", "description", "type", "type_params", "filter", "depends_on")
            if field in record
        }
        result.append({"unique_id": unique_id, **copied, "meta": deepcopy(meta)})
    return result


def _excluded(record: Mapping[str, Any], unique_id: str) -> bool:
    if record.get("resource_type") in {"test", "unit_test", "fixture"} or unique_id.startswith("test."):
        return True
    config = record.get("config")
    if isinstance(config, Mapping) and config.get("enabled") is False:
        return True
    searchable = " ".join(
        str(record.get(key, "")) for key in ("name", "alias", "path", "original_file_path", "unique_id")
    ).lower()
    return "fixture" in searchable or "tests/fixtures" in searchable


def _layer(kind: str, record: Mapping[str, Any]) -> str:
    if kind in {"semantic_model", "semantic"}:
        return "semantic"
    if kind == "metric":
        return "metrics"
    schema = record.get("schema")
    mapping = {"01_landing": "landing", "02_bronze": "bronze", "03_silver": "silver", "04_gold": "gold"}
    if isinstance(schema, str) and schema in mapping:
        return mapping[schema]
    if kind == "source":
        return "landing"
    searchable = " ".join(str(record.get(key, "")) for key in ("name", "path", "original_file_path")).lower()
    if "stg_" in searchable:
        return "silver"
    if "fact_" in searchable or "dim_" in searchable or "mart_" in searchable:
        return "gold"
    if "br_" in searchable:
        return "bronze"
    return "dbt"
