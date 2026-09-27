"""Build the release-bound business catalogue for the WDI modeled platform."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from wdi_semantic import METRICS as WDI_METRIC_SPECS


SOURCE_ID = "world_bank_wdi"
_DEFAULT_NAME = "World Development Indicators (WDI)"
_DATASET_FIELDS = (
    "dataset_id", "table_name", "layer", "model_name", "row_count",
    "min_date", "max_date", "date_column", "columns",
)


class WdiBusinessCatalogError(ValueError):
    """Raised when WDI catalogue inputs do not match the release contract."""


def build_wdi_business_catalog(*, code_sha: str, source_config: dict,
                               ingestion_state: dict, dbt_manifest: dict,
                               dataset_metadata: list[dict]) -> dict:
    if not isinstance(code_sha, str) or not code_sha.strip():
        raise WdiBusinessCatalogError("code_sha must be a non-empty string")
    configured = _source_config(source_config)
    if SOURCE_ID not in configured:
        raise WdiBusinessCatalogError("source configuration must include world_bank_wdi")
    state = _source_state(ingestion_state)
    if set(state) - {SOURCE_ID}:
        raise WdiBusinessCatalogError("WDI ingestion state contains unknown sources")
    if not isinstance(dataset_metadata, list):
        raise WdiBusinessCatalogError("dataset_metadata must be a list")
    source_entry = _source_entry(configured[SOURCE_ID], state.get(SOURCE_ID, {}))
    metrics = _source_metrics(dbt_manifest)
    return {
        "format_version": 1,
        "code_sha": code_sha,
        "sources": [source_entry],
        "datasets": [
            {field: deepcopy(item[field]) for field in _DATASET_FIELDS if field in item}
            for item in dataset_metadata if isinstance(item, Mapping)
        ],
        "lineage": _build_lineage(dbt_manifest, metrics),
        "metrics": metrics,
        "metrics_status": "source_defined" if metrics else "awaiting_business_approval",
        "metrics_explanation": (
            "Release-bound WDI archive and modeled-value coverage metrics are identity selectors at one archive snapshot date. Heterogeneous indicator observations remain queryable and are not exposed as a generic additive metric."
            if metrics else "No governed WDI semantic metrics are present in this release's dbt manifest."
        ),
    }


def _source_config(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WdiBusinessCatalogError("source_config must be an object")
    sources = value.get("sources", value)
    if not isinstance(sources, Mapping):
        raise WdiBusinessCatalogError("source_config.sources must be an object")
    return {key: item for key, item in sources.items() if key == SOURCE_ID}


def _source_state(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WdiBusinessCatalogError("ingestion_state must be an object")
    sources = value.get("sources", value)
    if not isinstance(sources, Mapping):
        raise WdiBusinessCatalogError("ingestion_state.sources must be an object")
    return sources


def _source_entry(configured: Any, state: Any) -> dict[str, Any]:
    metadata = configured.get("metadata", configured) if isinstance(configured, Mapping) else {}
    status = state if isinstance(state, Mapping) else {}
    result = {
        "source_id": SOURCE_ID,
        "name": metadata.get("display_name") or metadata.get("source_name") or _DEFAULT_NAME,
        "description": metadata.get("description") or "",
        "status": "published_snapshot",
        "checked_through": status.get("checked_through"),
        "latest_observation_date": status.get("latest_observation_date"),
        "last_successful_ingestion_at": status.get("last_successful_ingestion_at"),
        "last_attempt_at": status.get("last_attempt_at"),
        "raw_response_count": status.get("raw_response_count", 0),
        "coverage_complete": status.get("coverage_complete"),
    }
    for field in (
        "provider_url", "documentation_url", "publication_schedule_url", "frequency",
        "coverage_start", "reuse_terms_url", "reuse_summary", "quote_unit",
        "methodology_notes",
    ):
        if field in metadata:
            result[field] = deepcopy(metadata[field])
    if isinstance(status.get("coverage"), Mapping):
        result["coverage"] = deepcopy(status["coverage"])
    return result


def _records(manifest: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for collection in ("sources", "nodes", "semantic_models", "metrics"):
        values = manifest.get(collection, {}) if isinstance(manifest, Mapping) else {}
        if isinstance(values, Mapping):
            for unique_id, record in values.items():
                if isinstance(unique_id, str) and isinstance(record, Mapping) and not _excluded(record, unique_id):
                    result[unique_id] = record
    return result


def _build_lineage(manifest: Mapping[str, Any], metrics: list[dict[str, Any]]) -> dict:
    records = _records(manifest)
    included: set[str] = set()

    def visit(unique_id: str) -> None:
        if unique_id in included or unique_id not in records:
            return
        included.add(unique_id)
        dependencies = records[unique_id].get("depends_on", {})
        nodes = dependencies.get("nodes", []) if isinstance(dependencies, Mapping) else []
        if isinstance(nodes, list):
            for dependency in nodes:
                if isinstance(dependency, str):
                    visit(dependency)

    for unique_id, record in records.items():
        if record.get("resource_type") == "model" and any(
            token in str(record.get("name", "")) for token in ("wdi_", "_wdi")
        ):
            visit(unique_id)
    for metric in metrics:
        visit(metric["unique_id"])

    nodes = []
    for unique_id in sorted(included):
        record = records[unique_id]
        kind = record.get("resource_type") or unique_id.split(".", 1)[0]
        label = record.get("name") or record.get("alias") or unique_id.rsplit(".", 1)[-1]
        nodes.append({
            "id": unique_id, "label": label, "kind": kind,
            "layer": _layer(kind, record),
            "description": record.get("description") or f"dbt {kind} {label}.",
        })
    edges, seen = [], set()
    for unique_id in sorted(included):
        dependencies = records[unique_id].get("depends_on", {})
        values = dependencies.get("nodes", []) if isinstance(dependencies, Mapping) else []
        if isinstance(values, list):
            for dependency in values:
                edge = (dependency, unique_id)
                if dependency in included and edge not in seen:
                    seen.add(edge)
                    edges.append({"from": dependency, "to": unique_id})
    return {"nodes": nodes, "edges": edges}


def _source_metrics(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    values = manifest.get("metrics", {}) if isinstance(manifest, Mapping) else {}
    if not isinstance(values, Mapping):
        return []
    result = []
    for unique_id, record in sorted(values.items()):
        if not isinstance(record, Mapping) or _excluded(record, unique_id):
            continue
        if record.get("name") not in WDI_METRIC_SPECS:
            continue
        config = record.get("config", {})
        meta = config.get("meta") if isinstance(config, Mapping) else None
        if not isinstance(meta, Mapping) or meta.get("definition_status") != "source_defined":
            continue
        copied = {field: deepcopy(record[field]) for field in (
            "name", "label", "description", "type", "type_params", "filter", "depends_on"
        ) if field in record}
        result.append({"unique_id": unique_id, **copied, "meta": deepcopy(meta)})
    return result


def _excluded(record: Mapping[str, Any], unique_id: str) -> bool:
    if record.get("resource_type") in {"test", "unit_test", "fixture"} or unique_id.startswith("test."):
        return True
    config = record.get("config")
    if isinstance(config, Mapping) and config.get("enabled") is False:
        return True
    searchable = " ".join(str(record.get(key, "")) for key in (
        "name", "alias", "path", "original_file_path", "unique_id"
    )).lower()
    return "fixture" in searchable or "tests/fixtures" in searchable


def _layer(kind: str, record: Mapping[str, Any]) -> str:
    if kind in {"semantic_model", "semantic"}:
        return "semantic"
    if kind == "metric":
        return "metrics"
    schema = record.get("schema")
    return {"01_landing": "landing", "02_bronze": "bronze", "03_silver": "silver", "04_gold": "gold"}.get(schema, "landing" if kind == "source" else "dbt")
