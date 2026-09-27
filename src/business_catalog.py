"""Build the release-bound business catalogue for the NBP platform.

The catalogue is deliberately a small, pure metadata transformation.  Ingestion
state is supplied by the ingestion state protocol, and dbt's manifest is the
only authority used for lineage.  In particular, this module does not inspect
files (or their modification times) and does not manufacture freshness or
coverage values.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping


SOURCE_IDS = (
    "nbp_exchange_rates_table_a",
    "nbp_exchange_rates_table_b",
    "nbp_exchange_rates_table_c",
    "nbp_gold_prices",
)
_SOURCE_ID_SET = frozenset(SOURCE_IDS)
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

# These are business-facing labels for the four currently supported NBP feeds.
# A configuration-provided display name/label takes precedence when present.
_DEFAULT_SOURCE_NAMES = {
    "nbp_exchange_rates_table_a": "NBP Table A (Convertible FX)",
    "nbp_exchange_rates_table_b": "NBP Table B (Middle FX)",
    "nbp_exchange_rates_table_c": "NBP Table C (Bid / Ask FX)",
    "nbp_gold_prices": "NBP Gold Prices",
}
_DEFAULT_SOURCE_DESCRIPTIONS = {
    source_id: ""
    for source_id in SOURCE_IDS
}


class BusinessCatalogError(ValueError):
    """Raised when catalogue inputs do not match the release contract."""


def build_business_catalog(
    *,
    code_sha: str,
    source_config: dict,
    ingestion_state: dict,
    dbt_manifest: dict,
    dataset_metadata: list[dict],
) -> dict:
    """Build a deterministic, release-specific business catalogue.

    ``ingestion_state`` is expected to have a ``sources`` mapping whose values
    have already been normalized to ``checked_through``,
    ``latest_observation_date``, ``last_successful_ingestion_at``,
    ``last_attempt_at`` and ``raw_response_count``.  A direct source mapping is
    accepted as a convenience for callers that have already unwrapped it.

    The returned lineage graph contains the exported models named by
    ``dataset_metadata[*].model_name`` and all of their enabled ancestors that
    are present in the dbt manifest.  Edges point from an upstream dependency
    to its downstream node.
    """
    _require_nonempty_string(code_sha, "code_sha")
    configured_sources = _source_config_mapping(source_config)
    state_sources = _state_source_mapping(ingestion_state)
    _require_exact_source_ids(configured_sources, "source configuration")
    _require_no_unknown_source_ids(state_sources, "ingestion state")
    if not isinstance(dataset_metadata, list):
        raise BusinessCatalogError("dataset_metadata must be a list")
    for item in dataset_metadata:
        if not isinstance(item, Mapping):
            raise BusinessCatalogError("each dataset metadata entry must be an object")

    sources = [
        _build_source_entry(source_id, configured_sources[source_id], state_sources.get(source_id, {}))
        for source_id in SOURCE_IDS
    ]
    lineage = _build_lineage(dbt_manifest, dataset_metadata, configured_sources)

    # Keep dataset metadata release-bound and available to catalogue consumers.
    # It is copied so a caller cannot mutate the catalogue through its input.
    datasets = [
        {field: deepcopy(item[field]) for field in _DATASET_FIELDS if field in item}
        for item in dataset_metadata
    ]
    metrics = _source_metrics(dbt_manifest)
    return {
        "format_version": 1,
        "code_sha": code_sha,
        "sources": sources,
        "datasets": datasets,
        "lineage": lineage,
        "metrics": metrics,
        "metrics_status": "source_defined" if metrics else "awaiting_business_approval",
        "metrics_explanation": (
            "Source-defined daily NBP observations. Use the governed native query command; "
            "prices are not additive over dates, currencies or quotation types."
            if metrics else "No governed source metrics are present in this release's dbt manifest."
        ),
    }


def _source_config_mapping(source_config: Any) -> Mapping[str, Any]:
    if not isinstance(source_config, Mapping):
        raise BusinessCatalogError("source_config must be an object")
    value = source_config.get("sources", source_config)
    if not isinstance(value, Mapping):
        raise BusinessCatalogError("source_config.sources must be an object")
    return value


def _state_source_mapping(ingestion_state: Any) -> Mapping[str, Any]:
    if not isinstance(ingestion_state, Mapping):
        raise BusinessCatalogError("ingestion_state must be an object")
    value = ingestion_state.get("sources", ingestion_state)
    if not isinstance(value, Mapping):
        raise BusinessCatalogError("ingestion_state.sources must be an object")
    return value


def _require_exact_source_ids(value: Mapping[str, Any], label: str) -> None:
    ids = set(value)
    missing = sorted(_SOURCE_ID_SET - ids)
    if missing:
        raise BusinessCatalogError(f"{label} is missing required NBP sources: {missing}")


def _require_no_unknown_source_ids(value: Mapping[str, Any], label: str) -> None:
    extra = sorted(set(value) - _SOURCE_ID_SET)
    if extra:
        raise BusinessCatalogError(f"{label} contains unknown sources: {extra}")


def _require_nonempty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BusinessCatalogError(f"{label} must be a non-empty string")
    return value


def _metadata_block(configured: Any) -> Mapping[str, Any]:
    if not isinstance(configured, Mapping):
        return {}
    metadata = configured.get("metadata")
    return metadata if isinstance(metadata, Mapping) else configured


def _build_source_entry(source_id: str, configured: Any, state: Any) -> dict[str, Any]:
    metadata = _metadata_block(configured)
    state_map = state if isinstance(state, Mapping) else {}

    # ``source_name`` in sources.yaml is the provider (NBP), while the
    # catalogue name is the business-facing dataset label.  Prefer an explicit
    # display field and otherwise use the stable labels for these four feeds.
    name = (
        metadata.get("display_name")
        or metadata.get("name")
        or metadata.get("label")
        or _DEFAULT_SOURCE_NAMES[source_id]
    )
    if not isinstance(name, str) or not name.strip():
        name = _DEFAULT_SOURCE_NAMES[source_id]
    description = metadata.get("description", _DEFAULT_SOURCE_DESCRIPTIONS[source_id])
    if not isinstance(description, str):
        description = str(description) if description is not None else ""

    # These values are intentionally read only from normalized state keys.  In
    # particular, file timestamps and state updated_at values are not freshness
    # evidence and are never consulted.
    result: dict[str, Any] = {
        "source_id": source_id,
        "name": name,
        "description": description,
        "status": "published_snapshot",
        "checked_through": state_map.get("checked_through"),
        "latest_observation_date": state_map.get("latest_observation_date"),
        "last_successful_ingestion_at": state_map.get("last_successful_ingestion_at"),
        "last_attempt_at": state_map.get("last_attempt_at"),
        "raw_response_count": state_map.get("raw_response_count", 0),
        # None explicitly communicates that completeness is unknown.  No date
        # or file inventory is sufficient to infer this flag.
        "coverage_complete": state_map.get("coverage_complete"),
    }
    for field in ("provider_url", "documentation_url", "publication_schedule_url", "frequency",
                  "coverage_start", "reuse_terms_url", "reuse_summary", "quote_unit", "methodology_notes"):
        if field in metadata:
            result[field] = deepcopy(metadata[field])
    raw_count = result["raw_response_count"]
    if not isinstance(raw_count, int) or isinstance(raw_count, bool) or raw_count < 0:
        raise BusinessCatalogError(f"{source_id} raw_response_count must be a nonnegative integer")
    if result["coverage_complete"] is not None and not isinstance(result["coverage_complete"], bool):
        raise BusinessCatalogError(f"{source_id} coverage_complete must be a boolean or null")
    return result


def _build_lineage(
    dbt_manifest: Any,
    dataset_metadata: list[dict],
    configured_sources: Mapping[str, Any],
) -> dict[str, list[dict[str, str]]]:
    if not isinstance(dbt_manifest, Mapping):
        raise BusinessCatalogError("dbt_manifest must be an object")

    records: dict[str, Mapping[str, Any]] = {}
    for section in ("sources", "nodes", "semantic_models", "metrics"):
        values = dbt_manifest.get(section, {})
        if not isinstance(values, Mapping):
            continue
        for key, raw in values.items():
            if not isinstance(raw, Mapping):
                continue
            unique_id = raw.get("unique_id", key)
            if isinstance(unique_id, str) and unique_id:
                records[unique_id] = raw

    exported_ids: set[str] = set()
    for metadata in dataset_metadata:
        model_name = metadata.get("model_name") or metadata.get("model_id")
        if not isinstance(model_name, str) or not model_name:
            continue
        matches = [
            unique_id
            for unique_id, record in records.items()
            if unique_id == model_name or record.get("name") == model_name or record.get("alias") == model_name
        ]
        exported_ids.update(matches)

    included: set[str] = set()

    def visit(unique_id: str) -> None:
        if unique_id in included:
            return
        record = records.get(unique_id)
        if record is None or _excluded_manifest_record(unique_id, record):
            return
        included.add(unique_id)
        dependencies = record.get("depends_on", {})
        dependency_ids = dependencies.get("nodes", []) if isinstance(dependencies, Mapping) else []
        if not isinstance(dependency_ids, list):
            return
        for dependency_id in dependency_ids:
            if isinstance(dependency_id, str) and dependency_id in records:
                visit(dependency_id)

    for unique_id in sorted(exported_ids):
        visit(unique_id)
    for metric in _source_metrics(dbt_manifest):
        visit(metric["unique_id"])

    nodes: list[dict[str, str]] = []
    for unique_id in sorted(included):
        record = records[unique_id]
        resource_type = record.get("resource_type")
        kind = resource_type if isinstance(resource_type, str) and resource_type else _kind_from_id(unique_id)
        source_id = _source_id_for_record(unique_id, record)
        metadata = _metadata_block(configured_sources.get(source_id, {})) if source_id else {}
        if source_id:
            label = _source_label(source_id, metadata)
            description = metadata.get("description") or record.get("description") or ""
        else:
            label = record.get("name") or record.get("alias") or unique_id.rsplit(".", 1)[-1]
            description = record.get("description") or ""
        if not isinstance(label, str) or not label.strip():
            label = unique_id.rsplit(".", 1)[-1]
        if not isinstance(description, str) or not description.strip():
            description = f"dbt {kind} {label}."
        nodes.append({
            "id": unique_id,
            "label": label,
            "kind": kind,
            "layer": _layer_for_record(kind, record),
            "description": description,
        })

    edges: list[dict[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()
    for unique_id in sorted(included):
        record = records[unique_id]
        dependencies = record.get("depends_on", {})
        dependency_ids = dependencies.get("nodes", []) if isinstance(dependencies, Mapping) else []
        if not isinstance(dependency_ids, list):
            continue
        for dependency_id in dependency_ids:
            edge = (dependency_id, unique_id)
            if dependency_id in included and edge not in seen_edges:
                seen_edges.add(edge)
                edges.append({"from": dependency_id, "to": unique_id})
    return {"nodes": nodes, "edges": edges}


def _source_metrics(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Use only real dbt metric records explicitly marked as source definitions."""
    metrics = manifest.get("metrics", {})
    if not isinstance(metrics, Mapping):
        return []
    result = []
    for unique_id, record in sorted(metrics.items()):
        if not isinstance(record, Mapping) or _excluded_manifest_record(unique_id, record):
            continue
        config = record.get("config", {})
        meta = config.get("meta", {}) if isinstance(config, Mapping) else {}
        if not meta:
            meta = record.get("meta", {})
        if not isinstance(meta, Mapping) or meta.get("definition_status") != "source_defined":
            continue
        result.append({"unique_id": unique_id, **{
            field: deepcopy(record[field]) for field in
            ("name", "label", "description", "type", "type_params", "filter", "depends_on") if field in record
        }, "meta": deepcopy(meta)})
    return result


def _excluded_manifest_record(unique_id: str, record: Mapping[str, Any]) -> bool:
    resource_type = record.get("resource_type")
    if resource_type in {"test", "unit_test", "fixture"} or unique_id.startswith("test."):
        return True
    config = record.get("config")
    if isinstance(config, Mapping) and config.get("enabled") is False:
        return True
    if record.get("enabled") is False:
        return True
    searchable = " ".join(str(record.get(key, "")) for key in ("name", "alias", "path", "original_file_path", "unique_id")).lower()
    if "fixture" in searchable or "tests/fixtures" in searchable:
        return True
    name = str(record.get("name", ""))
    if name.startswith("bdl_") or "gus_bdl" in searchable:
        return True
    return False


def _source_id_for_record(unique_id: str, record: Mapping[str, Any]) -> str | None:
    candidates = [unique_id, record.get("name"), record.get("identifier"), record.get("alias")]
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        for source_id in SOURCE_IDS:
            if candidate == source_id or candidate.endswith(f".{source_id}") or candidate.endswith(f"_{source_id}"):
                return source_id
    return None


def _source_label(source_id: str, metadata: Mapping[str, Any]) -> str:
    value = metadata.get("display_name") or metadata.get("name") or metadata.get("label")
    return value if isinstance(value, str) and value.strip() else _DEFAULT_SOURCE_NAMES[source_id]


def _kind_from_id(unique_id: str) -> str:
    return unique_id.split(".", 1)[0] if "." in unique_id else "dbt"


def _layer_for_record(kind: str, record: Mapping[str, Any]) -> str:
    config = record.get("config")
    meta = config.get("meta") if isinstance(config, Mapping) else None
    if isinstance(meta, Mapping) and isinstance(meta.get("layer"), str) and meta["layer"].strip():
        return meta["layer"]
    direct_meta = record.get("meta")
    if isinstance(direct_meta, Mapping) and isinstance(direct_meta.get("layer"), str) and direct_meta["layer"].strip():
        return direct_meta["layer"]
    if kind in {"semantic_model", "semantic"}:
        return "semantic"
    if kind == "metric":
        return "metrics"
    schema = record.get("schema")
    schema_layers = {
        "01_landing": "landing",
        "02_bronze": "bronze",
        "03_silver": "silver",
        "04_gold": "gold",
    }
    if isinstance(schema, str) and schema.strip().lower() in schema_layers:
        return schema_layers[schema.strip().lower()]
    if kind == "source":
        # Older manifests did not record the physical source schema. Their
        # configured source boundary was Bronze, so preserve that interpretation.
        return "bronze"
    searchable = " ".join(str(record.get(key, "")) for key in ("name", "path", "original_file_path")).lower()
    if "staging" in searchable or ".stg_" in searchable or "stg_" in searchable:
        return "silver"
    if "mart" in searchable or "gold" in searchable or "fact_" in searchable or "dim_" in searchable:
        return "gold"
    if kind in {"seed", "snapshot"}:
        return "bronze"
    return "dbt"


__all__ = ["BusinessCatalogError", "SOURCE_IDS", "build_business_catalog"]
