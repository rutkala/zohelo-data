"""Contract constants for the progressive Eurostat modeled release."""

EUROSTAT_SOURCE_ID = "eurostat"
EUROSTAT_RELEASE_SCOPE = "eurostat_progressive_api_platform"

EUROSTAT_PLATFORM_DATASETS = {
    "bronze_eurostat_observations": ("02_bronze", "model.zohelo_data.br_eurostat_observations"),
    "eurostat_observation_revisions": ("03_silver", "model.zohelo_data.eurostat_observation_revisions"),
    "eurostat_observations": ("03_silver", "model.zohelo_data.stg_eurostat_observations"),
    "dim_eurostat_dataset": ("04_gold", "model.zohelo_data.dim_eurostat_dataset"),
    "dim_eurostat_geography": ("04_gold", "model.zohelo_data.dim_eurostat_geography"),
    "dim_eurostat_period": ("04_gold", "model.zohelo_data.dim_eurostat_period"),
    "fact_eurostat_observations": ("04_gold", "model.zohelo_data.fact_eurostat_observations"),
    "mart_eurostat_coverage": ("04_gold", "model.zohelo_data.mart_eurostat_coverage"),
}

EUROSTAT_PLATFORM_DATE_COLUMNS = {
    "bronze_eurostat_observations": None,
    "eurostat_observation_revisions": None,
    "eurostat_observations": None,
    "dim_eurostat_dataset": None,
    "dim_eurostat_geography": None,
    "dim_eurostat_period": "period_start_date",
    "fact_eurostat_observations": "period_start_date",
    "mart_eurostat_coverage": "snapshot_date",
}

EUROSTAT_PLATFORM_MODEL_NAMES = {
    dataset_id: model_id.rsplit(".", 1)[-1]
    for dataset_id, (_layer, model_id) in EUROSTAT_PLATFORM_DATASETS.items()
}
