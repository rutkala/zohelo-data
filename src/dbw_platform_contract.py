"""Contract constants for the GUS DBW modeled platform release."""

DBW_SOURCE_ID = "gus_dbw"

DBW_PLATFORM_DATASETS = {
    "bronze_dbw_dictionaries": ("02_bronze", "model.zohelo_data.br_dbw_dictionaries"),
    "bronze_dbw_indicators": ("02_bronze", "model.zohelo_data.br_dbw_indicators"),
    "bronze_dbw_metadata": ("02_bronze", "model.zohelo_data.br_dbw_metadata"),
    "bronze_dbw_observations": ("02_bronze", "model.zohelo_data.br_dbw_observations"),
    "dbw_dictionaries": ("03_silver", "model.zohelo_data.stg_dbw_dictionaries"),
    "dbw_indicators": ("03_silver", "model.zohelo_data.stg_dbw_indicators"),
    "dbw_metadata": ("03_silver", "model.zohelo_data.stg_dbw_metadata"),
    "dbw_observations": ("03_silver", "model.zohelo_data.stg_dbw_observations"),
    "dim_dbw_indicator": ("04_gold", "model.zohelo_data.dim_dbw_indicator"),
    "fact_dbw_observations": ("04_gold", "model.zohelo_data.fact_dbw_observations"),
    "mart_dbw_coverage": ("04_gold", "model.zohelo_data.mart_dbw_coverage"),
}

DBW_PLATFORM_DATE_COLUMNS = {
    "bronze_dbw_dictionaries": None,
    "bronze_dbw_indicators": None,
    "bronze_dbw_metadata": None,
    "bronze_dbw_observations": "period_year",
    "dbw_dictionaries": None,
    "dbw_indicators": None,
    "dbw_metadata": None,
    "dbw_observations": "period_year",
    "dim_dbw_indicator": None,
    "fact_dbw_observations": "period_year",
    "mart_dbw_coverage": None,
}

DBW_PLATFORM_MODEL_NAMES = {
    dataset_id: model_id.rsplit(".", 1)[-1]
    for dataset_id, (_layer, model_id) in DBW_PLATFORM_DATASETS.items()
}
