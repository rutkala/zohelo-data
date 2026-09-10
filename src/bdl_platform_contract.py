"""Contract constants for the BDL modeled platform release."""

BDL_SOURCE_ID = "gus_bdl"

BDL_PLATFORM_DATASETS = {
    "bronze_bdl_variables": ("02_bronze", "model.zohelo_data.br_bdl_variables"),
    "bronze_bdl_subjects": ("02_bronze", "model.zohelo_data.br_bdl_subjects"),
    "bronze_bdl_units": ("02_bronze", "model.zohelo_data.br_bdl_units"),
    "bronze_bdl_dictionary_entries": ("02_bronze", "model.zohelo_data.br_bdl_dictionary_entries"),
    "bronze_bdl_years": ("02_bronze", "model.zohelo_data.br_bdl_years"),
    "bronze_bdl_observations": ("02_bronze", "model.zohelo_data.br_bdl_observations"),
    "bdl_variables": ("03_silver", "model.zohelo_data.stg_bdl_variables"),
    "bdl_subjects": ("03_silver", "model.zohelo_data.stg_bdl_subjects"),
    "bdl_units": ("03_silver", "model.zohelo_data.stg_bdl_units"),
    "bdl_dictionary_entries": ("03_silver", "model.zohelo_data.stg_bdl_dictionary_entries"),
    "bdl_observation_revisions": ("03_silver", "model.zohelo_data.bdl_observation_revisions"),
    "bdl_observations": ("03_silver", "model.zohelo_data.stg_bdl_observations"),
    "dim_bdl_period": ("04_gold", "model.zohelo_data.dim_bdl_period"),
    "dim_bdl_subject": ("04_gold", "model.zohelo_data.dim_bdl_subject"),
    "dim_bdl_variable": ("04_gold", "model.zohelo_data.dim_bdl_variable"),
    "dim_bdl_unit": ("04_gold", "model.zohelo_data.dim_bdl_unit"),
    "fact_bdl_observations": ("04_gold", "model.zohelo_data.fact_bdl_observations"),
    "mart_bdl_coverage": ("04_gold", "model.zohelo_data.mart_bdl_coverage"),
}

BDL_PLATFORM_DATE_COLUMNS = {
    "bronze_bdl_variables": None,
    "bronze_bdl_subjects": None,
    "bronze_bdl_units": None,
    "bronze_bdl_dictionary_entries": None,
    "bronze_bdl_years": None,
    "bronze_bdl_observations": None,
    "bdl_variables": None,
    "bdl_subjects": None,
    "bdl_units": None,
    "bdl_dictionary_entries": None,
    "bdl_observation_revisions": None,
    "bdl_observations": None,
    "dim_bdl_period": "period_start_date",
    "dim_bdl_subject": None,
    "dim_bdl_variable": None,
    "dim_bdl_unit": None,
    "fact_bdl_observations": "period_start_date",
    "mart_bdl_coverage": "snapshot_date",
}

BDL_PLATFORM_MODEL_NAMES = {
    dataset_id: model_id.rsplit('.', 1)[-1]
    for dataset_id, (_layer, model_id) in BDL_PLATFORM_DATASETS.items()
}
