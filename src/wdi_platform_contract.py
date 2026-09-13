"""Contract constants for the complete-archive WDI modeled release."""

WDI_SOURCE_ID = "world_bank_wdi"
WDI_RELEASE_SCOPE = "wdi_platform"

WDI_ARCHIVE_MEMBERS = {
    "WDICountry.csv": "ZOHELO_WDI_COUNTRY_CSV",
    "WDICountry-Series.csv": "ZOHELO_WDI_COUNTRY_SERIES_CSV",
    "WDIData.csv": "ZOHELO_WDI_DATA_CSV",
    "WDIFootNote.csv": "ZOHELO_WDI_FOOTNOTE_CSV",
    "WDISeries.csv": "ZOHELO_WDI_SERIES_CSV",
    "WDISeries-Time.csv": "ZOHELO_WDI_SERIES_TIME_CSV",
}

WDI_PLATFORM_DATASETS = {
    "bronze_wdi_country": ("02_bronze", "model.zohelo_data.br_wdi_country"),
    "bronze_wdi_country_series": ("02_bronze", "model.zohelo_data.br_wdi_country_series"),
    "bronze_wdi_data": ("02_bronze", "model.zohelo_data.br_wdi_data"),
    "bronze_wdi_footnote": ("02_bronze", "model.zohelo_data.br_wdi_footnote"),
    "bronze_wdi_series": ("02_bronze", "model.zohelo_data.br_wdi_series"),
    "bronze_wdi_series_time": ("02_bronze", "model.zohelo_data.br_wdi_series_time"),
    "wdi_countries": ("03_silver", "model.zohelo_data.stg_wdi_countries"),
    "wdi_indicators": ("03_silver", "model.zohelo_data.stg_wdi_indicators"),
    "wdi_observations": ("03_silver", "model.zohelo_data.stg_wdi_observations"),
    "dim_wdi_geography": ("04_gold", "model.zohelo_data.dim_wdi_geography"),
    "dim_wdi_indicator": ("04_gold", "model.zohelo_data.dim_wdi_indicator"),
    "dim_wdi_year": ("04_gold", "model.zohelo_data.dim_wdi_year"),
    "fact_wdi_observations": ("04_gold", "model.zohelo_data.fact_wdi_observations"),
    "mart_wdi_coverage": ("04_gold", "model.zohelo_data.mart_wdi_coverage"),
}

WDI_PLATFORM_DATE_COLUMNS = {
    "bronze_wdi_country": None,
    "bronze_wdi_country_series": None,
    "bronze_wdi_data": None,
    "bronze_wdi_footnote": None,
    "bronze_wdi_series": None,
    "bronze_wdi_series_time": None,
    "wdi_countries": None,
    "wdi_indicators": None,
    "wdi_observations": "observation_date",
    "dim_wdi_geography": None,
    "dim_wdi_indicator": None,
    "dim_wdi_year": "year_start_date",
    "fact_wdi_observations": "observation_date",
    "mart_wdi_coverage": "snapshot_date",
}

WDI_ALLOW_ZERO_ROWS = frozenset({
    "bronze_wdi_country_series",
    "bronze_wdi_footnote",
    "bronze_wdi_series_time",
})

WDI_PLATFORM_MODEL_NAMES = {
    dataset_id: model_id.rsplit(".", 1)[-1]
    for dataset_id, (_layer, model_id) in WDI_PLATFORM_DATASETS.items()
}
