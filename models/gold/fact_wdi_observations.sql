{{ config(materialized='table', enabled=var('enable_wdi', false)) }}

select
    country_code as geography_key,
    indicator_code as indicator_key,
    observation_year as year_key,
    observation_date,
    observation_value,
    observation_value_text,
    archive_sha256,
    archive_retrieved_at_utc
from {{ ref('stg_wdi_observations') }}
