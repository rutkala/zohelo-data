{{ config(
    materialized='table',
    alias='bdl_web_observations',
    enabled=var('enable_gus_bdl_web', false)
) }}

select
    subgroup_id,
    selection_id,
    unit_id,
    unit_name,
    terc_code,
    period_raw,
    period_year,
    val_raw,
    val_numeric,
    measure_unit,
    attr_name,
    dimensions_json,
    raw_archive_file,
    processed_at_utc
from {{ source('bronze_bdl_web', 'observations') }}
