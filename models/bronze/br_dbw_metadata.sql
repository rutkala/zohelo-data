{{ config(materialized='table', alias='dbw_metadata', enabled=var('enable_gus_dbw', false)) }}
{{ assert_dbw_bronze_release() }}

select
    indicator_id,
    metric_name,
    metric_name_en,
    description,
    frequency,
    measure_unit,
    data_source,
    legal_basis,
    last_update,
    processed_at_utc
from {{ source('bronze_dbw', 'metadata') }}
