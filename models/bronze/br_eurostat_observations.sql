{{ config(materialized='table', alias='eurostat_observations', enabled=var('enable_eurostat', false)) }}

select
    dataset_id,
    dataset_label,
    geo_code,
    freq_code,
    unit_code,
    period_key,
    dimension_key_json,
    dimension_key_sha256,
    value_json,
    value_numeric,
    status_code,
    is_missing,
    source_updated_at,
    retrieved_at_utc,
    response_sha256,
    task_id,
    lane
from {{ source('landing', 'eurostat_decoded_observations') }}
