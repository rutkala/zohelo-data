{{ config(alias='dbw_metadata', enabled=var('enable_gus_dbw', false)) }}

select
    cast(indicator_id as integer) as indicator_id,
    trim(metric_name) as metric_name,
    trim(metric_name_en) as metric_name_en,
    trim(description) as description,
    trim(frequency) as frequency,
    trim(measure_unit) as measure_unit,
    trim(data_source) as data_source,
    trim(legal_basis) as legal_basis,
    last_update,
    processed_at_utc
from {{ ref('br_dbw_metadata') }}
