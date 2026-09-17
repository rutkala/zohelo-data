{{ config(materialized='table', enabled=var('enable_eurostat', false)) }}

select
    observations.dataset_id as dataset_key,
    observations.geo_code as geography_key,
    observations.period_key,
    periods.period_start_date,
    periods.period_end_date,
    periods.period_granularity,
    observations.freq_code,
    observations.unit_code,
    observations.dimension_key_json,
    observations.dimension_key_sha256,
    observations.value_numeric,
    observations.value_json,
    observations.status_code,
    observations.is_missing,
    observations.revision_count,
    observations.replayed_response_count,
    observations.current_revision_event_type,
    observations.source_updated_at,
    observations.first_seen_at_utc,
    observations.last_seen_at_utc,
    observations.response_sha256,
    observations.task_id
from {{ ref('stg_eurostat_observations') }} as observations
left join {{ ref('dim_eurostat_period') }} as periods
    on periods.period_key = observations.period_key
   and periods.freq_code = observations.freq_code
