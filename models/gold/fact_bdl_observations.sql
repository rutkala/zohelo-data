{{ config(materialized='table') }}

select
    observations.variable_id as variable_key,
    variables.subject_key,
    observations.unit_id as unit_key,
    observations.period_key,
    periods.period_start_date,
    periods.period_end_date,
    periods.period_granularity,
    observations.attribute_key,
    observations.attr_id,
    observations.value_numeric,
    observations.value_formatted,
    observations.precision,
    observations.measure_unit_id,
    variables.measure_unit_name_pl,
    variables.measure_unit_name_en,
    observations.aggregate_id,
    observations.aggregate_name_pl,
    observations.aggregate_name_en,
    observations.attribute_name_pl,
    observations.attribute_name_en,
    observations.is_attribute_flagged,
    observations.revision_count,
    observations.replayed_response_count,
    observations.current_revision_event_type,
    observations.source_last_update_at,
    observations.first_seen_at_utc,
    observations.last_seen_at_utc,
    observations.canonical_record_sha256,
    observations.previous_record_sha256,
    observations.response_sha256,
    observations.task_id
from {{ ref('stg_bdl_observations') }} as observations
left join {{ ref('dim_bdl_period') }} as periods
    on periods.period_key = observations.period_key
left join {{ ref('dim_bdl_variable') }} as variables
    on variables.variable_key = observations.variable_id
