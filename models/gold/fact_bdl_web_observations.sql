{{ config(
    materialized='table',
    enabled=var('enable_gus_bdl_web', false)
) }}

select
    obs.observation_key,
    obs.subgroup_id as subgroup_key,
    obs.unit_id as unit_key,
    obs.terc_code,
    obs.unit_name,
    obs.period_key,
    obs.period_year,
    periods.period_start_date,
    periods.period_end_date,
    periods.period_granularity,
    obs.val_numeric,
    obs.val_raw,
    obs.measure_unit,
    obs.attr_name,
    obs.dimensions_json,
    obs.raw_archive_file,
    obs.processed_at_utc
from {{ ref('stg_bdl_web_observations') }} as obs
left join {{ ref('dim_bdl_period') }} as periods
    on periods.period_key = obs.period_key
left join {{ ref('dim_bdl_subgroup') }} as subgroups
    on subgroups.subgroup_key = obs.subgroup_id
