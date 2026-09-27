{{ config(materialized='table', enabled=var('enable_gus_dbw', false)) }}

select
    count(distinct indicator_id) as total_indicators,
    count(*) as total_observations,
    min(period_year) as earliest_year,
    max(period_year) as latest_year,
    current_timestamp as snapshot_timestamp_utc
from {{ ref('fact_dbw_observations') }}
