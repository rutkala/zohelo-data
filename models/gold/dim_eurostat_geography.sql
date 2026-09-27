{{ config(materialized='table', enabled=var('enable_eurostat', false)) }}

select
    geo_code as geography_key,
    count(distinct dataset_id) as modeled_dataset_count,
    count(*) as modeled_cell_count,
    min(period_key) as first_period_key,
    max(period_key) as last_period_key
from {{ ref('stg_eurostat_observations') }}
group by geo_code
