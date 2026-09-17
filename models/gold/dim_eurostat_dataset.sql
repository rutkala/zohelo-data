{{ config(materialized='table', enabled=var('enable_eurostat', false)) }}

select
    dataset_id as dataset_key,
    arg_max(dataset_label, last_seen_at_utc) as dataset_label,
    count(distinct geo_code) as modeled_geography_count,
    count(*) as modeled_cell_count,
    count(*) filter (where not is_missing) as modeled_value_count,
    min(period_key) as first_period_key,
    max(period_key) as last_period_key,
    max(source_updated_at) as source_updated_at,
    max(last_seen_at_utc) as last_seen_at_utc
from {{ ref('stg_eurostat_observations') }}
group by dataset_id
