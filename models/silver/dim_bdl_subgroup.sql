{{ config(
    materialized='table',
    enabled=var('enable_gus_bdl_web', false)
) }}

with distinct_subgroups as (
    select distinct
        subgroup_id
    from {{ ref('stg_bdl_web_observations') }}
)
select
    subgroup_id as subgroup_key,
    subgroup_id,
    'https://bdl.stat.gov.pl/bdl/dane/podgrup/tablica' as source_url,
    current_timestamp as modeled_at_utc
from distinct_subgroups
