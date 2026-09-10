{{ config(materialized='table') }}

with landing_counts as (
    select
        cast(max(retrieved_at_utc) as date) as snapshot_date,
        max(retrieved_at_utc) as latest_retrieved_at_utc,
        count(*) as landed_accepted_total
    from {{ source('landing', 'gus_bdl_responses') }}
    where source_id = 'gus_bdl'
),
variable_counts as (
    select
        max(source_universe_total) as source_universe_total,
        count(*) as discovered_total
    from {{ ref('stg_bdl_variables') }}
),
modeled_counts as (
    select
        count(distinct variable_key) as modeled_total,
        count(*) as modeled_observation_total
    from {{ ref('fact_bdl_observations') }}
)
select
    landing_counts.snapshot_date,
    landing_counts.latest_retrieved_at_utc,
    variable_counts.source_universe_total,
    variable_counts.discovered_total,
    landing_counts.landed_accepted_total,
    modeled_counts.modeled_total,
    modeled_counts.modeled_observation_total,
    case
        when variable_counts.source_universe_total > 0 then cast(variable_counts.discovered_total as double) / variable_counts.source_universe_total
        else null
    end as discovery_coverage_ratio,
    case
        when variable_counts.source_universe_total > 0 then cast(modeled_counts.modeled_total as double) / variable_counts.source_universe_total
        else null
    end as modeled_coverage_ratio,
    case
        when variable_counts.discovered_total > 0 then cast(landing_counts.landed_accepted_total as double) / variable_counts.discovered_total
        else null
    end as landed_responses_per_discovered_variable_ratio
from landing_counts
cross join variable_counts
cross join modeled_counts
