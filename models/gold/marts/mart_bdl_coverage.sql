{{ config(materialized='table') }}

with landing_counts as (
    select
        cast(max(retrieved_at_utc) as date) as snapshot_date,
        max(retrieved_at_utc) as latest_retrieved_at_utc,
        count(*) as landed_accepted_total
    from {{ source('landing', 'gus_bdl_responses') }}
    where source_id = 'gus_bdl'
),
latest_catalogue_total as (
    select source_universe_total
    from (
        select distinct
            task_id,
            retrieved_at_utc,
            response_sha256,
            source_universe_total
        from {{ ref('br_bdl_variables') }}
        where response_kind = 'catalogue'
          and source_universe_total is not null
    )
    order by retrieved_at_utc desc, task_id desc, response_sha256 desc
    limit 1
),
variable_counts as (
    select
        (select source_universe_total from latest_catalogue_total) as source_universe_total,
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
