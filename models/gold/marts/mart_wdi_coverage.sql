{{ config(materialized='table', enabled=var('enable_wdi', false)) }}

with evidence as (
    select
        cast(retrieved_at_utc as date) as snapshot_date,
        cast(retrieved_at_utc as timestamp) as latest_retrieved_at_utc,
        cast(current_distribution_total as bigint) as current_archive_total,
        cast(member_total as bigint) as archive_member_total
    from {{ source('wdi_bulk', 'archive_evidence') }}
),
source_counts as (
    select
        (select count(*) from {{ ref('br_wdi_country') }}) as source_geography_total,
        (select count(*) from {{ ref('br_wdi_series') }}) as source_indicator_total,
        (select count(*) from {{ ref('br_wdi_data') }}) as source_value_total
),
modeled_counts as (
    select
        count(distinct geography_key) as modeled_geography_total,
        count(distinct indicator_key) as modeled_indicator_total,
        count(*) as modeled_observation_total,
        min(observation_date) as first_observation_date,
        max(observation_date) as latest_observation_date
    from {{ ref('fact_wdi_observations') }}
)
select
    evidence.snapshot_date,
    evidence.latest_retrieved_at_utc,
    evidence.current_archive_total,
    evidence.archive_member_total,
    source_counts.source_geography_total,
    source_counts.source_indicator_total,
    source_counts.source_value_total,
    modeled_counts.modeled_geography_total,
    modeled_counts.modeled_indicator_total,
    modeled_counts.modeled_observation_total,
    modeled_counts.first_observation_date,
    modeled_counts.latest_observation_date,
    case when source_counts.source_value_total > 0
        then cast(modeled_counts.modeled_observation_total as double) / source_counts.source_value_total
        else null end as modeled_value_coverage_ratio
from evidence
cross join source_counts
cross join modeled_counts
