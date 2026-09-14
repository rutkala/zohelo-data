{{ config(materialized='table', enabled=var('enable_eurostat', false)) }}

with observations as (
    select * from {{ ref('fact_eurostat_observations') }}
),
responses as (
    select count(distinct response_sha256) as modeled_response_total,
           max(last_seen_at_utc) as latest_retrieved_at_utc
    from observations
),
counts as (
    select
        count(distinct dataset_key) as modeled_dataset_total,
        count(distinct dataset_key || ':' || geography_key) as modeled_series_total,
        count(*) as modeled_cell_total,
        count(*) filter (where not is_missing) as modeled_value_total
    from observations
),
full_source as (
    select * from {{ source('landing', 'eurostat_full_source_coverage') }}
)
select
    cast(responses.latest_retrieved_at_utc as date) as snapshot_date,
    responses.latest_retrieved_at_utc,
    3::bigint as admitted_dataset_total,
    81::bigint as admitted_series_total,
    counts.modeled_dataset_total,
    counts.modeled_series_total,
    responses.modeled_response_total,
    counts.modeled_cell_total,
    counts.modeled_value_total,
    cast(counts.modeled_dataset_total as double) / 3 as admitted_dataset_coverage_ratio,
    cast(counts.modeled_series_total as double) / 81 as admitted_series_coverage_ratio,
    full_source.catalogue_distributions,
    full_source.validated_current_distributions,
    full_source.pending_tasks as pending_distribution_tasks,
    full_source.failed_pending_tasks as failed_pending_distribution_tasks,
    full_source.accepted_distributions,
    full_source.received_raw_bytes,
    cast(full_source.validated_current_distributions as double) / nullif(full_source.catalogue_distributions, 0) as full_distribution_coverage_ratio,
    full_source.inventories_current,
    cast(full_source.catalogue_checked_on as date) as catalogue_checked_on,
    full_source.coverage_status = 'complete_current_catalogue' as complete_official_catalogue
from responses cross join counts cross join full_source
