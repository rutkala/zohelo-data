{{ config(alias='eurostat_observation_revisions', enabled=var('enable_eurostat', false)) }}

with distinct_versions as (
    select
        dataset_id,
        dimension_key_sha256,
        min(dataset_label) as dataset_label,
        min(geo_code) as geo_code,
        min(freq_code) as freq_code,
        min(unit_code) as unit_code,
        min(period_key) as period_key,
        min(dimension_key_json) as dimension_key_json,
        value_json,
        min(value_numeric) as value_numeric,
        status_code,
        is_missing,
        min(source_updated_at) as source_updated_at,
        min(retrieved_at_utc) as first_seen_at_utc,
        max(retrieved_at_utc) as last_seen_at_utc,
        count(*) as replayed_response_count,
        arg_min(
            response_sha256,
            (retrieved_at_utc, response_sha256, task_id)
        ) as first_response_sha256,
        arg_max(
            response_sha256,
            (retrieved_at_utc, response_sha256, task_id)
        ) as last_response_sha256,
        arg_min(
            task_id,
            (retrieved_at_utc, response_sha256, task_id)
        ) as first_task_id,
        arg_max(
            task_id,
            (retrieved_at_utc, response_sha256, task_id)
        ) as last_task_id
    from {{ ref('br_eurostat_observations') }}
    group by dataset_id, dimension_key_sha256, value_json, status_code, is_missing
),
ordered as (
    select
        *,
        row_number() over version_window as revision_number,
        row_number() over (
            partition by dataset_id, dimension_key_sha256
            order by last_seen_at_utc desc, value_json desc, coalesce(status_code, '') desc
        ) as current_rank,
        count(*) over (partition by dataset_id, dimension_key_sha256) as revision_count,
        lag(value_json) over version_window as previous_value_json,
        lag(status_code) over version_window as previous_status_code
    from distinct_versions
    window version_window as (
        partition by dataset_id, dimension_key_sha256
        order by first_seen_at_utc, value_json, coalesce(status_code, '')
    )
)
select
    *,
    current_rank = 1 as is_current,
    case
        when previous_value_json is null then 'first_observed'
        when value_json is distinct from previous_value_json then 'observed_value_changed'
        when status_code is distinct from previous_status_code then 'observed_status_changed'
        else 'observed_metadata_changed'
    end as revision_event_type
from ordered
