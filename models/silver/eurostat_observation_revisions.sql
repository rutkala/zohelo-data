{{ config(alias='eurostat_observation_revisions', enabled=var('enable_eurostat', false)) }}

with chronological as (
    select
        *,
        lag(value_json) over observation_window as previous_value_json,
        lag(status_code) over observation_window as previous_status_code,
        lag(is_missing) over observation_window as previous_is_missing,
        lag(source_updated_at) over observation_window as previous_source_updated_at,
        row_number() over observation_window as observation_number
    from {{ ref('br_eurostat_observations') }}
    window observation_window as (
        partition by dataset_id, dimension_key_sha256
        order by retrieved_at_utc, response_sha256, task_id
    )
),
run_starts as (
    select
        *,
        case
            when observation_number = 1 then 1
            when value_json is distinct from previous_value_json then 1
            when status_code is distinct from previous_status_code then 1
            when is_missing is distinct from previous_is_missing then 1
            when source_updated_at is distinct from previous_source_updated_at then 1
            else 0
        end as starts_revision
    from chronological
),
numbered as (
    select
        *,
        sum(starts_revision) over (
            partition by dataset_id, dimension_key_sha256
            order by retrieved_at_utc, response_sha256, task_id
            rows between unbounded preceding and current row
        ) as revision_number
    from run_starts
),
revision_runs as (
    select
        dataset_id,
        dimension_key_sha256,
        revision_number,
        arg_min(dataset_label, (retrieved_at_utc, response_sha256, task_id)) as dataset_label,
        arg_min(geo_code, (retrieved_at_utc, response_sha256, task_id)) as geo_code,
        arg_min(freq_code, (retrieved_at_utc, response_sha256, task_id)) as freq_code,
        arg_min(unit_code, (retrieved_at_utc, response_sha256, task_id)) as unit_code,
        arg_min(period_key, (retrieved_at_utc, response_sha256, task_id)) as period_key,
        arg_min(dimension_key_json, (retrieved_at_utc, response_sha256, task_id)) as dimension_key_json,
        arg_min(value_json, (retrieved_at_utc, response_sha256, task_id)) as value_json,
        arg_min(value_numeric, (retrieved_at_utc, response_sha256, task_id)) as value_numeric,
        arg_min(status_code, (retrieved_at_utc, response_sha256, task_id)) as status_code,
        arg_min(is_missing, (retrieved_at_utc, response_sha256, task_id)) as is_missing,
        arg_min(source_updated_at, (retrieved_at_utc, response_sha256, task_id)) as source_updated_at,
        min(retrieved_at_utc) as first_seen_at_utc,
        max(retrieved_at_utc) as last_seen_at_utc,
        count(*) as replayed_response_count,
        arg_min(response_sha256, (retrieved_at_utc, response_sha256, task_id)) as first_response_sha256,
        arg_max(response_sha256, (retrieved_at_utc, response_sha256, task_id)) as last_response_sha256,
        arg_min(task_id, (retrieved_at_utc, response_sha256, task_id)) as first_task_id,
        arg_max(task_id, (retrieved_at_utc, response_sha256, task_id)) as last_task_id
    from numbered
    group by dataset_id, dimension_key_sha256, revision_number
),
with_previous as (
    select
        *,
        revision_number = max(revision_number) over (
            partition by dataset_id, dimension_key_sha256
        ) as is_current,
        max(revision_number) over (
            partition by dataset_id, dimension_key_sha256
        ) as revision_count,
        lag(value_json) over revision_window as previous_value_json,
        lag(status_code) over revision_window as previous_status_code,
        lag(is_missing) over revision_window as previous_is_missing
    from revision_runs
    window revision_window as (
        partition by dataset_id, dimension_key_sha256
        order by revision_number
    )
)
select
    *,
    case
        when revision_number = 1 then 'first_observed'
        when value_json is distinct from previous_value_json then 'observed_value_changed'
        when status_code is distinct from previous_status_code then 'observed_status_changed'
        when is_missing is distinct from previous_is_missing then 'observed_missing_state_changed'
        else 'observed_metadata_changed'
    end as revision_event_type
from with_previous
