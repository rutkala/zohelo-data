{{ config(alias='bdl_observation_revisions') }}

with canonical as (
    select
        variable_id,
        unit_id,
        period_key,
        attr_id,
        value_numeric,
        value_formatted,
        precision,
        measure_unit_id,
        aggregate_id,
        source_last_update_at,
        retrieved_at_utc,
        response_sha256,
        task_id,
        json_object(
            'variable_id', variable_id,
            'unit_id', unit_id,
            'period_key', period_key,
            'attr_id', attr_id,
            'value_numeric', value_numeric,
            'value_formatted', value_formatted,
            'precision', precision,
            'measure_unit_id', measure_unit_id,
            'aggregate_id', aggregate_id,
            'source_last_update_at', case
                when source_last_update_at is null then null
                else strftime(source_last_update_at, '%Y-%m-%dT%H:%M:%S')
            end
        ) as canonical_record_json
    from {{ ref('br_bdl_observations') }}
),
distinct_versions as (
    select
        variable_id,
        unit_id,
        period_key,
        attr_id,
        min(value_numeric) as value_numeric,
        min(value_formatted) as value_formatted,
        min(precision) as precision,
        min(measure_unit_id) as measure_unit_id,
        min(aggregate_id) as aggregate_id,
        min(source_last_update_at) as source_last_update_at,
        canonical_record_json,
        sha256(canonical_record_json) as canonical_record_sha256,
        min(retrieved_at_utc) as first_seen_at_utc,
        max(retrieved_at_utc) as last_seen_at_utc,
        count(*) as replayed_response_count,
        min(response_sha256) as first_response_sha256,
        max(response_sha256) as last_response_sha256,
        min(task_id) as first_task_id,
        max(task_id) as last_task_id
    from canonical
    group by variable_id, unit_id, period_key, attr_id, canonical_record_json
),
ordered as (
    select
        *,
        row_number() over observation_window as revision_number,
        row_number() over (
            partition by variable_id, unit_id, period_key, coalesce(attr_id, -1)
            order by last_seen_at_utc desc, canonical_record_sha256 desc
        ) as current_rank,
        count(*) over (
            partition by variable_id, unit_id, period_key, coalesce(attr_id, -1)
        ) as revision_count,
        lag(canonical_record_sha256) over observation_window as previous_record_sha256,
        lag(value_numeric) over observation_window as previous_value_numeric,
        lag(value_formatted) over observation_window as previous_value_formatted,
        lag(precision) over observation_window as previous_precision,
        lag(source_last_update_at) over observation_window as previous_source_last_update_at
    from distinct_versions
    window observation_window as (
        partition by variable_id, unit_id, period_key, coalesce(attr_id, -1)
        order by first_seen_at_utc, canonical_record_sha256
    )
)
select
    variable_id,
    unit_id,
    period_key,
    attr_id,
    coalesce(cast(attr_id as varchar), 'reported') as attribute_key,
    revision_number,
    revision_count,
    current_rank = 1 as is_current,
    case
        when previous_record_sha256 is null then 'first_observed'
        when value_numeric is distinct from previous_value_numeric then 'observed_value_changed'
        when value_formatted is distinct from previous_value_formatted
            or precision is distinct from previous_precision then 'observed_representation_changed'
        else 'observed_metadata_changed'
    end as revision_event_type,
    value_numeric,
    value_formatted,
    precision,
    measure_unit_id,
    aggregate_id,
    source_last_update_at,
    first_seen_at_utc,
    last_seen_at_utc,
    replayed_response_count,
    canonical_record_json,
    canonical_record_sha256,
    previous_record_sha256,
    previous_value_numeric,
    previous_value_formatted,
    previous_precision,
    previous_source_last_update_at,
    first_response_sha256,
    last_response_sha256,
    first_task_id,
    last_task_id
from ordered
