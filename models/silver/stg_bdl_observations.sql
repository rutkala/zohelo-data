{{ config(alias='bdl_observations') }}

with current_versions as (
    select *
    from {{ ref('bdl_observation_revisions') }}
    where is_current
),
aggregate_labels as (
    select
        entry_id,
        min(entry_name) filter (where lang = 'pl') as aggregate_name_pl,
        min(entry_name) filter (where lang = 'en') as aggregate_name_en
    from {{ ref('stg_bdl_dictionary_entries') }}
    where dictionary_resource = 'aggregates'
    group by entry_id
),
attribute_labels as (
    select
        entry_id,
        min(entry_name) filter (where lang = 'pl') as attribute_name_pl,
        min(entry_name) filter (where lang = 'en') as attribute_name_en
    from {{ ref('stg_bdl_dictionary_entries') }}
    where dictionary_resource = 'attributes'
    group by entry_id
)
select
    current_versions.variable_id,
    current_versions.unit_id,
    current_versions.period_key,
    current_versions.attr_id,
    current_versions.attribute_key,
    current_versions.value_numeric,
    current_versions.value_formatted,
    current_versions.precision,
    current_versions.measure_unit_id,
    current_versions.aggregate_id,
    aggregate_labels.aggregate_name_pl,
    aggregate_labels.aggregate_name_en,
    attribute_labels.attribute_name_pl,
    attribute_labels.attribute_name_en,
    current_versions.attr_id is not null as is_attribute_flagged,
    current_versions.revision_count,
    current_versions.replayed_response_count,
    current_versions.revision_event_type as current_revision_event_type,
    current_versions.source_last_update_at,
    current_versions.first_seen_at_utc,
    current_versions.last_seen_at_utc,
    current_versions.canonical_record_sha256,
    current_versions.previous_record_sha256,
    current_versions.last_response_sha256 as response_sha256,
    current_versions.last_task_id as task_id
from current_versions
left join aggregate_labels
    on aggregate_labels.entry_id = cast(current_versions.aggregate_id as varchar)
left join attribute_labels
    on attribute_labels.entry_id = cast(current_versions.attr_id as varchar)
