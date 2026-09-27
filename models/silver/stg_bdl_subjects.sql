{{ config(alias='bdl_subjects') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by subject_id, lang, response_kind
            order by retrieved_at_utc desc, response_sha256 desc
        ) as row_number
    from {{ ref('br_bdl_subjects') }}
),
latest as (
    select * from ranked where row_number = 1
)
select
    subject_id,
    min(parent_subject_id) filter (where parent_subject_id is not null) as parent_subject_id,
    bool_or(coalesce(has_variables, false)) as has_variables,
    min(subject_name) filter (where lang = 'pl' and subject_name is not null) as subject_name_pl,
    min(subject_name) filter (where lang = 'en' and subject_name is not null) as subject_name_en,
    min(description) filter (where lang = 'pl' and description is not null) as description_pl,
    min(description) filter (where lang = 'en' and description is not null) as description_en,
    min(child_subject_ids_json) filter (where child_subject_ids_json is not null) as child_subject_ids_json,
    min(subject_levels_json) filter (where subject_levels_json is not null) as subject_levels_json,
    min(years_json) filter (where years_json is not null) as years_json,
    min(availability_json) filter (where availability_json is not null) as availability_json,
    min(dimensions_json) filter (where dimensions_json is not null) as dimensions_json,
    max(last_update_at) as last_update_at,
    max(retrieved_at_utc) as modeled_at_utc
from latest
group by subject_id
