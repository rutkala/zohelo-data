{{ config(materialized='table') }}

select
    subject_id as subject_key,
    parent_subject_id,
    coalesce(subject_name_pl, subject_name_en) as subject_name,
    subject_name_pl,
    subject_name_en,
    description_pl,
    description_en,
    has_variables,
    child_subject_ids_json,
    subject_levels_json,
    years_json,
    availability_json,
    dimensions_json,
    last_update_at,
    modeled_at_utc
from {{ ref('stg_bdl_subjects') }}
