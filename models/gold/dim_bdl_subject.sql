{{ config(materialized='table') }}

with known_subjects as (
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
),
inferred_subjects as (
    select
        v.subject_id as subject_key,
        cast(null as varchar) as parent_subject_id,
        concat('Subject ', v.subject_id) as subject_name,
        cast(null as varchar) as subject_name_pl,
        cast(null as varchar) as subject_name_en,
        cast(null as varchar) as description_pl,
        cast(null as varchar) as description_en,
        true as has_variables,
        cast(null as varchar) as child_subject_ids_json,
        cast(null as varchar) as subject_levels_json,
        cast(null as varchar) as years_json,
        cast(null as varchar) as availability_json,
        cast(null as varchar) as dimensions_json,
        cast(null as timestamp) as last_update_at,
        max(v.modeled_at_utc) as modeled_at_utc
    from {{ ref('stg_bdl_variables') }} v
    where v.subject_id is not null
      and not exists (
          select 1 from known_subjects s where s.subject_key = v.subject_id
      )
    group by v.subject_id
)
select * from known_subjects
union all
select * from inferred_subjects
