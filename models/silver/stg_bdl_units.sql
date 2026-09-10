{{ config(alias='bdl_units') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by resource_kind, unit_id, lang, response_kind
            order by retrieved_at_utc desc, response_sha256 desc
        ) as row_number
    from {{ ref('br_bdl_units') }}
),
latest as (
    select * from ranked where row_number = 1
)
select
    resource_kind,
    unit_id,
    resource_kind = 'locality' as is_locality,
    min(parent_unit_id) filter (where parent_unit_id is not null) as parent_unit_id,
    max(level) as level,
    min(kind) filter (where kind is not null) as kind,
    bool_or(coalesce(has_description, false)) as has_description,
    min(unit_name) filter (where lang = 'pl' and unit_name is not null) as unit_name_pl,
    min(unit_name) filter (where lang = 'en' and unit_name is not null) as unit_name_en,
    min(description) filter (where lang = 'pl' and description is not null) as description_pl,
    min(description) filter (where lang = 'en' and description is not null) as description_en,
    min(years_json) filter (where years_json is not null) as years_json,
    min(availability_json) filter (where availability_json is not null) as availability_json,
    max(last_update_at) as last_update_at,
    max(retrieved_at_utc) as modeled_at_utc
from latest
group by resource_kind, unit_id
