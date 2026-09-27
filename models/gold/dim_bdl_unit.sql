{{ config(materialized='table') }}

select
    unit_id as unit_key,
    resource_kind,
    is_locality,
    parent_unit_id,
    coalesce(unit_name_pl, unit_name_en) as unit_name,
    unit_name_pl,
    unit_name_en,
    level,
    kind,
    has_description,
    description_pl,
    description_en,
    years_json,
    availability_json,
    last_update_at,
    modeled_at_utc
from {{ ref('stg_bdl_units') }}
