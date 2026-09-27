{{ config(materialized='table') }}

select
    variable_id as variable_key,
    subject_id as subject_key,
    coalesce(name_component_1_pl, name_component_1_en) as variable_name,
    concat_ws(' / ', nullif(name_component_1_pl, ''), nullif(name_component_2_pl, ''), nullif(name_component_3_pl, ''), nullif(name_component_4_pl, ''), nullif(name_component_5_pl, '')) as variable_name_pl,
    concat_ws(' / ', nullif(name_component_1_en, ''), nullif(name_component_2_en, ''), nullif(name_component_3_en, ''), nullif(name_component_4_en, ''), nullif(name_component_5_en, '')) as variable_name_en,
    level,
    measure_unit_id,
    measure_unit_name_pl,
    measure_unit_name_en,
    description_pl,
    description_en,
    years_json,
    source_universe_total,
    modeled_at_utc
from {{ ref('stg_bdl_variables') }}
