{{ config(alias='bdl_variables') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by variable_id, lang, response_kind
            order by retrieved_at_utc desc, response_sha256 desc
        ) as row_number
    from {{ ref('br_bdl_variables') }}
),
latest as (
    select * from ranked where row_number = 1
),
known_variables as (
    select
        variable_id,
        max(source_universe_total) as source_universe_total,
        min(subject_id) filter (where subject_id is not null) as subject_id,
        max(level) as level,
        max(measure_unit_id) as measure_unit_id,
        min(measure_unit_name) filter (where response_kind = 'catalogue' and lang = 'pl' and measure_unit_name is not null) as measure_unit_name_pl,
        min(measure_unit_name) filter (where response_kind = 'catalogue' and lang = 'en' and measure_unit_name is not null) as measure_unit_name_en,
        min(name_component_1) filter (where response_kind = 'catalogue' and lang = 'pl' and name_component_1 is not null) as name_component_1_pl,
        min(name_component_2) filter (where response_kind = 'catalogue' and lang = 'pl' and name_component_2 is not null) as name_component_2_pl,
        min(name_component_3) filter (where response_kind = 'catalogue' and lang = 'pl' and name_component_3 is not null) as name_component_3_pl,
        min(name_component_4) filter (where response_kind = 'catalogue' and lang = 'pl' and name_component_4 is not null) as name_component_4_pl,
        min(name_component_5) filter (where response_kind = 'catalogue' and lang = 'pl' and name_component_5 is not null) as name_component_5_pl,
        min(name_component_1) filter (where lang = 'en' and name_component_1 is not null) as name_component_1_en,
        min(name_component_2) filter (where lang = 'en' and name_component_2 is not null) as name_component_2_en,
        min(name_component_3) filter (where lang = 'en' and name_component_3 is not null) as name_component_3_en,
        min(name_component_4) filter (where lang = 'en' and name_component_4 is not null) as name_component_4_en,
        min(name_component_5) filter (where lang = 'en' and name_component_5 is not null) as name_component_5_en,
        min(description) filter (where response_kind = 'detail' and lang = 'pl' and description is not null) as description_pl,
        min(description) filter (where response_kind = 'detail' and lang = 'en' and description is not null) as description_en,
        min(years_json) filter (where response_kind = 'detail' and years_json is not null) as years_json,
        max(retrieved_at_utc) as modeled_at_utc
    from latest
    group by variable_id
),
inferred_variables as (
    select
        obs.variable_id,
        max(obs.source_universe_total) as source_universe_total,
        cast(null as varchar) as subject_id,
        cast(null as integer) as level,
        max(obs.measure_unit_id) as measure_unit_id,
        cast(null as varchar) as measure_unit_name_pl,
        cast(null as varchar) as measure_unit_name_en,
        concat('Variable ', obs.variable_id) as name_component_1_pl,
        cast(null as varchar) as name_component_2_pl,
        cast(null as varchar) as name_component_3_pl,
        cast(null as varchar) as name_component_4_pl,
        cast(null as varchar) as name_component_5_pl,
        concat('Variable ', obs.variable_id) as name_component_1_en,
        cast(null as varchar) as name_component_2_en,
        cast(null as varchar) as name_component_3_en,
        cast(null as varchar) as name_component_4_en,
        cast(null as varchar) as name_component_5_en,
        cast(null as varchar) as description_pl,
        cast(null as varchar) as description_en,
        cast(null as varchar) as years_json,
        max(obs.retrieved_at_utc) as modeled_at_utc
    from {{ ref('br_bdl_observations') }} obs
    where obs.variable_id is not null
      and not exists (
          select 1 from known_variables kv where kv.variable_id = obs.variable_id
      )
    group by obs.variable_id
)
select * from known_variables
union all
select * from inferred_variables
