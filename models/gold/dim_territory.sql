{{ config(materialized='table', alias='dim_territory', enabled=var('enable_gus_teryt', false)) }}

with territories as (
    select * from {{ ref('stg_teryt_territories') }}
),

voivodeships as (
    select
        voivodeship_code,
        unit_name as voivodeship_name
    from territories
    where level = 'wojewodztwo'
),

counties as (
    select
        voivodeship_code,
        county_code,
        unit_name as county_name
    from territories
    where level = 'powiat'
)

select
    t.teryt_code,
    t.level,
    t.unit_name,
    t.unit_type_name,
    t.voivodeship_code,
    v.voivodeship_name,
    t.county_code,
    c.county_name,
    t.municipality_code,
    t.municipality_type_code,
    t.parent_teryt_code,
    t.effective_date,
    t.ingested_at_utc
from territories t
left join voivodeships v
    on t.voivodeship_code = v.voivodeship_code
left join counties c
    on t.voivodeship_code = c.voivodeship_code
    and t.county_code = c.county_code
