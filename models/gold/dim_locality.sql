{{ config(materialized='table', alias='dim_locality', enabled=var('enable_gus_teryt', false)) }}

with localities as (
    select * from {{ ref('stg_teryt_localities') }}
),

parent_locs as (
    select
        locality_id,
        locality_name as parent_locality_name
    from localities
    where is_parent_locality = true
),

territories as (
    select
        teryt_code,
        unit_name as municipality_name,
        voivodeship_name,
        county_name
    from {{ ref('dim_territory') }}
    where level = 'gmina'
)

select
    l.locality_id,
    l.locality_name,
    l.is_parent_locality,
    l.parent_locality_id,
    p.parent_locality_name,
    l.teryt_gmina_code,
    t.municipality_name,
    t.county_name,
    t.voivodeship_name,
    l.locality_type_code,
    l.effective_date,
    l.ingested_at_utc
from localities l
left join parent_locs p
    on l.parent_locality_id = p.locality_id
left join territories t
    on l.teryt_gmina_code = t.teryt_code
