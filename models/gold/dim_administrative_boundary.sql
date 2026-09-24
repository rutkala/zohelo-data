{{ config(materialized='table', alias='dim_administrative_boundary', enabled=var('enable_gugik_prg', false)) }}

with boundaries as (
    select * from {{ ref('stg_prg_boundaries') }}
),

territories as (
    select
        teryt_code,
        level,
        unit_name,
        unit_type_name,
        voivodeship_code,
        voivodeship_name,
        county_code,
        county_name,
        municipality_code,
        parent_teryt_code
    from {{ ref('dim_territory') }}
)

select
    b.teryt_code,
    b.level,
    b.unit_name,
    t.unit_type_name,
    t.voivodeship_code,
    t.voivodeship_name,
    t.county_code,
    t.county_name,
    t.municipality_code,
    t.parent_teryt_code,
    b.surface_area_ha,
    b.surface_area_km2,
    b.regon,
    b.geometry_wkt,
    b.extracted_at_utc as ingested_at_utc
from boundaries b
left join territories t
    on b.teryt_code = t.teryt_code
