{{ config(materialized='table', alias='prg_boundaries', enabled=var('enable_gugik_prg', false)) }}

with country as (
    select
        'PL' as teryt_code,
        'kraj' as level,
        country_name as unit_name,
        surface_area_ha,
        surface_area_ha / 100.0 as surface_area_km2,
        cast(null as varchar) as regon,
        geometry_wkt,
        extracted_at_utc
    from {{ source('bronze_prg', 'country') }}
),

voivodeships as (
    select
        teryt_code,
        'wojewodztwo' as level,
        voivodeship_name as unit_name,
        surface_area_ha,
        surface_area_ha / 100.0 as surface_area_km2,
        regon,
        geometry_wkt,
        extracted_at_utc
    from {{ source('bronze_prg', 'voivodeships') }}
),

counties as (
    select
        teryt_code,
        'powiat' as level,
        county_name as unit_name,
        surface_area_ha,
        surface_area_ha / 100.0 as surface_area_km2,
        regon,
        geometry_wkt,
        extracted_at_utc
    from {{ source('bronze_prg', 'counties') }}
),

municipalities as (
    select
        teryt_code,
        'gmina' as level,
        municipality_name as unit_name,
        surface_area_ha,
        surface_area_ha / 100.0 as surface_area_km2,
        regon,
        geometry_wkt,
        extracted_at_utc
    from {{ source('bronze_prg', 'municipalities') }}
)

select * from country
union all
select * from voivodeships
union all
select * from counties
union all
select * from municipalities
