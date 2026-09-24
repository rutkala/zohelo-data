{{ config(materialized='table', alias='dim_street', enabled=var('enable_gus_teryt', false)) }}

with streets as (
    select * from {{ ref('stg_teryt_streets') }}
),

localities as (
    select
        locality_id,
        locality_name,
        municipality_name,
        county_name,
        voivodeship_name
    from {{ ref('dim_locality') }}
)

select
    s.street_id,
    s.locality_id,
    l.locality_name,
    s.teryt_gmina_code,
    l.municipality_name,
    l.county_name,
    l.voivodeship_name,
    s.street_prefix,
    s.street_primary_name,
    s.street_secondary_name,
    s.full_street_name,
    s.effective_date,
    s.ingested_at_utc
from streets s
left join localities l
    on s.locality_id = l.locality_id
