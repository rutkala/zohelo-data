{{ config(materialized='table', alias='teryt_territories', enabled=var('enable_gus_teryt', false)) }}

with source_terc as (
    select
        woj,
        pow,
        gmi,
        rodz,
        nazwa,
        nazwa_dod,
        stan_na,
        level,
        teryt_code,
        parent_teryt_code,
        extracted_at_utc
    from {{ source('bronze_teryt', 'terc') }}
)

select
    teryt_code,
    level,
    woj as voivodeship_code,
    pow as county_code,
    gmi as municipality_code,
    rodz as municipality_type_code,
    nazwa as unit_name,
    nazwa_dod as unit_type_name,
    parent_teryt_code,
    stan_na as effective_date,
    extracted_at_utc as ingested_at_utc
from source_terc
