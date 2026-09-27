{{ config(materialized='table', alias='teryt_localities', enabled=var('enable_gus_teryt', false)) }}

with source_simc as (
    select
        woj,
        pow,
        gmi,
        rodz_gmi,
        teryt_gmina_code,
        rm,
        mz,
        nazwa,
        sym,
        sympod,
        is_parent_locality,
        stan_na,
        extracted_at_utc
    from {{ source('bronze_teryt', 'simc') }}
)

select
    sym as locality_id,
    sympod as parent_locality_id,
    is_parent_locality,
    teryt_gmina_code,
    woj as voivodeship_code,
    pow as county_code,
    gmi as municipality_code,
    rodz_gmi as municipality_type_code,
    rm as locality_type_code,
    mz as common_name_presence_code,
    nazwa as locality_name,
    stan_na as effective_date,
    extracted_at_utc as ingested_at_utc
from source_simc
