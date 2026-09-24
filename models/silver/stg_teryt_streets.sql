{{ config(materialized='table', alias='teryt_streets', enabled=var('enable_gus_teryt', false)) }}

with source_ulic as (
    select
        woj,
        pow,
        gmi,
        rodz_gmi,
        teryt_gmina_code,
        sym,
        sym_ul,
        cecha,
        nazwa_1,
        nazwa_2,
        full_street_name,
        stan_na,
        extracted_at_utc
    from {{ source('bronze_teryt', 'ulic') }}
)

select
    sym as locality_id,
    sym_ul as street_id,
    teryt_gmina_code,
    cecha as street_prefix,
    nazwa_1 as street_primary_name,
    nazwa_2 as street_secondary_name,
    full_street_name,
    stan_na as effective_date,
    extracted_at_utc as ingested_at_utc
from source_ulic
