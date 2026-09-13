{{ config(materialized='table', alias='wdi_data', enabled=var('enable_wdi', false)) }}

with source_rows as (
    select * from {{ source('wdi_bulk', 'data') }}
),
annual_values as (
    unpivot source_rows
    on columns(* exclude ("Country Name", "Country Code", "Indicator Name", "Indicator Code"))
    into name year_key value observation_value_text
)
select
    trim("Country Name") as country_name,
    trim("Country Code") as country_code,
    trim("Indicator Name") as indicator_name,
    trim("Indicator Code") as indicator_code,
    cast(year_key as integer) as observation_year,
    trim(observation_value_text) as observation_value_text
from annual_values
where regexp_full_match(year_key, '(18|19|20|21)[0-9]{2}')
  and nullif(trim(observation_value_text), '') is not null
