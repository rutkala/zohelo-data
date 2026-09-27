{{ config(alias='wdi_countries', enabled=var('enable_wdi', false)) }}

select
    trim("Country Code") as country_code,
    nullif(trim("Short Name"), '') as short_name,
    nullif(trim("Table Name"), '') as table_name,
    nullif(trim("Long Name"), '') as long_name,
    nullif(trim("2-alpha code"), '') as iso2_code,
    nullif(trim("Currency Unit"), '') as currency_unit,
    nullif(trim("Region"), '') as region_name,
    nullif(trim("Income Group"), '') as income_group,
    nullif(trim("Lending category"), '') as lending_category,
    nullif(trim("Special Notes"), '') as special_notes
from {{ ref('br_wdi_country') }}
where nullif(trim("Country Code"), '') is not null
qualify row_number() over (partition by trim("Country Code") order by trim("Country Code")) = 1
