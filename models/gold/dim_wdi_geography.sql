{{ config(materialized='table', enabled=var('enable_wdi', false)) }}

select
    country_code as geography_key,
    country_code,
    iso2_code,
    coalesce(short_name, table_name, long_name, country_code) as geography_name,
    long_name,
    region_name,
    income_group,
    lending_category,
    currency_unit,
    special_notes,
    case when region_name = 'Aggregates' then 'source_published_aggregate' else 'country_or_territory' end as geography_type
from {{ ref('stg_wdi_countries') }}
