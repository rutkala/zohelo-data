{{ config(materialized='table', alias='wdi_country_series', enabled=var('enable_wdi', false)) }}

select * from {{ source('wdi_bulk', 'country_series') }}
