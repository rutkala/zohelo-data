{{ config(materialized='table', alias='wdi_country', enabled=var('enable_wdi', false)) }}

select * from {{ source('wdi_bulk', 'country') }}
