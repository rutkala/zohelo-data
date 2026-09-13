{{ config(materialized='table', alias='wdi_series', enabled=var('enable_wdi', false)) }}

select * from {{ source('wdi_bulk', 'series') }}
