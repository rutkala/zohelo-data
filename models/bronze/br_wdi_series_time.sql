{{ config(materialized='table', alias='wdi_series_time', enabled=var('enable_wdi', false)) }}

select * from {{ source('wdi_bulk', 'series_time') }}
