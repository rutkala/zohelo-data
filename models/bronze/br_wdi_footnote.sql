{{ config(materialized='table', alias='wdi_footnote', enabled=var('enable_wdi', false)) }}

select * from {{ source('wdi_bulk', 'footnote') }}
