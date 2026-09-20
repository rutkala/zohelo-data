{{ config(materialized='table', alias='dbw_indicators', enabled=var('enable_gus_dbw', false)) }}

select
    indicator_id,
    indicator_name,
    indicator_name_en,
    domain_id,
    domain_name,
    domain_name_en,
    area_id,
    area_name,
    area_name_en,
    processed_at_utc
from {{ source('bronze_dbw', 'indicators') }}
