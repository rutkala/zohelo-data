{{ config(
    materialized='table',
    alias='br_opendata_locations',
    enabled=var('enable_opendata', false)
) }}

select
    record_id,
    data_source,
    bq_dataset,
    name_full,
    record_type,
    addr_line1,
    addr_city,
    addr_state,
    addr_postal_code,
    addr_country,
    geo_latitude,
    geo_longitude,
    placekey,
    bq_id
from {{ source('bronze_opendata', 'locations') }}
