{{ config(
    materialized='table',
    alias='br_opendata_organizations',
    enabled=var('enable_opendata', false)
) }}

select
    record_id,
    data_source,
    bq_dataset,
    name_org,
    name_type,
    record_type,
    addr_line1,
    addr_city,
    addr_state,
    addr_postal_code,
    addr_country,
    addr_type,
    geo_latitude,
    geo_longitude,
    placekey,
    bq_id,
    rel_anchor_domain,
    rel_anchor_key
from {{ source('bronze_opendata', 'organizations') }}
