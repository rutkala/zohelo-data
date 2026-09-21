{{ config(alias='dbw_indicators', enabled=var('enable_gus_dbw', false)) }}

select
    cast(indicator_id as integer) as indicator_id,
    trim(indicator_name) as indicator_name,
    trim(indicator_name_en) as indicator_name_en,
    cast(domain_id as integer) as domain_id,
    trim(domain_name) as domain_name,
    trim(domain_name_en) as domain_name_en,
    cast(area_id as integer) as area_id,
    trim(area_name) as area_name,
    trim(area_name_en) as area_name_en,
    processed_at_utc
from {{ ref('br_dbw_indicators') }}
