{{ config(materialized='table', alias='dbw_indicators', enabled=var('enable_gus_dbw', false)) }}
{{ assert_dbw_bronze_release() }}

select
    indicator_id,
    indicator_name,
    indicator_name_en,
    thematic_area,
    "domain" as domain_name,
    taxonomy_path,
    node_id,
    parent_id,
    processed_at_utc
from {{ source('bronze_dbw', 'indicators') }}
