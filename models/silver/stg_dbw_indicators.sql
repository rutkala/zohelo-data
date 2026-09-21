{{ config(alias='dbw_indicators', enabled=var('enable_gus_dbw', false)) }}

select
    cast(indicator_id as integer) as indicator_id,
    trim(indicator_name) as indicator_name,
    trim(indicator_name_en) as indicator_name_en,
    trim(thematic_area) as thematic_area,
    trim(domain_name) as domain_name,
    trim(taxonomy_path) as taxonomy_path,
    trim(node_id) as node_id,
    trim(parent_id) as parent_id,
    processed_at_utc
from {{ ref('br_dbw_indicators') }}
