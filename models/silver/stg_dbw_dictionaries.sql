{{ config(alias='dbw_dictionaries', enabled=var('enable_gus_dbw', false)) }}

select
    cast(indicator_id as integer) as indicator_id,
    trim(column_name) as column_name,
    trim(dictionary_name) as dictionary_name,
    cast(element_id as integer) as element_id,
    trim(element_name) as element_name,
    processed_at_utc
from {{ ref('br_dbw_dictionaries') }}
