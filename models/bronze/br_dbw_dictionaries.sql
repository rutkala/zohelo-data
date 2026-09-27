{{ config(materialized='table', alias='dbw_dictionaries', enabled=var('enable_gus_dbw', false)) }}
{{ assert_dbw_bronze_release() }}

select
    indicator_id,
    column_name,
    dictionary_name,
    element_id,
    element_name,
    processed_at_utc
from {{ source('bronze_dbw', 'dictionaries') }}
