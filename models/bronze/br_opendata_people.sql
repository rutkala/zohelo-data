{{ config(
    materialized='table',
    alias='br_opendata_people',
    enabled=var('enable_opendata', false)
) }}

select
    record_id,
    data_source,
    bq_dataset,
    name_full,
    name_first,
    name_last,
    record_type,
    addr_country,
    group_assn_id_number,
    group_assn_id_type,
    rel_pointer_domain,
    rel_pointer_key,
    rel_pointer_role,
    linkedin
from {{ source('bronze_opendata', 'people') }}
