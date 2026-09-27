{{ config(materialized='table', alias='bdl_years') }}

with year_pages as (
    select
        source_id,
        task_id,
        retrieved_at_utc,
        raw_sha256 as response_sha256,
        raw_size_bytes,
        cast(json_extract_string(payload_utf8, '$.totalRecords') as bigint) as source_universe_total,
        item.value as year_json
    from {{ source('landing', 'gus_bdl_responses') }}
    cross join json_each(payload_utf8, '$.results') as item
    where source_id = 'gus_bdl'
      and task_kind = 'years'
)
select
    source_id,
    task_id,
    retrieved_at_utc,
    response_sha256,
    raw_size_bytes,
    source_universe_total,
    cast(json_extract_string(year_json, '$.id') as integer) as year_id,
    year_json as source_record_json
from year_pages
