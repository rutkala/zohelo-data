{{ config(materialized='table', alias='bdl_units') }}

with catalogue_pages as (
    select
        source_id,
        task_id,
        case when task_kind = 'localities' then 'locality' else 'unit' end as resource_kind,
        'catalogue' as response_kind,
        json_extract_string(request_json, '$.params.lang') as lang,
        retrieved_at_utc,
        raw_sha256 as response_sha256,
        raw_size_bytes,
        cast(json_extract_string(payload_utf8, '$.totalRecords') as bigint) as source_universe_total,
        cast(json_extract_string(payload_utf8, '$.page') as integer) as response_page,
        cast(json_extract_string(payload_utf8, '$.pageSize') as integer) as response_page_size,
        item.value as unit_json
    from {{ source('landing', 'gus_bdl_responses') }}
    cross join json_each(payload_utf8, '$.results') as item
    where source_id = 'gus_bdl'
      and task_kind in ('units', 'localities')
),
detail_pages as (
    select
        source_id,
        task_id,
        case when task_kind = 'locality_detail' then 'locality' else 'unit' end as resource_kind,
        'detail' as response_kind,
        json_extract_string(request_json, '$.params.lang') as lang,
        retrieved_at_utc,
        raw_sha256 as response_sha256,
        raw_size_bytes,
        cast(null as bigint) as source_universe_total,
        cast(null as integer) as response_page,
        cast(null as integer) as response_page_size,
        payload_utf8 as unit_json
    from {{ source('landing', 'gus_bdl_responses') }}
    where source_id = 'gus_bdl'
      and task_kind in ('unit_detail', 'locality_detail')
)
select
    source_id,
    task_id,
    resource_kind,
    response_kind,
    lang,
    retrieved_at_utc,
    response_sha256,
    raw_size_bytes,
    source_universe_total,
    response_page,
    response_page_size,
    json_extract_string(unit_json, '$.id') as unit_id,
    json_extract_string(unit_json, '$.parentId') as parent_unit_id,
    json_extract_string(unit_json, '$.name') as unit_name,
    cast(json_extract_string(unit_json, '$.level') as integer) as level,
    json_extract_string(unit_json, '$.kind') as kind,
    cast(json_extract_string(unit_json, '$.hasDescription') as boolean) as has_description,
    json_extract_string(unit_json, '$.description') as description,
    cast(json_extract(unit_json, '$.years') as varchar) as years_json,
    cast(json_extract(unit_json, '$.availability') as varchar) as availability_json,
    try_cast(json_extract_string(unit_json, '$.lastUpdate') as timestamp) as last_update_at,
    unit_json as source_record_json
from catalogue_pages
union all
select
    source_id,
    task_id,
    resource_kind,
    response_kind,
    lang,
    retrieved_at_utc,
    response_sha256,
    raw_size_bytes,
    source_universe_total,
    response_page,
    response_page_size,
    json_extract_string(unit_json, '$.id') as unit_id,
    json_extract_string(unit_json, '$.parentId') as parent_unit_id,
    json_extract_string(unit_json, '$.name') as unit_name,
    cast(json_extract_string(unit_json, '$.level') as integer) as level,
    json_extract_string(unit_json, '$.kind') as kind,
    cast(json_extract_string(unit_json, '$.hasDescription') as boolean) as has_description,
    json_extract_string(unit_json, '$.description') as description,
    cast(json_extract(unit_json, '$.years') as varchar) as years_json,
    cast(json_extract(unit_json, '$.availability') as varchar) as availability_json,
    try_cast(json_extract_string(unit_json, '$.lastUpdate') as timestamp) as last_update_at,
    unit_json as source_record_json
from detail_pages
