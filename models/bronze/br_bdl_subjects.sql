{{ config(materialized='table', alias='bdl_subjects') }}

with catalogue_pages as (
    select
        source_id,
        task_id,
        'catalogue' as response_kind,
        json_extract_string(request_json, '$.params.lang') as lang,
        retrieved_at_utc,
        raw_sha256 as response_sha256,
        raw_size_bytes,
        cast(json_extract_string(payload_utf8, '$.totalRecords') as bigint) as source_universe_total,
        cast(json_extract_string(payload_utf8, '$.page') as integer) as response_page,
        cast(json_extract_string(payload_utf8, '$.pageSize') as integer) as response_page_size,
        item.value as subject_json
    from {{ source('landing', 'gus_bdl_responses') }}
    cross join json_each(payload_utf8, '$.results') as item
    where source_id = 'gus_bdl'
      and task_kind = 'subjects'
),
detail_pages as (
    select
        source_id,
        task_id,
        'detail' as response_kind,
        json_extract_string(request_json, '$.params.lang') as lang,
        retrieved_at_utc,
        raw_sha256 as response_sha256,
        raw_size_bytes,
        cast(null as bigint) as source_universe_total,
        cast(null as integer) as response_page,
        cast(null as integer) as response_page_size,
        payload_utf8 as subject_json
    from {{ source('landing', 'gus_bdl_responses') }}
    where source_id = 'gus_bdl'
      and task_kind = 'subject_detail'
)
select
    source_id,
    task_id,
    response_kind,
    lang,
    retrieved_at_utc,
    response_sha256,
    raw_size_bytes,
    source_universe_total,
    response_page,
    response_page_size,
    json_extract_string(subject_json, '$.id') as subject_id,
    json_extract_string(subject_json, '$.parentId') as parent_subject_id,
    json_extract_string(subject_json, '$.name') as subject_name,
    cast(json_extract_string(subject_json, '$.hasVariables') as boolean) as has_variables,
    cast(json_extract(subject_json, '$.children') as varchar) as child_subject_ids_json,
    cast(json_extract(subject_json, '$.levels') as varchar) as subject_levels_json,
    json_extract_string(subject_json, '$.description') as description,
    cast(json_extract(subject_json, '$.years') as varchar) as years_json,
    cast(json_extract(subject_json, '$.availability') as varchar) as availability_json,
    cast(json_extract(subject_json, '$.dimensions') as varchar) as dimensions_json,
    try_cast(json_extract_string(subject_json, '$.lastUpdate') as timestamp) as last_update_at,
    subject_json as source_record_json
from catalogue_pages
union all
select
    source_id,
    task_id,
    response_kind,
    lang,
    retrieved_at_utc,
    response_sha256,
    raw_size_bytes,
    source_universe_total,
    response_page,
    response_page_size,
    json_extract_string(subject_json, '$.id') as subject_id,
    json_extract_string(subject_json, '$.parentId') as parent_subject_id,
    json_extract_string(subject_json, '$.name') as subject_name,
    cast(json_extract_string(subject_json, '$.hasVariables') as boolean) as has_variables,
    cast(json_extract(subject_json, '$.children') as varchar) as child_subject_ids_json,
    cast(json_extract(subject_json, '$.levels') as varchar) as subject_levels_json,
    json_extract_string(subject_json, '$.description') as description,
    cast(json_extract(subject_json, '$.years') as varchar) as years_json,
    cast(json_extract(subject_json, '$.availability') as varchar) as availability_json,
    cast(json_extract(subject_json, '$.dimensions') as varchar) as dimensions_json,
    try_cast(json_extract_string(subject_json, '$.lastUpdate') as timestamp) as last_update_at,
    subject_json as source_record_json
from detail_pages
