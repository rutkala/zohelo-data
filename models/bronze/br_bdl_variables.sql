{{ config(materialized='table', alias='bdl_variables') }}

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
        item.value as variable_json
    from {{ source('landing', 'gus_bdl_responses') }}
    cross join json_each(payload_utf8, '$.results') as item
    where source_id = 'gus_bdl'
      and task_kind = 'variables'
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
        payload_utf8 as variable_json
    from {{ source('landing', 'gus_bdl_responses') }}
    where source_id = 'gus_bdl'
      and task_kind = 'variable_detail'
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
    cast(json_extract_string(variable_json, '$.id') as bigint) as variable_id,
    json_extract_string(variable_json, '$.subjectId') as subject_id,
    json_extract_string(variable_json, '$.n1') as name_component_1,
    json_extract_string(variable_json, '$.n2') as name_component_2,
    json_extract_string(variable_json, '$.n3') as name_component_3,
    json_extract_string(variable_json, '$.n4') as name_component_4,
    json_extract_string(variable_json, '$.n5') as name_component_5,
    cast(json_extract_string(variable_json, '$.level') as integer) as level,
    cast(json_extract_string(variable_json, '$.measureUnitId') as integer) as measure_unit_id,
    json_extract_string(variable_json, '$.measureUnitName') as measure_unit_name,
    json_extract_string(variable_json, '$.description') as description,
    cast(json_extract(variable_json, '$.years') as varchar) as years_json,
    variable_json as source_record_json
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
    cast(json_extract_string(variable_json, '$.id') as bigint) as variable_id,
    json_extract_string(variable_json, '$.subjectId') as subject_id,
    json_extract_string(variable_json, '$.n1') as name_component_1,
    json_extract_string(variable_json, '$.n2') as name_component_2,
    json_extract_string(variable_json, '$.n3') as name_component_3,
    json_extract_string(variable_json, '$.n4') as name_component_4,
    json_extract_string(variable_json, '$.n5') as name_component_5,
    cast(json_extract_string(variable_json, '$.level') as integer) as level,
    cast(json_extract_string(variable_json, '$.measureUnitId') as integer) as measure_unit_id,
    json_extract_string(variable_json, '$.measureUnitName') as measure_unit_name,
    json_extract_string(variable_json, '$.description') as description,
    cast(json_extract(variable_json, '$.years') as varchar) as years_json,
    variable_json as source_record_json
from detail_pages
