{{ config(materialized='table', alias='bdl_observations') }}

with response_pages as (
    select
        source_id,
        task_id,
        retrieved_at_utc,
        raw_sha256 as response_sha256,
        raw_size_bytes,
        cast(json_extract_string(request_json, '$.params.page') as integer) as requested_page,
        cast(json_extract_string(payload_utf8, '$.totalRecords') as bigint) as source_universe_total,
        cast(json_extract_string(payload_utf8, '$.variableId') as bigint) as variable_id,
        cast(json_extract_string(payload_utf8, '$.measureUnitId') as integer) as measure_unit_id,
        cast(json_extract_string(payload_utf8, '$.aggregateId') as integer) as aggregate_id,
        try_cast(json_extract_string(payload_utf8, '$.lastUpdate') as timestamp) as source_last_update_at,
        item.value as unit_json
    from {{ source('landing', 'gus_bdl_responses') }}
    cross join json_each(payload_utf8, '$.results') as item
    where source_id = 'gus_bdl'
      and task_kind = 'data_by_variable'
),
unit_values as (
    select
        source_id,
        task_id,
        retrieved_at_utc,
        response_sha256,
        raw_size_bytes,
        requested_page,
        source_universe_total,
        variable_id,
        measure_unit_id,
        aggregate_id,
        source_last_update_at,
        json_extract_string(unit_json, '$.id') as unit_id,
        json_extract_string(unit_json, '$.name') as unit_name,
        value.value as value_json
    from response_pages
    cross join json_each(unit_json, '$.values') as value
)
select
    source_id,
    task_id,
    retrieved_at_utc,
    response_sha256,
    raw_size_bytes,
    requested_page,
    source_universe_total,
    variable_id,
    measure_unit_id,
    aggregate_id,
    source_last_update_at,
    unit_id,
    unit_name,
    json_extract_string(value_json, '$.year') as period_key,
    try_cast(coalesce(
        json_extract_string(value_json, '$.val'),
        json_extract_string(value_json, '$.value')
    ) as double) as value_numeric,
    json_extract_string(value_json, '$.valueFormatted') as value_formatted,
    cast(json_extract_string(value_json, '$.precision') as integer) as precision,
    cast(json_extract_string(value_json, '$.attrId') as integer) as attr_id,
    value_json as source_record_json
from unit_values
