{{ config(materialized='table', alias='bdl_dictionary_entries') }}

with dictionary_pages as (
    select
        source_id,
        task_id,
        regexp_extract(task_id, '^discovery:dictionary:([^:]+):', 1) as dictionary_resource,
        json_extract_string(request_json, '$.params.lang') as lang,
        retrieved_at_utc,
        raw_sha256 as response_sha256,
        raw_size_bytes,
        cast(json_extract_string(payload_utf8, '$.totalRecords') as bigint) as source_universe_total,
        item.value as entry_json
    from {{ source('landing', 'gus_bdl_responses') }}
    cross join json_each(payload_utf8, '$.results') as item
    where source_id = 'gus_bdl'
      and task_kind = 'dictionary'
)
select
    source_id,
    task_id,
    dictionary_resource,
    lang,
    retrieved_at_utc,
    response_sha256,
    raw_size_bytes,
    source_universe_total,
    coalesce(
        json_extract_string(entry_json, '$.id'),
        json_extract_string(entry_json, '$.code')
    ) as entry_id,
    coalesce(
        json_extract_string(entry_json, '$.name'),
        json_extract_string(entry_json, '$.label'),
        json_extract_string(entry_json, '$.nazwa'),
        json_extract_string(entry_json, '$.value')
    ) as entry_name,
    entry_json as source_record_json
from dictionary_pages
