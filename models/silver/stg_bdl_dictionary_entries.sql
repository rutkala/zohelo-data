{{ config(alias='bdl_dictionary_entries') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by dictionary_resource, entry_id, lang
            order by retrieved_at_utc desc, response_sha256 desc
        ) as row_number
    from {{ ref('br_bdl_dictionary_entries') }}
)
select
    dictionary_resource,
    entry_id,
    lang,
    entry_name,
    source_record_json,
    retrieved_at_utc,
    response_sha256
from ranked
where row_number = 1
