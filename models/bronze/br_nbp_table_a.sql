{{ config(materialized='table', alias='nbp_exchange_rates_table_a') }}

with batches as (
    select *
    from {{ source('landing', 'nbp_batches') }}
    where source_id = 'nbp_exchange_rates_table_a'
),
pages as (
    select
        source_id,
        batch_id,
        cast(ingestion_sequence as bigint) as ingestion_sequence,
        cast(requested_start_date as date) as requested_start_date,
        cast(requested_end_date as date) as requested_end_date,
        cast(retrieved_at_utc as timestamp) as retrieved_at_utc,
        response_sha256,
        raw_file_id,
        body_json,
        publication.value as publication_json
    from batches
    cross join json_each(batches.body_json) as publication
),
observations as (
    select
        source_id,
        batch_id,
        ingestion_sequence,
        requested_start_date,
        requested_end_date,
        retrieved_at_utc,
        response_sha256,
        raw_file_id,
        json_extract_string(publication_json, '$.table') as source_table,
        json_extract_string(publication_json, '$.no') as no,
        cast(json_extract_string(publication_json, '$.effectiveDate') as date) as effective_date,
        cast(null as date) as trading_date,
        json_extract_string(rate.value, '$.currency') as currency,
        json_extract_string(rate.value, '$.country') as country,
        json_extract_string(rate.value, '$.symbol') as symbol,
        json_extract_string(rate.value, '$.code') as code,
        cast(json_extract_string(rate.value, '$.mid') as double) as mid,
        cast(null as double) as bid,
        cast(null as double) as ask
    from pages
    cross join json_each(publication_json, '$.rates') as rate
)
select * from observations
