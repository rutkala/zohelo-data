{{ config(materialized='table') }}

with batches as (
    select *
    from {{ source('nbp_verified_raw', 'nbp_batches') }}
    where source_id = 'nbp_gold_prices'
),
observations as (
    select
        source_id,
        batch_id,
        cast(ingestion_sequence as bigint) as ingestion_sequence,
        cast(requested_start_date as date) as requested_start_date,
        cast(requested_end_date as date) as requested_end_date,
        cast(retrieved_at_utc as timestamp) as retrieved_at_utc,
        response_sha256,
        raw_file_id,
        cast(json_extract_string(observation.value, '$.data') as date) as effective_date,
        cast(json_extract_string(observation.value, '$.cena') as decimal(18, 2)) as price_pln_per_gram
    from batches
    cross join json_each(batches.body_json) as observation
)
select * from observations
