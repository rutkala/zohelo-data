{{ config(materialized='table') }}

with current_quotes as (
    select * from {{ ref('stg_nbp_table_a') }}
    union all
    select * from {{ ref('stg_nbp_table_b') }}
    union all
    select * from {{ ref('stg_nbp_table_c') }}
)
select
    source_table as source_table_key,
    effectiveDate as effective_date,
    tradingDate as trading_date,
    code as currency_key,
    'PLN' as quote_currency_key,
    mid,
    bid,
    ask,
    no as source_no,
    source_id,
    batch_id,
    ingestion_sequence,
    requested_start_date,
    requested_end_date,
    retrieved_at_utc,
    response_sha256,
    raw_file_id
from current_quotes
