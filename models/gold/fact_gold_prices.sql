{{ config(materialized='table') }}

select
    effectiveDate as effective_date,
    'nbp_gold_1000_gram' as commodity_key,
    'PLN' as quote_currency_key,
    price_pln_per_gram as price_pln_per_gram_1000,
    price_pln_per_gram as raw_cena,
    source_id,
    batch_id,
    ingestion_sequence,
    requested_start_date,
    requested_end_date,
    retrieved_at_utc,
    response_sha256,
    raw_file_id
from {{ ref('stg_nbp_gold_prices') }}
