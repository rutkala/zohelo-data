{{ config(materialized='table') }}

with observations as (
    select
        source_id, source_table, effective_date, code, currency, country, symbol, no, trading_date,
        mid, bid, ask, ingestion_sequence, batch_id, retrieved_at_utc,
        requested_start_date, requested_end_date, response_sha256, raw_file_id
    from {{ ref('br_nbp_table_a') }}
    union all
    select
        source_id, source_table, effective_date, code, currency, country, symbol, no, trading_date,
        mid, bid, ask, ingestion_sequence, batch_id, retrieved_at_utc,
        requested_start_date, requested_end_date, response_sha256, raw_file_id
    from {{ ref('br_nbp_table_b') }}
    union all
    select
        source_id, source_table, effective_date, code, currency, country, symbol, no, trading_date,
        mid, bid, ask, ingestion_sequence, batch_id, retrieved_at_utc,
        requested_start_date, requested_end_date, response_sha256, raw_file_id
    from {{ ref('br_nbp_table_c') }}
    union all
    select
        source_id, cast(null as varchar), effective_date, cast(null as varchar),
        cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as date),
        cast(price_pln_per_gram as double), cast(null as double), cast(null as double),
        ingestion_sequence, batch_id, retrieved_at_utc,
        requested_start_date, requested_end_date, response_sha256, raw_file_id
    from {{ ref('br_nbp_gold_prices') }}
),
canonical_request_observations as (
    select
        source_id,
        min(source_table) filter (where source_table is not null) as source_table,
        effective_date,
        code,
        min(currency) filter (where currency is not null) as currency,
        min(country) filter (where country is not null) as country,
        min(symbol) filter (where symbol is not null) as symbol,
        min(no) filter (where no is not null) as no,
        min(trading_date) as trading_date,
        min(mid) as mid,
        min(bid) as bid,
        min(ask) as ask,
        ingestion_sequence,
        min(batch_id) as batch_id,
        min(retrieved_at_utc) as retrieved_at_utc,
        min(requested_start_date) as requested_start_date,
        min(requested_end_date) as requested_end_date,
        min(response_sha256) as response_sha256,
        min(raw_file_id) as raw_file_id
    from observations
    group by source_id, effective_date, code, ingestion_sequence
),
ordered as (
    select
        *,
        lag(mid) over observation_window as previous_mid,
        lag(bid) over observation_window as previous_bid,
        lag(ask) over observation_window as previous_ask,
        lag(source_table) over observation_window as previous_source_table,
        lag(currency) over observation_window as previous_currency,
        lag(country) over observation_window as previous_country,
        lag(symbol) over observation_window as previous_symbol,
        lag(no) over observation_window as previous_no,
        lag(trading_date) over observation_window as previous_trading_date,
        lag(response_sha256) over observation_window as previous_response_sha256,
        lag(ingestion_sequence) over observation_window as previous_ingestion_sequence
    from canonical_request_observations
    window observation_window as (
        partition by source_id, effective_date, coalesce(code, '__nbp_gold__')
        order by ingestion_sequence, retrieved_at_utc, batch_id, response_sha256
    )
),
classified as (
    select
        *,
        case
            when previous_ingestion_sequence is null then null
            when mid is distinct from previous_mid
              or bid is distinct from previous_bid
              or ask is distinct from previous_ask
                then 'source_value_changed'
            when source_table is distinct from previous_source_table
              or currency is distinct from previous_currency
              or country is distinct from previous_country
              or symbol is distinct from previous_symbol
              or no is distinct from previous_no
              or trading_date is distinct from previous_trading_date
                then 'source_metadata_changed'
        end as event_type
    from ordered
)
select
    source_id, source_table, effective_date, code, event_type,
    previous_ingestion_sequence, ingestion_sequence,
    previous_response_sha256, response_sha256,
    previous_mid, mid, previous_bid, bid, previous_ask, ask,
    previous_source_table, source_table as current_source_table,
    previous_currency, currency, previous_country, country, previous_symbol, symbol,
    previous_no, no, previous_trading_date, trading_date,
    batch_id, raw_file_id, requested_start_date, requested_end_date,
    retrieved_at_utc as detected_at_utc
from classified
where event_type is not null
