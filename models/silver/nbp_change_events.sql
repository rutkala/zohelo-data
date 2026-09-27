{{ config(materialized='table') }}

{#
  This ledger compares canonical typed records from retained, validated HTTP-200
  response envelopes. A 404 has no landing envelope and cannot create an event.
  Presence is compared only when the response still contains the same source
  publication date; an entirely absent publication remains ambiguous.
#}
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
typed_records as (
    select
        *,
        to_json(struct_pack(
            record_contract_version := 'nbp_typed_record_v1',
            source_id := source_id,
            source_table := source_table,
            effective_date := strftime(effective_date, '%Y-%m-%d'),
            code := code,
            currency := currency,
            country := country,
            symbol := symbol,
            no := no,
            trading_date := case when trading_date is null then null else strftime(trading_date, '%Y-%m-%d') end,
            mid := mid,
            bid := bid,
            ask := ask
        )) as canonical_record_json
    from canonical_request_observations
),
records_with_hash as (
    select *, sha256(canonical_record_json) as canonical_record_sha256
    from typed_records
),
batch_dates as (
    select
        source_id,
        effective_date,
        ingestion_sequence,
        min(source_table) filter (where source_table is not null) as source_table,
        min(batch_id) as batch_id,
        min(retrieved_at_utc) as retrieved_at_utc,
        min(requested_start_date) as requested_start_date,
        min(requested_end_date) as requested_end_date,
        min(response_sha256) as response_sha256,
        min(raw_file_id) as raw_file_id
    from records_with_hash
    group by source_id, effective_date, ingestion_sequence
),
known_keys as (
    select
        source_id,
        effective_date,
        code,
        min(ingestion_sequence) as first_seen_ingestion_sequence
    from records_with_hash
    group by source_id, effective_date, code
),
presence_timeline as (
    select
        keys.source_id,
        coalesce(record.source_table, batch.source_table) as source_table,
        keys.effective_date,
        keys.code,
        record.currency,
        record.country,
        record.symbol,
        record.no,
        record.trading_date,
        record.mid,
        record.bid,
        record.ask,
        batch.ingestion_sequence,
        batch.batch_id,
        batch.retrieved_at_utc,
        batch.requested_start_date,
        batch.requested_end_date,
        batch.response_sha256,
        batch.raw_file_id,
        record.canonical_record_json,
        record.canonical_record_sha256,
        record.ingestion_sequence is not null as is_present
    from known_keys as keys
    inner join batch_dates as batch
        on keys.source_id = batch.source_id
        and keys.effective_date = batch.effective_date
        and batch.ingestion_sequence >= keys.first_seen_ingestion_sequence
    left join records_with_hash as record
        on keys.source_id = record.source_id
        and keys.effective_date = record.effective_date
        and keys.code is not distinct from record.code
        and batch.ingestion_sequence = record.ingestion_sequence
),
ordered as (
    select
        *,
        lag(is_present) over observation_window as previous_is_present,
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
        lag(canonical_record_json) over observation_window as previous_record_canonical_json,
        lag(canonical_record_sha256) over observation_window as previous_record_sha256,
        lag(ingestion_sequence) over observation_window as previous_ingestion_sequence
    from presence_timeline
    window observation_window as (
        partition by source_id, effective_date, coalesce(code, '__nbp_gold__')
        order by ingestion_sequence, retrieved_at_utc, batch_id, response_sha256
    )
),
classified as (
    select
        *,
        case
            when previous_is_present is null then null
            when previous_is_present and not is_present then 'source_record_missing'
            when not previous_is_present and is_present then 'source_record_reappeared'
            when is_present and previous_is_present and (
                mid is distinct from previous_mid
                or bid is distinct from previous_bid
                or ask is distinct from previous_ask
            ) then 'source_value_changed'
            when is_present and previous_is_present and (
                source_table is distinct from previous_source_table
                or currency is distinct from previous_currency
                or country is distinct from previous_country
                or symbol is distinct from previous_symbol
                or no is distinct from previous_no
                or trading_date is distinct from previous_trading_date
            ) then 'source_metadata_changed'
        end as event_type,
        to_json(list_filter([
            case when is_present is distinct from previous_is_present then 'record_presence' end,
            case when mid is distinct from previous_mid then 'mid' end,
            case when bid is distinct from previous_bid then 'bid' end,
            case when ask is distinct from previous_ask then 'ask' end,
            case when source_table is distinct from previous_source_table then 'source_table' end,
            case when currency is distinct from previous_currency then 'currency' end,
            case when country is distinct from previous_country then 'country' end,
            case when symbol is distinct from previous_symbol then 'symbol' end,
            case when no is distinct from previous_no then 'no' end,
            case when trading_date is distinct from previous_trading_date then 'trading_date' end
        ], field -> field is not null)) as changed_fields
    from ordered
)
select
    source_id,
    source_table,
    effective_date,
    code,
    event_type,
    case
        when event_type in ('source_record_missing', 'source_record_reappeared')
            then 'candidate_for_investigation'
        else 'observed_typed_record_change'
    end as evidence_status,
    false as provider_event_asserted,
    'nbp_typed_record_v1' as record_contract_version,
    'typed_record_comparison_v1' as detection_method,
    changed_fields,
    previous_ingestion_sequence,
    ingestion_sequence,
    previous_response_sha256,
    response_sha256,
    previous_record_sha256,
    canonical_record_sha256 as current_record_sha256,
    previous_mid,
    mid,
    previous_bid,
    bid,
    previous_ask,
    ask,
    previous_source_table,
    source_table as current_source_table,
    previous_currency,
    currency,
    previous_country,
    country,
    previous_symbol,
    symbol,
    previous_no,
    no,
    previous_trading_date,
    trading_date,
    batch_id,
    raw_file_id,
    requested_start_date,
    requested_end_date,
    retrieved_at_utc as detected_at_utc
from classified
where event_type is not null
