{{ config(materialized='table') }}

select
    'nbp_gold_1000_gram' as commodity_key,
    'nbp_gold_prices' as source_id,
    'PLN per gram at 1000 fineness' as published_unit
