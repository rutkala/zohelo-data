{{ config(materialized='table') }}

with observed_currencies as (
    select code as currency_key, currency as source_currency_name from {{ ref('stg_nbp_table_a') }}
    union all
    select code as currency_key, currency as source_currency_name from {{ ref('stg_nbp_table_b') }}
    union all
    select code as currency_key, currency as source_currency_name from {{ ref('stg_nbp_table_c') }}
),
current_codes as (
    select currency_key, min(source_currency_name) as source_currency_name
    from observed_currencies
    where currency_key is not null
    group by currency_key
)
select currency_key, source_currency_name
from current_codes
union all
select 'PLN' as currency_key, cast(null as varchar) as source_currency_name
where not exists (select 1 from current_codes where currency_key = 'PLN')
