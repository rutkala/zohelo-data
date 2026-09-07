{{ config(materialized='table') }}

with observed_dates as (
    select effectiveDate as date_key from {{ ref('stg_nbp_table_a') }}
    union all select effectiveDate from {{ ref('stg_nbp_table_b') }}
    union all select effectiveDate from {{ ref('stg_nbp_table_c') }}
    union all select tradingDate from {{ ref('stg_nbp_table_c') }} where tradingDate is not null
    union all select effectiveDate from {{ ref('stg_nbp_gold_prices') }}
),
bounds as (
    select min(date_key) as first_date, max(date_key) as last_date
    from observed_dates
)
select
    spine.date_key::date as date_key,
    extract(year from spine.date_key)::integer as calendar_year,
    extract(month from spine.date_key)::integer as calendar_month,
    extract(day from spine.date_key)::integer as calendar_day,
    extract(isodow from spine.date_key)::integer as iso_weekday
from bounds
cross join generate_series(first_date, last_date, interval 1 day) as spine(date_key)
