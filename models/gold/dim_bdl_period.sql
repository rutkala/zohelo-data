{{ config(materialized='table') }}

with distinct_periods as (
    select distinct period_key
    from {{ ref('stg_bdl_observations') }}
)
select
    period_key,
    case
        when regexp_matches(period_key, '^[0-9]{4}$') then 'year'
        when regexp_matches(period_key, '^[0-9]{4}-Q[1-4]$') then 'quarter'
        when regexp_matches(period_key, '^[0-9]{4}-[0-9]{2}$') then 'month'
        when regexp_matches(period_key, '^[0-9]{4}-M(0[1-9]|1[0-2])$') then 'month'
        when regexp_matches(period_key, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') then 'day'
        else 'other'
    end as period_granularity,
    case
        when regexp_matches(period_key, '^[0-9]{4}$') then make_date(cast(substr(period_key, 1, 4) as integer), 1, 1)
        when regexp_matches(period_key, '^[0-9]{4}-Q[1-4]$') then make_date(
            cast(substr(period_key, 1, 4) as integer),
            ((cast(substr(period_key, 7, 1) as integer) - 1) * 3) + 1,
            1
        )
        when regexp_matches(period_key, '^[0-9]{4}-[0-9]{2}$') then make_date(
            cast(substr(period_key, 1, 4) as integer),
            cast(substr(period_key, 6, 2) as integer),
            1
        )
        when regexp_matches(period_key, '^[0-9]{4}-M(0[1-9]|1[0-2])$') then make_date(
            cast(substr(period_key, 1, 4) as integer),
            cast(substr(period_key, 7, 2) as integer),
            1
        )
        when regexp_matches(period_key, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') then cast(period_key as date)
        else cast(null as date)
    end as period_start_date,
    case
        when regexp_matches(period_key, '^[0-9]{4}$') then make_date(cast(substr(period_key, 1, 4) as integer), 12, 31)
        when regexp_matches(period_key, '^[0-9]{4}-Q1$') then make_date(cast(substr(period_key, 1, 4) as integer), 3, 31)
        when regexp_matches(period_key, '^[0-9]{4}-Q2$') then make_date(cast(substr(period_key, 1, 4) as integer), 6, 30)
        when regexp_matches(period_key, '^[0-9]{4}-Q3$') then make_date(cast(substr(period_key, 1, 4) as integer), 9, 30)
        when regexp_matches(period_key, '^[0-9]{4}-Q4$') then make_date(cast(substr(period_key, 1, 4) as integer), 12, 31)
        when regexp_matches(period_key, '^[0-9]{4}-[0-9]{2}$') then last_day(make_date(cast(substr(period_key, 1, 4) as integer), cast(substr(period_key, 6, 2) as integer), 1))
        when regexp_matches(period_key, '^[0-9]{4}-M(0[1-9]|1[0-2])$') then last_day(make_date(cast(substr(period_key, 1, 4) as integer), cast(substr(period_key, 7, 2) as integer), 1))
        when regexp_matches(period_key, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') then cast(period_key as date)
        else cast(null as date)
    end as period_end_date,
    case
        when regexp_matches(period_key, '^[0-9]{4}') then cast(substr(period_key, 1, 4) as integer)
        else null
    end as calendar_year,
    case
        when regexp_matches(period_key, '^[0-9]{4}-Q[1-4]$') then cast(substr(period_key, 7, 1) as integer)
        else null
    end as quarter_number
from distinct_periods
