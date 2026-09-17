{{ config(materialized='table', enabled=var('enable_eurostat', false)) }}

with periods as (
    select distinct period_key, freq_code
    from {{ ref('stg_eurostat_observations') }}
)
select
    period_key,
    freq_code,
    case
        when regexp_matches(period_key, '^[0-9]{4}$') then 'year'
        when regexp_matches(period_key, '^[0-9]{4}-[0-9]{2}$') then 'month'
        else 'other'
    end as period_granularity,
    case
        when regexp_matches(period_key, '^[0-9]{4}$') then make_date(cast(period_key as integer), 1, 1)
        when regexp_matches(period_key, '^[0-9]{4}-[0-9]{2}$') then make_date(cast(substr(period_key, 1, 4) as integer), cast(substr(period_key, 6, 2) as integer), 1)
        else cast(null as date)
    end as period_start_date,
    case
        when regexp_matches(period_key, '^[0-9]{4}$') then make_date(cast(period_key as integer), 12, 31)
        when regexp_matches(period_key, '^[0-9]{4}-[0-9]{2}$') then last_day(make_date(cast(substr(period_key, 1, 4) as integer), cast(substr(period_key, 6, 2) as integer), 1))
        else cast(null as date)
    end as period_end_date
from periods
