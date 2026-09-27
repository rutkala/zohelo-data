{{ config(materialized='table') }}

select cast(date_day as date) as date_day
from generate_series(date '2024-01-01', date '2024-01-04', interval 1 day) as days(date_day)
