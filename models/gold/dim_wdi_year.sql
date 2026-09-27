{{ config(materialized='table', enabled=var('enable_wdi', false)) }}

select distinct
    observation_year as year_key,
    observation_year,
    make_date(observation_year, 1, 1) as year_start_date
from {{ ref('stg_wdi_observations') }}
