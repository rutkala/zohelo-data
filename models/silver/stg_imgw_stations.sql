{{ config(materialized='table', alias='stg_imgw_stations', enabled=var('enable_imgw_pib', false)) }}

with source_data as (
    select
        station_code,
        station_name,
        station_num,
        processed_at_utc
    from {{ source('bronze_imgw', 'stations') }}
)

select
    station_code,
    trim(station_name) as station_name,
    station_num,
    processed_at_utc
from source_data
where station_code is not null and trim(station_code) != ''
