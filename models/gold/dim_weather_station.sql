{{ config(
    materialized='table',
    schema='04_gold',
    alias='dim_weather_station',
    enabled=var('enable_imgw_pib', false)
) }}

select
    md5(cast(station_code as varchar)) as station_key,
    station_code,
    station_name,
    station_num,
    'IMGW-PIB' as source_provider,
    processed_at_utc
from {{ ref('stg_imgw_stations') }}
