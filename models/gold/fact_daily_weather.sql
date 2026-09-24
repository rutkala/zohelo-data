{{ config(
    materialized='table',
    schema='04_gold',
    alias='fact_daily_weather',
    enabled=var('enable_imgw_pib', false)
) }}

select
    md5(cast(w.station_code as varchar) || '_' || cast(w.observation_date as varchar)) as weather_observation_key,
    md5(cast(w.station_code as varchar)) as station_key,
    w.station_code,
    w.observation_date,
    w.tmax_celsius,
    w.tmin_celsius,
    w.tmean_celsius,
    w.precipitation_mm,
    w.precipitation_type_standardized,
    w.precipitation_type_raw,
    w.snow_depth_cm,
    w.sunshine_hours,
    w.wind_speed_ms,
    w.relative_humidity_pct,
    w.pressure_sea_level_hpa,
    w.raw_archive_file,
    w.processed_at_utc
from {{ ref('stg_imgw_synoptic_daily') }} w
