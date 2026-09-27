{{ config(
    materialized='table',
    schema='04_gold',
    alias='mart_weather_daily_summary',
    enabled=var('enable_imgw_pib', false)
) }}

select
    f.observation_date,
    date_trunc('month', f.observation_date) as observation_month,
    date_trunc('year', f.observation_date) as observation_year,
    f.station_key,
    s.station_code,
    s.station_name,
    f.tmax_celsius,
    f.tmin_celsius,
    f.tmean_celsius,
    (f.tmax_celsius - f.tmin_celsius) as diurnal_temperature_range_celsius,
    f.precipitation_mm,
    f.precipitation_type_standardized,
    case when f.precipitation_mm > 0.0 then 1 else 0 end as has_precipitation,
    case when f.precipitation_mm >= 10.0 then 1 else 0 end as is_heavy_rain_day,
    case when f.tmin_celsius < 0.0 then 1 else 0 end as is_frost_day,
    case when f.tmax_celsius >= 25.0 then 1 else 0 end as is_summer_day,
    case when f.tmax_celsius >= 30.0 then 1 else 0 end as is_hot_day,
    f.snow_depth_cm,
    f.sunshine_hours,
    f.wind_speed_ms,
    f.relative_humidity_pct,
    f.pressure_sea_level_hpa
from {{ ref('fact_daily_weather') }} f
left join {{ ref('dim_weather_station') }} s
  on f.station_key = s.station_key
