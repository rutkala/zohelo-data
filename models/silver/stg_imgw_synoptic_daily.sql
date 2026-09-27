{{ config(materialized='table', alias='stg_imgw_synoptic_daily', enabled=var('enable_imgw_pib', false)) }}

with source_data as (
    select
        station_code,
        station_name,
        observation_date,
        tmax_celsius,
        tmin_celsius,
        tmean_celsius,
        precipitation_mm,
        precipitation_type,
        snow_depth_cm,
        sunshine_hours,
        wind_speed_ms,
        relative_humidity_pct,
        pressure_sea_level_hpa,
        raw_archive_file,
        processed_at_utc
    from {{ source('bronze_imgw', 'synoptic_daily') }}
)

select
    station_code,
    trim(station_name) as station_name,
    observation_date,
    cast(tmax_celsius as double) as tmax_celsius,
    cast(tmin_celsius as double) as tmin_celsius,
    cast(tmean_celsius as double) as tmean_celsius,
    cast(precipitation_mm as double) as precipitation_mm,
    case
        when precipitation_type = 'W' then 'liquid'
        when precipitation_type = 'S' then 'solid'
        when precipitation_type = 'D' then 'mixed'
        else null
    end as precipitation_type_standardized,
    precipitation_type as precipitation_type_raw,
    cast(snow_depth_cm as double) as snow_depth_cm,
    cast(sunshine_hours as double) as sunshine_hours,
    cast(wind_speed_ms as double) as wind_speed_ms,
    cast(relative_humidity_pct as double) as relative_humidity_pct,
    cast(pressure_sea_level_hpa as double) as pressure_sea_level_hpa,
    raw_archive_file,
    processed_at_utc
from source_data
where station_code is not null
  and observation_date is not null
