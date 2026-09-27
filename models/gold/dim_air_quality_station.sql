{{ config(
    materialized='table',
    schema='04_gold',
    alias='dim_air_quality_station',
    enabled=var('enable_gios_pjp', false)
) }}

SELECT
    s.station_code,
    s.old_station_code,
    s.station_name,
    s.station_type,
    s.area_type,
    s.latitude,
    s.longitude,
    s.voivodeship,
    s.city,
    s.address,
    COUNT(DISTINCT sn.sensor_code) AS active_sensors_count,
    s.processed_at_utc
FROM {{ ref('stg_gios_stations') }} s
LEFT JOIN {{ ref('stg_gios_sensors') }} sn
    ON s.station_code = sn.station_code
GROUP BY
    s.station_code,
    s.old_station_code,
    s.station_name,
    s.station_type,
    s.area_type,
    s.latitude,
    s.longitude,
    s.voivodeship,
    s.city,
    s.address,
    s.processed_at_utc
