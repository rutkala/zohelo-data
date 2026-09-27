{{ config(
    materialized='view',
    schema='03_silver',
    alias='stg_gios_sensors',
    enabled=var('enable_gios_pjp', false)
) }}

SELECT
    sensor_code,
    station_code,
    UPPER(TRIM(pollutant)) AS pollutant,
    LOWER(TRIM(averaging_interval)) AS averaging_interval,
    measurement_type,
    processed_at_utc
FROM {{ source('bronze_gios', 'sensors') }}
