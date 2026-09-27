{{ config(
    materialized='view',
    schema='03_silver',
    alias='stg_gios_measurements',
    enabled=var('enable_gios_pjp', false)
) }}

SELECT
    station_code,
    sensor_code,
    UPPER(TRIM(pollutant)) AS pollutant,
    LOWER(TRIM(averaging_interval)) AS averaging_interval,
    CAST(measurement_timestamp AS TIMESTAMP) AS measurement_timestamp,
    CAST(observation_date AS DATE) AS observation_date,
    CAST(value AS DOUBLE) AS value,
    unit,
    processed_at_utc
FROM {{ source('bronze_gios', 'measurements') }}
WHERE value IS NOT NULL AND value >= 0
