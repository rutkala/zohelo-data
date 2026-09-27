{{ config(
    materialized='table',
    schema='04_gold',
    alias='fact_daily_air_quality',
    enabled=var('enable_gios_pjp', false)
) }}

SELECT
    m.observation_date,
    m.station_code,
    m.pollutant,
    m.unit,
    ROUND(AVG(m.value), 2) AS avg_daily_value,
    ROUND(MIN(m.value), 2) AS min_daily_value,
    ROUND(MAX(m.value), 2) AS max_daily_value,
    COUNT(*) AS observations_count,
    MAX(m.processed_at_utc) AS processed_at_utc
FROM {{ ref('stg_gios_measurements') }} m
GROUP BY
    m.observation_date,
    m.station_code,
    m.pollutant,
    m.unit
