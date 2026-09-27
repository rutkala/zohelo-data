{{ config(
    materialized='view',
    schema='03_silver',
    alias='stg_gios_stations',
    enabled=var('enable_gios_pjp', false)
) }}

SELECT
    station_code,
    old_station_code,
    station_name,
    station_type,
    area_type,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    UPPER(TRIM(voivodeship)) AS voivodeship,
    TRIM(city) AS city,
    TRIM(address) AS address,
    processed_at_utc
FROM {{ source('bronze_gios', 'stations') }}
