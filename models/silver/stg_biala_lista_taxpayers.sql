{{ config(
    materialized='view',
    schema='03_silver',
    alias='stg_biala_lista_taxpayers',
    enabled=var('enable_mf_biala_lista', false)
) }}

SELECT
    hash,
    status,
    snapshot_date,
    processed_at_utc
FROM {{ source('bronze_mf_biala_lista', 'taxpayers') }}
