{{ config(
    materialized='view',
    schema='04_gold',
    alias='dim_vat_taxpayer_account',
    enabled=var('enable_mf_biala_lista', false)
) }}

SELECT
    hash,
    status AS taxpayer_status,
    snapshot_date,
    processed_at_utc
FROM {{ ref('stg_biala_lista_taxpayers') }}
