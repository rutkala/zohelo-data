{{ config(
    materialized='view',
    schema='03_silver',
    alias='stg_biala_lista_masks',
    enabled=var('enable_mf_biala_lista', false)
) }}

SELECT
    account_mask,
    bank_prefix,
    snapshot_date,
    processed_at_utc
FROM {{ source('bronze_mf_biala_lista', 'masks') }}
