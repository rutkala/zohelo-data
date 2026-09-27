{{ config(
    materialized='view',
    schema='03_silver',
    alias='stg_gleif_entities',
    enabled=var('enable_gleif', false)
) }}

SELECT
    lei,
    legal_name,
    legal_country,
    legal_city,
    legal_postal_code,
    hq_country,
    hq_city,
    entity_status,
    entity_category,
    legal_form_code,
    validation_authority_id,
    validation_authority_entity_id,
    initial_registration_date,
    last_update_date,
    registration_status,
    managing_lou,
    processed_at_utc
FROM {{ source('bronze_gleif', 'lei2') }}
