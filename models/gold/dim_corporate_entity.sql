{{ config(
    materialized='view',
    schema='04_gold',
    alias='dim_corporate_entity',
    enabled=var('enable_gleif', false)
) }}

SELECT
    e.lei,
    e.legal_name,
    e.legal_country,
    e.legal_city,
    e.legal_postal_code,
    e.hq_country,
    e.hq_city,
    e.entity_status,
    e.entity_category,
    e.validation_authority_id,
    e.validation_authority_entity_id,
    CASE 
        WHEN e.validation_authority_id = 'RA000484' THEN 'KRS'
        WHEN e.validation_authority_id = 'RA000654' THEN 'REGON'
        ELSE NULL
    END AS national_registry_type,
    e.registration_status,
    e.managing_lou,
    r.parent_lei AS direct_parent_lei,
    r.relationship_type AS parent_relationship_type,
    e.processed_at_utc
FROM {{ ref('stg_gleif_entities') }} e
LEFT JOIN {{ ref('stg_gleif_relationships') }} r
    ON e.lei = r.child_lei
    AND r.relationship_type IN ('IS_DIRECTLY_CONSOLIDATED_BY', 'IS_ULTIMATELY_CONSOLIDATED_BY')
    AND r.relationship_status = 'ACTIVE'
