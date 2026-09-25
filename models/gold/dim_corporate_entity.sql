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
    r_direct.parent_lei AS direct_parent_lei,
    r_ultimate.parent_lei AS ultimate_parent_lei,
    e.processed_at_utc
FROM {{ ref('stg_gleif_entities') }} e
LEFT JOIN {{ ref('stg_gleif_relationships') }} r_direct
    ON e.lei = r_direct.child_lei
    AND r_direct.relationship_type = 'IS_DIRECTLY_CONSOLIDATED_BY'
    AND r_direct.relationship_status = 'ACTIVE'
LEFT JOIN {{ ref('stg_gleif_relationships') }} r_ultimate
    ON e.lei = r_ultimate.child_lei
    AND r_ultimate.relationship_type = 'IS_ULTIMATELY_CONSOLIDATED_BY'
    AND r_ultimate.relationship_status = 'ACTIVE'
