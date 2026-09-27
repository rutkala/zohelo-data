{{ config(
    materialized='view',
    schema='03_silver',
    alias='stg_gleif_relationships',
    enabled=var('enable_gleif', false)
) }}

SELECT
    child_lei,
    child_node_type,
    parent_lei,
    parent_node_type,
    relationship_type,
    relationship_status,
    period_start_date,
    period_end_date,
    registration_status,
    managing_lou,
    processed_at_utc
FROM {{ source('bronze_gleif', 'relationship_records') }}
