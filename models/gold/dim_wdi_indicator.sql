{{ config(materialized='table', enabled=var('enable_wdi', false)) }}

select
    indicator_code as indicator_key,
    indicator_code,
    indicator_name,
    topic,
    short_definition,
    long_definition,
    unit_of_measure,
    periodicity,
    aggregation_method,
    source_organization,
    license_type,
    limitations_and_exceptions
from {{ ref('stg_wdi_indicators') }}
