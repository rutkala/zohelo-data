{{ config(materialized='table', enabled=var('enable_gus_dbw', false)) }}

select
    ind.indicator_id as indicator_key,
    ind.indicator_id,
    ind.indicator_name,
    ind.indicator_name_en,
    ind.domain_id,
    ind.domain_name,
    ind.domain_name_en,
    ind.area_id,
    ind.area_name,
    ind.area_name_en,
    meta.description,
    meta.frequency,
    meta.measure_unit,
    meta.data_source,
    meta.legal_basis,
    meta.last_update
from {{ ref('stg_dbw_indicators') }} ind
left join {{ ref('stg_dbw_metadata') }} meta
    on ind.indicator_id = meta.indicator_id
