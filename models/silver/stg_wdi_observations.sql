{{ config(alias='wdi_observations', enabled=var('enable_wdi', false)) }}

select
    country_code,
    indicator_code,
    observation_year,
    make_date(observation_year, 1, 1) as observation_date,
    observation_value_text,
    try_cast(observation_value_text as double) as observation_value,
    evidence.raw_sha256 as archive_sha256,
    cast(evidence.retrieved_at_utc as timestamp) as archive_retrieved_at_utc
from {{ ref('br_wdi_data') }}
cross join {{ source('wdi_bulk', 'archive_evidence') }} as evidence
