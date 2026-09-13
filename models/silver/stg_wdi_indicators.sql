{{ config(alias='wdi_indicators', enabled=var('enable_wdi', false)) }}

select
    trim("Series Code") as indicator_code,
    nullif(trim("Indicator Name"), '') as indicator_name,
    nullif(trim("Topic"), '') as topic,
    nullif(trim("Short definition"), '') as short_definition,
    nullif(trim("Long definition"), '') as long_definition,
    nullif(trim("Unit of measure"), '') as unit_of_measure,
    nullif(trim("Periodicity"), '') as periodicity,
    nullif(trim("Aggregation method"), '') as aggregation_method,
    nullif(trim("Source"), '') as source_organization,
    nullif(trim("License Type"), '') as license_type,
    nullif(trim("Limitations and exceptions"), '') as limitations_and_exceptions
from {{ ref('br_wdi_series') }}
where nullif(trim("Series Code"), '') is not null
qualify row_number() over (partition by trim("Series Code") order by trim("Series Code")) = 1
