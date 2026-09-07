{{ config(materialized='table' if var('nbp_verified_batches', false) else 'view') }}
{% if var('nbp_verified_batches', false) %}
{{ nbp_current_exchange_rates('br_nbp_table_c', 'C') }}
{% else %}
with bronze_data as (
    {{ nbp_exchange_rate_bronze("nbp_exchange_rates_table_c") }}
),
flattened_rates as (
    select
        effectiveDate,
        json_extract_string(rate.value, '$.currency') as currency,
        json_extract_string(rate.value, '$.code') as code,
        cast(json_extract_string(rate.value, '$.bid') as double) as bid,
        cast(json_extract_string(rate.value, '$.ask') as double) as ask,
        source_file
    from bronze_data
    cross join json_each(bronze_data.rates_json) as rate
),
conflicting_keys as (
    select code, effectiveDate
    from flattened_rates
    group by code, effectiveDate
    having count(distinct bid) > 1
        or count(distinct ask) > 1
        or (count(*) filter (where bid is null) > 0
            and count(*) filter (where bid is not null) > 0)
        or (count(*) filter (where ask is null) > 0
            and count(*) filter (where ask is not null) > 0)
),
validation as (
    select case
        when count(*) > 0 then error(
            'Conflicting NBP table C rates found for the same code and effective date; '
            || cast(min(effectiveDate) as varchar)
        )
        else 1
    end as validation_result
    from conflicting_keys
),
deduplicated_rates as (
    select
        effectiveDate,
        currency,
        code,
        bid,
        ask,
        row_number() over (
            partition by code, effectiveDate
            -- Metadata-only tie-break; source filenames are not correction provenance.
            order by
                (currency is not null) desc,
                currency asc nulls last,
                bid asc nulls last,
                ask asc nulls last
        ) as row_number
    from flattened_rates
)

select
    rates.effectiveDate,
    rates.currency,
    rates.code,
    rates.bid,
    rates.ask
from deduplicated_rates as rates
cross join validation
where rates.row_number = 1
  and validation.validation_result = 1

{% endif %}
