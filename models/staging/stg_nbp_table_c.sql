with bronze_data as (
    select *
    from read_parquet(
        '{{ env_var("ZOHELO_DATA_ROOT", "/tmp/zohelo_data") }}/02_bronze/nbp_exchange_rates_table_c/*.parquet',
        union_by_name = true,
        filename = true
    )
),
flattened_rates as (
    select
        cast(effectiveDate as date) as effectiveDate,
        rate_item.currency as currency,
        rate_item.code as code,
        rate_item.bid as bid,
        rate_item.ask as ask,
        filename as source_file
    from bronze_data
    cross join unnest(rates) as rate(rate_item)
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
