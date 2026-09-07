with bronze_data as (
    select *
    from read_parquet(
        '{{ env_var("ZOHELO_DATA_ROOT", "/tmp/zohelo_data") }}/02_bronze/nbp_exchange_rates_table_a/*.parquet',
        union_by_name = true,
        filename = true
    )
),
flattened_rates as (
    select
        cast(effectiveDate as date) as effectiveDate,
        rate_item.currency as currency,
        rate_item.code as code,
        rate_item.mid as mid,
        filename as source_file
    from bronze_data
    cross join unnest(rates) as rate(rate_item)
),
conflicting_keys as (
    select code, effectiveDate
    from flattened_rates
    group by code, effectiveDate
    having count(distinct mid) > 1
        or (count(*) filter (where mid is null) > 0
            and count(*) filter (where mid is not null) > 0)
),
validation as (
    select case
        when count(*) > 0 then error(
            'Conflicting NBP table A rates found for the same code and effective date; '
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
        mid,
        row_number() over (
            partition by code, effectiveDate
            -- Metadata-only tie-break; source filenames are not correction provenance.
            order by
                (currency is not null) desc,
                currency asc nulls last,
                mid asc nulls last
        ) as row_number
    from flattened_rates
)

select
    rates.effectiveDate,
    rates.currency,
    rates.code,
    rates.mid
from deduplicated_rates as rates
cross join validation
where rates.row_number = 1
  and validation.validation_result = 1
