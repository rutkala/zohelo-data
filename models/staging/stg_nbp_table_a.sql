with bronze_data as (
    select *
    from read_parquet(
        '/tmp/zohelo_data/02_bronze/nbp_exchange_rates_table_a/*.parquet',
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
)

select
    effectiveDate,
    currency,
    code,
    mid
from flattened_rates
qualify row_number() over (
    partition by code, effectiveDate
    order by source_file desc
) = 1
