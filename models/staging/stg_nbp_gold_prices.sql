with bronze_data as (
    select *
    from read_parquet(
        '{{ env_var("ZOHELO_DATA_ROOT", "/tmp/zohelo_data") }}/02_bronze/nbp_gold_prices/*.parquet',
        union_by_name = true,
        filename = true
    )
),
typed_prices as (
    select
        cast(data as date) as effectiveDate,
        cast(cena as decimal(18, 2)) as price_pln_per_gram
    from bronze_data
),
date_values as (
    select
        effectiveDate,
        count(distinct price_pln_per_gram) as distinct_prices
    from typed_prices
    group by effectiveDate
),
conflicting_dates as (
    select effectiveDate
    from date_values
    where distinct_prices > 1
),
validation as (
    select case
        when count(*) > 0 then error(
            'Conflicting NBP gold prices found for the same publication date; '
            || cast(min(effectiveDate) as varchar)
        )
        else 1
    end as validation_result
    from conflicting_dates
),
deduplicated_prices as (
    select
        effectiveDate,
        price_pln_per_gram,
        row_number() over (
            partition by effectiveDate
            order by price_pln_per_gram asc
        ) as row_number
    from typed_prices
)

select
    prices.effectiveDate,
    prices.price_pln_per_gram
from deduplicated_prices as prices
cross join validation
where prices.row_number = 1
  and validation.validation_result = 1
