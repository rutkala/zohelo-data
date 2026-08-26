with source_data as (
    select *
    from {{ source('bronze', 'nbp_exchange_rates_table_a') }}
)

select *
from source_data
