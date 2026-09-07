{{ config(materialized='table') }}

select 'A' as source_table_key, 'nbp_exchange_rates_table_a' as source_id
union all select 'B', 'nbp_exchange_rates_table_b'
union all select 'C', 'nbp_exchange_rates_table_c'
