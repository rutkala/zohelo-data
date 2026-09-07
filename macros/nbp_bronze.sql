{% macro nbp_exchange_rate_bronze(dataset_name) %}
    {% set data_root = env_var("ZOHELO_DATA_ROOT", "/tmp/zohelo_data") %}
    {% set pattern = data_root ~ "/02_bronze/" ~ dataset_name ~ "/*.parquet" %}
    {% set quoted_pattern = "'" ~ pattern | replace("'", "''") ~ "'" %}

    {#
      Inspect each Parquet file separately before combining it. Legacy files can
      infer a nullable nested currency as JSON while newer files infer VARCHAR;
      union_by_name cannot reconcile those nested STRUCT types. Converting the
      nested rates array to JSON per file gives every branch one stable type.
      During parse/docs compilation there is no query result, so retain the
      normal missing-glob read to keep compilation valid while real execution
      still fails clearly when the input directory is absent.
    #}
    {% if execute %}
        {% set file_query %}
            select file
            from glob({{ quoted_pattern }})
            order by file
        {% endset %}
        {% set file_result = run_query(file_query) %}
        {% set files = file_result.columns[0].values() %}
    {% else %}
        {% set files = [] %}
    {% endif %}

    {% if files | length > 0 %}
        {% for file in files %}
            select
                cast(effectiveDate as date) as effectiveDate,
                to_json(rates) as rates_json,
                filename as source_file
            from read_parquet(
                '{{ file | replace("'", "''") }}',
                union_by_name = true,
                filename = true
            )
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    {% else %}
        select
            cast(effectiveDate as date) as effectiveDate,
            to_json(rates) as rates_json,
            filename as source_file
        from read_parquet(
            {{ quoted_pattern }},
            union_by_name = true,
            filename = true
        )
    {% endif %}
{% endmacro %}

{% macro nbp_verified_batches_enabled() %}
    {{ return(var('nbp_verified_batches', false)) }}
{% endmacro %}

{% macro nbp_current_exchange_rates(bronze_model, table_code) %}
with source_observations as (
    select *
    from {{ ref(bronze_model) }}
    where source_table = '{{ table_code }}'
),
conflicting_same_request_values as (
    select source_id, effective_date, code, ingestion_sequence
    from source_observations
    group by source_id, effective_date, code, ingestion_sequence
    having count(distinct mid) > 1
        or count(distinct bid) > 1
        or count(distinct ask) > 1
),
validation as (
    select case when count(*) > 0 then error(
        'Conflicting NBP values for duplicate source observations in one ingestion sequence'
    ) else 1 end as validation_result
    from conflicting_same_request_values
),
canonical_request_observations as (
    select
        source_id,
        min(source_table) as source_table,
        effective_date,
        code,
        min(currency) filter (where currency is not null) as currency,
        min(no) filter (where no is not null) as no,
        min(trading_date) as trading_date,
        min(mid) as mid,
        min(bid) as bid,
        min(ask) as ask,
        ingestion_sequence,
        min(batch_id) as batch_id,
        min(requested_start_date) as requested_start_date,
        min(requested_end_date) as requested_end_date,
        min(retrieved_at_utc) as retrieved_at_utc,
        min(response_sha256) as response_sha256,
        min(raw_file_id) as raw_file_id
    from source_observations
    group by source_id, effective_date, code, ingestion_sequence
),
ranked as (
    select
        *,
        row_number() over (
            partition by effective_date, code
            order by ingestion_sequence desc, retrieved_at_utc desc, batch_id desc, response_sha256 desc
        ) as current_row_number
    from canonical_request_observations
),
current_rows as (
    select *
    from ranked
    cross join validation
    where current_row_number = 1 and validation.validation_result = 1
)
select
    effective_date as effectiveDate,
    currency,
    code,
    mid,
    bid,
    ask,
    source_table,
    no,
    trading_date as tradingDate,
    source_id,
    batch_id,
    ingestion_sequence,
    requested_start_date,
    requested_end_date,
    retrieved_at_utc,
    response_sha256,
    raw_file_id
from current_rows
{% endmacro %}

{% macro nbp_current_gold_prices() %}
with conflicting_same_request_values as (
    select source_id, effective_date, ingestion_sequence
    from {{ ref('br_nbp_gold_prices') }}
    group by source_id, effective_date, ingestion_sequence
    having count(distinct price_pln_per_gram) > 1
),
validation as (
    select case when count(*) > 0 then error(
        'Conflicting NBP gold values for duplicate source observations in one ingestion sequence'
    ) else 1 end as validation_result
    from conflicting_same_request_values
),
canonical_request_observations as (
    select
        source_id,
        effective_date,
        min(price_pln_per_gram) as price_pln_per_gram,
        ingestion_sequence,
        min(batch_id) as batch_id,
        min(requested_start_date) as requested_start_date,
        min(requested_end_date) as requested_end_date,
        min(retrieved_at_utc) as retrieved_at_utc,
        min(response_sha256) as response_sha256,
        min(raw_file_id) as raw_file_id
    from {{ ref('br_nbp_gold_prices') }}
    group by source_id, effective_date, ingestion_sequence
),
ranked as (
    select *, row_number() over (
        partition by effective_date
        order by ingestion_sequence desc, retrieved_at_utc desc, batch_id desc, response_sha256 desc
    ) as current_row_number
    from canonical_request_observations
)
select
    effective_date as effectiveDate,
    price_pln_per_gram,
    source_id,
    batch_id,
    ingestion_sequence,
    requested_start_date,
    requested_end_date,
    retrieved_at_utc,
    response_sha256,
    raw_file_id
from ranked
cross join validation
where current_row_number = 1 and validation.validation_result = 1
{% endmacro %}

{% test nbp_unique_combination(model, combination_of_columns) %}
select
    {% for column in combination_of_columns %}
    {{ column }}{% if not loop.last %}, {% endif %}
    {% endfor %}
from {{ model }}
group by
    {% for column in combination_of_columns %}
    {{ column }}{% if not loop.last %}, {% endif %}
    {% endfor %}
having count(*) > 1
{% endtest %}
