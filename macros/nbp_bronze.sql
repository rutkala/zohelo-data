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
