{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Storage layer names are the public relation schemas in every target. #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
