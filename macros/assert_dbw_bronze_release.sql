{% macro assert_dbw_bronze_release() %}
  {% if execute %}
    {% set release_id = env_var('ZOHELO_DBW_BRONZE_RELEASE_ID', '') %}
    {% if release_id | length != 64 %}
      {{ exceptions.raise_compiler_error('ZOHELO_DBW_BRONZE_RELEASE_ID must select a 64-character DBW native snapshot release.') }}
    {% endif %}
    {% for character in release_id %}
      {% if character not in '0123456789abcdef' %}
        {{ exceptions.raise_compiler_error('ZOHELO_DBW_BRONZE_RELEASE_ID must be a lowercase SHA-256 value.') }}
      {% endif %}
    {% endfor %}
    {% set data_root = env_var('ZOHELO_DATA_ROOT', '/tmp/zohelo_data') %}
    {% set release_root = data_root ~ '/02_bronze/gus_dbw/releases/' ~ release_id %}
    {% set marker_path = release_root ~ '/_control/bronze-complete-v1-' ~ release_id ~ '.json' %}
    {% set verification_query %}
      select count(*)
      from read_json_auto('{{ marker_path | replace("'", "''") }}')
      where schema_version = 1
        and record_type = 'gus_dbw_bronze_completion'
        and source_id = 'gus_dbw'
        and status = 'complete_native_snapshot'
        and release_id = '{{ release_id }}'
        and completed_indicators > 0
        and observation_partitions = completed_indicators
        and dictionary_partitions = completed_indicators
        and observation_partitions = (
          select count(*) from glob('{{ release_root | replace("'", "''") }}/observations/part_*.parquet')
        )
        and dictionary_partitions = (
          select count(*) from glob('{{ release_root | replace("'", "''") }}/dictionaries/dict_*.parquet')
        )
        and observation_inventory_sha256 = (
          select sha256(string_agg(regexp_extract(file, '[^/]+$'), chr(10) order by regexp_extract(file, '[^/]+$')))
          from glob('{{ release_root | replace("'", "''") }}/observations/part_*.parquet')
        )
        and dictionary_inventory_sha256 = (
          select sha256(string_agg(regexp_extract(file, '[^/]+$'), chr(10) order by regexp_extract(file, '[^/]+$')))
          from glob('{{ release_root | replace("'", "''") }}/dictionaries/dict_*.parquet')
        )
        and observation_content_inventory_sha256 = (
          select sha256(string_agg(
            regexp_extract(filename, '[^/]+$') || chr(0) || sha256(content),
            chr(10) order by regexp_extract(filename, '[^/]+$')
          ))
          from read_blob('{{ release_root | replace("'", "''") }}/observations/part_*.parquet')
        )
        and dictionary_content_inventory_sha256 = (
          select sha256(string_agg(
            regexp_extract(filename, '[^/]+$') || chr(0) || sha256(content),
            chr(10) order by regexp_extract(filename, '[^/]+$')
          ))
          from read_blob('{{ release_root | replace("'", "''") }}/dictionaries/dict_*.parquet')
        )
        and taxonomy_sha256 = (
          select sha256(content)
          from read_blob('{{ release_root | replace("'", "''") }}/taxonomy/br_dbw_indicators.parquet')
        )
        and metadata_sha256 = (
          select sha256(content)
          from read_blob('{{ release_root | replace("'", "''") }}/metadata/br_dbw_metadata.parquet')
        )
        and consolidated_dictionary_sha256 = (
          select sha256(content)
          from read_blob('{{ release_root | replace("'", "''") }}/dictionaries/br_dbw_dictionaries.parquet')
        )
    {% endset %}
    {% set verification = run_query(verification_query) %}
    {% if verification.columns[0].values()[0] != 1 %}
      {{ exceptions.raise_compiler_error('Selected DBW Bronze release has no valid complete-snapshot marker.') }}
    {% endif %}
  {% endif %}
{% endmacro %}
