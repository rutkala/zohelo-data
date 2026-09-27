{{ config(
    alias='stg_bdl_web_observations',
    enabled=var('enable_gus_bdl_web', false)
) }}

with ranked as (
    select
        subgroup_id,
        selection_id,
        unit_id,
        unit_name,
        terc_code,
        period_raw,
        period_year,
        val_raw,
        val_numeric,
        measure_unit,
        attr_name,
        dimensions_json,
        raw_archive_file,
        processed_at_utc,
        row_number() over (
            partition by
                subgroup_id,
                unit_id,
                period_raw,
                dimensions_json,
                coalesce(attr_name, '')
            order by processed_at_utc desc
        ) as dedupe_rank
    from {{ ref('br_bdl_web_observations') }}
)
select
    md5(concat_ws('||', subgroup_id, unit_id, period_raw, dimensions_json, coalesce(attr_name, ''))) as observation_key,
    subgroup_id,
    selection_id,
    unit_id,
    unit_name,
    terc_code,
    period_raw as period_key,
    period_year,
    val_raw,
    val_numeric,
    measure_unit,
    attr_name,
    dimensions_json,
    raw_archive_file,
    processed_at_utc
from ranked
where dedupe_rank = 1
