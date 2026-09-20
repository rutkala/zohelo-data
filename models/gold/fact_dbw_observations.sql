{{ config(
    materialized='table',
    enabled=var('enable_gus_dbw', false)
) }}

select
    obs.indicator_id as indicator_key,
    obs.indicator_id,
    obs.przekroj_id,
    obs.wymiar_1,
    obs.pozycja_1,
    obs.wymiar_2,
    obs.pozycja_2,
    obs.wymiar_3,
    obs.pozycja_3,
    obs.wymiar_4,
    obs.pozycja_4,
    obs.wymiar_5,
    obs.pozycja_5,
    obs.wymiar_6,
    obs.pozycja_6,
    obs.wymiar_7,
    obs.pozycja_7,
    obs.wymiar_8,
    obs.pozycja_8,
    obs.wymiar_9,
    obs.pozycja_9,
    obs.okres_id,
    obs.sposob_prezentacji_miara_id,
    obs.period_year,
    obs.wartosc_raw,
    obs.wartosc_numeric,
    obs.precyzja,
    obs.flaga_id,
    obs.brak_wartosci_id,
    obs.tajnosci_id,
    obs.raw_archive_file,
    obs.processed_at_utc
from {{ ref('stg_dbw_observations') }} obs
