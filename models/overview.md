{% docs __overview__ %}
# Zohelo Data catalogue

The NBP data project follows five durable storage areas. dbt owns the modeled Bronze, Silver and Gold relations. Python owns source extraction, file transfer and release publication.

| Storage area | dbt catalogue boundary | Current contents |
| --- | --- | --- |
| Landing (`01_landing`) | External source | `01_landing.nbp_batches`, the immutable verified NBP request envelopes used by the current build |
| Bronze (`02_bronze`) | Physical dbt schema and legacy external source | `nbp_exchange_rates_table_a`, `nbp_exchange_rates_table_b`, `nbp_exchange_rates_table_c`, `nbp_gold_prices`; legacy Parquet inputs appear as the `bronze` source only in compatibility builds |
| Silver (`03_silver`) | Physical dbt schema | `nbp_exchange_rates_table_a`, `nbp_exchange_rates_table_b`, `nbp_exchange_rates_table_c`, `nbp_gold_prices`, `nbp_change_events` |
| Gold (`04_gold`) | Physical dbt schema | `dim_date`, `dim_currency`, `dim_source_table`, `dim_commodity`, `fact_fx_quotes`, `fact_gold_prices`, and the retained baseline `mart_exchange_rates_daily` model |
| Archive (`05_archive`) | File-retention boundary; no dbt relation | Legacy archived source files; current v2 raw and release history is retained immutably without automatic deletion |

Use the physical schema and table names together in portal SQL, for example `"03_silver"."nbp_exchange_rates_table_a"`. The Project tree retains stable logical dbt model identifiers (`br_*` and `stg_*`) for lineage; their physical aliases match the published tables.

The current platform release publishes 15 physical data tables: four Bronze, five Silver and six Gold relations. The baseline mart remains in the dbt project for its historical compatibility check and is outside that release contract. Archive is intentionally absent from the dependency graph because archived files are not transformation inputs.

## Source-defined daily metrics

The semantic layer defines five daily NBP observations: Table A middle rate,
Table B middle rate, Table C buy rate, Table C sell rate, and the NBP price in
PLN per gram of gold of 1000 fineness. The Metrics and Semantic Models branches
show their actual dbt definitions and dependencies.

Each FX result retains publication date, currency and source table. Gold retains
publication date and commodity. Values are not additive over time or currencies;
no period average, return, conversion, spread or forward filling is implied.
The supported native query interface enforces these dimensions. A published zero
stays zero; an absent publication stays absent. Provider documentation, frequency
and reuse evidence are attached to the existing source/bronze metadata.
{% enddocs %}
