/** Explicit, release-backed SQL examples for the NBP v2 browser catalogue. */
export const NBP_FX_DIMENSION_JOIN = {
  title: "FX quotes with currency and date",
  tables: [
    { layerName: "04_gold", tableName: "fact_fx_quotes" },
    { layerName: "04_gold", tableName: "dim_currency" },
    { layerName: "04_gold", tableName: "dim_date" },
  ],
  // This example preserves each published fact row. It does not aggregate rates.
  sql: `SELECT
  quotes."effective_date",
  dates."calendar_year",
  dates."calendar_month",
  quotes."source_table_key",
  quotes."currency_key",
  currencies."source_currency_name",
  quotes."quote_currency_key",
  quotes."mid",
  quotes."bid",
  quotes."ask"
FROM "04_gold"."fact_fx_quotes" AS quotes
JOIN "04_gold"."dim_currency" AS currencies
  ON quotes."currency_key" = currencies."currency_key"
JOIN "04_gold"."dim_date" AS dates
  ON quotes."effective_date" = dates."date_key"
WHERE quotes."source_table_key" = 'A'
  AND quotes."currency_key" = 'USD'
ORDER BY quotes."effective_date" DESC
LIMIT 10;`,
} as const;
