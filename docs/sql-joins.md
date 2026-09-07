# Joining NBP v2 tables in the portal

Reference the released physical tables directly in SQL. When **Run** is selected, the portal identifies schema-qualified published tables in SELECT queries and loads them automatically, including references inside joins, unions, and CTEs. You do not need to preload tables in the Explorer or use a special join action.

The automatic load has a combined 64 MiB Google Drive download budget per browser DuckDB session. A query whose referenced released tables exceed the budget should show an actionable limit error before execution. This behavior is deployed in [PR 63](https://github.com/rutkala/zohelo-data/pull/63).

This example joins published FX quotations to their currency label and calendar date:

```sql
SELECT
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
LIMIT 10;
```

`fact_fx_quotes` has one row per NBP source table, effective date, and currency. Use `source_table_key` to keep Tables A, B, and C distinct. The join returns published `mid`, `bid`, and `ask` source values; it does not calculate a daily average or a governed metric.

A CTE can be used normally:

```sql
WITH usd_quotes AS (
  SELECT "effective_date", "mid"
  FROM "04_gold"."fact_fx_quotes"
  WHERE "source_table_key" = 'A' AND "currency_key" = 'USD'
)
SELECT *
FROM usd_quotes
ORDER BY "effective_date" DESC
LIMIT 10;
```

Use the generated `"layer"."table"` names when referring to published data. Automatic loading applies to SELECT queries, including joins, unions, nested queries and CTEs. Bare table names follow DuckDB’s local schema rules. Local DDL/DML stays on the normal engine path; load any required published relation with a SELECT first.
