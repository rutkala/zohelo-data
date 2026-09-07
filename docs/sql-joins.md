# Joining NBP v2 tables in the portal

The portal loads one physical table when you select it in the Lakehouse Explorer. To join tables, load each table that appears in your SQL first. All tables in a query must come from the release badge currently shown in the explorer.

For the built-in example, select **Open join example** in the business catalogue. It loads these exact `04_gold` tables from the pinned release and opens an SQL tab without running it:

- `fact_fx_quotes`
- `dim_currency`
- `dim_date`

Review or edit the SQL, then choose **Run Query**. The example joins each published FX quotation to its currency label and calendar date:

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

`fact_fx_quotes` has one row per NBP source table, effective date, and currency. Use `source_table_key` to keep Tables A, B, and C distinct. The join keys are `currency_key` to `dim_currency.currency_key` and `effective_date` to `dim_date.date_key`. The query returns stored `mid`, `bid`, and `ask` values; it does not calculate a daily average or any governed metric.

To write another join, select every table you need once, then reference its quoted physical name: `"04_gold"."table_name"`. The browser has a 64 MiB Google Drive download limit per DuckDB session. If the selected tables exceed that limit or Drive authorization changes, the editor is not opened; sign in again or use a smaller set of tables.
