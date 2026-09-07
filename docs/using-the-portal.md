# Using the portal

The NBP v2 data release is live as of 7 September 2026. It has published source and lineage metadata and gold tables; governed metric definitions are not published. See the [release evidence](releases/2026-09-07-nbp-platform.md).

Open [Zohelo-data](https://data.zohelo.com/) and choose **Sign in** in the **Lakehouse (Google Drive)** panel. Approve the existing Google read-only Drive login. After **Drive Connected** appears, refresh the lakehouse if needed. The release badge identifies the data snapshot in use.

Open [Data catalogue](https://data.zohelo.com/?view=catalog) from Home or the left navigation. It is the single catalogue for the connected release:

- **Sources and datasets** show source descriptions, coverage, frequency, licence/reuse context, and status. **Checked through** is validated request coverage; **Latest observation**, **Last successful ingestion**, and **Last attempt** answer different questions.
- **Lineage** shows published release relationships from source through gold.
- **Metrics** shows published metric definitions for that release. The current v2 release has none because no metric is approved or implemented.
- **Technical dbt docs** describe deployed engineering models; they do not add a second business catalogue.

The portal is being updated to remove project planning and owner-review content from this data experience. Current status and the questions that need an owner answer are in [the delivery plan](deliverables.md); answer there by sending the question ID in chat.

## SQL

Write SQL against quoted physical names such as `"04_gold"."fact_gold_prices"`. On **Run**, the portal is being updated to find every released table referenced by the statement and load it automatically before execution. This applies to ordinary queries, joins, unions, and common table expressions (CTEs); selecting tables in the Explorer or using a special join action is not required.

Automatic loading is limited to a combined 64 MiB Google Drive download budget per browser DuckDB session. If the referenced released tables exceed that budget, the portal should explain the limit before running the query. This behavior is in active implementation and must pass portal checks and deployment before it can be treated as live.

```sql
SELECT "effective_date", "price_pln_per_gram_1000"
FROM "04_gold"."fact_gold_prices"
ORDER BY "effective_date" DESC
LIMIT 10;
```

```sql
SELECT
  quotes."effective_date",
  currencies."source_currency_name",
  quotes."mid"
FROM "04_gold"."fact_fx_quotes" AS quotes
JOIN "04_gold"."dim_currency" AS currencies
  ON quotes."currency_key" = currencies."currency_key"
WHERE quotes."source_table_key" = 'A'
  AND quotes."currency_key" = 'USD'
ORDER BY quotes."effective_date" DESC
LIMIT 10;
```

See [joining NBP v2 tables](sql-joins.md) for the released-table names and join keys.

Use the generated `"layer"."table"` names when referring to published data. Automatic loading applies to SELECT queries, including joins, unions, nested queries and CTEs. Bare table names follow DuckDB’s local schema rules. Local DDL/DML stays on the normal engine path; load any required published relation with a SELECT first.
