# Using the portal

The NBP v2 data release is live as of 7 September 2026. It has published source and lineage metadata and gold tables; governed metric definitions are not published. See the [release evidence](releases/2026-09-07-nbp-platform.md).

Open [Zohelo-data](https://data.zohelo.com/) and choose **Sign in** in the **Lakehouse (Google Drive)** panel. Approve the existing Google read-only Drive login. After **Drive Connected** appears, refresh the lakehouse if needed. The release badge identifies the data snapshot in use.

Open [Data catalogue](https://data.zohelo.com/?view=catalog) from Home or the left navigation. It is the single catalogue for the connected release and uses dbt's own viewer:

- Its **overview** lists the four released NBP feeds, their ingestion status, checked-through dates and latest observations.
- **Model pages** provide descriptions and columns. Released ingestion details are added to the existing bronze models' metadata.
- The native **lineage graph** follows the published dbt dependencies through gold. Source and model identifiers reflect the real graph.

On a phone, use **Browse** inside the catalogue to open its Project or Database tree. Select a model to return to its details. The portal's **Tables** button opens the SQL data explorer.

No governed metric definitions are published for the current release. Full provider licence/reuse, frequency and coverage metadata remains a catalogue follow-up in [the delivery plan](deliverables.md).

Project status and owner questions are kept in [the delivery plan](deliverables.md). Send a question ID and your answer in chat; the assistant maintains that record.

## SQL

**Browser workspace** (previously labeled `memory`) is the temporary DuckDB database running in your browser. It holds tables loaded for your queries; Google Drive keeps the durable data. You do not need to manage this workspace or preload tables to join them.

Write SQL against quoted physical names such as `"04_gold"."fact_gold_prices"`. On **Run**, the portal finds referenced published tables and loads their verified data automatically before SELECT or CREATE VIEW execution. This applies to ordinary queries, joins, unions, and common table expressions (CTEs); selecting tables in the Explorer or using a special join action is not required.

Automatic loading is limited to a combined 64 MiB Google Drive download budget per browser DuckDB session. If the referenced released tables exceed that budget, the portal should explain the limit before running the query. This behavior is deployed in [PR 63](https://github.com/rutkala/zohelo-data/pull/63). Catalogue metadata has a separate 16 MiB download limit; query memory usage is not bounded by these download limits.

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

Use the generated `"layer"."table"` names when referring to published data. Automatic loading applies to SELECT queries, including joins, unions, nested queries and CTEs. Bare table names follow DuckDB’s local schema rules. CREATE VIEW also loads the published relations used in its SELECT or WITH query. Other local DDL/DML stays on the normal engine path.


## Table actions and views

Use the **⋮** next to a published table or workspace relation. It is available on hover or keyboard focus and remains visible on touch screens.

- **Query as SELECT** opens generated SQL in a new tab and keeps your existing draft. Press Run when ready.
- **Insert in SQL editor** inserts the quoted relation at the current selection in the active SQL tab.
- **Copy quoted name** copies a name ready for SQL.
- On desktop, drag a table into the SQL canvas to insert its quoted name at the drop point. An empty canvas receives a SELECT query. Insertion can be undone normally.

To name a SELECT or WITH query, choose **Create view**, enter a view name and create it. The definition opens in its own SQL tab, runs, and the new view appears in **Browser workspace**. An existing relation with that name is not replaced. You can also write the definition directly:

```sql
CREATE VIEW "main"."my_gold_prices" AS
SELECT effective_date, price_pln_per_gram_1000
FROM "04_gold"."fact_gold_prices";
```

Then query `"main"."my_gold_prices"` from another SQL tab. These are views in the current browser database session, not shared platform publications. Save the creation SQL if you want to recreate one after restarting that session.
