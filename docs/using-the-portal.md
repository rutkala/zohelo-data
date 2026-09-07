# Using the portal

**The NBP v2 data release is live as of 7 September 2026.** The source and lineage catalogue and gold tables are available; governed metrics await business definitions. See the [release evidence](releases/2026-09-07-nbp-platform.md).

Open [Zohelo-data](https://data.zohelo.com/) and choose **Sign in** in the **Lakehouse (Google Drive)** panel. Approve the existing Google read-only Drive login. After **Drive Connected** appears, refresh the lakehouse if needed. The release badge identifies the data snapshot currently loaded.

Open [Business catalogue](https://data.zohelo.com/?view=catalog) from Home or the left navigation. The Explorer also has **Open full catalogue**. Select one of its three views:

- **sources** lists the four NBP sources and their business descriptions. **Checked through** is the date covered by the snapshot’s validation state. **Latest observation** is the newest date observed; it does not by itself prove complete coverage. Also review **Last successful ingestion**, **Last attempt**, and **Raw responses**. A last attempt records an attempt, not a freshness guarantee.
- **lineage** lists each published node with its kind and layer. **Dependencies** shows the recorded upstream-to-downstream links.
- **metrics** currently says: “Governed metric definitions are awaiting business approval.” No business calculation is approved yet.

**Technical dbt docs** opens the generated engineering documentation within the catalogue. It describes deployed code; the business catalogue describes the connected data release.

Open [Review & decisions](https://data.zohelo.com/?view=review) for the researched NBP metric proposal and next-source research. Add your comments there, then use **Copy response for chat** and paste here, or attach the downloaded response. Drafts stay on this browser/profile; they do not sync or reach the assistant automatically. See the [business review guide](business-review.md).

To try a join, choose **Open join example** in the full catalogue and then **Run** in the SQL editor. The helper loads the FX fact, currency dimension and date dimension from your connected release and opens a query joining them. For another join, select each required table once in the Explorer, then reference them together in one SQL tab.

For SQL, expand **04_gold**, select a table, or open a SQL tab and use the physical names below. For multi-table queries, see [joining NBP v2 tables](sql-joins.md). These examples return the ten most recent rows:

```sql
SELECT "effective_date", "price_pln_per_gram_1000"
FROM "04_gold"."fact_gold_prices"
ORDER BY "effective_date" DESC
LIMIT 10;
```

```sql
SELECT "effective_date", "currency_key", "quote_currency_key", "mid"
FROM "04_gold"."fact_fx_quotes"
WHERE "source_table_key" = 'A'
  AND "currency_key" = 'USD'
ORDER BY "effective_date" DESC
LIMIT 10;
```
