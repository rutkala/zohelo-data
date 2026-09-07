# Using the portal

**For the v2 data release. Availability awaits successful data publication; the current live portal still serves the v1 silver release.**

Open [Zohelo-data](https://data.zohelo.com/) and choose **Sign in** in the **Lakehouse (Google Drive)** panel. Approve the existing Google read-only Drive login. After **Drive Connected** appears, refresh the lakehouse if needed. The release badge identifies the data snapshot currently loaded.

Open **Business catalogue** in the Lakehouse Explorer. Select one of its three views:

- **sources** lists the four NBP sources and their business descriptions. **Checked through** is the date covered by the snapshot’s validation state. **Latest observation** is the newest date observed; it does not by itself prove complete coverage. Also review **Last successful ingestion**, **Last attempt**, and **Raw responses**. A last attempt records an attempt, not a freshness guarantee.
- **lineage** lists each published node with its kind and layer. **Dependencies** shows the recorded upstream-to-downstream links.
- **metrics** currently says: “Governed metric definitions are awaiting business approval.” No business calculation is approved yet.

For SQL, expand **04_gold**, select a table, or open a SQL tab and use the physical names below. These examples return the ten most recent rows:

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
