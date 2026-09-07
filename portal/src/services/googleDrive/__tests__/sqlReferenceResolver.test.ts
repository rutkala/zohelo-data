import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createRequire } from "node:module";
import type { ReleaseDataset } from "../types";
import { resolvePublishedTableReferences } from "../sqlReferenceResolver";

const datasets = [
  { layer: "04_gold", table_name: "fact_fx_quotes" },
  { layer: "04_gold", table_name: "dim_currency" },
  { layer: "04_gold", table_name: "dim_date" },
  { layer: "03_silver", table_name: "rates" },
  { layer: "04_gold", table_name: "rates" },
].map((dataset) => ({
  ...dataset,
  dataset_id: dataset.table_name,
  row_count: 1,
  min_date: null,
  max_date: null,
  columns:
    dataset.table_name === "fact_fx_quotes"
      ? [
          { name: "effective_date", type: "DATE" },
          { name: "currency_key", type: "VARCHAR" },
          { name: "mid", type: "DOUBLE" },
        ]
      : dataset.table_name === "dim_currency"
        ? [
            { name: "currency_key", type: "VARCHAR" },
            { name: "source_currency_name", type: "VARCHAR" },
          ]
        : dataset.table_name === "dim_date"
          ? [
              { name: "date_key", type: "DATE" },
              { name: "calendar_year", type: "INTEGER" },
            ]
          : [{ name: "id", type: "INTEGER" }],
  files: [],
})) as ReleaseDataset[];

const require = createRequire(import.meta.url);
interface BlockingConnection {
  query(sql: string): { getChildAt(index: number): { get(index: number): unknown } | null };
  close(): void;
}
let connection: BlockingConnection;

beforeAll(async () => {
  // This is the node-blocking build of the exact duckdb-wasm package shipped
  // to browsers. It proves the parser API handles the SQL forms below.
  const duckdb = require("@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs");
  const db = await duckdb.createDuckDB(
    {
      mvp: {
        mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm"),
        mainWorker: null,
      },
      eh: {
        mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm"),
        mainWorker: null,
      },
    },
    new duckdb.VoidLogger(),
    duckdb.NODE_RUNTIME
  );
  await db.instantiate(() => {});
  connection = db.connect();
}, 60_000);

afterAll(() => connection?.close());

const resolve = (sql: string) =>
  resolvePublishedTableReferences(
    { query: async (query) => connection.query(query) },
    sql,
    datasets
  );

describe("published SQL reference resolution", () => {
  it("uses the shipped DuckDB parser for CTEs, nested selects, unions, and quoted relations", async () => {
    const sql = `
      WITH rates AS (
        SELECT * FROM "04_gold"."fact_fx_quotes"
      ), filtered AS (
        SELECT * FROM (SELECT * FROM rates) nested_rates
      )
      SELECT filtered.currency_key FROM filtered
      JOIN "04_gold"."dim_currency" USING (currency_key)
      UNION ALL
      SELECT quotes.currency_key FROM "04_gold"."fact_fx_quotes" AS quotes
      JOIN "04_gold"."dim_date" ON true
    `;
    const resolved = await resolve(sql);

    expect(resolved.map((table) => `${table.layerName}.${table.datasetName}`)).toEqual([
      "04_gold.fact_fx_quotes",
      "04_gold.dim_currency",
      "04_gold.dim_date",
    ]);
  }, 30_000);

  it("scopes CTE names while retaining a separately qualified published table", async () => {
    const resolved = await resolve(`
      WITH fact_fx_quotes AS (SELECT 1 AS id)
      SELECT * FROM fact_fx_quotes
      UNION ALL SELECT * FROM "04_gold"."fact_fx_quotes"
    `);
    expect(resolved.map((table) => `${table.layerName}.${table.datasetName}`)).toEqual([
      "04_gold.fact_fx_quotes",
    ]);
  });

  it("does not load a release table named through another DuckDB catalog", async () => {
    await expect(
      resolve('SELECT * FROM foreign_catalog."04_gold"."fact_fx_quotes"')
    ).resolves.toEqual([]);
  });
});
