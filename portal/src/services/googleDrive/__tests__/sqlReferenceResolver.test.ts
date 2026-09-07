import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
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

  it("loads published dependencies for CREATE VIEW SELECT using the exact WASM parser", async () => {
    const resolved = await resolve(`
      CREATE VIEW "main"."daily AS rates" AS
      SELECT * FROM "04_gold"."fact_fx_quotes"
      JOIN "04_gold"."dim_currency" USING (currency_key);
    `);

    expect(resolved.map((table) => `${table.layerName}.${table.datasetName}`)).toEqual([
      "04_gold.fact_fx_quotes",
      "04_gold.dim_currency",
    ]);
  });

  it("loads CREATE OR REPLACE TEMP VIEW dependencies through comments and a CTE", async () => {
    const resolved = await resolve(`
      /* AS and ; here are trivia */
      CREATE OR REPLACE TEMP VIEW "main"."rates AS view" ("AS", currency_key) AS
      WITH selected AS (
        SELECT 'AS; in a value' AS "AS", currency_key
        FROM "04_gold"."fact_fx_quotes"
      )
      SELECT selected.*, currency.source_currency_name
      FROM selected
      JOIN "04_gold"."dim_currency" AS currency USING (currency_key);
      -- one trailing comment is part of the same submission
    `);

    expect(resolved.map((table) => `${table.layerName}.${table.datasetName}`)).toEqual([
      "04_gold.fact_fx_quotes",
      "04_gold.dim_currency",
    ]);
  });

  it("preserves SELECT-only batch reference discovery", async () => {
    const resolved = await resolve(`
      SELECT * FROM "03_silver"."rates";
      SELECT * FROM "04_gold"."dim_date";
    `);
    expect(resolved.map((table) => `${table.layerName}.${table.datasetName}`)).toEqual([
      "03_silver.rates",
      "04_gold.dim_date",
    ]);
  });

  it("does not execute a mixed statement batch during discovery", async () => {
    connection.query('CREATE TEMP TABLE "resolver_side_effect" (id INTEGER)');
    try {
      await expect(
        resolve(`
          SELECT * FROM "04_gold"."rates";
          DROP TABLE "resolver_side_effect";
        `)
      ).resolves.toEqual([]);
      const tables = connection.query(`
        SELECT count(*)::INTEGER
        FROM duckdb_tables()
        WHERE table_name = 'resolver_side_effect'
      `);
      expect(tables.getChildAt(0)?.get(0)).toBe(1);
    } finally {
      connection.query('DROP TABLE IF EXISTS "resolver_side_effect"');
    }
  });

  it("matches exact WASM handling of ordinary and escaped string backslashes", () => {
    const ordinary = connection.query("SELECT 'a\\' AS value");
    const escaped = connection.query("SELECT E'a\\'' AS value");
    expect(ordinary.getChildAt(0)?.get(0)).toBe("a\\");
    expect(escaped.getChildAt(0)?.get(0)).toBe("a'");
  });

  it("does not load a release table named through another DuckDB catalog", async () => {
    await expect(
      resolve('SELECT * FROM foreign_catalog."04_gold"."fact_fx_quotes"')
    ).resolves.toEqual([]);
  });

  it("does not submit general DDL, CREATE VIEW batches, or lexical errors to the parser", async () => {
    const parser = { query: vi.fn() };
    const discover = (sql: string) => resolvePublishedTableReferences(parser, sql, datasets);

    await expect(discover('DROP TABLE "04_gold"."rates"')).resolves.toEqual([]);
    await expect(
      discover('CREATE VIEW "main"."x" AS SELECT * FROM "04_gold"."rates"; DROP TABLE "main"."x"')
    ).resolves.toEqual([]);
    await expect(discover(`CREATE VIEW "main"."x" AS SELECT 'unfinished`)).resolves.toEqual([]);
    expect(parser.query).not.toHaveBeenCalled();
  });
});
