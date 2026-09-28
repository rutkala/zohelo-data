import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { createRequire } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import type { Table } from "apache-arrow";
import type * as duckdb from "@duckdb/duckdb-wasm";
import { resultToJSON } from "@/services/duckdb/resultParser";
import { loadFileIntoDuckDB, loadTableIntoDuckDB, loadTablesIntoDuckDB } from "../lakehouseBridge";
import { nameBronzeDataset } from "../bronzeNames";
import { sha256Hex } from "../releaseCatalog";
import { fetchDriveFileBuffer, listDataFilesInFolder } from "../driveApi";
import type { LakehouseFile } from "../types";

vi.mock("../driveApi", () => ({ fetchDriveFileBuffer: vi.fn(), listDataFilesInFolder: vi.fn() }));

// Exercise the shipped WASM SQL engine; only network transport is mocked.
interface BlockingConnection {
  query(sql: string): Table;
  close(): void;
}
interface BlockingBindings {
  instantiate(progress: () => void): Promise<void>;
  connect(): BlockingConnection;
  registerFileBuffer(path: string, bytes: Uint8Array): void;
  copyFileToBuffer(path: string): Uint8Array;
  dropFile(path: string): void;
}
const require = createRequire(import.meta.url);
let engine: BlockingBindings;
let connection: BlockingConnection;
let db: duckdb.AsyncDuckDB;
let conn: duckdb.AsyncDuckDBConnection;
let directory: string;
let counter = 0;
const downloads = new Map<string, Uint8Array>();
const facade = () => ({
  registerFileBuffer: vi.fn(async (path: string, bytes: Uint8Array) =>
    engine.registerFileBuffer(path, bytes)
  ),
});
const rows = (target: string) =>
  resultToJSON(connection.query(`SELECT * FROM ${target} ORDER BY id`)).data;

beforeAll(async () => {
  const module = require("@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs");
  engine = await module.createDuckDB(
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
    new module.VoidLogger(),
    module.NODE_RUNTIME
  );
  await engine.instantiate(() => {});
  connection = engine.connect();
  conn = { query: async (sql: string) => connection.query(sql) } as duckdb.AsyncDuckDBConnection;
  directory = mkdtempSync(join(process.cwd(), "node_modules", ".lakehouse-tests-"));
}, 60000);

afterAll(() => {
  connection?.close();
  if (directory) rmSync(directory, { recursive: true, force: true });
});
beforeEach(() => {
  vi.resetAllMocks();
  downloads.clear();
  db = facade() as unknown as duckdb.AsyncDuckDB;
  vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) => {
    const bytes = downloads.get(id);
    if (!bytes) throw new Error("Download unavailable");
    return bytes.slice();
  });
  vi.mocked(listDataFilesInFolder).mockResolvedValue([]);
});

function file(sql: string, name = "data.parquet", layer = "02_bronze"): LakehouseFile {
  const id = `fixture-${++counter}`;
  const path = join(directory, `${id}.parquet`);
  connection.query(`COPY (${sql}) TO '${path.replace(/'/g, "''")}' (FORMAT PARQUET)`);
  downloads.set(id, engine.copyFileToBuffer(path).slice());
  engine.dropFile(path);
  return { id, name, layer, tableName: "rates" };
}
const load = (files: LakehouseFile[], dataset = "rates", layer = "02_bronze") =>
  loadTableIntoDuckDB(db, conn, dataset, null, files, "fixture-token", layer);

describe("Drive data in the real DuckDB engine", () => {
  it("marks published views with an OID and opaque comment that changes on replacement", async () => {
    const name = `owned_view_${++counter}`;
    const result = await load([file("SELECT 17 AS id")], name);
    const catalog = () => resultToJSON(connection.query(
      `SELECT view_oid, comment FROM duckdb_views() WHERE schema_name = '02_bronze' AND view_name = '${name}'`
    )).data[0] as { view_oid: number | bigint; comment: string };
    expect(String(catalog().view_oid)).toBe(result.identity.oid);
    expect(catalog().comment).toBe(result.identity.marker);
    expect(result.identity.marker).toMatch(/^zohelo-published-view:v1:[a-f0-9]{32}$/);

    const other = engine.connect();
    try {
      other.query(`CREATE OR REPLACE VIEW "02_bronze"."${name}" AS SELECT 99 AS id`);
      expect(String(catalog().view_oid)).not.toBe(result.identity.oid);
      expect(catalog().comment).not.toBe(result.identity.marker);
      expect(rows(result.queryTarget)).toEqual([{ id: 99 }]);
    } finally {
      other.close();
    }
  }, 15000);

  it("preserves another connection's view created after the transaction checks occupancy", async () => {
    const name = `occupied_view_${++counter}`;
    const input = file("SELECT 20 AS id");
    const other = engine.connect();
    try {
      await expect(loadTablesIntoDuckDB(db, conn, [{ datasetName: name,
        tableFolderId: null, files: [input], layerName: "02_bronze" }], "fixture-token", undefined,
      async () => {
        expect(resultToJSON(connection.query(
          `SELECT table_name FROM information_schema.tables WHERE table_schema = '02_bronze' AND table_name = '${name}'`
        )).data).toEqual([]);
        other.query(`CREATE VIEW "02_bronze"."${name}" AS SELECT 77 AS id`);
      })).rejects.toThrow();
      expect(rows(`"02_bronze"."${name}"`)).toEqual([{ id: 77 }]);
    } finally {
      other.close();
    }
  });

  it("rolls back a DROP when another connection replaces the checked view", async () => {
    const name = `refresh_view_${++counter}`;
    const loaded = await load([file("SELECT 21 AS id")], name);
    const other = engine.connect();
    try {
      connection.query("BEGIN TRANSACTION;");
      const seen = resultToJSON(connection.query(
        `SELECT view_oid, comment FROM duckdb_views() WHERE schema_name = '02_bronze' AND view_name = '${name}'`
      )).data[0] as { view_oid: number | bigint; comment: string };
      expect(String(seen.view_oid)).toBe(loaded.identity.oid);
      expect(seen.comment).toBe(loaded.identity.marker);
      other.query(`CREATE OR REPLACE VIEW "02_bronze"."${name}" AS SELECT 88 AS id`);
      expect(() => connection.query(`DROP VIEW "02_bronze"."${name}";`)).toThrow();
      connection.query("ROLLBACK;");
      expect(rows(loaded.queryTarget)).toEqual([{ id: 88 }]);
    } finally {
      other.close();
    }
  });

  it("queries canonical and old qualified Bronze names over the same complete pinned files", async () => {
    const first = file("SELECT 1 AS id, 'one' AS label");
    const second = file("SELECT 1 AS id, 'one' AS label UNION ALL SELECT 2, 'two'");
    const named = nameBronzeDataset({
      dataset_id: "bronze_bdl_variables", layer: "02_bronze", table_name: "bdl_variables",
      columns: [], files: [first, second],
    }, "gus_bdl");
    await loadTablesIntoDuckDB(db, conn, [named.legacyName!, named.dataset.table_name].map(
      (name) => ({ datasetName: name, tableFolderId: null, files: named.dataset.files,
        layerName: "02_bronze" })
    ), "fixture-token");
    const canonical = '"02_bronze"."gus_bdl_variables"';
    const legacy = '"02_bronze"."bdl_variables"';
    expect(rows(canonical)).toEqual(rows(legacy));
    expect(rows(canonical)).toEqual([
      { id: 1, label: "one" }, { id: 1, label: "one" }, { id: 2, label: "two" },
    ]);
    expect(resultToJSON(connection.query(
      `SELECT count(*) AS n FROM ${canonical} c JOIN ${legacy} l ON c.id = l.id`
    )).data).toEqual([{ n: 5n }]);
    expect(resultToJSON(connection.query(`SELECT CASE WHEN
      (SELECT COUNT(*) FROM ${canonical}) = (SELECT COUNT(*) FROM ${legacy})
      AND NOT EXISTS (SELECT * FROM ${canonical} EXCEPT ALL SELECT * FROM ${legacy})
      AND NOT EXISTS (SELECT * FROM ${legacy} EXCEPT ALL SELECT * FROM ${canonical})
      THEN 'BRONZE_MATCH' ELSE 'MISMATCH' END AS bronze_check,
      (SELECT COUNT(*) FROM ${canonical}) AS row_count`)).data).toEqual([
      { bronze_check: "BRONZE_MATCH", row_count: 3n },
    ]);
    expect(resultToJSON(connection.query(`SELECT CASE WHEN
      (SELECT COUNT(*) FROM ${canonical}) = (SELECT COUNT(*) FROM ${legacy})
      AND NOT EXISTS (SELECT * FROM (SELECT * FROM ${canonical} LIMIT 20)
        EXCEPT ALL SELECT * FROM (SELECT * FROM ${legacy} LIMIT 20))
      AND NOT EXISTS (SELECT * FROM (SELECT * FROM ${legacy} LIMIT 20)
        EXCEPT ALL SELECT * FROM (SELECT * FROM ${canonical} LIMIT 20))
      THEN 'MATCH_GUS_BDL' ELSE 'MISMATCH' END AS bronze_check_gus_bdl,
      (SELECT COUNT(*) FROM ${canonical}) AS row_count`)).data).toEqual([
      { bronze_check_gus_bdl: "MATCH_GUS_BDL", row_count: 3n },
    ]);
    expect(fetchDriveFileBuffer).toHaveBeenCalledTimes(2);
  }, 30_000);

  it("queries every selected Parquet file, preserves duplicate rows and aligns columns by name", async () => {
    const first = file("SELECT 1 AS id, 'one' AS label");
    const second = file("SELECT 'one' AS label, 1 AS id UNION ALL SELECT 'two', 2");
    const result = await load([first, second]);
    expect(result.loadedFiles).toHaveLength(2);
    expect(rows(result.queryTarget)).toEqual([
      { id: 1, label: "one" },
      { id: 1, label: "one" },
      { id: 2, label: "two" },
    ]);
  }, 30000);

  it("keeps bronze and silver with identical filenames independently queryable", async () => {
    const bronze = await load([file("SELECT 10 AS id")]);
    const silver = await load(
      [file("SELECT 20 AS id", "data.parquet", "03_silver")],
      "rates",
      "03_silver"
    );
    expect(rows(bronze.queryTarget)).toEqual([{ id: 10 }]);
    expect(rows(silver.queryTarget)).toEqual([{ id: 20 }]);
  });

  it("loads a fact and its dimensions together so the explicit NBP join returns one row per fact", async () => {
    const quotes = file(
      `SELECT * FROM (
      VALUES
        ('A', DATE '2026-09-01', 'USD', 'PLN', 3.91::DOUBLE, NULL::DOUBLE, NULL::DOUBLE),
        ('A', DATE '2026-09-02', 'USD', 'PLN', 3.92::DOUBLE, NULL::DOUBLE, NULL::DOUBLE)
    ) AS source(source_table_key, effective_date, currency_key, quote_currency_key, mid, bid, ask)`,
      "fact_fx_quotes.parquet",
      "04_gold"
    );
    const currencies = file(
      "SELECT 'USD' AS currency_key, 'US dollar' AS source_currency_name UNION ALL SELECT 'PLN', NULL",
      "dim_currency.parquet",
      "04_gold"
    );
    const dates = file(
      "SELECT DATE '2026-09-01' AS date_key, 2026 AS calendar_year UNION ALL SELECT DATE '2026-09-02', 2026",
      "dim_date.parquet",
      "04_gold"
    );

    const result = await loadTablesIntoDuckDB(
      db,
      conn,
      [
        {
          datasetName: "fact_fx_quotes",
          tableFolderId: null,
          files: [quotes],
          layerName: "04_gold",
        },
        {
          datasetName: "dim_currency",
          tableFolderId: null,
          files: [currencies],
          layerName: "04_gold",
        },
        { datasetName: "dim_date", tableFolderId: null, files: [dates], layerName: "04_gold" },
      ],
      "fixture-token"
    );

    expect(result.queryTargets).toEqual([
      '"04_gold"."fact_fx_quotes"',
      '"04_gold"."dim_currency"',
      '"04_gold"."dim_date"',
    ]);
    expect(
      resultToJSON(
        connection.query(`SELECT
          quotes.effective_date,
          quotes.currency_key,
          currencies.source_currency_name,
          dates.calendar_year,
          quotes.mid
        FROM "04_gold"."fact_fx_quotes" AS quotes
        JOIN "04_gold"."dim_currency" AS currencies
          ON quotes.currency_key = currencies.currency_key
        JOIN "04_gold"."dim_date" AS dates
          ON quotes.effective_date = dates.date_key
        ORDER BY quotes.effective_date`)
      ).data
    ).toEqual([
      {
        effective_date: "2026-09-01",
        currency_key: "USD",
        source_currency_name: "US dollar",
        calendar_year: 2026,
        mid: 3.91,
      },
      {
        effective_date: "2026-09-02",
        currency_key: "USD",
        source_currency_name: "US dollar",
        calendar_year: 2026,
        mid: 3.92,
      },
    ]);
  }, 30000);

  it("reloads exactly the requested membership without sweeping in previously loaded files", async () => {
    const one = file("SELECT 1 AS id"),
      two = file("SELECT 2 AS id");
    await load([one, two]);
    const current = await load([two]);
    expect(rows(current.queryTarget)).toEqual([{ id: 2 }]);
  });

  it("does not replace existing views when a grouped load cannot download every selected table", async () => {
    const existing = await load([file("SELECT 71 AS id")]);
    const unavailable = { ...file("SELECT 72 AS id"), id: "unavailable" };

    await expect(
      loadTablesIntoDuckDB(
        db,
        conn,
        [
          {
            datasetName: "fact_fx_quotes",
            tableFolderId: null,
            files: [file("SELECT 73 AS id")],
            layerName: "04_gold",
          },
          {
            datasetName: "dim_currency",
            tableFolderId: null,
            files: [unavailable],
            layerName: "04_gold",
          },
        ],
        "fixture-token"
      )
    ).rejects.toThrow(/unavailable/);

    expect(rows(existing.queryTarget)).toEqual([{ id: 71 }]);
    expect(rows("active_layer")).toEqual([{ id: 71 }]);
  });

  it("does not publish grouped views when the caller's pinned session changes before publication", async () => {
    const existing = await load([file("SELECT 81 AS id")]);
    await expect(
      loadTablesIntoDuckDB(
        db,
        conn,
        [
          {
            datasetName: "fact_fx_quotes",
            tableFolderId: null,
            files: [file("SELECT 82 AS id")],
            layerName: "04_gold",
          },
        ],
        "fixture-token",
        undefined,
        () => {
          throw new Error("Pinned session changed");
        }
      )
    ).rejects.toThrow("Pinned session changed");

    expect(rows(existing.queryTarget)).toEqual([{ id: 81 }]);
    expect(rows("active_layer")).toEqual([{ id: 81 }]);
  });

  it("rejects unauthenticated, empty and fake entries without fabricating data", async () => {
    await expect(
      loadTableIntoDuckDB(db, conn, "absent", null, [], "", "02_bronze")
    ).rejects.toThrow(/Sign in/);
    await expect(load([], "absent")).rejects.toThrow(/No data files/);
    await expect(
      load(
        [{ id: "demo_file", name: "data.parquet", tableName: "absent", layer: "02_bronze" }],
        "absent"
      )
    ).rejects.toThrow(/real Drive file/);
    expect(() => rows('"02_bronze"."absent"')).toThrow();
    expect(fetchDriveFileBuffer).not.toHaveBeenCalled();
  });

  it("rejects a release file whose downloaded digest differs before registering it", async () => {
    const input = file("SELECT 14 AS id");
    const releaseFile = {
      ...input,
      sha256: "0".repeat(64),
      size: downloads.get(input.id)!.byteLength,
    };
    await expect(load([releaseFile])).rejects.toThrow(/SHA-256/);
    expect(db.registerFileBuffer).not.toHaveBeenCalled();
  });

  it("does not reuse a registered path when a release changes the declared digest", async () => {
    const input = file("SELECT 15 AS id");
    const size = downloads.get(input.id)!.byteLength;
    const correct = { ...input, size, sha256: await sha256Hex(downloads.get(input.id)!) };
    await load([correct]);
    await expect(load([{ ...correct, sha256: "0".repeat(64) }])).rejects.toThrow(/SHA-256/);
    expect(fetchDriveFileBuffer).toHaveBeenCalledTimes(2);
  });

  it("preserves the last loaded dataset when a later download fails", async () => {
    const good = await load([file("SELECT 7 AS id")]);
    const part = file("SELECT 8 AS id");
    const unavailable = { ...part, id: "unavailable" };
    await expect(load([part, unavailable])).rejects.toThrow(/unavailable/);
    expect(rows(good.queryTarget)).toEqual([{ id: 7 }]);
    expect(rows("active_layer")).toEqual([{ id: 7 }]);
  });

  it("rolls back invalid files and permits a later successful selection", async () => {
    const good = await load([file("SELECT 11 AS id")]);
    const bad = file("SELECT 12 AS id");
    downloads.set(bad.id, new TextEncoder().encode("not parquet"));
    await expect(load([bad])).rejects.toThrow();
    expect(rows(good.queryTarget)).toEqual([{ id: 11 }]);
    expect(rows("active_layer")).toEqual([{ id: 11 }]);
    const recovered = await load([file("SELECT 13 AS id")]);
    expect(rows(recovered.queryTarget)).toEqual([{ id: 13 }]);
  });

  it("previews a single file without replacing the whole-dataset view", async () => {
    const one = file("SELECT 1 AS id"),
      two = file("SELECT 2 AS id");
    const full = await load([one, two]);
    const single = await loadFileIntoDuckDB(db, conn, "rates", one, "fixture-token");
    expect(single.queryTarget).not.toBe(full.queryTarget);
    expect(rows(single.queryTarget)).toEqual([{ id: 1 }]);
    expect(rows(full.queryTarget)).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it("quotes SQL identifiers and literal paths including wildcard characters", async () => {
    const input = file("SELECT 42 AS id", "quo'te*[x].parquet");
    const result = await load([input], 'rates"quote', 'layer"quote');
    expect(rows(result.queryTarget)).toEqual([{ id: 42 }]);
  });

  it("lists data files when a dataset folder has not been expanded", async () => {
    const input = file("SELECT 9 AS id");
    vi.mocked(listDataFilesInFolder).mockResolvedValue([input]);
    const result = await loadTableIntoDuckDB(
      db,
      conn,
      "rates",
      "folder",
      [],
      "fixture-token",
      "02_bronze"
    );
    expect(listDataFilesInFolder).toHaveBeenCalledWith("folder", "fixture-token");
    expect(rows(result.queryTarget)).toEqual([{ id: 9 }]);
  });

  it("registers files again for a fresh database handle", async () => {
    const input = file("SELECT 1 AS id");
    await load([input]);
    const fresh = facade();
    await loadTableIntoDuckDB(
      fresh as unknown as duckdb.AsyncDuckDB,
      conn,
      "rates",
      null,
      [input],
      "fixture-token",
      "02_bronze"
    );
    expect(fresh.registerFileBuffer).toHaveBeenCalledTimes(1);
    expect(fetchDriveFileBuffer).toHaveBeenCalledTimes(2);
  });
});
