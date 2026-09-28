import * as duckdb from "@duckdb/duckdb-wasm";
import wasmUrl from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import workerUrl from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

const quote = (value) => `'${value.replaceAll("'", "''")}'`;

window.runLazyHttpProbe = async ({ protectedBase, publicUrl, bearer }) => {
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  let connection;
  try {
    await db.instantiate(wasmUrl);
    await db.open({ filesystem: { allowFullHTTPReads: false, forceFullHTTPReads: false } });
    connection = await db.connect();
    await connection.query("LOAD httpfs");
    await connection.query("SET auto_fallback_to_full_download=false");
    await connection.query("SET force_download=false");

    // Before the secret exists, the protected endpoint must reject the engine.
    let unauthenticatedRejected = false;
    try {
      await connection.query(`SELECT count(*) FROM read_parquet(${quote(`${protectedBase}unauthenticated.parquet`)})`);
    } catch {
      unauthenticatedRejected = true;
    }
    if (!unauthenticatedRejected) throw new Error("unauthenticated_read_succeeded");

    await connection.query(
      `CREATE TEMPORARY SECRET fixture_http (TYPE http, SCOPE ${quote(protectedBase)}, EXTRA_HTTP_HEADERS MAP {'Authorization': ${quote(`Bearer ${bearer}`)}})`
    );

    const count = await connection.query(
      `SELECT count(*) AS n FROM read_parquet(${quote(`${protectedBase}count.parquet`)})`
    );
    const projection = await connection.query(
      `SELECT sum(id) AS n FROM read_parquet(${quote(`${protectedBase}projection.parquet`)})`
    );
    const filtered = await connection.query(
      `SELECT count(*) AS n FROM read_parquet(${quote(`${protectedBase}filter.parquet`)}) WHERE id BETWEEN 100 AND 110`
    );
    const unscoped = await connection.query(
      `SELECT count(*) AS n FROM read_parquet(${quote(publicUrl)})`
    );
    let siblingRejected = false;
    try {
      await connection.query(`SELECT count(*) FROM read_parquet(${quote(`${new URL(protectedBase).origin}/sibling.parquet`)})`);
    } catch {
      siblingRejected = true;
    }
    if (!siblingRejected) throw new Error("sibling_read_succeeded");
    await connection.query("DROP SECRET fixture_http");
    let afterDropRejected = false;
    try {
      await connection.query(`SELECT count(*) FROM read_parquet(${quote(`${protectedBase}after-drop.parquet`)})`);
    } catch {
      afterDropRejected = true;
    }
    if (!afterDropRejected) throw new Error("after_drop_read_succeeded");

    const scalar = (table) => Number(table.toArray()[0].n);
    return {
      unauthenticatedRejected,
      count: scalar(count),
      projection: scalar(projection),
      filtered: scalar(filtered),
      unscoped: scalar(unscoped),
      siblingRejected,
      afterDropRejected,
    };
  } finally {
    await connection?.close();
    await db.terminate();
  }
};

window.lazyHttpProbeReady = true;
