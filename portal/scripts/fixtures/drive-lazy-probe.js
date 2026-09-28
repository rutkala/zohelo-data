import * as duckdb from "@duckdb/duckdb-wasm";
import wasmUrl from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import workerUrl from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

const quote = (value) => `'${value.replaceAll("'", "''")}'`;

window.runDriveLazyProbe = async ({ scope, headUrl, revisionUrl, token, expectedRows, indicatorId }) => {
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  let connection;
  const outcome = { head: null, revision: null };
  try {
    await db.instantiate(wasmUrl);
    await db.open({ filesystem: { allowFullHTTPReads: false, forceFullHTTPReads: false } });
    connection = await db.connect();
    await window.markDriveProbePhase("load_httpfs");
    await connection.query("LOAD httpfs");
    await window.markDriveProbePhase("configure_http");
    await connection.query("SET auto_fallback_to_full_download=false");
    await connection.query("SET force_download=false");
    await connection.query(
      `CREATE TEMPORARY SECRET drive_range_probe (TYPE http, SCOPE ${quote(scope)}, EXTRA_HTTP_HEADERS MAP {'Authorization': ${quote(`Bearer ${token}`)}})`
    );
    const probe = async (url) => {
      if (!url) return { status: "revision_unavailable" };
      try {
        const count = await connection.query(`SELECT count(*) AS n FROM read_parquet(${quote(url)})`);
        if (Number(count.toArray()[0].n) !== expectedRows) return { status: "count_mismatch" };
        const preview = await connection.query(
          `SELECT indicator_id FROM read_parquet(${quote(url)}) LIMIT 5`
        );
        const rows = preview.toArray();
        if (!rows.length || rows.length > 5 || rows.some((row) => Number(row.indicator_id) !== indicatorId))
          return { status: "preview_mismatch" };
        return { status: "verified", previewRows: rows.length };
      } catch {
        return { status: "query_error" };
      }
    };
    await window.markDriveProbePhase("head_query");
    outcome.head = await probe(headUrl);
    await window.markDriveProbePhase("revision_query");
    outcome.revision = await probe(revisionUrl);
    return outcome;
  } finally {
    if (connection) {
      await connection.query("DROP SECRET IF EXISTS drive_range_probe").catch(() => undefined);
      await connection.close();
    }
    await db.terminate();
  }
};

window.driveLazyProbeReady = true;
