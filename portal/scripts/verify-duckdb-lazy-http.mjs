/**
 * Disposable browser capability probe for the exact installed DuckDB-WASM.
 * Synthetic Parquet and a synthetic bearer token only. No Drive credentials,
 * production requests, application loader changes, or published data writes.
 */
import { createServer as createHttpServer } from "node:http";
import { createRequire } from "node:module";
import { createServer as createViteServer } from "vite";
import { chromium } from "@playwright/test";

const require = createRequire(import.meta.url);
const bearer = "fixture-only-bearer";
const rowCount = 250_000;
const parquet = await (async () => {
  const duckdb = require("@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs");
  const db = await duckdb.createDuckDB({
    mvp: { mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm"), mainWorker: null },
    eh: { mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm"), mainWorker: null },
  }, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate(() => {});
  const conn = db.connect();
  try {
    conn.query(`COPY (SELECT range AS id, md5(range::VARCHAR) AS payload FROM range(${rowCount})) TO '/lazy-http-fixture.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 10000)`);
    return Buffer.from(db.copyFileToBuffer("/lazy-http-fixture.parquet"));
  } finally {
    conn.close();
  }
})();

const events = [];
const cors = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, HEAD, OPTIONS",
  "access-control-allow-headers": "authorization, range, content-type",
  "access-control-expose-headers": "accept-ranges, content-length, content-range, etag",
  "accept-ranges": "bytes",
  etag: '"synthetic-parquet-v1"',
};

function dataServer(protectedOrigin) {
  return createHttpServer((request, response) => {
    const path = new URL(request.url ?? "/", "http://fixture.invalid").pathname;
    const authorized = request.headers.authorization === `Bearer ${bearer}`;
    const event = { origin: protectedOrigin ? "protected" : "public", path, method: request.method, status: 0,
      range: request.headers.range ?? null, authorization: request.headers.authorization ?? null,
      requestedHeaders: request.headers["access-control-request-headers"] ?? null, bytes: 0 };
    events.push(event);
    const send = (status, headers = {}, body) => {
      event.status = status;
      event.bytes = request.method === "HEAD" || !body ? 0 : body.length;
      response.writeHead(status, { ...cors, ...headers });
      response.end(request.method === "HEAD" ? undefined : body);
    };
    if (request.method === "OPTIONS") return send(204);
    if (!path.endsWith(".parquet")) return send(404);
    if (protectedOrigin && !authorized) return send(401);
    if (!protectedOrigin && event.authorization) return send(403);

    const range = request.headers.range;
    if (range) {
      const match = /^bytes=(\d+)-(\d*)$/.exec(range);
      if (!match) return send(416);
      const start = Number(match[1]);
      const end = match[2] ? Math.min(Number(match[2]), parquet.length - 1) : parquet.length - 1;
      if (start > end || start >= parquet.length) return send(416);
      const body = parquet.subarray(start, end + 1);
      return send(206, { "content-type": "application/octet-stream", "content-length": body.length,
        "content-range": `bytes ${start}-${end}/${parquet.length}` }, body);
    }
    return send(200, { "content-type": "application/octet-stream", "content-length": parquet.length }, parquet);
  });
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return `http://127.0.0.1:${server.address().port}/`;
}

const protectedServer = dataServer(true);
const publicServer = dataServer(false);
let vite;
let browser;
let stage = "starting";
try {
  const protectedBase = `${await listen(protectedServer)}protected/`;
  const publicUrl = `${await listen(publicServer)}public.parquet`;
  vite = await createViteServer({ configFile: false,
    optimizeDeps: { entries: ["scripts/fixtures/lazy-http-probe.html"] },
    server: { host: "127.0.0.1", port: 0, strictPort: false },
    clearScreen: false, logLevel: "error" });
  await vite.listen();
  const appUrl = vite.resolvedUrls.local[0];
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  stage = "loading_browser_worker";
  await page.goto(`${appUrl}scripts/fixtures/lazy-http-probe.html`);
  await page.waitForFunction(() => window.lazyHttpProbeReady === true, undefined, { timeout: 60_000 });
  stage = "reading_parquet";
  const result = await page.evaluate((input) => window.runLazyHttpProbe(input), { protectedBase, publicUrl, bearer });

  const protectedEvents = events.filter((event) => event.origin === "protected");
  const publicEvents = events.filter((event) => event.origin === "public");
  const scenario = (name) => {
    const subset = protectedEvents.filter((event) => event.path === `/protected/${name}.parquet`);
    const reads = subset.filter((event) => event.method === "GET" && event.status === 206);
    const received = reads.reduce((total, event) => total + event.bytes, 0);
    if (!reads.length || received >= parquet.length / 2) throw new Error(`${name}_not_materially_lazy`);
    if (subset.some((event) => event.method === "GET" && event.status === 200))
      throw new Error(`${name}_full_get`);
    if (subset.some((event) => event.method !== "OPTIONS" && (event.status >= 400 || event.authorization !== `Bearer ${bearer}`)) ||
        subset.some((event) => event.method === "OPTIONS" && event.status !== 204))
      throw new Error(`${name}_authentication_failed`);
    return { requests: subset.length, rangeGets: reads.length, bytes: received };
  };
  if (!result.unauthenticatedRejected || !protectedEvents.some((event) => event.path.endsWith("unauthenticated.parquet") && event.status === 401))
    throw new Error("missing_auth_not_rejected");
  if (!result.siblingRejected || !protectedEvents.some((event) => event.path === "/sibling.parquet" && event.status === 401) ||
      protectedEvents.some((event) => event.path === "/sibling.parquet" && event.authorization))
    throw new Error("bearer_leaked_to_sibling_path");
  if (!result.afterDropRejected || !protectedEvents.some((event) => event.path.endsWith("after-drop.parquet") && event.status === 401) ||
      protectedEvents.some((event) => event.path.endsWith("after-drop.parquet") && event.authorization))
    throw new Error("bearer_survived_drop_secret");
  if (!protectedEvents.some((event) => event.method === "OPTIONS" && event.status === 204 &&
      event.requestedHeaders?.toLowerCase().includes("authorization")))
    throw new Error("missing_cors_preflight");
  if (result.count !== rowCount || result.filtered !== 11 || result.unscoped !== rowCount ||
      result.projection !== ((rowCount - 1) * rowCount) / 2)
    throw new Error("query_result_mismatch");
  if (!publicEvents.some((event) => event.method === "GET" && event.status === 206) ||
      publicEvents.some((event) => event.authorization))
    throw new Error("bearer_leaked_to_public_origin");
  const summaries = Object.fromEntries(["count", "projection", "filter"].map((name) => [name, scenario(name)]));
  console.log(JSON.stringify({ result: "pass", package: "@duckdb/duckdb-wasm@1.33.1-dev64.0",
    parquetBytes: parquet.length, rows: rowCount, protected: summaries,
    unauthenticatedStatuses: protectedEvents.filter((event) => event.path.endsWith("unauthenticated.parquet")).map((event) => event.status),
    public: { requests: publicEvents.length, bearerHeaders: publicEvents.filter((event) => event.authorization).length } }));
} catch (error) {
  console.error(JSON.stringify({ result: "fail", stage, category: error instanceof Error ? error.message.replaceAll(bearer, "[fixture-token]").replaceAll(/https?:\/\/[^\s]+/g, "[url]").slice(0, 160) : "unknown",
    requests: events.map(({ origin, method, status, range, bytes }) => ({ origin, method, status, ranged: !!range, bytes })) }));
  process.exitCode = 1;
} finally {
  await browser?.close();
  await vite?.close();
  await Promise.all([protectedServer, publicServer].map((server) => new Promise((resolve) => server.close(resolve))));
}
