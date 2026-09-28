/**
 * Disposable browser capability probe for the exact installed DuckDB-WASM.
 * Synthetic Parquet and a synthetic bearer token only. No Drive credentials,
 * production requests, application loader changes, or published data writes.
 */
import { createServer as createHttpServer } from "node:http";
import { createRequire } from "node:module";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { createServer as createViteServer } from "vite";
import { chromium } from "@playwright/test";

const require = createRequire(import.meta.url);
const duckdbWasmVersion = JSON.parse(readFileSync(new URL("../node_modules/@duckdb/duckdb-wasm/package.json", import.meta.url))).version;
const bearer = "fixture-only-bearer";
const rowCount = 250_000;
const multiFileRows = 50_000;
const { parquet, multiParquets } = await (async () => {
  const directory = mkdtempSync(join(process.cwd(), "node_modules", ".lazy-http-parquet-"));
  const parquetPath = join(directory, "fixture.parquet");
  try {
    const duckdb = require("@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs");
    const db = await duckdb.createDuckDB({
      mvp: { mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm"), mainWorker: null },
      eh: { mainModule: require.resolve("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm"), mainWorker: null },
    }, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
    await db.instantiate(() => {});
    const conn = db.connect();
    try {
      conn.query(`COPY (SELECT range AS id, md5(range::VARCHAR) AS payload FROM range(${rowCount})) TO '${parquetPath.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE 10000)`);
      const parquet = Buffer.from(db.copyFileToBuffer(parquetPath));
      const multiParquets = [];
      for (let indicator = 1; indicator <= 5; indicator++) {
        const partPath = join(directory, `part_${indicator}.parquet`);
        conn.query(`COPY (SELECT ${indicator} AS indicator_id, range AS row_id, md5(range::VARCHAR) AS payload FROM range(${multiFileRows})) TO '${partPath.replaceAll("'", "''")}' (FORMAT PARQUET, ROW_GROUP_SIZE 10000)`);
        multiParquets.push(Buffer.from(db.copyFileToBuffer(partPath)));
      }
      return { parquet, multiParquets };
    } finally {
      conn.close();
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
})();

const events = [];
const extensionRequests = [];
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

    const multiMatch = /^\/protected\/multi\/(limit|count|filter)\/part_([1-5])\.parquet$/.exec(path);
    if (path.startsWith("/protected/multi/") && !multiMatch) return send(404);
    const file = multiMatch ? multiParquets[Number(multiMatch[2]) - 1] : parquet;
    const range = request.headers.range;
    if (range) {
      const match = /^bytes=(\d+)-(\d*)$/.exec(range);
      if (!match) return send(416);
      const start = Number(match[1]);
      const end = match[2] ? Math.min(Number(match[2]), file.length - 1) : file.length - 1;
      if (start > end || start >= file.length) return send(416);
      const body = file.subarray(start, end + 1);
      return send(206, { "content-type": "application/octet-stream", "content-length": body.length,
        "content-range": `bytes ${start}-${end}/${file.length}` }, body);
    }
    return send(200, { "content-type": "application/octet-stream", "content-length": file.length }, file);
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
const extensionHeaderChecks = [];
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
  const context = await browser.newContext();
  context.on("request", (request) => {
    const url = new URL(request.url());
    if (url.origin !== "https://extensions.duckdb.org") return;
    const observed = { origin: url.origin, path: url.pathname, method: request.method(),
      authorizationPresent: null, status: null };
    extensionRequests.push(observed);
    extensionHeaderChecks.push(request.allHeaders().then((headers) => {
      observed.authorizationPresent = !!headers.authorization;
    }));
  });
  context.on("response", (response) => {
    const url = new URL(response.url());
    if (url.origin !== "https://extensions.duckdb.org") return;
    const pending = extensionRequests.findLast((request) => request.path === url.pathname && request.status === null);
    if (pending) pending.status = response.status();
  });
  const page = await context.newPage();
  stage = "loading_browser_worker";
  await page.goto(`${appUrl}scripts/fixtures/lazy-http-probe.html`);
  await page.waitForFunction(() => window.lazyHttpProbeReady === true, undefined, { timeout: 60_000 });
  const phaseMarks = [];
  await page.exposeFunction("markLazyPhase", (name) => phaseMarks.push({ name, eventIndex: events.length }));
  stage = "reading_parquet";
  const result = await page.evaluate((input) => window.runLazyHttpProbe(input), { protectedBase, publicUrl, bearer });
  await Promise.all(extensionHeaderChecks);

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
  if (summaries.filter.bytes >= summaries.projection.bytes / 2)
    throw new Error("filter_did_not_prune_row_groups");
  stage = "checking_multi_file_ranges";
  const multiTotalBytes = multiParquets.reduce((total, file) => total + file.length, 0);
  const multiPhases = {};
  for (let index = 0; index < phaseMarks.length - 1; index++) {
    const start = phaseMarks[index];
    const end = phaseMarks[index + 1];
    const subset = events.slice(start.eventIndex, end.eventIndex)
      .filter((event) => event.path.startsWith("/protected/multi/"));
    const ranged = subset.filter((event) => event.method === "GET" && event.status === 206);
    if (subset.some((event) => event.method === "GET" && event.status !== 206) ||
        subset.some((event) => event.method === "OPTIONS" && event.status !== 204) ||
        subset.some((event) => event.method !== "OPTIONS" && (event.status >= 400 || event.authorization !== `Bearer ${bearer}`)))
      throw new Error("multi_file_auth_or_range_failed");
    multiPhases[start.name] = {
      requests: subset.length,
      heads: subset.filter((event) => event.method === "HEAD").length,
      rangeGets: ranged.length,
      bytes: ranged.reduce((total, event) => total + event.bytes, 0),
      filesRequested: new Set(subset.map((event) => event.path)).size,
    };
  }
  const multiScenarios = Object.fromEntries(["limit", "count", "filter"].map((name) => {
    const bind = multiPhases[`${name}_bind`];
    const query = multiPhases[`${name}_query`];
    if (!bind || !query || bind.rangeGets + query.rangeGets === 0)
      throw new Error(`${name}_missing_multi_file_ranges`);
    return [name, { bind, query, totalBytes: bind.bytes + query.bytes }];
  }));
  if (multiScenarios.limit.totalBytes >= multiTotalBytes / 2)
    throw new Error("multi_file_limit_not_materially_lazy");
  if (result.multiLimit !== 1000 || result.multiCount !== multiFileRows * 5 ||
      result.multiFiltered !== multiFileRows * 2)
    throw new Error("multi_file_query_result_mismatch");
  if (!extensionRequests.some((request) => request.path.endsWith("/httpfs.duckdb_extension.wasm")) ||
      extensionRequests.some((request) => request.authorizationPresent))
    throw new Error("httpfs_extension_request_unverified");
  console.log(JSON.stringify({ result: "pass", package: `@duckdb/duckdb-wasm@${duckdbWasmVersion}`,
    parquetBytes: parquet.length, rows: rowCount, protected: summaries,
    extensionRequests,
    multiFile: { files: multiParquets.length, totalBytes: multiTotalBytes, rows: multiFileRows * 5,
      scenarios: multiScenarios },
    unauthenticatedStatuses: protectedEvents.filter((event) => event.path.endsWith("unauthenticated.parquet")).map((event) => event.status),
    public: { requests: publicEvents.length, bearerHeaders: publicEvents.filter((event) => event.authorization).length } }));
} catch (error) {
  console.error(JSON.stringify({ result: "fail", stage, category: error instanceof Error ? error.message.replaceAll(bearer, "[fixture-token]").replaceAll(/https?:\/\/[^\s]+/g, "[url]").slice(0, 160) : "unknown",
    extensionRequests,
    requests: events.map(({ origin, method, status, range, bytes }) => ({ origin, method, status, ranged: !!range, bytes })) }));
  process.exitCode = 1;
} finally {
  await browser?.close();
  await vite?.close();
  await Promise.all([protectedServer, publicServer].map((server) => new Promise((resolve) => server.close(resolve))));
}
