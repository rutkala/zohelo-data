#!/usr/bin/env node
/** Read-only feasibility probe. Receipts contain no Drive IDs, URLs, tokens, rows or payloads. */
import { createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { createServer as createViteServer } from "vite";
import { chromium } from "@playwright/test";

const [outputPath] = process.argv.slice(2);
const expectedSha = process.env.EXPECTED_GIT_SHA;
const rootId = process.env.DRIVE_ROOT_ID;
const productionRootId = "1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf";
const sourceId = "gus_dbw_retained_bronze";
const driveOrigin = "https://www.googleapis.com";
const maxMetadataBytes = 8 * 1024 * 1024;
const minPartBytes = 2 * 1024 * 1024;
const maxPartBytes = 8 * 1024 * 1024;
const maxBrowserRangeBytes = 2 * 1024 * 1024;
const maxBrowserRequestedBytes = 16 * 1024 * 1024;
const maxBrowserRequests = 48;
const maxNodeDriveRequests = 50;
const maxNodeMetadataBytes = 24 * 1024 * 1024;
const idPattern = /^[A-Za-z0-9_-]{1,255}$/;
const shaPattern = /^[a-f0-9]{64}$/;
if (!outputPath || !/^[a-f0-9]{40}$/.test(expectedSha ?? "") ||
    expectedSha !== process.env.GITHUB_SHA || rootId !== productionRootId)
  throw new Error("Reviewed main SHA and production Drive root are required");
for (const name of ["GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET", "GOOGLE_OAUTH_REFRESH_TOKEN"])
  if (!process.env[name]) throw new Error("OAuth configuration missing");

const receipt = { format_version: 1, status: "failed", expected_git_sha: expectedSha,
  read_only: false, transport_only: true, whole_file_sha_verified: false,
  manifest_hash_verified: false, index_hash_verified: false, file_metadata_matches_manifest: false,
  candidate_size_bytes: null, expected_rows: null, head_revision_present: false,
  revision_metadata_status: null, revision_keep_forever: null, revision_metadata_matches_file: false,
  node_drive_requests: 0, node_metadata_bytes: 0,
  post_metadata_unchanged: false,
  head: null, revision: null, browser_http: { head: {}, revision: {}, blocked: 0,
    requests: 0, unauthenticated_requests: 0, requested_range_bytes: 0,
    body_bytes: 0, unknown_body_count: 0, content_range_mismatches: 0 } };
let stage = "oauth";
let browser;
let vite;
let deadline;
let timedOut = false;
let attemptedWrite = false;
let token;

const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const validId = (id) => typeof id === "string" && idPattern.test(id);
const validSha = (value) => typeof value === "string" && shaPattern.test(value);
const validSize = (value, maximum) => Number.isSafeInteger(value) && value > 0 && value <= maximum;

async function boundedBytes(response, limit) {
  if (!response.ok || Number(response.headers.get("content-length")) > limit)
    throw new Error("bounded_metadata_response_failed");
  const reader = response.body?.getReader();
  if (!reader) throw new Error("metadata_body_unavailable");
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > limit) {
      await reader.cancel();
      throw new Error("metadata_limit_exceeded");
    }
    chunks.push(value);
  }
  receipt.node_metadata_bytes += total;
  if (receipt.node_metadata_bytes > maxNodeMetadataBytes)
    throw new Error("node_metadata_budget_exceeded");
  return Buffer.concat(chunks, total);
}

async function driveGet(url, maximum = 64 * 1024) {
  if (url.origin !== driveOrigin || url.pathname.includes("/upload/"))
    throw new Error("unscoped_drive_metadata_request");
  if (++receipt.node_drive_requests > maxNodeDriveRequests)
    throw new Error("node_drive_request_limit");
  const response = await fetch(url, { headers: { Authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(60_000) });
  return boundedBytes(response, maximum);
}

async function list(parentId) {
  if (!validId(parentId)) throw new Error("invalid_folder_identity");
  const files = [];
  const seen = new Set();
  let pageToken;
  do {
    if (seen.size >= 8) throw new Error("folder_page_limit");
    const url = new URL("/drive/v3/files", driveOrigin);
    url.searchParams.set("q", `'${parentId}' in parents and trashed=false`);
    url.searchParams.set("fields", "nextPageToken,incompleteSearch,files(id,name,mimeType,size,parents)");
    url.searchParams.set("pageSize", "1000");
    if (pageToken) url.searchParams.set("pageToken", pageToken);
    const page = JSON.parse(await driveGet(url, 512 * 1024));
    if (!Array.isArray(page.files) || page.incompleteSearch === true ||
        (page.nextPageToken !== undefined && typeof page.nextPageToken !== "string"))
      throw new Error("incomplete_folder_listing");
    files.push(...page.files);
    pageToken = page.nextPageToken || undefined;
    if (pageToken && seen.has(pageToken)) throw new Error("folder_page_cycle");
    if (pageToken) seen.add(pageToken);
  } while (pageToken);
  return files;
}

function one(items, name, folder = true) {
  const matches = items.filter((item) => item.name === name &&
    (item.mimeType === "application/vnd.google-apps.folder") === folder);
  if (matches.length !== 1 || !validId(matches[0].id)) throw new Error("missing_or_ambiguous_snapshot_path");
  return matches[0];
}

async function exactFile(id, declaredBytes, limit) {
  if (!validId(id) || !validSize(Number(declaredBytes), limit)) throw new Error("invalid_metadata_descriptor");
  const url = new URL(`/drive/v3/files/${encodeURIComponent(id)}`, driveOrigin);
  url.searchParams.set("alt", "media");
  const bytes = await driveGet(url, limit);
  if (bytes.length !== Number(declaredBytes)) throw new Error("metadata_size_mismatch");
  return bytes;
}

async function fileMetadata(id) {
  const url = new URL(`/drive/v3/files/${encodeURIComponent(id)}`, driveOrigin);
  url.searchParams.set("fields", "id,name,size,sha256Checksum,md5Checksum,headRevisionId,version,trashed,mimeType,capabilities(canDownload),parents");
  return JSON.parse(await driveGet(url));
}

try {
  const auth = await fetch("https://oauth2.googleapis.com/token", { method: "POST",
    signal: AbortSignal.timeout(60_000), body: new URLSearchParams({ grant_type: "refresh_token",
      client_id: process.env.GOOGLE_OAUTH_CLIENT_ID,
      client_secret: process.env.GOOGLE_OAUTH_CLIENT_SECRET,
      refresh_token: process.env.GOOGLE_OAUTH_REFRESH_TOKEN }) });
  if (!auth.ok) throw new Error("oauth_failed");
  token = (await auth.json()).access_token;
  if (typeof token !== "string" || !token) throw new Error("oauth_token_missing");

  stage = "snapshot_identity";
  if (one(await list("root"), "zohelo-data").id !== rootId)
    throw new Error("production_root_mismatch");
  const control = one(await list(rootId), "06_control");
  const campaigns = one(await list(control.id), "source_campaigns");
  const source = one(await list(campaigns.id), sourceId);
  const pointerFile = one(await list(source.id), "current-landing.json", false);
  const pointer = JSON.parse(await exactFile(pointerFile.id, pointerFile.size, 64 * 1024));
  if (pointer.format_version !== 1 || pointer.source_id !== sourceId ||
      !/^[a-f0-9-]{36}$/.test(pointer.snapshot_id ?? "") ||
      !validId(pointer.manifest_file_id) || !validSha(pointer.manifest_sha256) ||
      !validSize(pointer.manifest_size_bytes, 1024 * 1024) ||
      pointer.manifest_file_name !== `manifest-${pointer.snapshot_id}.json`)
    throw new Error("invalid_retained_pointer");

  stage = "manifest_hash";
  const manifestMeta = await fileMetadata(pointer.manifest_file_id);
  if (manifestMeta.name !== pointer.manifest_file_name ||
      Number(manifestMeta.size) !== pointer.manifest_size_bytes || manifestMeta.trashed === true)
    throw new Error("manifest_metadata_mismatch");
  const manifestBytes = await exactFile(pointer.manifest_file_id,
    pointer.manifest_size_bytes, 1024 * 1024);
  if (sha256(manifestBytes) !== pointer.manifest_sha256)
    throw new Error("manifest_hash_mismatch");
  receipt.manifest_hash_verified = true;
  const manifest = JSON.parse(manifestBytes);
  const observations = Array.isArray(manifest.datasets)
    ? manifest.datasets.find((item) => item.name === "observations") : null;
  if (manifest.kind !== "retained_bronze_snapshot" || manifest.source_id !== sourceId ||
      manifest.snapshot_id !== pointer.snapshot_id ||
      ![1, 2].includes(manifest.format_version) || manifest.status !== "validated" ||
      manifest.layer !== "02_bronze" || !validSha(manifest.inventory_sha256) ||
      manifest.indicator_count !== 1550 || !Number.isSafeInteger(manifest.published_indicator_count) ||
      !Number.isSafeInteger(manifest.pending_indicator_count) ||
      manifest.published_indicator_count + manifest.pending_indicator_count !== 1550 ||
      !observations || !Number.isSafeInteger(observations.row_count) || observations.row_count < 1 ||
      !manifest.indicator_index ||
      !validId(manifest.indicator_index.id) || !validSha(manifest.indicator_index.sha256) ||
      !validSize(manifest.indicator_index.size, maxMetadataBytes))
    throw new Error("invalid_retained_manifest");

  stage = "index_hash";
  const indexBytes = await exactFile(manifest.indicator_index.id,
    manifest.indicator_index.size, maxMetadataBytes);
  if (sha256(indexBytes) !== manifest.indicator_index.sha256)
    throw new Error("index_hash_mismatch");
  receipt.index_hash_verified = true;
  const index = JSON.parse(indexBytes);
  if (index.format_version !== manifest.format_version ||
      index.kind !== "retained_bronze_indicator_index" || index.source_id !== sourceId ||
      index.inventory_sha256 !== manifest.inventory_sha256 || index.indicator_count !== 1550 ||
      index.published_indicator_count !== manifest.published_indicator_count ||
      index.pending_indicator_count !== manifest.pending_indicator_count ||
      !Array.isArray(index.indicators) || index.indicators.length !== 1550)
    throw new Error("invalid_retained_index");
  const published = index.indicators.filter((item) => item.status === "published");
  const totalRows = index.indicators.reduce((sum, item) => sum + item.row_count, 0);
  const pending = index.indicators.filter((item) => item.status === "pending");
  if (published.length !== manifest.published_indicator_count ||
      pending.length !== manifest.pending_indicator_count ||
      totalRows !== observations.row_count || !Number.isSafeInteger(totalRows) ||
      index.indicators.some((item) => !Number.isSafeInteger(item.indicator_id) ||
        !Number.isSafeInteger(item.row_count) || item.row_count < 0 ||
        !["pending", "published"].includes(item.status) ||
        !Array.isArray(item.parts) ||
        item.parts.reduce((sum, part) => sum + part.row_count, 0) !== item.row_count ||
        (item.status === "published") !== (item.parts.length > 0)))
    throw new Error("retained_index_totals_mismatch");
  const candidates = index.indicators.flatMap((indicator) => {
    if (indicator.status !== "published" || !Number.isSafeInteger(indicator.indicator_id) ||
        !Array.isArray(indicator.parts)) return [];
    return indicator.parts.filter((part) => validId(part.id) && validSha(part.sha256) &&
      validSize(part.size, maxPartBytes) && part.size >= minPartBytes &&
      Number.isSafeInteger(part.row_count) && part.row_count > 0 &&
      Number.isSafeInteger(part.part) && part.part > 0)
      .map((part) => ({ ...part, indicatorId: indicator.indicator_id }));
  }).sort((a, b) => a.size - b.size || a.indicatorId - b.indicatorId || a.part - b.part);
  if (!candidates.length) throw new Error("no_bounded_published_part");
  const selected = candidates[0];
  receipt.candidate_size_bytes = selected.size;
  receipt.expected_rows = selected.row_count;

  stage = "file_metadata";
  const selectedMeta = await fileMetadata(selected.id);
  if (selectedMeta.id !== selected.id || selectedMeta.name !== selected.name ||
      Number(selectedMeta.size) !== selected.size ||
      selectedMeta.sha256Checksum?.toLowerCase() !== selected.sha256 ||
      selectedMeta.trashed === true || selectedMeta.capabilities?.canDownload === false)
    throw new Error("selected_file_metadata_mismatch");
  receipt.file_metadata_matches_manifest = true;
  const headRevisionId = validId(selectedMeta.headRevisionId) ? selectedMeta.headRevisionId : null;
  receipt.head_revision_present = !!headRevisionId;
  if (headRevisionId) {
    const revisionMetaUrl = new URL(`/drive/v3/files/${encodeURIComponent(selected.id)}/revisions/${encodeURIComponent(headRevisionId)}`, driveOrigin);
    revisionMetaUrl.searchParams.set("fields", "id,keepForever,md5Checksum,size,mimeType");
    if (++receipt.node_drive_requests > maxNodeDriveRequests)
      throw new Error("node_drive_request_limit");
    const revisionResponse = await fetch(revisionMetaUrl, { headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(60_000) });
    receipt.revision_metadata_status = revisionResponse.status;
    if (revisionResponse.ok) {
      const revisionMeta = JSON.parse(await boundedBytes(revisionResponse, 64 * 1024));
      if (revisionMeta.id !== headRevisionId) throw new Error("revision_identity_mismatch");
      receipt.revision_keep_forever = revisionMeta.keepForever === true;
      receipt.revision_metadata_matches_file = Number(revisionMeta.size) === selected.size &&
        (!selectedMeta.md5Checksum || revisionMeta.md5Checksum === selectedMeta.md5Checksum);
      if (!receipt.revision_metadata_matches_file) throw new Error("revision_file_mismatch");
    } else {
      await revisionResponse.body?.cancel();
    }
  }

  stage = "browser_range";
  const headUrl = new URL(`/drive/v3/files/${encodeURIComponent(selected.id)}`, driveOrigin);
  headUrl.searchParams.set("alt", "media");
  const revisionUrl = headRevisionId
    ? new URL(`/drive/v3/files/${encodeURIComponent(selected.id)}/revisions/${encodeURIComponent(headRevisionId)}?alt=media`, driveOrigin)
    : null;
  const allowed = new Map([[headUrl.href, "head"]]);
  if (revisionUrl) allowed.set(revisionUrl.href, "revision");
  const responses = [];
  let fullGetSeen = false;
  vite = await createViteServer({ configFile: false,
    optimizeDeps: { entries: ["scripts/fixtures/drive-lazy-probe.html"] },
    server: { host: "127.0.0.1", port: 0, strictPort: false },
    clearScreen: false, logLevel: "error" });
  await vite.listen();
  const fixtureOrigin = new URL(vite.resolvedUrls.local[0]).origin;
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ serviceWorkers: "block" });
  await context.route("**/*", (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (url.origin === fixtureOrigin) {
      if (request.headers().authorization) {
        receipt.browser_http.blocked++;
        return route.abort("blockedbyclient");
      }
      return route.continue();
    }
    const kind = allowed.get(url.href);
    const method = request.method();
    if (!kind || !["GET", "HEAD", "OPTIONS"].includes(method)) {
      receipt.browser_http.blocked++;
      if (method !== "GET" && method !== "HEAD" && method !== "OPTIONS") attemptedWrite = true;
      return route.abort("blockedbyclient");
    }
    if (++receipt.browser_http.requests > maxBrowserRequests || fullGetSeen) {
      receipt.browser_http.blocked++;
      return route.abort("blockedbyclient");
    }
    if (method !== "OPTIONS" && request.headers().authorization !== `Bearer ${token}`)
      receipt.browser_http.unauthenticated_requests++;
    if (method === "GET") {
      const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers().range ?? "");
      if (!match || Number(match[2]) < Number(match[1]) ||
          Number(match[2]) >= selected.size ||
          Number(match[2]) - Number(match[1]) + 1 > maxBrowserRangeBytes) {
        receipt.browser_http.blocked++;
        return route.abort("blockedbyclient");
      }
      receipt.browser_http.requested_range_bytes += Number(match[2]) - Number(match[1]) + 1;
      if (receipt.browser_http.requested_range_bytes > maxBrowserRequestedBytes) {
        receipt.browser_http.blocked++;
        return route.abort("blockedbyclient");
      }
    }
    return route.continue();
  });
  const page = await context.newPage();
  page.on("response", (response) => {
    const kind = allowed.get(response.url());
    if (!kind) return;
    const method = response.request().method();
    const key = receipt.browser_http[kind];
    const label = `${method.toLowerCase()}_${response.status()}`;
    key[label] = (key[label] ?? 0) + 1;
    if (method === "GET" && response.status() === 200) fullGetSeen = true;
    let expectedLength = null;
    if (method === "GET" && response.status() === 206) {
      const range = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(response.headers()["content-range"] ?? "");
      const requested = /^bytes=(\d+)-(\d+)$/.exec(response.request().headers().range ?? "");
      if (!range || Number(range[3]) !== selected.size ||
          Number(range[2]) < Number(range[1]) || !requested ||
          range[1] !== requested[1] || range[2] !== requested[2])
        receipt.browser_http.content_range_mismatches++;
      else expectedLength = Number(range[2]) - Number(range[1]) + 1;
    }
    // A 200 may be the entire selected file; record and fail without copying it
    // into the receipt process. The selected file itself is capped at 8 MiB.
    if (method === "GET" && response.status() === 206) {
      responses.push(response.body().then((body) => {
        if (expectedLength === null || body.byteLength !== expectedLength)
          receipt.browser_http.content_range_mismatches++;
        receipt.browser_http.body_bytes += body.byteLength;
        key.body_bytes = (key.body_bytes ?? 0) + body.byteLength;
      }).catch(() => { receipt.browser_http.unknown_body_count++; }));
    }
  });
  await page.goto(`${fixtureOrigin}/scripts/fixtures/drive-lazy-probe.html`);
  await page.waitForFunction(() => window.driveLazyProbeReady === true, undefined, { timeout: 60_000 });
  deadline = setTimeout(() => { timedOut = true; void page.close(); }, 120_000);
  const result = await page.evaluate((input) => window.runDriveLazyProbe(input), {
    scope: `${driveOrigin}/drive/v3/files/${encodeURIComponent(selected.id)}`,
    headUrl: headUrl.href, revisionUrl: revisionUrl?.href ?? null, token,
    expectedRows: selected.row_count, indicatorId: selected.indicatorId,
  });
  clearTimeout(deadline);
  deadline = null;
  await Promise.all(responses);
  receipt.head = result.head;
  receipt.revision = result.revision;
  const after = await fileMetadata(selected.id);
  receipt.post_metadata_unchanged = after.id === selectedMeta.id &&
    after.headRevisionId === selectedMeta.headRevisionId &&
    after.version === selectedMeta.version && Number(after.size) === selected.size &&
    after.sha256Checksum?.toLowerCase() === selected.sha256 &&
    after.md5Checksum === selectedMeta.md5Checksum && after.trashed !== true;
  if (!receipt.post_metadata_unchanged) throw new Error("file_changed_during_probe");
  if (attemptedWrite || receipt.browser_http.blocked ||
      receipt.browser_http.unauthenticated_requests || fullGetSeen ||
      receipt.browser_http.content_range_mismatches ||
      receipt.browser_http.unknown_body_count || result.head?.status !== "verified" ||
      !(receipt.browser_http.head.get_206 > 0) ||
      receipt.browser_http.head.body_bytes >= selected.size)
    throw new Error("head_range_not_verified");
  if (result.revision?.status === "verified" &&
      (!(receipt.browser_http.revision.get_206 > 0) ||
       receipt.browser_http.revision.body_bytes >= selected.size))
    throw new Error("revision_range_not_verified");
  receipt.status = "observed";
} catch (error) {
  receipt.failure_stage = stage;
  receipt.failure_category = timedOut ? "browser_deadline" :
    error instanceof Error && /^[a-z_]+$/.test(error.message) ? error.message : "unexpected_error";
  process.exitCode = 1;
} finally {
  receipt.read_only = !attemptedWrite;
  if (deadline) clearTimeout(deadline);
  await browser?.close().catch(() => undefined);
  await vite?.close().catch(() => undefined);
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${JSON.stringify(receipt, null, 2)}\n`);
  console.log(JSON.stringify({ status: receipt.status, stage: receipt.failure_stage ?? "complete",
    category: receipt.failure_category ?? "none", head: receipt.head?.status ?? "unavailable",
    revision: receipt.revision?.status ?? "unavailable" }));
}
