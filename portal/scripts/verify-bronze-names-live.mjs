#!/usr/bin/env node
/** Manual, read-only check of deployed Bronze SQL names. No payloads or credentials enter the receipt. */
import { chromium } from "@playwright/test";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

const [outputPath] = process.argv.slice(2);
const expectedSha = process.env.EXPECTED_DEPLOYED_SHA;
const rootId = process.env.DRIVE_ROOT_ID;
const origin = "https://data.zohelo.com";
if (!outputPath || !/^[a-f0-9]{40}$/.test(expectedSha ?? "") ||
    rootId !== "1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf" || expectedSha !== process.env.GITHUB_SHA)
  throw new Error("Exact reviewed deployment SHA and production Drive root are required");
for (const key of ["GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET", "GOOGLE_OAUTH_REFRESH_TOKEN"])
  if (!process.env[key]) throw new Error("OAuth configuration missing");

const result = { format_version: 1, status: "failed", expected_deployed_sha: expectedSha,
  dbw_modeled_pointer: null, retained_dbw_snapshot_id: null,
  dbw_whole_table_verified: false, checked: [], read_only: false };
let stage = "oauth";
let browser;
try {
  const response = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST", signal: AbortSignal.timeout(60000),
    body: new URLSearchParams({ grant_type: "refresh_token",
      client_id: process.env.GOOGLE_OAUTH_CLIENT_ID,
      client_secret: process.env.GOOGLE_OAUTH_CLIENT_SECRET,
      refresh_token: process.env.GOOGLE_OAUTH_REFRESH_TOKEN }),
  });
  if (!response.ok) throw new Error("OAuth refresh failed");
  const token = (await response.json()).access_token;
  if (typeof token !== "string" || !token) throw new Error("OAuth access token missing");
  stage = "deployed_sha";
  const build = await fetch(`${origin}/portal-build.json`, { signal: AbortSignal.timeout(30000) });
  if (!build.ok || (await build.json()).git_commit !== expectedSha)
    throw new Error("Deployed code SHA mismatch");

  stage = "dbw_pointer_inventory";
  const list = async (parent) => {
    const files = [];
    let pageToken;
    const seen = new Set();
    do {
      const url = new URL("https://www.googleapis.com/drive/v3/files");
      url.searchParams.set("q", `'${parent}' in parents and trashed=false`);
      url.searchParams.set("fields", "nextPageToken,incompleteSearch,files(id,name,mimeType,parents)");
      url.searchParams.set("pageSize", "1000");
      if (pageToken) url.searchParams.set("pageToken", pageToken);
      const pageResponse = await fetch(url, { headers: { Authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(60000) });
      if (!pageResponse.ok) throw new Error("Drive pointer inventory failed");
      const page = await pageResponse.json();
      if (!Array.isArray(page.files) || page.incompleteSearch === true ||
          (page.nextPageToken !== undefined && typeof page.nextPageToken !== "string"))
        throw new Error("Drive pointer inventory incomplete");
      files.push(...page.files);
      pageToken = page.nextPageToken;
      if (pageToken && seen.has(pageToken)) throw new Error("Drive pointer pagination cycle");
      if (pageToken) seen.add(pageToken);
    } while (pageToken);
    return files;
  };
  const one = (items, name, folder = true) => {
    const found = items.filter((item) => item.name === name &&
      (item.mimeType === "application/vnd.google-apps.folder") === folder);
    if (found.length > 1) throw new Error("Ambiguous Drive pointer path");
    return found[0];
  };
  const projects = await list("root");
  if (one(projects, "zohelo-data")?.id !== rootId) throw new Error("Production Drive root mismatch");
  const rootChildren = await list(rootId);
  const releases = one(rootChildren, "releases");
  const releaseFolders = releases ? await list(releases.id) : [];
  const platformPointer = async (source) => {
    const canonicalFolder = one(releaseFolders, source);
    const legacyFolder = one(rootChildren, `${source}-platform`);
    const canonical = canonicalFolder ? one(await list(canonicalFolder.id), "current-release.json", false) : null;
    const legacy = legacyFolder ? one(await list(legacyFolder.id), "current-release.json", false) : null;
    const state = canonical && legacy ? "conflict" : canonical ? "canonical" : legacy ? "legacy" :
      canonicalFolder || legacyFolder ? "container_without_pointer" : "absent";
    if (state === "conflict" || state === "container_without_pointer")
      throw new Error("Modeled release pointer is ambiguous or incomplete");
    return state;
  };
  const bdlPointer = await platformPointer("bdl");
  const wdiPointer = await platformPointer("wdi");
  result.dbw_modeled_pointer = await platformPointer("dbw");
  const control = one(rootChildren, "06_control");
  const campaigns = control ? one(await list(control.id), "source_campaigns") : null;
  const campaignFolders = campaigns ? await list(campaigns.id) : [];
  const snapshotPointer = async (source) => {
    const folder = one(campaignFolders, source);
    return folder ? one(await list(folder.id), "current-landing.json", false) : null;
  };
  const retainedPointer = await snapshotPointer("gus_dbw_retained_bronze");
  const opendataPointer = await snapshotPointer("opendata_org_people_bronze");
  if (retainedPointer) {
    const pointerResponse = await fetch(
      `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(retainedPointer.id)}?alt=media`,
      { headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(60000) }
    );
    if (!pointerResponse.ok || Number(pointerResponse.headers.get("content-length")) > 65536)
      throw new Error("Retained DBW pointer unavailable or oversized");
    const pointer = await pointerResponse.json();
    if (pointer.source_id !== "gus_dbw_retained_bronze" ||
        !/^[0-9a-f-]{36}$/.test(pointer.snapshot_id ?? ""))
      throw new Error("Retained DBW pointer invalid");
    result.retained_dbw_snapshot_id = pointer.snapshot_id;
  }
  if (result.dbw_modeled_pointer !== "absent" && retainedPointer)
    throw new Error("Modeled and retained DBW relations coexist in the current catalog");

  stage = "portal_bootstrap";
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 },
    acceptDownloads: false, serviceWorkers: "block" });
  await context.addInitScript((accessToken) => {
    if (location.origin === "https://data.zohelo.com")
      sessionStorage.setItem("zohelo_gdrive_access_token", accessToken);
  }, token);
  let attemptedWrite = false;
  let phase = "bootstrap";
  const mediaRequests = { bootstrap: 0, sql: 0 };
  let sqlMediaBytesKnown = 0;
  let sqlMediaBytesUnknown = 0;
  await context.route("https://www.googleapis.com/drive/**", (route) => {
    const request = route.request();
    if (!["GET", "OPTIONS"].includes(request.method())) {
      attemptedWrite = true;
      return route.abort("blockedbyclient");
    }
    if (new URL(request.url()).searchParams.get("alt") === "media") mediaRequests[phase]++;
    return route.continue();
  });
  await context.route("https://www.googleapis.com/upload/drive/**", (route) => {
    attemptedWrite = true;
    return route.abort("blockedbyclient");
  });
  const page = await context.newPage();
  page.on("response", (item) => {
    if (phase !== "sql" || !item.url().startsWith("https://www.googleapis.com/drive/") ||
        new URL(item.url()).searchParams.get("alt") !== "media") return;
    const length = Number(item.headers()["content-length"]);
    if (Number.isSafeInteger(length) && length >= 0) sqlMediaBytesKnown += length;
    else sqlMediaBytesUnknown++;
  });
  page.setDefaultTimeout(120000);
  await page.goto(origin, { waitUntil: "domcontentloaded" });
  if (new URL(page.url()).origin !== origin) throw new Error("Unexpected portal origin");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.waitFor({ state: "visible" });
  await profile.getByPlaceholder("Profile name").fill("Bronze naming acceptance");
  await profile.getByRole("button", { name: "Create Profile", exact: true }).click();
  await profile.waitFor({ state: "hidden" });
  await page.getByRole("status").filter({ hasText: "Select a dataset to query." })
    .first().waitFor({ state: "visible" });
  const bronze = page.getByRole("button", { name: "02_bronze", exact: true });
  await bronze.waitFor();
  if ((await bronze.getAttribute("aria-expanded")) !== "true") await bronze.click();
  const bronzeTree = bronze.locator("..");
  // The layer appears before authenticated release and snapshot resolution completes.
  await bronzeTree.getByText("gus_dbw_indicators", { exact: true }).waitFor({ state: "visible" });

  const candidates = [
    { source: "gus_bdl", canonical: "gus_bdl_variables", legacy: "bdl_variables",
      expected: bdlPointer !== "absent" },
    { source: "world_bank_wdi", canonical: "world_bank_wdi_country", legacy: "wdi_country",
      expected: wdiPointer !== "absent" },
    { source: "gus_dbw", canonical: "gus_dbw_indicators", legacy: "br_dbw_indicators",
      expected: !!retainedPointer || result.dbw_modeled_pointer !== "absent" },
    { source: "opendata_org", canonical: "opendata_org_people", legacy: "br_opendata_people",
      expected: !!opendataPointer },
  ];
  stage = "bronze_catalog";
  const available = [];
  for (const candidate of candidates) {
    const present = await bronzeTree.getByText(candidate.canonical, { exact: true }).count() > 0;
    result.checked.push({ source: candidate.source, canonical: candidate.canonical,
      expected: candidate.expected, available: present, verified: false });
    if (candidate.expected && !present) throw new Error("Published Bronze name missing from catalog");
    if (present && await bronzeTree.getByText(candidate.legacy, { exact: true }).count())
      throw new Error("Legacy Bronze name remains in catalog tree");
    if (present) available.push(candidate);
  }
  if (available.length < 2 || !available.some((candidate) => candidate.source === "gus_dbw"))
    throw new Error("Representative Bronze source names are missing");
  result.bronze_tables_visible = await bronzeTree.locator('[draggable="true"]').count();
  result.expected_sources = candidates.filter((candidate) => candidate.expected).length;
  result.available_sources = available.length;
  phase = "sql";
  for (const candidate of available) {
    stage = `sql_${candidate.source}`;
    const c = `"02_bronze"."${candidate.canonical}"`;
    const l = `"02_bronze"."${candidate.legacy}"`;
    const column = `bronze_check_${candidate.source}`;
    const sentinel = `MATCH_${candidate.source.toUpperCase()}`;
    const sql = `SELECT CASE WHEN
      (SELECT COUNT(*) FROM ${c}) = (SELECT COUNT(*) FROM ${l})
      AND NOT EXISTS (
        SELECT * FROM (SELECT * FROM ${c} ORDER BY ALL LIMIT 20)
        EXCEPT ALL SELECT * FROM (SELECT * FROM ${l} ORDER BY ALL LIMIT 20))
      AND NOT EXISTS (
        SELECT * FROM (SELECT * FROM ${l} ORDER BY ALL LIMIT 20)
        EXCEPT ALL SELECT * FROM (SELECT * FROM ${c} ORDER BY ALL LIMIT 20))
      THEN '${sentinel}' ELSE 'MISMATCH' END AS ${column},
      (SELECT COUNT(*) FROM ${c}) AS row_count;`;
    await page.getByRole("button", { name: "New SQL query", exact: true }).click();
    const editor = page.locator(".monaco-editor .view-lines:visible").first();
    await editor.waitFor();
    await editor.click();
    await page.keyboard.press("ControlOrMeta+A");
    await page.keyboard.insertText(sql);
    await page.getByRole("button", { name: "Run Query", exact: true }).click();
    const matchingRow = page.getByRole("row").filter({ has: page.getByRole("cell", {
      name: sentinel, exact: true,
    }) });
    const deadline = Date.now() + 180000;
    while (Date.now() < deadline) {
      if (await page.getByText("Query Error", { exact: true }).count())
        throw new Error("Bronze SQL query failed");
      if (await page.getByRole("columnheader", { name: column, exact: true }).count() &&
          await matchingRow.count()) break;
      await page.waitForTimeout(1000);
    }
    if (!await matchingRow.count())
      throw new Error("Bronze SQL equivalence timed out");
    const rowText = await matchingRow.first().getByRole("cell").last().textContent();
    const count = Number(rowText?.replaceAll(",", ""));
    if (!Number.isSafeInteger(count) || count < 0) throw new Error("Bronze SQL count missing");
    Object.assign(result.checked.find((item) => item.source === candidate.source),
      { verified: true, rows: count });
  }
  if (attemptedWrite || sqlMediaBytesKnown > 512 * 1024 * 1024)
    throw new Error("Unexpected Drive operation or media budget");
  result.media_requests = mediaRequests;
  result.sql_media_bytes_with_content_length = sqlMediaBytesKnown;
  result.sql_media_responses_without_content_length = sqlMediaBytesUnknown;
  result.read_only = true;
  result.status = "verified";
} catch {
  result.failed_stage = stage;
  process.exitCode = 1;
} finally {
  if (browser) await browser.close();
  result.observed_at_utc = new Date().toISOString();
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, JSON.stringify(result, null, 2) + "\n");
  console.log(JSON.stringify(result));
}
