#!/usr/bin/env node
/** Manual production acceptance. Summaries contain no URLs, credentials, screenshots or traces. */
import { chromium } from "@playwright/test";
import { writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

const [outputPath] = process.argv.slice(2);
const expectedSha = process.env.EXPECTED_DEPLOYED_SHA;
const projectId = process.env.DRIVE_ROOT_ID;
const productionProjectId = "1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf";
if (
  !outputPath ||
  !/^[a-f0-9]{40}$/.test(expectedSha || "") ||
  !/^[A-Za-z0-9_-]+$/.test(projectId || "")
) {
  throw new Error(
    "Output path, exact deployed SHA and explicit production Drive root ID are required"
  );
}
if (projectId !== productionProjectId)
  throw new Error("Drive root is not the reviewed production project");
if (expectedSha !== process.env.GITHUB_SHA)
  throw new Error("Deployed SHA must match the reviewed main checkout");
for (const key of [
  "GOOGLE_OAUTH_CLIENT_ID",
  "GOOGLE_OAUTH_CLIENT_SECRET",
  "GOOGLE_OAUTH_REFRESH_TOKEN",
]) {
  if (!process.env[key]) throw new Error("OAuth configuration missing");
}
const origin = "https://data.zohelo.com";
const result = {
  format_version: 1,
  status: "failed",
  origin,
  expected_deployed_sha: expectedSha,
  source_folder_count: 0,
  family_count: 0,
  folders_probed: 0,
  files_probed: 0,
  managed_download_links_checked: 0,
  preview_checks: 0,
  large_link_checks: 0,
  metadata_sql_checks: 0,
  bdl_metadata: null,
  large_file_bytes: 0,
  large_file_format: null,
  per_source: [],
  read_only: false,
};
let browser;
let stage = "oauth";
try {
  const auth = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    signal: AbortSignal.timeout(60000),
    body: new URLSearchParams({
      grant_type: "refresh_token",
      client_id: process.env.GOOGLE_OAUTH_CLIENT_ID,
      client_secret: process.env.GOOGLE_OAUTH_CLIENT_SECRET,
      refresh_token: process.env.GOOGLE_OAUTH_REFRESH_TOKEN,
    }),
  });
  if (!auth.ok) throw new Error("OAuth refresh failed");
  const token = (await auth.json()).access_token;
  if (typeof token !== "string" || !token) throw new Error("OAuth access token missing");
  const build = await fetch(`${origin}/portal-build.json`, { signal: AbortSignal.timeout(30000) });
  if (!build.ok || (await build.json()).git_commit !== expectedSha)
    throw new Error("Deployed code SHA mismatch");
  const fields =
    "nextPageToken,files(id,name,mimeType,size,version,modifiedTime,sha256Checksum,webViewLink,webContentLink,capabilities(canDownload),parents)";
  let apiCalls = 0;
  async function list(parent) {
    let next,
      files = [];
    do {
      const url = new URL("https://www.googleapis.com/drive/v3/files");
      url.searchParams.set("q", `'${parent}' in parents and trashed=false`);
      url.searchParams.set("fields", fields);
      url.searchParams.set("pageSize", "1000");
      if (next) url.searchParams.set("pageToken", next);
      const response = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(60000),
      });
      apiCalls++;
      if (!response.ok) throw new Error("Drive metadata listing failed");
      const page = await response.json();
      if (!Array.isArray(page.files)) throw new Error("Incomplete Drive metadata listing");
      files.push(...page.files);
      next = page.nextPageToken;
    } while (next);
    return files;
  }
  stage = "resolve_production_root";
  const projects = (await list("root")).filter(
    (f) => f.name === "zohelo-data" && f.mimeType === "application/vnd.google-apps.folder"
  );
  if (projects.length !== 1 || projects[0].id !== projectId)
    throw new Error("Production project root does not match the portal");
  const roots = (await list(projectId)).filter(
    (f) => f.name === "01_landing" && f.mimeType === "application/vnd.google-apps.folder"
  );
  if (roots.length !== 1) throw new Error("Production Landing root is not unambiguous");
  const sources = (await list(roots[0].id)).filter(
    (f) => f.mimeType === "application/vnd.google-apps.folder"
  );
  const retainedBaseline = [
    "nbp_exchange_rates_table_a",
    "nbp_exchange_rates_table_b",
    "nbp_exchange_rates_table_c",
    "nbp_gold_prices",
    "gus_bdl",
    "world_bank_wdi",
    "eurostat",
    "opendata_org",
    "gus_dbw",
    "gus_teryt",
    "gugik_prg",
    "gleif",
    "mf_biala_lista",
  ];
  if (retainedBaseline.some((name) => !sources.some((folder) => folder.name === name)))
    throw new Error("At least one retained baseline source folder is missing");
  const families = new Set(sources.map((f) => (f.name.startsWith("nbp_") ? "nbp" : f.name)));
  result.source_folder_count = sources.length;
  result.family_count = families.size;
  if (sources.length < 13 || families.size < 10)
    throw new Error("Retained folder/family coverage below expected minimum");
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1000 },
    acceptDownloads: false,
    serviceWorkers: "block",
  });
  let previewCandidate = null;
  let largeCandidate = null;
  await context.addInitScript((accessToken) => {
    if (location.origin !== "https://data.zohelo.com") return;
    sessionStorage.setItem("zohelo_gdrive_access_token", accessToken);
    window.__nativeLink = null;
    window.open = () => ({
      opener: null,
      close() {},
      location: {
        replace(link) {
          window.__nativeLink = link;
        },
      },
    });
  }, token);
  let attemptedWrite = false;
  let mediaRequests = 0;
  let metadataDriveRequests = 0;
  let metadataListRequests = 0;
  let metadataMediaAttempts = 0;
  const guardDriveRequest = (route) => {
    const request = route.request();
    const method = request.method();
    if (method !== "GET" && method !== "OPTIONS") {
      attemptedWrite = true;
      return route.abort("blockedbyclient");
    }
    if (stage === "metadata_sql" && method === "GET") {
      metadataDriveRequests++;
      const url = new URL(request.url());
      if (url.pathname.endsWith("/files") &&
          /^\(.+ in parents(?: or .+ in parents)*\) and mimeType (?:=|!=)/.test(url.searchParams.get("q") || ""))
        metadataListRequests++;
      if (url.searchParams.get("alt") === "media") metadataMediaAttempts++;
    }
    if (method === "GET" && new URL(request.url()).searchParams.get("alt") === "media") {
      if (stage !== "bounded_preview" || !request.url().includes(`/${previewCandidate?.file?.id}?`))
        return route.abort("blockedbyclient");
      mediaRequests++;
    }
    return route.continue();
  };
  await context.route("https://www.googleapis.com/drive/**", guardDriveRequest);
  await context.route("https://www.googleapis.com/upload/drive/**", guardDriveRequest);
  const page = await context.newPage();
  page.setDefaultTimeout(120000);
  stage = "load_live_portal";
  await page.goto(origin, { waitUntil: "domcontentloaded" });
  if (new URL(page.url()).origin !== origin) throw new Error("Unexpected portal origin");
  // This context has no saved profile. React and IndexedDB bootstrap finish
  // after DOMContentLoaded; an immediate isVisible() can miss the dialog.
  stage = "wait_profile_dialog";
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.waitFor({ state: "visible" });
  stage = "create_profile";
  await profile.getByPlaceholder("Profile name").fill("Landing acceptance");
  await profile.getByRole("button", { name: "Create Profile", exact: true }).click();
  await profile.waitFor({ state: "hidden" });
  stage = "wait_landing_root";
  const section = page.getByRole("region", { name: "Landing", exact: true });
  await section.locator('[data-native-depth="0"]').waitFor();
  stage = "wait_landing_sources";
  // Root markup can appear while its metadata pages are still loading.
  for (const source of sources) {
    await section.locator(`[data-native-id="${source.id}"][data-native-folder="true"]`).waitFor();
  }
  if (
    (await section.count()) !== 1 ||
    (await page.getByText("Files on Drive", { exact: true }).count()) ||
    (await page.getByText("01_landing", { exact: true }).count())
  )
    throw new Error("Landing navigation is duplicated");
  async function revealActions(row, file) {
    const actions = row.getByRole("button", { name: `Actions for ${file.name}`, exact: true });
    if ((await actions.getAttribute("aria-expanded")) !== "true") await actions.click();
  }
  stage = "probe_source_folders";
  async function representative(folder, seen = new Set(), depth = 1, minimumDepth = 1) {
    if (seen.has(folder.id)) throw new Error("Drive folder cycle");
    seen.add(folder.id);
    const children = await list(folder.id);
    const large = children.find(
      (f) =>
        /\.(zip|gz|7z|tar)$/i.test(f.name) &&
        Number(f.size) > 512 * 1024 * 1024 &&
        f.capabilities?.canDownload &&
        /^https:\/\/drive\.google\.com\//.test(f.webContentLink || "")
    );
    if (large && !largeCandidate) largeCandidate = { path: [folder], file: large };
    const eligible = children.find(
      (f) =>
        f.mimeType !== "application/vnd.google-apps.folder" &&
        /\.(csv|json|jsonl|parquet)$/i.test(f.name) &&
        Number(f.size) > 0 &&
        Number(f.size) <= 8 * 1024 * 1024 &&
        /^[a-f0-9]{64}$/i.test(f.sha256Checksum || "") &&
        f.capabilities?.canDownload
    );
    if (eligible && !previewCandidate) previewCandidate = { path: [folder], file: eligible };
    const file =
      depth >= minimumDepth &&
      children.find(
        (f) =>
          f.mimeType !== "application/vnd.google-apps.folder" &&
          /^https:\/\/(?:drive\.google\.com|docs\.google\.com|drive\.usercontent\.google\.com)\//.test(
            f.webViewLink || ""
          )
      );
    if (file) return { path: [folder], file };
    for (const nested of children.filter(
      (f) => f.mimeType === "application/vnd.google-apps.folder"
    )) {
      const found = await representative(nested, seen, depth + 1, minimumDepth);
      if (previewCandidate?.path?.[0]?.id === nested.id) previewCandidate.path.unshift(folder);
      if (largeCandidate?.path?.[0]?.id === nested.id) largeCandidate.path.unshift(folder);
      if (found) return { path: [folder, ...found.path], file: found.file };
    }
    return null;
  }
  for (const source of sources) {
    const found = await representative(source);
    let row = section;
    for (const folder of found?.path ?? [source]) {
      row = row.locator(`[data-native-id="${folder.id}"]`).first();
      await row.waitFor();
      await row.getByRole("button", { name: `Expand ${folder.name}` }).click();
    }
    if (found) {
      const file = found.file;
      const fileRow = row.locator(`[data-native-id="${file.id}"]`).first();
      await fileRow.waitFor();
      if (!(await fileRow.innerText()).includes(file.name))
        throw new Error("Native filename mismatch");
      await revealActions(fileRow, file);
      await fileRow.getByRole("button", { name: "Open in Drive" }).click();
      await page.waitForFunction(() => !!window.__nativeLink);
      const opened = await page.evaluate(() => {
        const value = window.__nativeLink;
        window.__nativeLink = null;
        return value;
      });
      if (opened !== file.webViewLink)
        throw new Error("Native Open did not use the fresh Drive link");
      if (file.capabilities?.canDownload && file.webContentLink) {
        await fileRow.getByRole("button", { name: "Download via Drive" }).click();
        await page.waitForFunction(() => !!window.__nativeLink);
        const downloaded = await page.evaluate(() => {
          const value = window.__nativeLink;
          window.__nativeLink = null;
          return value;
        });
        if (downloaded !== file.webContentLink)
          throw new Error("Native Download did not use the Drive-managed link");
        result.managed_download_links_checked++;
      }
      result.files_probed++;
    }
    result.per_source.push({ source: source.name, file_link_checked: !!found });
    result.folders_probed++;
  }
  stage = "metadata_sql";
  const metadataSource = sources.find((f) => f.name === "gus_bdl");
  if (!metadataSource) throw new Error("Metadata SQL acceptance source folder missing");
  const metadataRow = section.locator(`[data-native-id="${metadataSource.id}"]`).first();
  await revealActions(metadataRow, metadataSource);
  const mediaBeforeMetadata = mediaRequests;
  const started = performance.now();
  await metadataRow.getByRole("button", { name: "Query file metadata" }).click();
  await page
    .getByRole("tab", { name: `Landing/${metadataSource.name} file metadata` })
    .waitFor({ timeout: 1200000 });
  await page.getByRole("columnheader", { name: "relative_path" }).first().waitFor({ timeout: 1200000 });
  await page.locator("table:visible tbody tr").first().waitFor({ timeout: 1200000 });
  const coldMs = Math.round(performance.now() - started);
  const completed = await page.getByRole("status").filter({ hasText: /^Scanned \d+ original file/ }).textContent();
  const summary = completed?.match(/Scanned (\d+) original file\(s\) across (\d+) folders \((\d+) Drive list pages\)/);
  if (!summary || Number(summary[1]) < 1 || Number(summary[2]) < 2 ||
      Number(summary[3]) !== metadataListRequests)
    throw new Error("BDL metadata scan did not report complete folder and page counts");
  const countEditor = page.locator(".monaco-editor .view-lines:visible").first();
  await countEditor.click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText('SELECT COUNT(*) AS file_count FROM "01_landing"."gus_bdl_files";');
  await page.getByRole("button", { name: "Run Query", exact: true }).click();
  await page.getByRole("columnheader", { name: "file_count" }).first().waitFor();
  await page.getByRole("cell", { name: summary[1], exact: true }).first().waitFor();
  if (
    (await page.getByText("Query Error", { exact: true }).count()) ||
    mediaRequests !== mediaBeforeMetadata || metadataMediaAttempts !== 0
  )
    throw new Error("Metadata SQL queried payload bytes or failed");
  const coldRequests = metadataDriveRequests;
  const coldListRequests = metadataListRequests;
  const warmStart = performance.now();
  const existingMetadataTabs = await page.getByRole("tab", {
    name: `Landing/${metadataSource.name} file metadata`, exact: true,
  }).count();
  await metadataRow.getByRole("button", { name: "Query file metadata" }).click();
  const warmTab = page.getByRole("tab", {
    name: `Landing/${metadataSource.name} file metadata`, exact: true,
  }).nth(existingMetadataTabs);
  await warmTab.waitFor({ state: "visible" });
  if (await warmTab.getAttribute("aria-selected") !== "true")
    throw new Error("Warm metadata query did not activate its new SQL tab");
  await page.getByRole("columnheader", { name: "relative_path" }).first().waitFor();
  await page.locator("table:visible tbody tr").first().waitFor();
  const warmMs = Math.round(performance.now() - warmStart);
  if (metadataDriveRequests !== coldRequests || metadataMediaAttempts !== 0 ||
      await page.getByText("Query Error", { exact: true }).count())
    throw new Error("Warm BDL metadata query unexpectedly read Drive again");
  result.bdl_metadata = {
    files: Number(summary[1]), folders: Number(summary[2]),
    scan_list_pages: Number(summary[3]), cold_drive_requests: coldRequests,
    cold_list_requests: coldListRequests, warm_drive_requests: metadataDriveRequests - coldRequests,
    cold_scan_and_query_ms: coldMs, warm_query_ms: warmMs,
    attempted_payload_reads: metadataMediaAttempts,
  };
  result.metadata_sql_checks = 1;
  // BDL's retained bulk folders are several levels deep. A source-root file
  // cannot stand in for this navigation check.
  const bdl = sources.find((f) => f.name === "gus_bdl");
  if (!bdl) throw new Error("Retained BDL source folder missing");
  const bdlBulk = (await list(bdl.id)).find(
    (f) => f.name === "web_bulk" && f.mimeType === "application/vnd.google-apps.folder"
  );
  if (!bdlBulk) throw new Error("Retained BDL bulk folder missing");
  const deep = await representative(bdlBulk, new Set(), 1, 3);
  if (previewCandidate?.path?.[0]?.id === bdlBulk.id) previewCandidate.path.unshift(bdl);
  if (largeCandidate?.path?.[0]?.id === bdlBulk.id) largeCandidate.path.unshift(bdl);
  if (!deep || deep.path.length < 3) throw new Error("BDL multi-level native path not verified");
  let bdlRow = section.locator(`[data-native-id="${bdl.id}"]`).first();
  if (await bdlRow.getByRole("button", { name: `Expand ${bdl.name}` }).count()) {
    await bdlRow.getByRole("button", { name: `Expand ${bdl.name}` }).click();
  }
  for (const folder of deep.path) {
    bdlRow = bdlRow.locator(`[data-native-id="${folder.id}"]`).first();
    await bdlRow.waitFor();
    const expand = bdlRow.getByRole("button", { name: `Expand ${folder.name}` });
    if (await expand.count()) await expand.click();
  }
  await bdlRow.locator(`[data-native-id="${deep.file.id}"]`).first().waitFor();
  result.bdl_depth_checked = deep.path.length + 1;
  if (!largeCandidate) throw new Error("No large Drive-managed native download link was observed");
  let largeRow = section;
  for (const folder of largeCandidate.path) {
    largeRow = largeRow.locator(`[data-native-id="${folder.id}"]`).first();
    await largeRow.waitFor();
    const expand = largeRow.getByRole("button", { name: `Expand ${folder.name}` });
    if (await expand.count()) await expand.click();
  }
  const archiveRow = largeRow.locator(`[data-native-id="${largeCandidate.file.id}"]`).first();
  await revealActions(archiveRow, largeCandidate.file);
  await archiveRow.getByRole("button", { name: "Download via Drive" }).click();
  await page.waitForFunction(() => !!window.__nativeLink);
  const largeLink = await page.evaluate(() => {
    const value = window.__nativeLink;
    window.__nativeLink = null;
    return value;
  });
  if (largeLink !== largeCandidate.file.webContentLink)
    throw new Error("Large file managed link mismatch");
  result.large_link_checks = 1;
  result.large_file_bytes = Number(largeCandidate.file.size);
  result.large_file_format = largeCandidate.file.name.split(".").pop().toLowerCase();
  if (previewCandidate) {
    stage = "bounded_preview";
    let previewRow = section;
    for (const folder of previewCandidate.path) {
      previewRow = previewRow.locator(`[data-native-id="${folder.id}"]`).first();
      await previewRow.waitFor();
      const expand = previewRow.getByRole("button", { name: `Expand ${folder.name}` });
      if (await expand.count()) await expand.click();
    }
    const fileRow = previewRow.locator(`[data-native-id="${previewCandidate.file.id}"]`).first();
    await revealActions(fileRow, previewCandidate.file);
    await fileRow.getByRole("button", { name: "Preview in SQL" }).click();
    await page.getByRole("tab", { name: `Landing/${previewCandidate.file.name}` }).waitFor();
    await page.getByRole("tab", { name: "Table", exact: true }).waitFor();
    await page.locator("table:visible tbody tr").first().waitFor({ timeout: 180000 });
    if (await page.getByText("Query Error", { exact: true }).count())
      throw new Error("Live preview SQL failed");
    if (mediaRequests !== 1) throw new Error("Bounded preview media-read count mismatch");
    result.preview_checks = 1;
  }
  if (
    result.folders_probed !== sources.length ||
    result.files_probed !== sources.length ||
    attemptedWrite ||
    mediaRequests !== result.preview_checks
  )
    throw new Error("Native metadata-only acceptance incomplete or unexpected Drive operation");
  if (result.preview_checks !== 1 || result.metadata_sql_checks !== 1)
    throw new Error("Native preview or metadata SQL acceptance incomplete");
  result.status = "verified";
  result.read_only = true;
  result.metadata_pages = apiCalls;
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
