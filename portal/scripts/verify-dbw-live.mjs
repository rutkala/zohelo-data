#!/usr/bin/env node
/** Real deployed-portal acceptance. No mocked Drive, screenshots, traces or token artifacts. */
import { chromium } from '@playwright/test';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { createHash } from 'node:crypto';

const [inputPath, outputPath] = process.argv.slice(2);
if (!inputPath || !outputPath) throw new Error('Consumer evidence and output paths are required');
const evidence = JSON.parse(await readFile(inputPath, 'utf8'));
const manifestBytes = await readFile(join(dirname(inputPath), 'manifest.json'));
const manifestDigest = createHash('sha256').update(manifestBytes).digest('hex');
const nativeManifest = JSON.parse(manifestBytes.toString('utf8'));
if (evidence.status !== 'verified' || evidence.pending_indicator_count !== 0 ||
    nativeManifest.snapshot_id !== evidence.snapshot_id || nativeManifest.source_id !== evidence.source_id ||
    !/^br_dbw_observations__indicator_\d+(?:__part_\d+)?$/.test(evidence.browser_probe?.table_name)) {
  throw new Error('Fresh complete native-consumer evidence is required');
}
const probe = evidence.browser_probe;
const counts = evidence.measured_parquet_rows;
for (const n of [probe.expected_rows, probe.indicator_id, counts.taxonomy, counts.metadata, counts.dictionaries]) {
  if (!Number.isSafeInteger(n) || n < 1) throw new Error('Consumer SQL expectations are invalid');
}
for (const key of ['GOOGLE_OAUTH_CLIENT_ID', 'GOOGLE_OAUTH_CLIENT_SECRET', 'GOOGLE_OAUTH_REFRESH_TOKEN']) {
  if (!process.env[key]) throw new Error('Required OAuth configuration is absent');
}
let browser;
let stage = 'authenticate';
const result = {
  format_version: 1, source_id: 'gus_dbw_retained_bronze',
  snapshot_id: evidence.snapshot_id, status: 'failed', portal_sql_verified: false,
  origin: 'https://data.zohelo.com',
  coverage_status: evidence.coverage_status, lineage_status: evidence.lineage_status,
};
try {
  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST', signal: AbortSignal.timeout(60000),
    body: new URLSearchParams({
      grant_type: 'refresh_token', client_id: process.env.GOOGLE_OAUTH_CLIENT_ID,
      client_secret: process.env.GOOGLE_OAUTH_CLIENT_SECRET,
      refresh_token: process.env.GOOGLE_OAUTH_REFRESH_TOKEN,
    }),
  });
  if (!response.ok) throw new Error('OAuth refresh failed');
  const token = (await response.json()).access_token;
  if (typeof token !== 'string' || !token) throw new Error('OAuth access token missing');
  stage = 'open_portal';
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await context.addInitScript((accessToken) => {
    if (location.origin === 'https://data.zohelo.com') {
      sessionStorage.setItem('zohelo_gdrive_access_token', accessToken);
    }
  }, token);
  let attemptedWrite = false;
  await context.route('https://www.googleapis.com/drive/v3/**', async route => {
    if (!['GET', 'OPTIONS'].includes(route.request().method())) {
      attemptedWrite = true;
      return route.abort('blockedbyclient');
    }
    return route.continue();
  });
  const page = await context.newPage();
  page.setDefaultTimeout(120000);
  const verifiedProbeFiles = new Set();
  const pendingChecks = [];
  let observedSnapshot = false;
  page.on('response', response => {
    const url = new URL(response.url());
    if (url.origin !== 'https://www.googleapis.com' || url.searchParams.get('alt') !== 'media' ||
        !response.ok()) return;
    const id = url.pathname.split('/').at(-1);
    if (probe.file_ids.includes(id)) verifiedProbeFiles.add(id);
    const check = (async () => {
      // Drive may serve JSON as application/octet-stream. Compare exact verified
      // bytes, not the MIME label or a self-declared matching snapshot UUID.
      const headers = response.headers();
      const length = Number(headers['content-length']);
      if (Number.isFinite(length) && length > 8 * 1024 * 1024) return;
      const bytes = await response.body();
      if (bytes.length !== manifestBytes.length) return;
      const digest = createHash('sha256').update(bytes).digest('hex');
      if (digest === manifestDigest) {
        observedSnapshot = true;
        result.portal_manifest_sha256 = digest;
      }
    })().catch(() => {});
    pendingChecks.push(check);
  });
  await page.goto('https://data.zohelo.com/', { waitUntil: 'domcontentloaded', timeout: 120000 });
  if (new URL(page.url()).origin !== result.origin) throw new Error('Unexpected portal origin');
  stage = 'create_disposable_profile';
  const profile = page.getByRole('dialog', { name: 'Create Profile' });
  await profile.waitFor({ state: 'visible' });
  await profile.getByPlaceholder('Profile name').fill('DBW release acceptance');
  await profile.getByRole('button', { name: 'Create Profile', exact: true }).click();
  await profile.waitFor({ state: 'hidden' });
  stage = 'load_published_catalog';
  for (let i = 0; i < 120 && !observedSnapshot; i++) await page.waitForTimeout(1000);
  if (!observedSnapshot) throw new Error('Expected DBW snapshot was not discovered');
  const editor = page.locator('.monaco-editor .view-lines:visible').first();
  await editor.waitFor({ state: 'visible' });
  const sql = `WITH o AS (
 SELECT count(*) AS n, count(indicator_id) AS ids, min(indicator_id) AS lo, max(indicator_id) AS hi
 FROM "02_bronze"."${probe.table_name}"
), t AS (SELECT count(*) AS n FROM "02_bronze"."br_dbw_indicators"),
 m AS (SELECT count(*) AS n FROM "02_bronze"."br_dbw_metadata"),
 d AS (SELECT count(*) AS n FROM "02_bronze"."br_dbw_dictionaries")
SELECT CASE WHEN o.n=${probe.expected_rows} AND o.ids=${probe.expected_rows}
 AND o.lo=${probe.indicator_id} AND o.hi=${probe.indicator_id}
 AND t.n=${counts.taxonomy} AND m.n=${counts.metadata} AND d.n=${counts.dictionaries}
 THEN 'DBW_SQL_VERIFIED' ELSE 'DBW_SQL_MISMATCH' END AS verification FROM o,t,m,d;`;
  stage = 'run_bronze_sql';
  await editor.click();
  await page.keyboard.press('ControlOrMeta+A');
  await page.keyboard.insertText(sql);
  await page.getByRole('button', { name: 'Run Query', exact: true }).click();
  await page.locator(':text-is("DBW_SQL_VERIFIED"):visible').first()
    .waitFor({ state: 'visible', timeout: 240000 });
  stage = 'verify_real_downloads';
  await Promise.all(pendingChecks);
  if (attemptedWrite || !probe.file_ids.every(id => verifiedProbeFiles.has(id))) {
    throw new Error('Real read-only published-file consumption was not established');
  }
  result.status = 'verified';
  result.portal_sql_verified = true;
  result.observation_table = probe.table_name;
  result.observation_selection_rows = probe.expected_rows;
  result.fixed_table_rows = { taxonomy: counts.taxonomy, metadata: counts.metadata, dictionaries: counts.dictionaries };
  result.verified_observation_file_count = verifiedProbeFiles.size;
  result.read_only = true;
} catch {
  result.failed_stage = stage;
  process.exitCode = 1;
} finally {
  if (browser) await browser.close();
  result.observed_at_utc = new Date().toISOString();
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, JSON.stringify(result, null, 2) + '\n');
  console.log(JSON.stringify(result));
}
