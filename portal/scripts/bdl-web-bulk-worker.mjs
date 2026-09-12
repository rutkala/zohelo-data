import { chromium } from '@playwright/test';
import fs from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

const email = process.env.GUS_BDL_WEB_EMAIL || '';
const password = process.env.GUS_BDL_WEB_PASSWORD || '';
const subgroupId = process.env.BDL_BULK_SUBGROUP_ID || '';
const subgroupName = process.env.BDL_BULK_SUBGROUP_NAME || subgroupId;
const subgroupUrl = process.env.BDL_BULK_URL || '';
const outDir = path.resolve(process.env.BDL_BULK_OUT_DIR || 'test-results/bdl-web-bulk');

if (!email || !password) throw new Error('Missing BDL web credentials');
if (!/^P[0-9]+$/.test(subgroupId)) throw new Error('Invalid BDL subgroup id');
if (!/^https:\/\/bdl\.stat\.gov\.pl\/bdl\/dane\/podgrup\/wymiary\/[0-9]+\/[0-9]+\/[0-9]+$/.test(subgroupUrl)) throw new Error('Invalid BDL subgroup URL');

await fs.mkdir(outDir, { recursive: true });
const safe = (s) => String(s ?? '').replaceAll(email, '[REDACTED_EMAIL]').replaceAll(password, '[REDACTED_PASSWORD]');
const result = { startedAt: new Date().toISOString(), subgroupId, subgroupName, subgroupUrl, login: false, dimensions: [], selectedInformation: null, status: 'started', archive: null };
async function saveResult() { await fs.writeFile(path.join(outDir, 'worker-result.json'), JSON.stringify(result, null, 2)); }
async function infoCount(page) {
  const body = await page.locator('body').innerText().catch(() => '');
  const match = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  return match ? Number(match[1].replace(/\s/g, '')) : null;
}
async function listExports(page) {
  await page.goto('https://bdl.stat.gov.pl/bdl/start', { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(900);
  return page.locator('[id$="_Export"]').evaluateAll((els, expected) => els.map((e) => ({
    id: e.id,
    text: (e.textContent || '').trim().replace(/\s+/g, ' '),
    href: e.getAttribute('href'),
    onclick: e.getAttribute('onclick'),
    cls: typeof e.className === 'string' ? e.className : '',
    disabled: 'disabled' in e ? !!e.disabled : false,
    rowText: (e.closest('tr')?.innerText || '').trim().replace(/\s+/g, ' ').slice(0, 2000),
  })).filter((x) => !expected || x.text.includes(expected) || x.rowText.includes(expected)), subgroupName);
}
function exportFingerprint(entry) {
  return crypto.createHash('sha256').update(JSON.stringify([entry.text, entry.rowText])).digest('hex');
}
async function findNewReadyExport(page, baselineFingerprints) {
  const entries = (await listExports(page)).filter((entry) => (
    !baselineFingerprints.has(exportFingerprint(entry)) &&
    !/aspNetDisabled/i.test(entry.cls) &&
    !entry.disabled &&
    (entry.href || entry.onclick)
  ));
  if (entries.length > 1) throw new Error('Generated BDL package matched multiple new Export controls');
  return entries[0] || null;
}
function providerClockValue(date, timeZone) {
  const values = Object.fromEntries(new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  }).formatToParts(date).filter((part) => part.type !== 'literal').map((part) => [part.type, Number(part.value)]));
  return Date.UTC(values.year, values.month - 1, values.day, values.hour, values.minute, values.second);
}
function validateProviderFilename(suggested, generationStartedAt) {
  const subgroupNumber = subgroupId.slice(1);
  const escapedNumber = subgroupNumber.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  if (!new RegExp(`(?:^|_)${escapedNumber}(?:_|\\.)`, 'i').test(suggested)) {
    throw new Error('BDL export filename does not identify the selected subgroup');
  }
  const timestamp = suggested.match(/_([0-9]{14})\.zip$/i)?.[1];
  if (!timestamp) throw new Error('BDL export filename does not contain a generation timestamp');
  const emitted = Date.UTC(
    Number(timestamp.slice(0, 4)), Number(timestamp.slice(4, 6)) - 1,
    Number(timestamp.slice(6, 8)), Number(timestamp.slice(8, 10)),
    Number(timestamp.slice(10, 12)), Number(timestamp.slice(12, 14)),
  );
  const providerStartedAt = providerClockValue(generationStartedAt, 'Europe/Warsaw');
  const providerFinishedAt = providerClockValue(new Date(), 'Europe/Warsaw');
  // Provider filenames have whole-second precision.  The start second is
  // ambiguous with an older pending export, so fail closed until the next one.
  const fresh = emitted > providerStartedAt && emitted <= providerFinishedAt;
  if (!fresh) throw new Error('BDL export filename predates the current generation request');
  return timestamp;
}
async function captureExport(page, entry, generationStartedAt) {
  const control = page.locator(`#${entry.id}`);
  if (!await control.count()) throw new Error(`Export control disappeared: ${entry.id}`);
  const downloadPromise = page.waitForEvent('download', { timeout: 180000 }).catch(() => null);
  await control.evaluate((el) => el.click());
  const download = await downloadPromise;
  if (!download) throw new Error('BDL Export action did not produce a browser download');
  const suggested = download.suggestedFilename();
  if (!suggested) throw new Error('BDL export did not provide a source filename');
  const providerGenerationTimestamp = validateProviderFilename(suggested, generationStartedAt);
  const filename = suggested.replace(/[^A-Za-z0-9._-]/g, '_');
  const target = path.join(outDir, `download-${filename}`);
  await download.saveAs(target);
  const metadata = await fs.stat(target);
  const prefix = Buffer.alloc(Math.min(16, metadata.size));
  const handle = await fs.open(target, 'r');
  try {
    await handle.read(prefix, 0, prefix.length, 0);
  } finally {
    await handle.close();
  }
  if (metadata.size < 4 || prefix[0] !== 0x50 || prefix[1] !== 0x4b) throw new Error('BDL bulk download is not a ZIP archive');
  const digest = crypto.createHash('sha256');
  await new Promise((resolve, reject) => {
    const stream = createReadStream(target);
    stream.on('data', (chunk) => digest.update(chunk));
    stream.on('end', resolve);
    stream.on('error', reject);
  });
  return { filename, suggestedFilename: suggested, providerGenerationTimestamp, bytes: metadata.size, sha256: digest.digest('hex'), firstBytesHex: prefix.toString('hex') };
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ acceptDownloads: true, locale: 'pl-PL' });
const page = await context.newPage();
try {
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie', { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
  await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
  await page.locator('#ctl00_ContentPlaceHolder_SignIn').click();
  await page.waitForTimeout(1800);
  const loginText = safe(await page.locator('body').innerText().catch(() => ''));
  result.login = !/Użytkownik:\s*Gość/i.test(loginText) && /Użytkownik:/i.test(loginText);
  if (!result.login) throw new Error('BDL web login failed');

  // An export left by an earlier run is not evidence for this subgroup.  Pin
  // every pre-existing control before requesting a new package and accept only
  // a new control created after the successful POST below.
  const baselineFingerprints = new Set((await listExports(page)).map(exportFingerprint));
  result.baselineExportFingerprints = baselineFingerprints.size;
  await page.goto(subgroupUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(1200);
  const clicked = new Set();
  for (let round = 0; round < 20; round++) {
    const candidates = await page.locator('[id$="_SelectAll"]').evaluateAll((els) => els.map((e) => ({ id: e.id, visible: !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length), disabled: 'disabled' in e ? !!e.disabled : false })));
    const next = candidates.filter((item) => item.visible && !item.disabled && !clicked.has(item.id)).sort((a,b) => a.id.localeCompare(b.id))[0];
    if (!next) break;
    await page.locator(`#${next.id}`).click();
    clicked.add(next.id);
    await page.waitForTimeout(1200);
    result.dimensions.push({ id: next.id, selectedInformation: await infoCount(page) });
  }
  result.selectedInformation = await infoCount(page);
  if (!Number.isFinite(result.selectedInformation) || result.selectedInformation <= 0) {
    result.status = 'web_bulk_unsupported';
    result.detail = 'No positive information selection was produced';
  } else if (result.selectedInformation <= 3500) {
    result.status = 'below_bulk_threshold';
  } else {
    let downloadButton = null;
    for (const id of ['ctl00_ContentPlaceHolder_download1', 'ctl00_ContentPlaceHolder_download2']) {
      const control = page.locator(`#${id}`);
      if (await control.isVisible().catch(() => false) && await control.isEnabled().catch(() => false)) { downloadButton = control; result.downloadButtonId = id; break; }
    }
    if (!downloadButton) {
      result.status = 'no_bulk_control';
      result.detail = `Selection ${result.selectedInformation} exceeded threshold but Pobierz did not enable`;
    } else {
      const currentPath = new URL(page.url()).pathname;
      const generationStartedAt = new Date();
      const responsePromise = page.waitForResponse((response) => {
        try { const u = new URL(response.url()); return response.request().method() === 'POST' && u.pathname === currentPath; } catch { return false; }
      }, { timeout: 360000 });
      await downloadButton.click();
      const response = await responsePromise;
      result.generation = { startedAt: generationStartedAt.toISOString(), status: response.status(), ok: response.ok(), elapsedMs: Date.now() - generationStartedAt.getTime() };
      if (!response.ok()) throw new Error(`BDL bulk generation returned HTTP ${response.status()}`);
      let ready = null;
      for (let attempt = 1; attempt <= 36; attempt++) {
        ready = await findNewReadyExport(page, baselineFingerprints);
        if (ready) { result.exportReadyPoll = attempt; break; }
        if (attempt < 36) await page.waitForTimeout(5000);
      }
      if (!ready) throw new Error('BDL package generated but no new bound Export control appeared');
      result.archive = await captureExport(page, ready, generationStartedAt);
      result.status = 'downloaded_generated_export';
    }
  }
  result.completedAt = new Date().toISOString();
  await saveResult();
  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  result.status = 'failed';
  result.error = safe(error?.stack || error?.message || error);
  result.failedAt = new Date().toISOString();
  await saveResult();
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
