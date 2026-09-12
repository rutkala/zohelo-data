import { chromium } from '@playwright/test';
import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const email = process.env.GUS_BDL_WEB_EMAIL || '';
const password = process.env.GUS_BDL_WEB_PASSWORD || '';
const subgroupId = process.env.BDL_BULK_SUBGROUP_ID || 'P2695';
const subgroupName = process.env.BDL_BULK_SUBGROUP_NAME || 'Wyniki finansowe wg sekcji PKD 2007';
const subgroupUrl = process.env.BDL_BULK_URL || 'https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/43/418/2695';
const outDir = path.resolve(process.env.BDL_BULK_OUT_DIR || 'test-results/bdl-web-bulk-probe-v3');

if (!email || !password) throw new Error('Missing BDL web credentials');
if (!/^P[0-9]+$/.test(subgroupId)) throw new Error('Invalid BDL subgroup id');
if (!/^https:\/\/bdl\.stat\.gov\.pl\/bdl\/dane\/podgrup\/wymiary\/[0-9]+\/[0-9]+\/[0-9]+$/.test(subgroupUrl)) {
  throw new Error('Invalid BDL subgroup URL');
}

await fs.mkdir(outDir, { recursive: true });
const safe = (s) => String(s ?? '').replaceAll(email, '[REDACTED_EMAIL]').replaceAll(password, '[REDACTED_PASSWORD]');
const result = {
  startedAt: new Date().toISOString(), subgroupId, subgroupName, subgroupUrl,
  login: false, dimensions: [], selectedInformation: null, status: 'started', archive: null,
};
async function saveResult() { await fs.writeFile(path.join(outDir, 'worker-result.json'), JSON.stringify(result, null, 2)); }
async function infoCount(page) {
  const body = await page.locator('body').innerText().catch(() => '');
  const match = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  return match ? Number(match[1].replace(/\s/g, '')) : null;
}
async function findReadyExport(page) {
  await page.goto('https://bdl.stat.gov.pl/bdl/start', { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(900);
  const entries = await page.locator('[id$="_Export"]').evaluateAll((els, expected) => els.map((e) => ({
    id: e.id,
    text: (e.textContent || '').trim().replace(/\s+/g, ' '),
    href: e.getAttribute('href'),
    onclick: e.getAttribute('onclick'),
    cls: typeof e.className === 'string' ? e.className : '',
    disabled: 'disabled' in e ? !!e.disabled : false,
    rowText: (e.closest('tr')?.innerText || '').trim().replace(/\s+/g, ' ').slice(0, 2000),
  })).filter((x) => !expected || x.text.includes(expected) || x.rowText.includes(expected)), subgroupName);
  return entries.find((x) => !/aspNetDisabled/i.test(x.cls) && !x.disabled && (x.href || x.onclick)) || null;
}
async function captureExport(page, entry) {
  const control = page.locator(`#${entry.id}`);
  if (!await control.count()) throw new Error(`Export control disappeared: ${entry.id}`);
  const downloadPromise = page.waitForEvent('download', { timeout: 180000 }).catch(() => null);
  await control.evaluate((el) => el.click());
  const download = await downloadPromise;
  if (!download) throw new Error('BDL Export action did not produce a browser download');
  const suggested = download.suggestedFilename() || `${subgroupId}.zip`;
  const filename = suggested.replace(/[^A-Za-z0-9._-]/g, '_');
  const target = path.join(outDir, `download-${filename}`);
  await download.saveAs(target);
  const body = await fs.readFile(target);
  if (body.length < 4 || body[0] !== 0x50 || body[1] !== 0x4b) throw new Error('BDL bulk download is not a ZIP archive');
  return { filename, suggestedFilename: suggested, bytes: body.length,
    sha256: crypto.createHash('sha256').update(body).digest('hex'),
    firstBytesHex: body.subarray(0, 16).toString('hex') };
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

  let ready = await findReadyExport(page);
  if (ready) {
    result.archive = await captureExport(page, ready);
    result.status = 'downloaded_existing_export';
  } else {
    await page.goto(subgroupUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForTimeout(1200);
    const clicked = new Set();
    for (let round = 0; round < 20; round++) {
      const candidates = await page.locator('[id$="_SelectAll"]').evaluateAll((els) => els.map((e) => ({
        id: e.id, visible: !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length),
        disabled: 'disabled' in e ? !!e.disabled : false,
      })));
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
        const started = Date.now();
        const responsePromise = page.waitForResponse((response) => {
          try { const u = new URL(response.url()); return response.request().method() === 'POST' && u.pathname === currentPath; } catch { return false; }
        }, { timeout: 360000 });
        await downloadButton.click();
        const response = await responsePromise;
        result.generation = { status: response.status(), ok: response.ok(), elapsedMs: Date.now() - started };
        if (!response.ok()) throw new Error(`BDL bulk generation returned HTTP ${response.status()}`);
        for (let attempt = 1; attempt <= 36; attempt++) {
          ready = await findReadyExport(page);
          if (ready) { result.exportReadyPoll = attempt; break; }
          if (attempt < 36) await page.waitForTimeout(5000);
        }
        if (!ready) throw new Error('BDL package generated but no ready Export control appeared');
        result.archive = await captureExport(page, ready);
        result.status = 'downloaded_generated_export';
      }
    }
  }
  result.completedAt = new Date().toISOString();
  await saveResult();
  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  result.status = 'failed'; result.error = safe(error?.stack || error?.message || error); result.failedAt = new Date().toISOString();
  await saveResult(); console.error(result.error); process.exitCode = 1;
} finally { await browser.close(); }
