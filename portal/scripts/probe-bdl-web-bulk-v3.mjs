import { chromium } from '@playwright/test';
import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const email = process.env.GUS_BDL_WEB_EMAIL || '';
const password = process.env.GUS_BDL_WEB_PASSWORD || '';
if (!email || !password) throw new Error('Missing BDL web credentials');

const outDir = path.resolve('test-results/bdl-web-bulk-probe-v3');
await fs.mkdir(outDir, { recursive: true });
const safe = (s) => String(s ?? '').replaceAll(email, '[REDACTED_EMAIL]').replaceAll(password, '[REDACTED_PASSWORD]');
const result = {
  startedAt: new Date().toISOString(),
  mode: 'retrieve-existing-export-only',
  subgroup: 'P2695',
  name: 'Wyniki finansowe wg sekcji PKD 2007',
  login: {},
  polls: [],
  archive: null,
};

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ acceptDownloads: true, locale: 'pl-PL' });
const page = await context.newPage();

async function saveResult() {
  await fs.writeFile(path.join(outDir, 'result.json'), JSON.stringify(result, null, 2));
}

try {
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie', { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
  await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
  await page.locator('#ctl00_ContentPlaceHolder_SignIn').click();
  await page.waitForTimeout(2200);
  const loginBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.login = {
    success: !/Użytkownik:\s*Gość/i.test(loginBody) && /Użytkownik:/i.test(loginBody),
    urlAfter: page.url(),
  };
  if (!result.login.success) throw new Error('BDL login failed');

  // Retrieval only: do not create another export. Find an existing P2695
  // Export control. BDL implements the working control as javascript:..., so
  // it must be clicked in the browser rather than fetched as an HTTP URL.
  let ready = null;
  for (let attempt = 1; attempt <= 20; attempt++) {
    await page.goto('https://bdl.stat.gov.pl/bdl/start', { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
    await page.waitForTimeout(1200);

    const entries = await page.locator('[id$="_Export"]').evaluateAll((els) => els
      .filter((e) => /Wyniki finansowe wg sekcji PKD 2007/i.test((e.textContent || '').trim()))
      .map((e) => ({
        id: e.id,
        text: (e.textContent || '').trim().replace(/\s+/g, ' '),
        href: e.getAttribute('href'),
        onclick: e.getAttribute('onclick'),
        cls: typeof e.className === 'string' ? e.className : '',
        disabled: 'disabled' in e ? !!e.disabled : false,
        rowText: (e.closest('tr')?.innerText || '').trim().replace(/\s+/g, ' ').slice(0, 1000),
      })));

    const clickable = entries.filter((x) => !/aspNetDisabled/i.test(x.cls) && !x.disabled && (x.href || x.onclick));
    result.polls.push({ attempt, at: new Date().toISOString(), entries, clickableCount: clickable.length });
    if (clickable.length) {
      ready = clickable[0];
      break;
    }
    if (attempt < 20) await page.waitForTimeout(5000);
  }

  if (!ready) {
    result.completedAt = new Date().toISOString();
    result.archiveReady = false;
    await saveResult();
    throw new Error('Existing P2695 bulk export control not found');
  }

  result.archiveReady = true;
  result.readyEntry = ready;

  const exportControl = page.locator(`#${ready.id}`);
  if (!await exportControl.isVisible().catch(() => false)) {
    throw new Error(`BDL export control ${ready.id} is not visible`);
  }

  // This is the critical behavior: use the same browser click a human uses.
  const downloadPromise = page.waitForEvent('download', { timeout: 180000 }).catch(() => null);
  const popupPromise = context.waitForEvent('page', { timeout: 10000 }).catch(() => null);
  await exportControl.click();

  const download = await downloadPromise;
  const popup = await popupPromise;
  if (!download) {
    result.popupUrl = popup?.url?.() || null;
    throw new Error('BDL Export click did not produce a browser download');
  }

  const suggested = download.suggestedFilename() || 'bdl-P2695-bulk-export.zip';
  const filename = suggested.replace(/[^A-Za-z0-9._-]/g, '_');
  const target = path.join(outDir, `download-${filename}`);
  await download.saveAs(target);
  const body = await fs.readFile(target);

  result.archive = {
    filename,
    bytes: body.length,
    sha256: crypto.createHash('sha256').update(body).digest('hex'),
    firstBytesHex: body.subarray(0, 16).toString('hex'),
    suggestedFilename: suggested,
  };
  result.completedAt = new Date().toISOString();
  await saveResult();
  console.log(JSON.stringify({
    archiveReady: result.archiveReady,
    readyEntryId: ready.id,
    readyHrefScheme: ready.href?.split(':', 1)[0] || null,
    archive: result.archive,
  }, null, 2));

  if (body.length === 0) process.exitCode = 1;
} catch (error) {
  result.error = safe(error?.stack || error?.message || error);
  result.failedAt = new Date().toISOString();
  await saveResult();
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
