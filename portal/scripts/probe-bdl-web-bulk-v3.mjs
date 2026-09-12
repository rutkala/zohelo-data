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

function filenameFromDisposition(value) {
  if (!value) return null;
  const utf = value.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf) return decodeURIComponent(utf[1]).replace(/[^A-Za-z0-9._-]/g, '_');
  const plain = value.match(/filename="?([^";]+)"?/i);
  return plain ? plain[1].replace(/[^A-Za-z0-9._-]/g, '_') : null;
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

  // Do not create another export. Re-open the authenticated home page until an
  // already-generated P2695 Export control receives a real href.
  let ready = null;
  for (let attempt = 1; attempt <= 45; attempt++) {
    await page.goto('https://bdl.stat.gov.pl/bdl/start', { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
    await page.waitForTimeout(1200);

    const entries = await page.locator('[id$="_Export"]').evaluateAll((els) => els
      .filter((e) => /Wyniki finansowe wg sekcji PKD 2007/i.test((e.textContent || '').trim()))
      .map((e) => ({
        id: e.id,
        text: (e.textContent || '').trim().replace(/\s+/g, ' '),
        href: e.getAttribute('href'),
        cls: typeof e.className === 'string' ? e.className : '',
        rowText: (e.closest('tr')?.innerText || '').trim().replace(/\s+/g, ' ').slice(0, 1000),
      })));

    const enabled = entries.filter((x) => x.href && !/aspNetDisabled/i.test(x.cls));
    result.polls.push({ attempt, at: new Date().toISOString(), entries, enabledCount: enabled.length });
    if (enabled.length) {
      // The grid is normally newest-first; use the first ready matching entry.
      ready = enabled[0];
      break;
    }
    if (attempt < 45) await page.waitForTimeout(10000);
  }

  if (!ready) {
    result.completedAt = new Date().toISOString();
    result.archiveReady = false;
    await saveResult();
    throw new Error('Existing P2695 bulk export is still not downloadable');
  }

  result.archiveReady = true;
  result.readyEntry = ready;
  const archiveUrl = new URL(ready.href, 'https://bdl.stat.gov.pl').href;

  // BrowserContext.request shares the authenticated browser cookies, so fetch
  // the archive directly rather than relying on a visible/collapsed sidebar link.
  const response = await context.request.get(archiveUrl, { timeout: 180000 });
  const body = await response.body();
  const headers = response.headers();
  const filename = filenameFromDisposition(headers['content-disposition']) || 'bdl-P2695-bulk-export.zip';
  const target = path.join(outDir, `download-${filename}`);
  await fs.writeFile(target, body);

  result.archive = {
    urlPath: new URL(archiveUrl).pathname,
    status: response.status(),
    ok: response.ok(),
    filename,
    bytes: body.length,
    contentType: headers['content-type'] || null,
    contentDisposition: headers['content-disposition'] || null,
    sha256: crypto.createHash('sha256').update(body).digest('hex'),
    firstBytesHex: body.subarray(0, 16).toString('hex'),
  };
  result.completedAt = new Date().toISOString();
  await saveResult();
  console.log(JSON.stringify(result.archive, null, 2));

  if (!response.ok() || body.length === 0) process.exitCode = 1;
} catch (error) {
  result.error = safe(error?.stack || error?.message || error);
  result.failedAt = new Date().toISOString();
  await saveResult();
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
