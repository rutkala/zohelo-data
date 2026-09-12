import { chromium } from '@playwright/test';
import fs from 'node:fs/promises';
import path from 'node:path';

const email = process.env.GUS_BDL_WEB_EMAIL || '';
const password = process.env.GUS_BDL_WEB_PASSWORD || '';
if (!email || !password) {
  throw new Error('Missing GUS_BDL_WEB_EMAIL or GUS_BDL_WEB_PASSWORD');
}

const outDir = path.resolve('test-results/bdl-web-bulk-probe');
await fs.mkdir(outDir, { recursive: true });

const safe = (value) => String(value ?? '')
  .replaceAll(email, '[REDACTED_EMAIL]')
  .replaceAll(password, '[REDACTED_PASSWORD]');

const events = [];
const result = {
  startedAt: new Date().toISOString(),
  login: { success: false },
  subgroup: { id: 'P1341', url: 'https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/3/7/1341' },
  selection: {},
  bulk: { clicked: false, directDownload: false, savedItemDetected: false },
};

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ acceptDownloads: true, locale: 'pl-PL' });
const page = await context.newPage();

const pushEvent = (kind, payload) => {
  events.push({ at: new Date().toISOString(), kind, ...payload });
};

page.on('request', (req) => {
  try {
    const u = new URL(req.url());
    if (u.hostname.endsWith('stat.gov.pl')) {
      pushEvent('request', {
        method: req.method(),
        path: u.pathname,
        resourceType: req.resourceType(),
      });
    }
  } catch {}
});
page.on('response', (res) => {
  try {
    const u = new URL(res.url());
    if (u.hostname.endsWith('stat.gov.pl')) {
      pushEvent('response', { status: res.status(), path: u.pathname });
    }
  } catch {}
});
page.on('dialog', async (dialog) => {
  pushEvent('dialog', { type: dialog.type(), message: safe(dialog.message()).slice(0, 500) });
  await dialog.accept();
});

async function visibleInputs() {
  return await page.locator('input, button, select').evaluateAll((els) => els.map((el) => ({
    tag: el.tagName,
    type: el.getAttribute('type'),
    id: el.id || null,
    name: el.getAttribute('name'),
    value: el.getAttribute('value'),
    placeholder: el.getAttribute('placeholder'),
    title: el.getAttribute('title'),
    text: (el.textContent || '').trim().slice(0, 120),
  })));
}

async function writeJson(name, data) {
  await fs.writeFile(path.join(outDir, name), JSON.stringify(data, null, 2));
}

async function clickAction(labelRegex) {
  const candidates = page.locator('input[type="submit"], input[type="button"], button, a');
  const count = await candidates.count();
  for (let i = 0; i < count; i++) {
    const el = candidates.nth(i);
    const text = `${await el.getAttribute('value') || ''} ${await el.textContent() || ''}`.trim();
    if (labelRegex.test(text) && await el.isVisible().catch(() => false)) {
      await el.click();
      return true;
    }
  }
  return false;
}

async function snapshot(name) {
  const body = safe((await page.locator('body').innerText().catch(() => ''))).slice(0, 30000);
  await writeJson(`${name}.json`, {
    url: page.url(),
    title: await page.title(),
    body,
    inputs: await visibleInputs(),
  });
}

try {
  // 1) Verify the newly configured web-account credentials without exposing them.
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie', { waitUntil: 'domcontentloaded', timeout: 45000 });
  await snapshot('01-login-page');

  const passwordInput = page.locator('input[type="password"]:visible').first();
  if (await passwordInput.count() === 0) throw new Error('BDL login password field not found');

  let emailInput = page.locator('input[type="email"]:visible').first();
  if (await emailInput.count() === 0) {
    const visibleText = page.locator('input[type="text"]:visible');
    const n = await visibleText.count();
    for (let i = 0; i < n; i++) {
      const el = visibleText.nth(i);
      const attrs = `${await el.getAttribute('name') || ''} ${await el.getAttribute('id') || ''} ${await el.getAttribute('placeholder') || ''}`;
      if (/mail|login|user|uzytk|użytk/i.test(attrs) || n === 1) {
        emailInput = el;
        break;
      }
    }
  }
  if (await emailInput.count() === 0) throw new Error('BDL login email field not found');

  await emailInput.fill(email);
  await passwordInput.fill(password);
  const clickedLogin = await clickAction(/logowanie|zaloguj/i);
  if (!clickedLogin) await passwordInput.press('Enter');
  await page.waitForLoadState('domcontentloaded', { timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(2000);

  const loggedBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.login.success = !/Użytkownik:\s*Gość/i.test(loggedBody) && !/Nieprawidłow|błędn.*has|invalid.*password/i.test(loggedBody);
  result.login.urlAfter = page.url();
  result.login.hasLogoutControl = /wyloguj/i.test(loggedBody);
  await snapshot('02-after-login');
  if (!result.login.success) throw new Error('BDL web login did not establish an authenticated session');

  // 2) Use a deliberately high-dimensional subgroup: P1341, individual years of age by sex, 1995-2025.
  await page.goto(result.subgroup.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForTimeout(2500);
  await snapshot('03-subgroup-before-selection');

  // Select all values in native selects and checkbox-backed widgets. Repeat because BDL loads dependent dimensions dynamically.
  let previousFingerprint = '';
  for (let round = 0; round < 8; round++) {
    await page.locator('select[multiple]').evaluateAll((sels) => {
      for (const sel of sels) {
        for (const option of sel.options) option.selected = true;
        sel.dispatchEvent(new Event('change', { bubbles: true }));
      }
    }).catch(() => {});

    await page.locator('input[type="checkbox"]').evaluateAll((boxes) => {
      for (const box of boxes) {
        if (!box.disabled && !box.checked) box.click();
      }
    }).catch(() => {});

    await page.waitForTimeout(1500);
    const body = await page.locator('body').innerText().catch(() => '');
    const countMatch = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
    const selectedInfo = countMatch ? Number(countMatch[1].replace(/\s/g, '')) : null;
    const checked = await page.locator('input[type="checkbox"]:checked').count().catch(() => 0);
    const optionSelected = await page.locator('select[multiple] option:checked').count().catch(() => 0);
    const fingerprint = `${selectedInfo}:${checked}:${optionSelected}`;
    result.selection[`round${round + 1}`] = { selectedInfo, checked, optionSelected };
    if (fingerprint === previousFingerprint) break;
    previousFingerprint = fingerprint;
  }
  await snapshot('04-subgroup-after-selection');

  const finalBody = await page.locator('body').innerText().catch(() => '');
  const finalCount = finalBody.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  result.selection.finalSelectedInfo = finalCount ? Number(finalCount[1].replace(/\s/g, '')) : null;
  result.selection.exceedsInteractiveLimit = (result.selection.finalSelectedInfo ?? 0) > 3500;

  // 3) Trigger the documented server-side "Pobierz" path. Capture a direct download if it occurs.
  const downloadPromise = page.waitForEvent('download', { timeout: 90000 }).catch(() => null);
  result.bulk.clicked = await clickAction(/pobierz|download/i);
  if (!result.bulk.clicked) throw new Error('BDL Pobierz action not found');

  const download = await downloadPromise;
  if (download) {
    const suggested = download.suggestedFilename().replace(/[^A-Za-z0-9._-]/g, '_');
    const target = path.join(outDir, `download-${suggested}`);
    await download.saveAs(target);
    const stat = await fs.stat(target);
    result.bulk.directDownload = true;
    result.bulk.download = { suggestedFilename: suggested, bytes: stat.size };
  } else {
    await page.waitForTimeout(3000);
  }
  await snapshot('05-after-pobierz');

  // 4) Inspect the authenticated "Pobrane podgrupy" library. Do not delete or alter any items.
  await page.goto('https://bdl.stat.gov.pl/BDL/start', { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForTimeout(2500);
  const startBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.bulk.savedItemDetected = /Pobrane podgrupy/i.test(startBody) && /Ludność wg pojedynczych roczników wieku i płci/i.test(startBody);
  result.bulk.candidateLinks = await page.locator('a').evaluateAll((links) => links
    .map((a) => ({ text: (a.textContent || '').trim(), href: a.href || '' }))
    .filter((x) => /pobran|podgrup|1341|pojedynczych roczników/i.test(`${x.text} ${x.href}`))
    .slice(0, 50));
  await snapshot('06-downloaded-subgroups');

  result.completedAt = new Date().toISOString();
  await writeJson('result.json', result);
  await writeJson('network-events.json', events.slice(-2000));

  console.log(JSON.stringify({
    loginSuccess: result.login.success,
    finalSelectedInfo: result.selection.finalSelectedInfo,
    exceedsInteractiveLimit: result.selection.exceedsInteractiveLimit,
    clickedPobierz: result.bulk.clicked,
    directDownload: result.bulk.directDownload,
    downloadedBytes: result.bulk.download?.bytes ?? null,
    savedItemDetected: result.bulk.savedItemDetected,
    candidateLinkCount: result.bulk.candidateLinks?.length ?? 0,
  }, null, 2));
} catch (error) {
  result.error = safe(error?.stack || error?.message || error);
  result.failedAt = new Date().toISOString();
  await writeJson('result.json', result);
  await writeJson('network-events.json', events.slice(-2000));
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
