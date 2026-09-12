import { chromium } from '@playwright/test';
import fs from 'node:fs/promises';
import path from 'node:path';

const email = process.env.GUS_BDL_WEB_EMAIL || '';
const password = process.env.GUS_BDL_WEB_PASSWORD || '';
if (!email || !password) throw new Error('Missing BDL web credentials');

const outDir = path.resolve('test-results/bdl-web-bulk-probe-v2');
await fs.mkdir(outDir, { recursive: true });
const safe = (s) => String(s ?? '').replaceAll(email, '[REDACTED_EMAIL]').replaceAll(password, '[REDACTED_PASSWORD]');
const events = [];
const result = { startedAt: new Date().toISOString(), login: {}, subgroup: {}, bulk: {} };

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ acceptDownloads: true, locale: 'pl-PL' });
const page = await context.newPage();

page.on('request', req => {
  try {
    const u = new URL(req.url());
    if (u.hostname.endsWith('stat.gov.pl')) events.push({ kind: 'request', method: req.method(), path: u.pathname, type: req.resourceType() });
  } catch {}
});
page.on('response', res => {
  try {
    const u = new URL(res.url());
    if (u.hostname.endsWith('stat.gov.pl')) events.push({ kind: 'response', status: res.status(), path: u.pathname });
  } catch {}
});
page.on('dialog', async d => { events.push({ kind: 'dialog', type: d.type(), message: safe(d.message()).slice(0, 500) }); await d.accept(); });

async function save(name) {
  const body = safe(await page.locator('body').innerText().catch(() => '')).slice(0, 40000);
  const controls = await page.locator('input,button,select,a').evaluateAll(els => els.map(el => ({
    tag: el.tagName, id: el.id || null, name: el.getAttribute('name'), type: el.getAttribute('type'),
    value: el.getAttribute('value'), text: (el.textContent || '').trim().slice(0, 160), href: el.getAttribute('href'),
    onclick: (el.getAttribute('onclick') || '').slice(0, 500)
  })).slice(0, 1000));
  await fs.writeFile(path.join(outDir, `${name}.json`), JSON.stringify({ url: page.url(), title: await page.title(), body, controls }, null, 2));
}
async function writeResult() {
  await fs.writeFile(path.join(outDir, 'result.json'), JSON.stringify(result, null, 2));
  await fs.writeFile(path.join(outDir, 'network-events.json'), JSON.stringify(events.slice(-4000), null, 2));
}

try {
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie', { waitUntil: 'networkidle', timeout: 60000 });
  await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
  await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
  const signIn = page.locator('#ctl00_ContentPlaceHolder_SignIn');
  result.login.signInOnclick = safe(await signIn.getAttribute('onclick'));
  await save('01-before-login-submit');

  const beforePosts = events.filter(x => x.kind === 'request' && x.method === 'POST').length;
  await signIn.click();
  await page.waitForTimeout(3500);
  let afterPosts = events.filter(x => x.kind === 'request' && x.method === 'POST').length;

  if (afterPosts === beforePosts) {
    result.login.usedPostbackFallback = true;
    await page.evaluate(() => {
      if (typeof window.__doPostBack === 'function') window.__doPostBack('ctl00$ContentPlaceHolder$SignIn', '');
      else document.querySelector('form')?.requestSubmit();
    });
    await page.waitForTimeout(3500);
    afterPosts = events.filter(x => x.kind === 'request' && x.method === 'POST').length;
  }

  const loginBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.login.postRequests = afterPosts - beforePosts;
  result.login.urlAfter = page.url();
  result.login.guestStillVisible = /Użytkownik:\s*Gość/i.test(loginBody);
  result.login.logoutVisible = /Wyloguj/i.test(loginBody);
  result.login.errorText = (loginBody.match(/(?:nieprawidłow|błędn|potwierd|aktywow)[^\n]{0,200}/ig) || []).slice(0, 10);
  result.login.success = !result.login.guestStillVisible && (result.login.logoutVisible || !/logowanie/i.test(page.url()));
  await save('02-after-login-submit');
  if (!result.login.success) throw new Error(`Login failed after ${result.login.postRequests} POST request(s)`);

  const subgroupUrl = 'https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/3/7/1341';
  await page.goto(subgroupUrl, { waitUntil: 'networkidle', timeout: 60000 });
  result.subgroup.url = page.url();
  result.subgroup.authenticated = !/Użytkownik:\s*Gość/i.test(await page.locator('body').innerText().catch(() => ''));
  await save('03-subgroup');

  const allControls = await page.locator('input,button,a').evaluateAll(els => els.map(el => ({
    id: el.id || '', value: el.getAttribute('value') || '', text: (el.textContent || '').trim(), title: el.getAttribute('title') || ''
  })));
  result.subgroup.selectionControls = allControls.filter(x => /wszyst|zaznacz|wybierz|pobierz|download/i.test(`${x.id} ${x.value} ${x.text} ${x.title}`)).slice(0, 100);

  // Click only explicit "select all" controls; do not guess individual data choices.
  for (const c of result.subgroup.selectionControls.filter(x => /wszyst/i.test(`${x.value} ${x.text} ${x.title}`))) {
    if (!c.id) continue;
    const loc = page.locator(`#${CSS.escape(c.id)}`);
    if (await loc.isVisible().catch(() => false)) {
      await loc.click().catch(() => {});
      await page.waitForTimeout(1200);
    }
  }
  await save('04-after-select-all');
  const body = await page.locator('body').innerText().catch(() => '');
  const match = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  result.bulk.selectedInformation = match ? Number(match[1].replace(/\s/g, '')) : null;

  const candidates = page.locator('button:visible,input[type="button"]:visible,input[type="submit"]:visible,a:visible');
  let downloadControl = null;
  for (let i = 0; i < await candidates.count(); i++) {
    const el = candidates.nth(i);
    const label = `${await el.getAttribute('value') || ''} ${await el.textContent() || ''} ${await el.getAttribute('title') || ''}`;
    if (/pobierz|download/i.test(label)) { downloadControl = el; break; }
  }

  if (downloadControl) {
    result.bulk.downloadControlFound = true;
    const downloadPromise = page.waitForEvent('download', { timeout: 30000 }).catch(() => null);
    await downloadControl.click();
    await page.waitForTimeout(3000);
    const dl = await downloadPromise;
    if (dl) {
      const fn = dl.suggestedFilename().replace(/[^A-Za-z0-9._-]/g, '_');
      const target = path.join(outDir, `download-${fn}`);
      await dl.saveAs(target);
      result.bulk.directDownload = { filename: fn, bytes: (await fs.stat(target)).size };
    }
  } else result.bulk.downloadControlFound = false;

  await save('05-after-download-action');
  result.completedAt = new Date().toISOString();
  await writeResult();
  console.log(JSON.stringify(result, null, 2));
} catch (e) {
  result.error = safe(e?.stack || e?.message || e);
  result.failedAt = new Date().toISOString();
  await writeResult();
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
