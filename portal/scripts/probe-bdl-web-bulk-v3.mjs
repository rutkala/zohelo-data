import { chromium } from '@playwright/test';
import fs from 'node:fs/promises';
import path from 'node:path';

const email = process.env.GUS_BDL_WEB_EMAIL || '';
const password = process.env.GUS_BDL_WEB_PASSWORD || '';
if (!email || !password) throw new Error('Missing BDL web credentials');

const outDir = path.resolve('test-results/bdl-web-bulk-probe-v3');
await fs.mkdir(outDir, { recursive: true });
const safe = (s) => String(s ?? '').replaceAll(email, '[REDACTED_EMAIL]').replaceAll(password, '[REDACTED_PASSWORD]');
const events = [];
const result = { startedAt: new Date().toISOString(), login: {}, dimensions: [], geography: {}, bulk: {} };

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ acceptDownloads: true, locale: 'pl-PL' });
const page = await context.newPage();

page.on('request', (r) => {
  try {
    const u = new URL(r.url());
    if (u.hostname.endsWith('stat.gov.pl')) events.push({ kind: 'request', method: r.method(), path: u.pathname, type: r.resourceType() });
  } catch {}
});
page.on('response', (r) => {
  try {
    const u = new URL(r.url());
    if (u.hostname.endsWith('stat.gov.pl')) events.push({ kind: 'response', status: r.status(), path: u.pathname });
  } catch {}
});
page.on('dialog', async (d) => {
  events.push({ kind: 'dialog', type: d.type(), message: safe(d.message()).slice(0, 500) });
  await d.accept();
});

async function write(name, data) {
  await fs.writeFile(path.join(outDir, name), JSON.stringify(data, null, 2));
}
async function save() {
  await write('result.json', result);
  await write('network-events.json', events.slice(-12000));
}
async function snapshot(name) {
  await write(`${name}.json`, {
    url: page.url(),
    title: await page.title(),
    body: safe(await page.locator('body').innerText().catch(() => '')).slice(0, 120000),
    controls: await page.locator('button,input,a,li,span').evaluateAll((els) => els.map((e) => ({
      id: e.id || null,
      text: (e.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 250),
      value: e.getAttribute('value'),
      title: e.getAttribute('title'),
      disabled: 'disabled' in e ? !!e.disabled : null,
      cls: typeof e.className === 'string' ? e.className : '',
      visible: !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length),
    })).filter((x) => /dalej|pobierz|download|export|podgrup|zaznacz|wybran|dodaj|transfer|rlb/i.test(`${x.id} ${x.text} ${x.value} ${x.title} ${x.cls}`)).slice(0, 1500)),
  });
}
async function dimensionState() {
  const body = await page.locator('body').innerText().catch(() => '');
  const m = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  return {
    selectedInformation: m ? Number(m[1].replace(/\s/g, '')) : null,
    counters: [...body.matchAll(/Zaznaczonych:\s*([0-9]+)\/([0-9]+)/gi)].map((x) => ({ selected: +x[1], total: +x[2] })),
  };
}
async function selectDimensionAll(id) {
  const button = page.locator(`#${id}`);
  if (!await button.isEnabled().catch(() => false)) throw new Error(`${id} disabled`);
  await button.click();
  await page.waitForTimeout(2200);
  return dimensionState();
}
async function clickEnabledNext() {
  for (const id of ['ctl00_ContentPlaceHolder_dalej1', 'ctl00_ContentPlaceHolder_dalej2']) {
    const button = page.locator(`#${id}`);
    if (await button.isVisible().catch(() => false) && await button.isEnabled().catch(() => false)) {
      await button.click();
      return id;
    }
  }
  return null;
}
async function geoCounts() {
  const body = await page.locator('body').innerText().catch(() => '');
  return {
    selectedCount: Number(body.match(/Wybranych elementów:\s*([0-9]+)/i)?.[1] || 0),
    availableCount: Number(body.match(/Elementów do wyboru:\s*([0-9]+)/i)?.[1] || 0),
  };
}

try {
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie', { waitUntil: 'networkidle', timeout: 60000 });
  await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
  await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
  await page.locator('#ctl00_ContentPlaceHolder_SignIn').click();
  await page.waitForTimeout(2300);
  const loginBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.login = {
    success: !/Użytkownik:\s*Gość/i.test(loginBody) && /Użytkownik:/i.test(loginBody),
    urlAfter: page.url(),
  };
  if (!result.login.success) throw new Error('BDL login failed');

  // P1341: 31 years x 3 sexes x 30 age values = 2,790 information combinations.
  await page.goto('https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/3/7/1341', { waitUntil: 'networkidle', timeout: 60000 });
  result.dimensions.push({ name: 'years', state: await selectDimensionAll('ctl00_ContentPlaceHolder_lata_SelectAll') });
  result.dimensions.push({ name: 'sex', state: await selectDimensionAll('ctl00_ContentPlaceHolder_wym1_SelectAll') });
  result.dimensions.push({ name: 'age', state: await selectDimensionAll('ctl00_ContentPlaceHolder_wym2_SelectAll') });
  result.bulk.selectedInformation = (await dimensionState()).selectedInformation;
  if (!await clickEnabledNext()) throw new Error('Dimension-stage Dalej disabled');
  await page.waitForTimeout(3000);

  await page.waitForFunction(() => /Jednostki terytorialne/i.test(document.body?.innerText || ''), null, { timeout: 30000 });
  result.geography.initial = await geoCounts();
  await snapshot('01-geography-initial');

  // Reproduce the real UI, but keep the proof bounded:
  // Zaznacz -> Zaznacz województwa -> > (add selected) -> confirm 16 -> Dalej.
  const menuButton = page.locator('#ctl00_ContentPlaceHolder_terytList_MenuButton');
  if (!await menuButton.isVisible().catch(() => false)) throw new Error('Geography Zaznacz menu button not found');
  await menuButton.click();
  await page.waitForTimeout(500);

  const voivodeshipItem = page.locator('.rmItem:visible').filter({ hasText: /^Zaznacz województwa$/ }).first();
  if (!await voivodeshipItem.isVisible().catch(() => false)) throw new Error('Geography "Zaznacz województwa" menu item not found');
  await voivodeshipItem.click();
  await page.waitForTimeout(1000);
  result.geography.afterHighlight = await geoCounts();
  await snapshot('02-geography-voivodeships-highlighted');

  // Telerik RadListBox uses rlbTransferTo for the single-right-arrow transfer button.
  let transferSelected = page.locator('xpath=//*[contains(concat(" ", normalize-space(@class), " "), " rlbTransferTo ") and not(contains(concat(" ", normalize-space(@class), " "), " rlbTransferAllTo "))]').filter({ visible: true }).first();
  if (!await transferSelected.isVisible().catch(() => false)) {
    transferSelected = page.locator('[title*="Dodaj zaznaczone"][title*="wybranych"]:visible').first();
  }
  if (!await transferSelected.isVisible().catch(() => false)) {
    const transferCandidates = await page.locator('[class*="rlbTransfer"]:visible,[title*="wybranych"]:visible').evaluateAll((els) => els.map((e) => ({
      id: e.id || null,
      title: e.getAttribute('title'),
      cls: typeof e.className === 'string' ? e.className : '',
      text: (e.textContent || '').trim(),
    })));
    result.geography.transferCandidates = transferCandidates;
    throw new Error(`Geography transfer-selected (>) control not found: ${JSON.stringify(transferCandidates)}`);
  }
  result.geography.transferId = await transferSelected.getAttribute('id');
  result.geography.transferTitle = await transferSelected.getAttribute('title');
  result.geography.transferClass = await transferSelected.getAttribute('class');
  await transferSelected.click();

  await page.waitForFunction(() => /Wybranych elementów:\s*16/i.test(document.body?.innerText || ''), null, { timeout: 15000 });
  result.geography.afterTransfer = await geoCounts();
  await snapshot('03-geography-transferred');

  const beforeGeoNext = page.url();
  const geoNextId = await clickEnabledNext();
  result.geography.nextId = geoNextId;
  if (!geoNextId) throw new Error(`Geography Dalej did not enable after transfer: ${JSON.stringify(result.geography.afterTransfer)}`);
  await page.waitForTimeout(6000);
  result.geography.transition = { before: beforeGeoNext, after: page.url() };
  result.bulk.afterGeographyUrl = page.url();
  await snapshot('04-after-geography-next');

  // Discover and trigger the resulting bulk-export action.
  const candidates = page.locator('button:visible,input[type="button"]:visible,input[type="submit"]:visible,a:visible');
  result.bulk.exportCandidates = [];
  let exportControl = null;
  for (let i = 0; i < await candidates.count(); i++) {
    const e = candidates.nth(i);
    const label = `${await e.getAttribute('value') || ''} ${await e.textContent() || ''} ${await e.getAttribute('title') || ''}`.trim();
    if (/pobierz|download|eksport|export/i.test(label)) {
      const enabled = await e.isEnabled().catch(() => false);
      result.bulk.exportCandidates.push({ label, id: await e.getAttribute('id'), enabled });
      if (!exportControl && enabled) exportControl = e;
    }
  }

  if (exportControl) {
    const before = page.url();
    const downloadPromise = page.waitForEvent('download', { timeout: 60000 }).catch(() => null);
    await exportControl.click();
    await page.waitForTimeout(8000);
    const download = await downloadPromise;
    result.bulk.exportClick = { beforeUrl: before, afterUrl: page.url() };
    if (download) {
      const filename = download.suggestedFilename().replace(/[^A-Za-z0-9._-]/g, '_');
      const target = path.join(outDir, `download-${filename}`);
      await download.saveAs(target);
      result.bulk.directDownload = { filename, bytes: (await fs.stat(target)).size };
    } else {
      result.bulk.directDownload = null;
    }
    await snapshot('05-after-export');
  } else {
    result.bulk.noEnabledExportControl = true;
  }

  await page.goto('https://bdl.stat.gov.pl/bdl/start', { waitUntil: 'networkidle', timeout: 60000 });
  await page.waitForTimeout(2500);
  const startBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.bulk.downloadedSubgroupsVisible = /Pobrane podgrupy/i.test(startBody);
  result.bulk.testSubgroupVisible = /Ludność wg pojedynczych roczników wieku i płci/i.test(startBody);
  result.bulk.bulkLinks = await page.locator('a').evaluateAll((as) => as.map((a) => ({ text: (a.textContent || '').trim(), href: a.href || '' })).filter((x) => /pobran|podgrup|1341|roczników wieku/i.test(`${x.text} ${x.href}`)).slice(0, 100));
  await snapshot('06-start-after-export');

  result.completedAt = new Date().toISOString();
  await save();
  console.log(JSON.stringify(result, null, 2));
} catch (error) {
  result.error = safe(error?.stack || error?.message || error);
  result.failedAt = new Date().toISOString();
  await save();
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
