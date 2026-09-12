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
const result = {
  startedAt: new Date().toISOString(),
  login: {},
  subgroup: {
    id: 'P2695',
    name: 'Wyniki finansowe wg sekcji PKD 2007',
    url: 'https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/43/418/2695',
  },
  dimensions: [],
  bulk: {},
};

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
  await write('network-events.json', events.slice(-15000));
}
async function snapshot(name) {
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      await page.waitForLoadState('domcontentloaded', { timeout: 10000 }).catch(() => {});
      const body = safe(await page.locator('body').innerText().catch(() => '')).slice(0, 120000);
      const controls = await page.locator('button,input,a,li,span').evaluateAll((els) => els.map((e) => ({
        id: e.id || null,
        text: (e.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 250),
        value: e.getAttribute('value'),
        title: e.getAttribute('title'),
        href: e.getAttribute('href'),
        disabled: 'disabled' in e ? !!e.disabled : null,
        cls: typeof e.className === 'string' ? e.className : '',
        visible: !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length),
      })).filter((x) => /dalej|pobierz|download|pobrane|podgrup|selectall|zaznacz|csv|xlsx?|zip/i.test(`${x.id} ${x.text} ${x.value} ${x.title} ${x.href} ${x.cls}`)).slice(0, 1500));
      await write(`${name}.json`, { url: page.url(), title: await page.title(), body, controls });
      return;
    } catch (error) {
      if (!/Execution context was destroyed|navigation/i.test(String(error)) || attempt === 3) throw error;
      await page.waitForTimeout(1500);
    }
  }
}
async function infoState() {
  const body = await page.locator('body').innerText().catch(() => '');
  const m = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  return {
    selectedInformation: m ? Number(m[1].replace(/\s/g, '')) : null,
    counters: [...body.matchAll(/Zaznaczonych:\s*([0-9]+)\/([0-9]+)/gi)].map((x) => ({ selected: +x[1], total: +x[2] })),
  };
}
async function selectAll(id, name) {
  const button = page.locator(`#${id}`);
  if (!await button.isVisible().catch(() => false)) throw new Error(`${name} select-all not found: ${id}`);
  if (!await button.isEnabled().catch(() => false)) throw new Error(`${name} select-all disabled: ${id}`);
  await button.click();
  await page.waitForTimeout(2200);
  const state = await infoState();
  result.dimensions.push({ name, action: 'select-all', id, state });
  return state;
}
async function buttonState(id) {
  const b = page.locator(`#${id}`);
  return {
    exists: await b.count() > 0,
    visible: await b.isVisible().catch(() => false),
    enabled: await b.isEnabled().catch(() => false),
    value: await b.getAttribute('value').catch(() => null),
  };
}

try {
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie', { waitUntil: 'networkidle', timeout: 60000 });
  await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
  await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
  await page.locator('#ctl00_ContentPlaceHolder_SignIn').click();
  await page.waitForTimeout(2200);
  const loginBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.login = { success: !/Użytkownik:\s*Gość/i.test(loginBody) && /Użytkownik:/i.test(loginBody), urlAfter: page.url() };
  if (!result.login.success) throw new Error('BDL login failed');

  // Use BDL's own documented over-limit example subgroup P2695.
  await page.goto(result.subgroup.url, { waitUntil: 'networkidle', timeout: 60000 });
  const subgroupBody = await page.locator('body').innerText().catch(() => '');
  result.subgroup.loaded = /P2695|Wyniki finansowe wg sekcji PKD 2007/i.test(subgroupBody);
  result.subgroup.pageTitle = await page.title();
  if (!result.subgroup.loaded) {
    await snapshot('01-subgroup-unexpected');
    throw new Error(`P2695 page did not load as expected: ${page.url()}`);
  }
  await snapshot('01-subgroup-initial');

  // Use native BDL callbacks for the cascading dimensions.
  await selectAll('ctl00_ContentPlaceHolder_lata_SelectAll', 'years');
  await selectAll('ctl00_ContentPlaceHolder_wym1_SelectAll', 'dimension-1');
  await selectAll('ctl00_ContentPlaceHolder_wym2_SelectAll', 'dimension-2');

  // Keep the proof reasonably bounded: select only as many values of the last
  // dimension as needed to cross BDL's 3,500-information threshold.
  const finalRoot = page.locator('#ctl00_ContentPlaceHolder_wym3');
  if (!await finalRoot.isVisible().catch(() => false)) {
    // If the current BDL schema differs from the historic manual, fall back to native select-all.
    await selectAll('ctl00_ContentPlaceHolder_wym3_SelectAll', 'dimension-3');
  } else {
    const items = finalRoot.locator('li.rlbItem');
    const count = await items.count();
    if (!count) throw new Error('P2695 final dimension contains no selectable items');
    for (let i = 0; i < count; i++) {
      await items.nth(i).click(i === 0 ? {} : { modifiers: ['Control'] });
      await page.waitForTimeout(350);
      const state = await infoState();
      if ((state.selectedInformation ?? 0) > 3500) {
        result.dimensions.push({ name: 'dimension-3', action: 'partial', selectedRows: i + 1, state });
        break;
      }
      if (i === count - 1) {
        result.dimensions.push({ name: 'dimension-3', action: 'partial-all', selectedRows: count, state });
      }
    }
  }

  result.bulk.beforeDownload = await infoState();
  result.bulk.thresholdExceeded = (result.bulk.beforeDownload.selectedInformation ?? 0) > 3500;
  result.bulk.download1 = await buttonState('ctl00_ContentPlaceHolder_download1');
  result.bulk.download2 = await buttonState('ctl00_ContentPlaceHolder_download2');
  result.bulk.next1 = await buttonState('ctl00_ContentPlaceHolder_dalej1');
  result.bulk.next2 = await buttonState('ctl00_ContentPlaceHolder_dalej2');
  await snapshot('02-over-limit-selection');

  if (!result.bulk.thresholdExceeded) throw new Error(`Could not exceed BDL bulk threshold: ${JSON.stringify(result.bulk.beforeDownload)}`);

  let downloadButton = null;
  for (const id of ['ctl00_ContentPlaceHolder_download1', 'ctl00_ContentPlaceHolder_download2']) {
    const b = page.locator(`#${id}`);
    if (await b.isVisible().catch(() => false) && await b.isEnabled().catch(() => false)) {
      downloadButton = b;
      result.bulk.downloadButtonId = id;
      break;
    }
  }
  if (!downloadButton) throw new Error(`BDL Pobierz did not enable above 3500: ${JSON.stringify({ download1: result.bulk.download1, download2: result.bulk.download2 })}`);

  const beforeClickUrl = page.url();
  const directDownloadPromise = page.waitForEvent('download', { timeout: 15000 }).catch(() => null);
  await downloadButton.click();
  await page.waitForTimeout(5000);
  result.bulk.afterDownloadClick = { before: beforeClickUrl, after: page.url() };
  const directDownload = await directDownloadPromise;
  if (directDownload) {
    const filename = directDownload.suggestedFilename().replace(/[^A-Za-z0-9._-]/g, '_');
    const target = path.join(outDir, `download-${filename}`);
    await directDownload.saveAs(target);
    result.bulk.directDownload = { filename, bytes: (await fs.stat(target)).size };
  } else {
    result.bulk.directDownload = null;
  }
  await snapshot('03-after-pobierz');

  // BDL documents bulk packages as appearing under "Pobrane podgrupy".
  // Poll the authenticated start page for up to 3 minutes for the generated entry.
  result.bulk.polls = [];
  for (let attempt = 1; attempt <= 18; attempt++) {
    await page.goto('https://bdl.stat.gov.pl/bdl/start', { waitUntil: 'networkidle', timeout: 60000 });
    await page.waitForTimeout(1200);
    const body = safe(await page.locator('body').innerText().catch(() => ''));
    const links = await page.locator('a').evaluateAll((as) => as.map((a) => ({
      text: (a.textContent || '').trim().replace(/\s+/g, ' '),
      href: a.href || '',
      title: a.getAttribute('title'),
    })).filter((x) => /2695|Wyniki finansowe|pobran|download|\.zip|\.csv|\.xlsx?/i.test(`${x.text} ${x.href} ${x.title || ''}`)).slice(0, 100));
    const hasPanel = /Pobrane podgrupy/i.test(body);
    const hasSubgroup = /Wyniki finansowe wg sekcji PKD 2007/i.test(body) || links.some((x) => /2695|Wyniki finansowe/i.test(`${x.text} ${x.href}`));
    result.bulk.polls.push({ attempt, hasPanel, hasSubgroup, links });
    if (hasSubgroup) {
      result.bulk.generatedEntryDetected = true;
      result.bulk.generatedEntryPoll = attempt;
      result.bulk.generatedLinks = links;
      await snapshot('04-generated-entry');
      break;
    }
    if (attempt < 18) await page.waitForTimeout(10000);
  }
  if (!result.bulk.generatedEntryDetected) {
    result.bulk.generatedEntryDetected = false;
    await snapshot('04-start-final');
  }

  result.completedAt = new Date().toISOString();
  await save();
  console.log(JSON.stringify({
    login: result.login.success,
    subgroupLoaded: result.subgroup.loaded,
    selectedInformation: result.bulk.beforeDownload?.selectedInformation,
    thresholdExceeded: result.bulk.thresholdExceeded,
    downloadButtonId: result.bulk.downloadButtonId,
    afterDownloadUrl: result.bulk.afterDownloadClick?.after,
    directDownload: result.bulk.directDownload,
    generatedEntryDetected: result.bulk.generatedEntryDetected,
    generatedEntryPoll: result.bulk.generatedEntryPoll ?? null,
    generatedLinkCount: result.bulk.generatedLinks?.length ?? 0,
  }, null, 2));

  // Treat successful triggering as the proof; async generation may legitimately take longer.
  if (!result.bulk.downloadButtonId) process.exitCode = 1;
} catch (error) {
  result.error = safe(error?.stack || error?.message || error);
  result.failedAt = new Date().toISOString();
  await save();
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
