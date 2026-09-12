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
    if (u.hostname.endsWith('stat.gov.pl')) events.push({ at: new Date().toISOString(), kind: 'request', method: r.method(), path: u.pathname, type: r.resourceType() });
  } catch {}
});
page.on('response', (r) => {
  try {
    const u = new URL(r.url());
    if (u.hostname.endsWith('stat.gov.pl')) events.push({ at: new Date().toISOString(), kind: 'response', status: r.status(), path: u.pathname });
  } catch {}
});
page.on('dialog', async (d) => {
  events.push({ at: new Date().toISOString(), kind: 'dialog', type: d.type(), message: safe(d.message()).slice(0, 500) });
  await d.accept();
});

async function write(name, data) {
  await fs.writeFile(path.join(outDir, name), JSON.stringify(data, null, 2));
}
async function save() {
  await write('result.json', result);
  await write('network-events.json', events.slice(-20000));
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
      })).filter((x) => /dalej|pobierz|download|pobrane|podgrup|selectall|zaznacz|csv|xlsx?|zip|export|rlbItem/i.test(`${x.id} ${x.text} ${x.value} ${x.title} ${x.href} ${x.cls}`)).slice(0, 2000));
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
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie', { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
  await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
  await page.locator('#ctl00_ContentPlaceHolder_SignIn').click();
  await page.waitForTimeout(2200);
  const loginBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.login = { success: !/Użytkownik:\s*Gość/i.test(loginBody) && /Użytkownik:/i.test(loginBody), urlAfter: page.url() };
  if (!result.login.success) throw new Error('BDL login failed');

  await page.goto(result.subgroup.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForTimeout(1800);
  const subgroupBody = await page.locator('body').innerText().catch(() => '');
  result.subgroup.loaded = /P2695|Wyniki finansowe wg sekcji PKD 2007/i.test(subgroupBody);
  result.subgroup.pageTitle = await page.title();
  if (!result.subgroup.loaded) throw new Error(`P2695 page did not load as expected: ${page.url()}`);

  // Populate the first three dimensions using BDL's native callbacks.
  await selectAll('ctl00_ContentPlaceHolder_lata_SelectAll', 'years');
  await selectAll('ctl00_ContentPlaceHolder_wym1_SelectAll', 'periods');
  await selectAll('ctl00_ContentPlaceHolder_wym2_SelectAll', 'subject');

  // The final visible Telerik list is the PKD-section dimension. Current counts
  // are 20 x 4 x 51, so one section should produce 4,080 information items.
  const visibleLists = page.locator('ul.rlbList:visible');
  const listCount = await visibleLists.count();
  result.dimensions.push({ name: 'visible-list-count', count: listCount });
  let finalSelectionSucceeded = false;
  if (listCount > 0) {
    const finalList = visibleLists.nth(listCount - 1);
    const finalItems = finalList.locator('li.rlbItem:visible');
    const finalCount = await finalItems.count();
    result.dimensions.push({ name: 'sections-meta', listCount: finalCount });
    if (finalCount > 0) {
      let selectedRows = 0;
      let state = await infoState();
      for (let i = 0; i < finalCount && (state.selectedInformation ?? 0) <= 3500; i++) {
        await finalItems.nth(i).click(i === 0 ? {} : { modifiers: ['Control'] });
        selectedRows++;
        await page.waitForTimeout(1400);
        state = await infoState();
      }
      result.dimensions.push({ name: 'sections', action: 'partial-visible-list', selectedRows, state });
      finalSelectionSucceeded = (state.selectedInformation ?? 0) > 3500;
    }
  }
  if (!finalSelectionSucceeded) {
    // Safe fallback if BDL's list markup changes.
    await selectAll('ctl00_ContentPlaceHolder_wym3_SelectAll', 'sections-fallback');
  }

  result.bulk.beforeDownload = await infoState();
  result.bulk.thresholdExceeded = (result.bulk.beforeDownload.selectedInformation ?? 0) > 3500;
  result.bulk.download1 = await buttonState('ctl00_ContentPlaceHolder_download1');
  result.bulk.download2 = await buttonState('ctl00_ContentPlaceHolder_download2');
  await snapshot('01-over-limit-selection');
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
  if (!downloadButton) throw new Error('BDL Pobierz did not enable above 3500');

  // Wait for the server-side archive-generation POST to finish.
  const currentPath = new URL(page.url()).pathname;
  const generationStarted = Date.now();
  const generationResponsePromise = page.waitForResponse((response) => {
    try {
      const u = new URL(response.url());
      return response.request().method() === 'POST' && u.pathname === currentPath;
    } catch {
      return false;
    }
  }, { timeout: 300000 }).catch((error) => ({ timeoutError: String(error) }));

  await downloadButton.click();
  result.bulk.generationClickAt = new Date().toISOString();
  const generationResponse = await generationResponsePromise;
  result.bulk.generationElapsedMs = Date.now() - generationStarted;
  if (generationResponse && !generationResponse.timeoutError) {
    result.bulk.generationResponse = { status: generationResponse.status(), url: generationResponse.url(), ok: generationResponse.ok() };
  } else {
    result.bulk.generationResponse = generationResponse;
  }
  await snapshot('02-after-generation-response');

  // Poll until the generated subgroup's Export link is actually enabled.
  result.bulk.polls = [];
  let readyExport = null;
  for (let attempt = 1; attempt <= 30; attempt++) {
    try {
      await page.goto('https://bdl.stat.gov.pl/bdl/start', { waitUntil: 'domcontentloaded', timeout: 30000 });
    } catch (error) {
      result.bulk.polls.push({ attempt, navigationError: String(error) });
      if (attempt < 30) await page.waitForTimeout(10000);
      continue;
    }
    await page.waitForTimeout(1500);

    const exports = await page.locator('[id$="_Export"]').evaluateAll((els) => els.map((e) => ({
      id: e.id,
      text: (e.textContent || '').trim().replace(/\s+/g, ' '),
      href: e.getAttribute('href'),
      cls: typeof e.className === 'string' ? e.className : '',
      disabled: 'disabled' in e ? !!e.disabled : null,
    })).filter((x) => /Wyniki finansowe wg sekcji PKD 2007/i.test(x.text)));

    const body = safe(await page.locator('body').innerText().catch(() => ''));
    const hasPanel = /Pobrane podgrupy/i.test(body);
    const hasSubgroup = /Wyniki finansowe wg sekcji PKD 2007/i.test(body);
    const ready = exports.find((x) => x.href && !/aspNetDisabled/i.test(x.cls) && !x.disabled) || null;
    result.bulk.polls.push({ attempt, hasPanel, hasSubgroup, exports, ready: !!ready });
    if (ready) {
      result.bulk.generatedEntryDetected = true;
      result.bulk.archiveReadyPoll = attempt;
      result.bulk.readyExport = ready;
      readyExport = ready;
      await snapshot('03-archive-ready');
      break;
    }
    if (attempt < 30) await page.waitForTimeout(10000);
  }

  if (!readyExport) {
    result.bulk.generatedEntryDetected = result.bulk.polls.some((x) => x.hasSubgroup);
    result.bulk.archiveReady = false;
    await snapshot('03-archive-not-ready');
  } else {
    result.bulk.archiveReady = true;

    // Download the actual generated archive and persist it only as CI evidence.
    const exportControl = page.locator(`#${readyExport.id}`);
    const downloadPromise = page.waitForEvent('download', { timeout: 120000 }).catch(() => null);
    await exportControl.click();
    const download = await downloadPromise;
    if (download) {
      const filename = download.suggestedFilename().replace(/[^A-Za-z0-9._-]/g, '_');
      const target = path.join(outDir, `download-${filename}`);
      await download.saveAs(target);
      result.bulk.archiveDownload = { filename, bytes: (await fs.stat(target)).size };
    } else {
      result.bulk.archiveDownload = null;
    }
    await snapshot('04-after-archive-download');
  }

  result.completedAt = new Date().toISOString();
  await save();
  console.log(JSON.stringify({
    login: result.login.success,
    subgroupLoaded: result.subgroup.loaded,
    selectedInformation: result.bulk.beforeDownload?.selectedInformation,
    thresholdExceeded: result.bulk.thresholdExceeded,
    generationResponse: result.bulk.generationResponse,
    generationElapsedMs: result.bulk.generationElapsedMs,
    archiveReady: result.bulk.archiveReady,
    archiveReadyPoll: result.bulk.archiveReadyPoll ?? null,
    archiveDownload: result.bulk.archiveDownload ?? null,
  }, null, 2));

  if (!result.bulk.generationResponse?.ok || !result.bulk.archiveReady || !result.bulk.archiveDownload) process.exitCode = 1;
} catch (error) {
  result.error = safe(error?.stack || error?.message || error);
  result.failedAt = new Date().toISOString();
  await save();
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
