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
      })).filter((x) => /dalej|pobierz|download|pobrane|podgrup|selectall|zaznacz|csv|xlsx?|zip|wym3/i.test(`${x.id} ${x.text} ${x.value} ${x.title} ${x.href} ${x.cls}`)).slice(0, 1600));
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
  if (!result.subgroup.loaded) {
    await snapshot('01-subgroup-unexpected');
    throw new Error(`P2695 page did not load as expected: ${page.url()}`);
  }

  // Native callbacks populate the four cascading dimensions.
  await selectAll('ctl00_ContentPlaceHolder_lata_SelectAll', 'years');
  await selectAll('ctl00_ContentPlaceHolder_wym1_SelectAll', 'periods');
  await selectAll('ctl00_ContentPlaceHolder_wym2_SelectAll', 'subject');

  // Current P2695 has enough combinations that one PKD-section value crosses 3,500.
  // Ask Telerik for the actual DOM id of the first item, then click it like a user.
  const finalMeta = await page.evaluate(() => {
    const c = typeof window.$find === 'function' ? window.$find('ctl00_ContentPlaceHolder_wym3') : null;
    if (!c) return { found: false };
    const items = c.get_items?.();
    const count = items?.get_count?.() ?? 0;
    return {
      found: true,
      count,
      first: count ? {
        id: items.getItem(0).get_element?.()?.id || null,
        text: items.getItem(0).get_text?.() || null,
      } : null,
    };
  });
  result.dimensions.push({ name: 'sections-meta', ...finalMeta });

  if (finalMeta.found && finalMeta.first?.id) {
    const firstItem = page.locator(`#${finalMeta.first.id}`);
    await firstItem.click();
    await page.waitForTimeout(2200);
    let state = await infoState();
    let selectedRows = 1;

    // Be adaptive if current BDL counts change: add final-dimension rows until >3500.
    while ((state.selectedInformation ?? 0) <= 3500 && selectedRows < finalMeta.count) {
      const nextMeta = await page.evaluate((idx) => {
        const c = window.$find?.('ctl00_ContentPlaceHolder_wym3');
        const item = c?.get_items?.()?.getItem?.(idx);
        return item ? { id: item.get_element?.()?.id || null, text: item.get_text?.() || null } : null;
      }, selectedRows);
      if (!nextMeta?.id) break;
      await page.locator(`#${nextMeta.id}`).click({ modifiers: ['Control'] });
      selectedRows++;
      await page.waitForTimeout(900);
      state = await infoState();
    }
    result.dimensions.push({ name: 'sections', action: 'partial', selectedRows, state });
  } else {
    // Conservative fallback if Telerik changes its client object shape.
    await selectAll('ctl00_ContentPlaceHolder_wym3_SelectAll', 'sections');
  }

  result.bulk.beforeDownload = await infoState();
  result.bulk.thresholdExceeded = (result.bulk.beforeDownload.selectedInformation ?? 0) > 3500;
  result.bulk.download1 = await buttonState('ctl00_ContentPlaceHolder_download1');
  result.bulk.download2 = await buttonState('ctl00_ContentPlaceHolder_download2');
  result.bulk.next1 = await buttonState('ctl00_ContentPlaceHolder_dalej1');
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
  if (!downloadButton) throw new Error(`BDL Pobierz did not enable above 3500: ${JSON.stringify({ download1: result.bulk.download1, download2: result.bulk.download2 })}`);

  // The BDL bulk action is an AJAX POST which can remain in flight while the
  // server builds the archive. Do not navigate away until it completes.
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
  const directDownloadPromise = page.waitForEvent('download', { timeout: 300000 }).catch(() => null);

  await downloadButton.click();
  result.bulk.generationClickAt = new Date().toISOString();
  const generationResponse = await generationResponsePromise;
  result.bulk.generationElapsedMs = Date.now() - generationStarted;
  if (generationResponse && !generationResponse.timeoutError) {
    result.bulk.generationResponse = {
      status: generationResponse.status(),
      url: generationResponse.url(),
      ok: generationResponse.ok(),
    };
  } else {
    result.bulk.generationResponse = generationResponse;
  }

  const directDownload = await Promise.race([
    directDownloadPromise,
    page.waitForTimeout(1000).then(() => null),
  ]);
  if (directDownload) {
    const filename = directDownload.suggestedFilename().replace(/[^A-Za-z0-9._-]/g, '_');
    const target = path.join(outDir, `download-${filename}`);
    await directDownload.saveAs(target);
    result.bulk.directDownload = { filename, bytes: (await fs.stat(target)).size };
  } else {
    result.bulk.directDownload = null;
  }
  await snapshot('02-after-generation-response');

  // Now inspect the authenticated start page. Use DOMContentLoaded rather than
  // networkidle because BDL keeps auxiliary requests alive.
  result.bulk.polls = [];
  for (let attempt = 1; attempt <= 18; attempt++) {
    try {
      await page.goto('https://bdl.stat.gov.pl/bdl/start', { waitUntil: 'domcontentloaded', timeout: 30000 });
    } catch (error) {
      result.bulk.polls.push({ attempt, navigationError: String(error) });
      if (attempt < 18) await page.waitForTimeout(10000);
      continue;
    }
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
      await snapshot('03-generated-entry');
      break;
    }
    if (attempt < 18) await page.waitForTimeout(10000);
  }
  if (!result.bulk.generatedEntryDetected) {
    result.bulk.generatedEntryDetected = false;
    await snapshot('03-start-final');
  }

  result.completedAt = new Date().toISOString();
  await save();
  console.log(JSON.stringify({
    login: result.login.success,
    subgroupLoaded: result.subgroup.loaded,
    selectedInformation: result.bulk.beforeDownload?.selectedInformation,
    thresholdExceeded: result.bulk.thresholdExceeded,
    downloadButtonId: result.bulk.downloadButtonId,
    generationResponse: result.bulk.generationResponse,
    generationElapsedMs: result.bulk.generationElapsedMs,
    directDownload: result.bulk.directDownload,
    generatedEntryDetected: result.bulk.generatedEntryDetected,
    generatedEntryPoll: result.bulk.generatedEntryPoll ?? null,
    generatedLinkCount: result.bulk.generatedLinks?.length ?? 0,
  }, null, 2));

  if (!result.bulk.generationResponse?.ok && !result.bulk.generatedEntryDetected && !result.bulk.directDownload) {
    process.exitCode = 1;
  }
} catch (error) {
  result.error = safe(error?.stack || error?.message || error);
  result.failedAt = new Date().toISOString();
  await save();
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
