// BDL Web UI transport only. No API observations and no archive/data processing.
import { chromium } from '@playwright/test';
import fs from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

const input = JSON.parse(await fs.readFile(process.env.BDL_WEB_TASK_PATH, 'utf8'));
const email = process.env.GUS_BDL_WEB_EMAIL || '';
const password = process.env.GUS_BDL_WEB_PASSWORD || '';
const out = path.resolve(process.env.BDL_BULK_OUT_DIR);
const scope = input.scope;
if (!email || !password || !/^P\d+$/.test(input.subgroup_id) ||
    !/^https:\/\/bdl\.stat\.gov\.pl\/bdl\/dane\/podgrup\/wymiary\/\d+\/\d+\/\d+$/.test(input.url) ||
    !['dimensions', 'layouts', 'territories', 'download', 'whole'].includes(scope?.kind)) throw new Error('Invalid Web selection task');
const result = { subgroup_id: input.subgroup_id, selection_id: input.selection_id, stage: 'login', status: 'started' };
const save = () => fs.writeFile(path.join(out, 'selection-result.json'), JSON.stringify(result, null, 2));
const safe = value => String(value).replaceAll(email, '[REDACTED_EMAIL]').replaceAll(password, '[REDACTED_PASSWORD]');
const same = (a, b) => a.length === b.length && new Set(a).size === a.length && [...a].sort().every((v, i) => v === [...b].sort()[i]);
const sessionPath = process.env.BDL_SESSION_STATE_PATH || path.join(path.dirname(out), 'bdl-session-state.json');
let hasSession = false;
try {
  await fs.access(sessionPath);
  hasSession = true;
} catch {}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  acceptDownloads: true,
  locale: 'pl-PL',
  storageState: hasSession ? sessionPath : undefined
});
const page = await context.newPage();
page.setDefaultTimeout(30000);
let rateLimited = false;
page.on('response', response => { if (response.status() === 429) rateLimited = true; });
await context.route('**/*', route => {
  const url = new URL(route.request().url());
  if (url.hostname === 'bdl.stat.gov.pl' && /^\/api(?:\/|$)/i.test(url.pathname)) return route.abort();
  return route.continue();
});

async function settle() {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForFunction(() => {
    const manager = window.Sys?.WebForms?.PageRequestManager?.getInstance?.();
    return !manager?.get_isInAsyncPostBack?.();
  }, null, { timeout: 120000 });
  // The ready state is rechecked after UI postback dispatch, not used as a success signal alone.
  await page.waitForTimeout(350);
  await page.waitForFunction(() => !window.Sys?.WebForms?.PageRequestManager?.getInstance?.()?.get_isInAsyncPostBack?.(), null, { timeout: 120000 });
  if (rateLimited) throw new Error('RATE_LIMIT: provider requested fewer requests');
  if (/\/errors\//i.test(new URL(page.url()).pathname)) throw new Error('PROVIDER: BDL server-error page');
}

async function dimensions() {
  await page.waitForFunction(() => {
    const visible = el => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
    const elements = Array.from(document.querySelectorAll('select[multiple], .RadListBox[id]')).filter(visible);
    return elements.length > 0 && elements.every(el => {
      if (el.tagName === 'SELECT') return true;
      return typeof window.$find === 'function' && !!window.$find(el.id)?.get_items;
    });
  }, null, { timeout: 15000 }).catch(() => {});
  return page.evaluate(() => {
    const visible = el => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
    const records = [];
    for (const el of document.querySelectorAll('select[multiple], .RadListBox[id]')) {
      if (!visible(el) || !el.id) continue;
      const list = typeof window.$find === 'function' ? window.$find(el.id) : null;
      let options, selected, kind;
      if (el.tagName === 'SELECT') {
        kind = 'select';
        options = [...el.options].filter(o => !o.disabled).map(o => ({ value: o.value, label: o.text.trim() }));
        selected = [...el.selectedOptions].map(o => o.value);
      } else if (list?.get_items && list?.get_selectedItems) {
        kind = 'listbox';
        const items = list.get_items();
        options = Array.from({ length: items.get_count() }, (_, i) => items.getItem(i))
          .filter(i => !i.get_enabled || i.get_enabled()).map(i => ({ value: String(i.get_value()), label: i.get_text().trim() }));
        selected = list.get_selectedItems().map(i => String(i.get_value()));
      } else continue;
      if (!options.length) continue;
      records.push({ id: el.id, kind, options, selected, year_axis: options.every(o => /^\d{4}$/.test(o.label)) });
    }
    return records;
  });
}

async function selectDimension(controlId, wanted) {
  const id = typeof controlId === 'string' ? controlId : controlId.id;
  const isSelect = await page.evaluate(id => document.getElementById(id)?.tagName === 'SELECT', id);
  if (isSelect) {
    await page.locator(`[id="${id}"]`).selectOption(wanted);
    await settle();
    return;
  }

  const totalOptions = await page.evaluate(id => window.$find(id)?.get_items?.()?.get_count?.() || 0, id);
  const selectAllBtn = page.locator(`[id="${id.replace('_ElementsList', '_SelectAll')}"]`);
  const canSelectAll = wanted.length === totalOptions &&
    await selectAllBtn.isVisible().catch(() => false) &&
    await selectAllBtn.isEnabled().catch(() => false);

  if (canSelectAll) {
    await selectAllBtn.click();
    await settle();
  } else {
    const itemDomIds = await page.evaluate(({ id, values }) => {
      const list = window.$find(id);
      if (!list) throw new Error('Listbox not found: ' + id);
      return values.map(v => {
        const item = list.findItemByValue(v);
        if (!item) throw new Error(`Item with value ${v} not found in ${id}`);
        return item.get_element()?.id;
      });
    }, { id, values: wanted });

    for (let i = 0; i < itemDomIds.length; i++) {
      const locator = page.locator(`[id="${itemDomIds[i]}"]`);
      if (i === 0) {
        await locator.click();
      } else {
        await locator.click({ modifiers: ['Control'] });
      }
    }
    await settle();
  }
}

async function infoCount() {
  const body = await page.locator('body').innerText();
  const match = body.match(/Wybrano\s+([\d\s]+)\s+informacji/i);
  const count = match ? Number(match[1].replace(/\s/g, '')) : NaN;
  if (!(count > 0)) throw new Error('PROTOCOL: no positive UI information selection');
  return count;
}

async function next() {
  const css = '#ctl00_ContentPlaceHolder_dalej, #ctl00_ContentPlaceHolder_dalej1, #ctl00_ContentPlaceHolder_dalej2';
  await page.waitForFunction((sel) => {
    const els = [...document.querySelectorAll(sel)];
    return els.some(e => !e.disabled && !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length));
  }, css, { timeout: 60000 });
  const button = page.locator('#ctl00_ContentPlaceHolder_dalej:visible, #ctl00_ContentPlaceHolder_dalej1:visible, #ctl00_ContentPlaceHolder_dalej2:visible').first();
  await button.click({ timeout: 60000 });
  await settle();
}

async function layouts() {
  return page.evaluate(() => {
    const visible = el => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
    const found = [];
    for (const el of document.querySelectorAll('select:not([multiple]), .RadComboBox[id]')) {
      if (!visible(el) || !el.id) continue;
      let options, kind;
      if (el.tagName === 'SELECT') {
        kind = 'select';
        options = [...el.options].filter(o => !o.disabled).map(o => ({ value: o.value, label: o.text.trim() }));
      } else {
        const box = window.$find?.(el.id);
        if (!box?.get_items) continue;
        kind = 'combobox';
        const items = box.get_items();
        options = Array.from({ length: items.get_count() }, (_, i) => items.getItem(i))
          .filter(i => !i.get_enabled || i.get_enabled()).map(i => ({ value: String(i.get_value()), label: i.get_text().trim() }));
      }
      if (options.some(o => /układ\s+(administracyjny|statystyczny)/i.test(o.label))) {
        found.push(options.map(o => ({ ...o, id: el.id, kind })));
      }
    }
    if (found.length !== 1) throw new Error(`PROTOCOL: expected one territorial layout selector, found ${found.length}`);
    return found[0];
  });
}

async function setLayout(layout) {
  const available = await layouts();
  if (!available.some(x => x.id === layout.id && x.kind === layout.kind && x.value === layout.value)) throw new Error('PROTOCOL: layout inventory changed');
  const current = await page.evaluate(({ id, kind }) => kind === 'select'
    ? document.getElementById(id).value : String(window.$find(id).get_selectedItem()?.get_value()), layout);
  if (current === layout.value) return;

  if (layout.kind === 'select') {
    await page.locator(`[id="${layout.id}"]`).selectOption(layout.value);
  } else {
    const arrow = page.locator(`[id="${layout.id}_Arrow"]`);
    if (await arrow.isVisible().catch(() => false)) await arrow.click();
    const targetOption = available.find(x => x.value === layout.value);
    const item = page.locator('.rcbItem').filter({ hasText: targetOption.label }).first();
    await item.click();
  }
  await settle();
  const selected = await page.evaluate(({ id, kind }) => kind === 'select'
    ? document.getElementById(id).value : String(window.$find(id).get_selectedItem()?.get_value()), layout);
  if (selected !== layout.value) throw new Error('PROTOCOL: layout selection did not persist');
}

async function territories() {
  return page.evaluate(() => {
    const visible = el => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
    const boxes = [...document.querySelectorAll('.RadListBox[id]')].filter(visible).map(el => {
      const box = window.$find?.(el.id);
      if (!box?.get_items) return null;
      const items = box.get_items();
      return { id: el.id, items: Array.from({ length: items.get_count() }, (_, i) => items.getItem(i))
        .map(i => ({ value: String(i.get_value()), label: i.get_text().trim() })) };
    }).filter(Boolean);
    if (boxes.length !== 2) throw new Error(`PROTOCOL: expected two territorial lists, found ${boxes.length}`);
    const [source, destination] = boxes;
    const match = document.body.innerText.match(/Elementów do wyboru:\s*([\d\s]+)/i);
    const count = match ? Number(match[1].replace(/\s/g, '')) : null;
    return { source: source.id, destination: destination.id, items: source.items, selected: destination.items, count };
  });
}

async function selectTerritories(wanted) {
  const before = await territories();
  if (before.selected.length) throw new Error('PROTOCOL: territorial destination was not empty in the fresh selection');
  if (before.count !== before.items.length) throw new Error('PROTOCOL: territorial inventory count mismatch');
  if (new Set(wanted).size !== wanted.length || !wanted.length || wanted.some(v => !before.items.some(i => i.value === v))) throw new Error('PROTOCOL: requested territorial identifiers are missing');
  await page.evaluate(({ id, wanted }) => {
    const box = window.$find(id);
    box.clearSelection();
    for (const value of wanted) box.findItemByValue(value).select();
    if (box.get_selectedItems().length !== wanted.length) throw new Error('PROTOCOL: territorial highlighting did not match the request');
  }, { id: before.source, wanted });
  const buttons = await page.locator('button').evaluateAll(els => els.filter(e => !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length))
    .map(e => ({ id: e.id, title: e.title || e.getAttribute('aria-label') || '', cls: e.className })));
  const add = buttons.find(b => /dodaj.*wybranych/i.test(b.title) && !/wszyst/i.test(b.title));
  if (add?.id) await page.locator(`[id="${add.id}"]`).click();
  else {
    // Same native transfer control family as BDL's verified transfer-all button.
    const button = page.locator('button.rlbTransferFrom:visible').first();
    if (!await button.count()) throw new Error(`PROTOCOL: no selected-items transfer button; controls ${JSON.stringify(buttons)}`);
    await button.click();
  }
  await settle();
  await page.waitForFunction(({ id, wanted }) => {
    const items = window.$find(id).get_items();
    const actual = Array.from({ length: items.get_count() }, (_, i) => String(items.getItem(i).get_value()));
    return actual.length === wanted.length && actual.every(v => wanted.includes(v));
  }, { id: before.destination, wanted }, { timeout: 30000 });
  result.selected_territories = wanted;
}

async function exportZip() {
  result.stage = 'table';
  await save();
  await next();
  // A table URL alone is NOT a ready table (the old worker returned too early).
  const deadline = Date.now() + 180000;
  let ready = false;
  while (Date.now() < deadline) {
    await settle();
    const exportBtn = page.getByText(/^(?:Eksport|Export)$/i, { exact: true }).first();
    if (await exportBtn.isVisible().catch(() => false)) { ready = true; break; }
    await page.waitForTimeout(1000);
  }
  if (!ready) throw new Error('PROVIDER_TIMEOUT: result table did not expose its export control');
  result.stage = 'download';
  await save();
  await page.getByText(/^(?:Eksport|Export)$/i, { exact: true }).first().click();
  const choice = page.getByText(/CSV\s*[–-]\s*(?:tablica\s+)?relacyj/i).first();
  await choice.waitFor({ state: 'visible', timeout: 15000 }).catch(() => {});
  const downloadPromise = page.waitForEvent('download', { timeout: 180000 });
  await choice.click({ noWaitAfter: true });
  const download = await downloadPromise;
  let name = download.suggestedFilename();
  const escapedNumber = input.subgroup_id.slice(1).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  if (!name || /[/\\\r\n\0]/.test(name) || name.toLowerCase() === 'download') {
    name = `DANE_${input.subgroup_id}.zip`;
  } else if (!name.toLowerCase().endsWith('.zip')) {
    name = `${name}.zip`;
  }
  if (!new RegExp(`(?:^|_)${escapedNumber}(?:_|\\.)`, 'i').test(name)) {
    name = `DANE_${input.subgroup_id}_${name}`;
  }
  const target = path.join(out, `download-${name}`);
  await download.saveAs(target);
  const stat = await fs.stat(target);
  if (stat.size < 4 || stat.size > 2 * 1024 ** 3) throw new Error('PROTOCOL: native transfer size outside envelope');
  const sha = crypto.createHash('sha256');
  let prefix = null;
  for await (const chunk of createReadStream(target)) { prefix ??= chunk.subarray(0, 4); sha.update(chunk); }
  if (prefix[0] !== 0x50 || prefix[1] !== 0x4b) throw new Error('PROTOCOL: provider download was not a native ZIP');
  result.archive = { filename: name, local_filename: path.basename(target), bytes: stat.size, sha256: sha.digest('hex') };
  result.status = 'download';
}

try {
  await fs.mkdir(out, { recursive: true });
  await save();
  let loggedIn = false;
  if (hasSession) {
    await page.goto(input.url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await settle();
    loggedIn = await page.evaluate(() => /Użytkownik:/i.test(document.body.innerText) && !/Użytkownik:\s*Gość/i.test(document.body.innerText));
  }
  if (!loggedIn) {
    await page.goto('https://bdl.stat.gov.pl/bdl/logowanie', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
    await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
    await page.locator('#ctl00_ContentPlaceHolder_SignIn').click();
    await settle();
    await page.waitForFunction(() => /Użytkownik:/i.test(document.body.innerText) && !/Użytkownik:\s*Gość/i.test(document.body.innerText), null, { timeout: 30000 });
    try {
      await context.storageState({ path: sessionPath });
    } catch {}
    await page.goto(input.url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await settle();
  }
  result.stage = 'dimensions';
  await save();
  if (scope.kind === 'whole' || (scope.kind === 'download' && Array.isArray(scope.territories) && scope.territories.length === 1 && scope.territories[0] === 'all')) {
    const clicked = new Set();
    for (let i = 0; i < 20; i++) {
      const all = await page.locator('[id$="_SelectAll"]').evaluateAll(els => els.filter(e => !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length) && !e.disabled).map(e => e.id));
      const id = all.find(id => !clicked.has(id));
      if (!id) break;
      await page.locator(`[id="${id}"]`).click();
      clicked.add(id);
      await settle();
    }
    const info = await infoCount();
    result.selected_information = info;
    if (info <= 3500) {
      result.stage = 'territories';
      await save();
      await next();
      await page.waitForURL(/\/bdl\/dane\/podgrup\/teryt/, { timeout: 45000 });
      await settle();
      const menu = page.locator('#ctl00_ContentPlaceHolder_terytList_MenuButton');
      if (await menu.isVisible().catch(() => false) && await menu.isEnabled().catch(() => false)) {
        await menu.click();
        const selectAll = page.locator('li.rmItem').filter({ hasText: 'Zaznacz wszystkie' }).first().locator('.rmLink');
        await selectAll.waitFor({ state: 'visible', timeout: 10000 });
        await selectAll.click();
        await page.waitForTimeout(500);
        const transferAll = page.locator('button.rlbTransferAllFrom').first();
        await transferAll.waitFor({ state: 'visible', timeout: 10000 });
        await transferAll.click();
        await settle();
        await page.waitForFunction(() => {
          const list = window.$find('ctl00_ContentPlaceHolder_terytList_SelectedUnits');
          return (list?.get_items?.()?.get_count?.() || 0) > 0;
        }, null, { timeout: 45000 }).catch(() => {});
        await exportZip();
      } else {
        throw new Error('PROVIDER: TERYT menu unavailable for whole-subgroup export');
      }
    } else {
      const downloadButton = page.locator('#ctl00_ContentPlaceHolder_download1:visible, #ctl00_ContentPlaceHolder_download2:visible').first();
      if (await downloadButton.count() && await downloadButton.isEnabled().catch(() => false)) {
        result.stage = 'download';
        await save();
        const downloadPromise = page.waitForEvent('download', { timeout: 360000 });
        await downloadButton.click({ noWaitAfter: true });
        const download = await downloadPromise;
        let name = download.suggestedFilename();
        const escapedNumber = input.subgroup_id.slice(1).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        if (!name || /[/\\\r\n\0]/.test(name) || name.toLowerCase() === 'download') {
          name = `DANE_${input.subgroup_id}.zip`;
        } else if (!name.toLowerCase().endsWith('.zip')) {
          name = `${name}.zip`;
        }
        if (!new RegExp(`(?:^|_)${escapedNumber}(?:_|\\.)`, 'i').test(name)) {
          name = `DANE_${input.subgroup_id}_${name}`;
        }
        const target = path.join(out, `download-${name}`);
        await download.saveAs(target);
        const stat = await fs.stat(target);
        const sha = crypto.createHash('sha256');
        for await (const chunk of createReadStream(target)) { sha.update(chunk); }
        result.archive = { filename: name, local_filename: path.basename(target), bytes: stat.size, sha256: sha.digest('hex') };
        result.status = 'download';
      } else {
        throw new Error('PROVIDER: whole-subgroup selection requires partitioning');
      }
    }
  } else if (scope.kind === 'dimensions') {
    const clicked = new Set();
    for (let i = 0; i < 20; i++) {
      const all = await page.locator('[id$="_SelectAll"]').evaluateAll(els => els.filter(e => !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length) && !e.disabled).map(e => e.id));
      const id = all.find(id => !clicked.has(id));
      if (!id) break;
      await page.locator(`[id="${id}"]`).click();
      clicked.add(id);
      await settle();
    }
    const inventory = await dimensions();
    if (!inventory.length || inventory.some(d => !same(d.selected, d.options.map(o => o.value)))) throw new Error('PROTOCOL: full dimension selection was not verified');
    result.dimensions = inventory.map(({ selected, ...d }) => d);
    result.selected_information = await infoCount();
    result.status = 'dimensions';
  } else {
    const targetControlIds = Object.keys(scope.dimensions).sort((a, b) => {
      const isLataA = /lata/i.test(a);
      const isLataB = /lata/i.test(b);
      if (isLataA && !isLataB) return -1;
      if (!isLataA && isLataB) return 1;
      return a.localeCompare(b, undefined, { numeric: true });
    });

    for (const controlId of targetControlIds) {
      const wantedValues = scope.dimensions[controlId];
      await page.waitForFunction(({ id, values }) => {
        const el = document.getElementById(id);
        if (!el) return false;
        if (el.tagName === 'SELECT') {
          const opts = new Set(Array.from(el.options).map(o => o.value));
          return values.every(v => opts.has(v));
        }
        const list = typeof window.$find === 'function' ? window.$find(id) : null;
        if (!list || !list.get_items) return false;
        return values.every(v => !!list.findItemByValue(v));
      }, { id: controlId, values: wantedValues }, { timeout: 45000 });

      await selectDimension(controlId, wantedValues);
    }

    const selected = await dimensions();
    if (!same(selected.map(d => d.id), targetControlIds)) throw new Error('PROTOCOL: dimension controls mismatch after selection');
    if (selected.some(d => !same(d.selected, scope.dimensions[d.id]))) throw new Error('PROTOCOL: exact dimension selection was not retained');
    result.selected_information = await infoCount();
    if (result.selected_information > 3500) throw new Error('PROTOCOL: selection still exceeds normal Web table limit');
    result.stage = 'territories';
    await save();
    await next();
    await page.waitForURL(/\/bdl\/dane\/podgrup\/teryt/, { timeout: 45000 });
    await settle();
    if (scope.kind === 'layouts') {
      result.layouts = await layouts();
      result.status = 'layouts';
    } else {
      await setLayout(scope.layout);
      if (scope.kind === 'territories') {
        const inventory = await territories();
        if (inventory.selected.length || inventory.count !== inventory.items.length || !inventory.items.length) throw new Error('PROTOCOL: complete fresh territorial inventory was not established');
        result.items = inventory.items;
        result.advertised_count = inventory.count;
        result.status = 'territories';
      } else {
        await selectTerritories(scope.territories);
        await exportZip();
      }
    }
  }
} catch (error) {
  result.error = safe(error?.message || error).slice(0, 1800);
  result.failure_class = rateLimited ? 'rate_limit' : result.stage === 'login' ? 'authentication_or_site' :
    /PROVIDER:/.test(result.error) ? 'provider_error' :
      /PROVIDER_TIMEOUT:/.test(result.error) || (error?.name === 'TimeoutError' && ['table', 'download'].includes(result.stage)) ? 'provider_timeout' : 'ui_protocol';
  result.status = 'failed';
  process.exitCode = 1;
} finally {
  await save();
  await browser.close();
}
