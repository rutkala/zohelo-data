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
const result = { startedAt: new Date().toISOString(), login: {}, dimensions: [], bulk: {} };

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ acceptDownloads: true, locale: 'pl-PL' });
const page = await context.newPage();
page.on('request', req => { try { const u = new URL(req.url()); if (u.hostname.endsWith('stat.gov.pl')) events.push({kind:'request',method:req.method(),path:u.pathname,type:req.resourceType()}); } catch {} });
page.on('response', res => { try { const u = new URL(res.url()); if (u.hostname.endsWith('stat.gov.pl')) events.push({kind:'response',status:res.status(),path:u.pathname}); } catch {} });
page.on('dialog', async d => { events.push({kind:'dialog',type:d.type(),message:safe(d.message()).slice(0,500)}); await d.accept(); });

async function write(name, data) { await fs.writeFile(path.join(outDir, name), JSON.stringify(data, null, 2)); }
async function snapshot(name) {
  const body = safe(await page.locator('body').innerText().catch(()=>'')).slice(0,50000);
  const widgets = await page.evaluate(() => Array.from(document.querySelectorAll('[id]')).map(el => ({
    id: el.id, tag: el.tagName, cls: typeof el.className === 'string' ? el.className : '',
    role: el.getAttribute('role'), text: (el.textContent || '').trim().replace(/\s+/g,' ').slice(0,300),
    disabled: 'disabled' in el ? !!el.disabled : null
  })).filter(x => /RadListBox|listbox|ContentPlaceHolder/i.test(`${x.cls} ${x.role} ${x.id}`)).slice(0,1200));
  await write(`${name}.json`, {url:page.url(), title:await page.title(), body, widgets});
}
async function saveResult() {
  await write('result.json', result);
  await write('network-events.json', events.slice(-5000));
}

async function getRadListBoxes() {
  return await page.evaluate(() => {
    const out = [];
    for (const el of document.querySelectorAll('.RadListBox[id], [id*="ListBox"], [id*="listBox"], [role="listbox"]')) {
      const id = el.id;
      if (!id || out.some(x => x.id === id)) continue;
      let c = null;
      try { c = typeof window.$find === 'function' ? window.$find(id) : null; } catch {}
      let count = null;
      try { count = c?.get_items?.().get_count?.() ?? null; } catch {}
      out.push({id, tag:el.tagName, cls:el.className || '', count, clientFound:!!c});
    }
    return out;
  });
}

async function selectAllRadListBox(id) {
  return await page.evaluate((id) => {
    const c = typeof window.$find === 'function' ? window.$find(id) : null;
    if (!c || !c.get_items) return {ok:false, reason:'client component not found'};
    const items = c.get_items();
    const count = items.get_count();
    try { c.trackChanges?.(); } catch {}
    let selected = 0;
    for (let i = 0; i < count; i++) {
      const item = items.getItem(i);
      try {
        if (typeof item.select === 'function') item.select();
        else if (typeof item.set_selected === 'function') item.set_selected(true);
        selected++;
      } catch {}
    }
    try { c.commitChanges?.(); } catch {}
    try { c.raise_selectedIndexChanged?.(); } catch {}
    return {ok:true,count,selected};
  }, id);
}

try {
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie', {waitUntil:'networkidle',timeout:60000});
  await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
  await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
  await page.locator('#ctl00_ContentPlaceHolder_SignIn').click();
  await page.waitForTimeout(2500);
  const loginBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.login.urlAfter = page.url();
  result.login.success = !/Użytkownik:\s*Gość/i.test(loginBody) && /Użytkownik:/i.test(loginBody);
  if (!result.login.success) throw new Error('Authenticated BDL login failed');

  await page.goto('https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/3/7/1341', {waitUntil:'networkidle',timeout:60000});
  await snapshot('01-subgroup-initial');

  // Cascading list boxes: select the first currently populated dimension, wait for AJAX, then repeat.
  for (let step = 0; step < 6; step++) {
    const boxes = await getRadListBoxes();
    await write(`rad-listboxes-step-${step}.json`, boxes);
    const already = new Set(result.dimensions.map(x => x.id));
    const next = boxes.find(x => x.clientFound && (x.count ?? 0) > 0 && !already.has(x.id));
    if (!next) break;
    const selection = await selectAllRadListBox(next.id);
    result.dimensions.push({id:next.id, beforeCount:next.count, selection});
    await page.waitForTimeout(2500);
    await snapshot(`02-after-dimension-${step+1}`);
  }

  const body = await page.locator('body').innerText().catch(()=>'');
  const match = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  result.bulk.selectedInformation = match ? Number(match[1].replace(/\s/g,'')) : null;
  const download = page.locator('#ctl00_ContentPlaceHolder_download1');
  result.bulk.downloadVisible = await download.isVisible().catch(()=>false);
  result.bulk.downloadEnabled = await download.isEnabled().catch(()=>false);
  await snapshot('03-before-download');

  if (!result.bulk.downloadEnabled) throw new Error(`Pobierz still disabled after cascading selections; selected=${result.bulk.selectedInformation}`);

  const dlPromise = page.waitForEvent('download', {timeout:45000}).catch(()=>null);
  await download.click();
  await page.waitForTimeout(4000);
  const dl = await dlPromise;
  if (dl) {
    const fn = dl.suggestedFilename().replace(/[^A-Za-z0-9._-]/g,'_');
    const target = path.join(outDir, `download-${fn}`);
    await dl.saveAs(target);
    result.bulk.directDownload = {filename:fn, bytes:(await fs.stat(target)).size};
  } else {
    result.bulk.directDownload = null;
  }
  await snapshot('04-after-download');

  await page.goto('https://bdl.stat.gov.pl/bdl/start', {waitUntil:'networkidle',timeout:60000});
  await page.waitForTimeout(2000);
  const startBody = safe(await page.locator('body').innerText().catch(() => ''));
  result.bulk.downloadedSubgroupsVisible = /Pobrane podgrupy/i.test(startBody);
  result.bulk.populationSubgroupMentioned = /Ludność wg pojedynczych roczników wieku i płci/i.test(startBody);
  await snapshot('05-start-after-download');

  result.completedAt = new Date().toISOString();
  await saveResult();
  console.log(JSON.stringify(result, null, 2));
} catch (e) {
  result.error = safe(e?.stack || e?.message || e);
  result.failedAt = new Date().toISOString();
  await saveResult();
  console.error(result.error);
  process.exitCode = 1;
} finally {
  await browser.close();
}
