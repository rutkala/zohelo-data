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
    disabled: 'disabled' in el ? !!el.disabled : null,
    title: el.getAttribute('title'), value: el.getAttribute('value')
  })).filter(x => /RadListBox|listbox|ContentPlaceHolder/i.test(`${x.cls} ${x.role} ${x.id}`)).slice(0,1500));
  await write(`${name}.json`, {url:page.url(), title:await page.title(), body, widgets});
}
async function saveResult() {
  await write('result.json', result);
  await write('network-events.json', events.slice(-5000));
}
async function bodyState() {
  const body = await page.locator('body').innerText().catch(()=> '');
  const total = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  const counters = [...body.matchAll(/Zaznaczonych:\s*([0-9]+)\/([0-9]+)/gi)].map(m => ({selected:Number(m[1]), total:Number(m[2])}));
  return { selectedInformation: total ? Number(total[1].replace(/\s/g,'')) : null, counters };
}
async function clickAndAwaitDimension(buttonId, nextButtonId = null) {
  const button = page.locator(`#${buttonId}`);
  const before = await bodyState();
  const beforePosts = events.filter(x => x.kind === 'request' && x.method === 'POST').length;
  if (!await button.isVisible().catch(()=>false)) return {ok:false, reason:'not visible', before};
  if (!await button.isEnabled().catch(()=>false)) return {ok:false, reason:'disabled', before};
  await button.click();
  await page.waitForTimeout(2500);
  if (nextButtonId) {
    await page.locator(`#${nextButtonId}`).waitFor({state:'visible', timeout:15000}).catch(()=>{});
  }
  const after = await bodyState();
  const afterPosts = events.filter(x => x.kind === 'request' && x.method === 'POST').length;
  return {ok:true, before, after, postRequests:afterPosts-beforePosts};
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

  const sequence = [
    ['ctl00_ContentPlaceHolder_lata_SelectAll','ctl00_ContentPlaceHolder_wym1_SelectAll','years'],
    ['ctl00_ContentPlaceHolder_wym1_SelectAll','ctl00_ContentPlaceHolder_wym2_SelectAll','sex'],
    ['ctl00_ContentPlaceHolder_wym2_SelectAll',null,'age'],
  ];
  for (const [buttonId,nextId,name] of sequence) {
    const state = await clickAndAwaitDimension(buttonId,nextId);
    result.dimensions.push({name, buttonId, ...state});
    await snapshot(`02-after-${name}`);
    if (!state.ok) break;
  }

  const state = await bodyState();
  result.bulk.selectedInformation = state.selectedInformation;
  result.bulk.counters = state.counters;
  const download = page.locator('#ctl00_ContentPlaceHolder_download1');
  result.bulk.downloadVisible = await download.isVisible().catch(()=>false);
  result.bulk.downloadEnabled = await download.isEnabled().catch(()=>false);
  const next = page.locator('#ctl00_ContentPlaceHolder_next1');
  result.bulk.nextEnabled = await next.isEnabled().catch(()=>false);
  await snapshot('03-before-download');

  if (result.bulk.downloadEnabled) {
    const dlPromise = page.waitForEvent('download', {timeout:45000}).catch(()=>null);
    await download.click();
    await page.waitForTimeout(4000);
    const dl = await dlPromise;
    if (dl) {
      const fn = dl.suggestedFilename().replace(/[^A-Za-z0-9._-]/g,'_');
      const target = path.join(outDir, `download-${fn}`);
      await dl.saveAs(target);
      result.bulk.directDownload = {filename:fn, bytes:(await fs.stat(target)).size};
    } else result.bulk.directDownload = null;
    await snapshot('04-after-download');
  } else if (result.bulk.nextEnabled) {
    // If BDL requires the geography stage before enabling its bulk package, capture that transition only.
    const beforeUrl = page.url();
    await next.click();
    await page.waitForTimeout(3000);
    result.bulk.nextTransition = {beforeUrl, afterUrl:page.url()};
    await snapshot('04-after-next');
  } else {
    throw new Error(`Neither Pobierz nor Dalej enabled; selected=${result.bulk.selectedInformation}`);
  }

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
