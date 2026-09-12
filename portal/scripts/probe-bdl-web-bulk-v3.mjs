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
const result = { startedAt: new Date().toISOString(), login: {}, dimensions: [], bulk: {}, geography: {} };

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ acceptDownloads: true, locale: 'pl-PL' });
const page = await context.newPage();
page.on('request', req => { try { const u = new URL(req.url()); if (u.hostname.endsWith('stat.gov.pl')) events.push({kind:'request',method:req.method(),path:u.pathname,type:req.resourceType()}); } catch {} });
page.on('response', res => { try { const u = new URL(res.url()); if (u.hostname.endsWith('stat.gov.pl')) events.push({kind:'response',status:res.status(),path:u.pathname}); } catch {} });
page.on('dialog', async d => { events.push({kind:'dialog',type:d.type(),message:safe(d.message()).slice(0,500)}); await d.accept(); });

async function write(name, data) { await fs.writeFile(path.join(outDir, name), JSON.stringify(data, null, 2)); }
async function snapshot(name) {
  const body = safe(await page.locator('body').innerText().catch(()=>'')).slice(0,80000);
  const widgets = await page.evaluate(() => Array.from(document.querySelectorAll('[id]')).map(el => ({
    id: el.id, tag: el.tagName, cls: typeof el.className === 'string' ? el.className : '',
    role: el.getAttribute('role'), text: (el.textContent || '').trim().replace(/\s+/g,' ').slice(0,400),
    disabled: 'disabled' in el ? !!el.disabled : null,
    title: el.getAttribute('title'), value: el.getAttribute('value')
  })).filter(x => /Rad|ListBox|Tree|Combo|ContentPlaceHolder|SelectAll|DeselectAll/i.test(`${x.cls} ${x.role} ${x.id}`)).slice(0,2500));
  await write(`${name}.json`, {url:page.url(), title:await page.title(), body, widgets});
}
async function saveResult() {
  await write('result.json', result);
  await write('network-events.json', events.slice(-6000));
}
async function bodyState() {
  const body = await page.locator('body').innerText().catch(()=> '');
  const total = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  const counters = [...body.matchAll(/Zaznaczonych:\s*([0-9]+)\/([0-9]+)/gi)].map(m => ({selected:Number(m[1]), total:Number(m[2])}));
  return { selectedInformation: total ? Number(total[1].replace(/\s/g,'')) : null, counters };
}
async function clickSelectAll(buttonId) {
  const button = page.locator(`#${buttonId}`);
  const before = await bodyState();
  if (!await button.isVisible().catch(()=>false)) return {ok:false, reason:'not visible', before};
  if (!await button.isEnabled().catch(()=>false)) return {ok:false, reason:'disabled', before};
  const beforePosts = events.filter(x => x.kind==='request' && x.method==='POST').length;
  await button.click();
  await page.waitForTimeout(2500);
  return {ok:true, before, after:await bodyState(), postRequests:events.filter(x => x.kind==='request' && x.method==='POST').length-beforePosts};
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

  for (const [buttonId,name] of [
    ['ctl00_ContentPlaceHolder_lata_SelectAll','years'],
    ['ctl00_ContentPlaceHolder_wym1_SelectAll','sex'],
    ['ctl00_ContentPlaceHolder_wym2_SelectAll','age'],
  ]) {
    const state = await clickSelectAll(buttonId);
    result.dimensions.push({name,buttonId,...state});
    await snapshot(`02-after-${name}`);
    if (!state.ok) throw new Error(`Could not select ${name}: ${state.reason}`);
  }

  const state = await bodyState();
  result.bulk.selectedInformation = state.selectedInformation;
  result.bulk.counters = state.counters;
  const download = page.locator('#ctl00_ContentPlaceHolder_download1');
  const next = page.locator('#ctl00_ContentPlaceHolder_dalej1');
  result.bulk.downloadEnabled = await download.isEnabled().catch(()=>false);
  result.bulk.nextEnabled = await next.isEnabled().catch(()=>false);
  await snapshot('03-before-next');

  if (result.bulk.downloadEnabled) {
    const dlPromise = page.waitForEvent('download',{timeout:30000}).catch(()=>null);
    await download.click();
    const dl = await dlPromise;
    if (dl) {
      const fn=dl.suggestedFilename().replace(/[^A-Za-z0-9._-]/g,'_');
      const target=path.join(outDir,`download-${fn}`); await dl.saveAs(target);
      result.bulk.directDownload={filename:fn,bytes:(await fs.stat(target)).size};
    }
    await snapshot('04-after-direct-download');
  } else {
    if (!result.bulk.nextEnabled) throw new Error(`Dalej unexpectedly disabled; selected=${result.bulk.selectedInformation}`);
    const beforeUrl=page.url();
    await next.click();
    await page.waitForTimeout(3500);
    result.bulk.nextTransition={beforeUrl,afterUrl:page.url()};
    await snapshot('04-geography-stage');

    const geoBody = await page.locator('body').innerText().catch(()=> '');
    result.geography.bodySignals = {
      hasJednostki:/Jednostk/i.test(geoBody), hasPoziom:/Poziom/i.test(geoBody), hasPobierz:/Pobierz/i.test(geoBody), hasDalej:/Dalej/i.test(geoBody)
    };
    result.geography.buttons = await page.locator('button').evaluateAll(btns => btns.map(b=>({id:b.id,text:(b.textContent||'').trim(),value:b.getAttribute('value'),title:b.getAttribute('title'),disabled:b.disabled,cls:b.className})).filter(x=>/pobierz|dalej|wszyst|zaznacz|wybierz/i.test(`${x.id} ${x.text} ${x.value} ${x.title}`)).slice(0,200));
    result.geography.radWidgets = await page.evaluate(() => Array.from(document.querySelectorAll('[id]')).map(el=>({id:el.id,tag:el.tagName,cls:typeof el.className==='string'?el.className:'',text:(el.textContent||'').trim().replace(/\s+/g,' ').slice(0,180)})).filter(x=>/Rad(Tree|List|Combo)|SelectAll|DeselectAll|unit|jednost|teryt|poziom/i.test(`${x.id} ${x.cls} ${x.text}`)).slice(0,1000));
  }

  result.completedAt=new Date().toISOString(); await saveResult(); console.log(JSON.stringify(result,null,2));
} catch(e) {
  result.error=safe(e?.stack||e?.message||e); result.failedAt=new Date().toISOString(); await saveResult(); console.error(result.error); process.exitCode=1;
} finally { await browser.close(); }
