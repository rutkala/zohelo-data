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
  const body = safe(await page.locator('body').innerText().catch(()=>'')).slice(0,90000);
  const controls = await page.locator('button,input,a').evaluateAll(els => els.map(el => ({
    id: el.id || null, tag: el.tagName, type: el.getAttribute('type'), text: (el.textContent || '').trim().replace(/\s+/g,' ').slice(0,240),
    value: el.getAttribute('value'), title: el.getAttribute('title'), href: el.getAttribute('href'), disabled: 'disabled' in el ? !!el.disabled : null,
    cls: typeof el.className === 'string' ? el.className : ''
  })).filter(x => /dalej|pobierz|download|zaznacz|select|podgrup|export|csv|xls|zip/i.test(`${x.id} ${x.text} ${x.value} ${x.title} ${x.href}`)).slice(0,500));
  await write(`${name}.json`, {url:page.url(), title:await page.title(), body, controls});
}
async function saveResult() {
  await write('result.json', result);
  await write('network-events.json', events.slice(-8000));
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
  await page.waitForTimeout(2300);
  return {ok:true, before, after:await bodyState(), postRequests:events.filter(x => x.kind==='request' && x.method==='POST').length-beforePosts};
}
async function clickEnabledByIds(ids) {
  for (const id of ids) {
    const loc = page.locator(`#${id}`);
    if (await loc.isVisible().catch(()=>false) && await loc.isEnabled().catch(()=>false)) {
      await loc.click();
      return id;
    }
  }
  return null;
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
  for (const [buttonId,name] of [
    ['ctl00_ContentPlaceHolder_lata_SelectAll','years'],
    ['ctl00_ContentPlaceHolder_wym1_SelectAll','sex'],
    ['ctl00_ContentPlaceHolder_wym2_SelectAll','age'],
  ]) {
    const state = await clickSelectAll(buttonId);
    result.dimensions.push({name,buttonId,...state});
    if (!state.ok) throw new Error(`Could not select ${name}: ${state.reason}`);
  }
  result.bulk.selectedInformation = (await bodyState()).selectedInformation;
  await snapshot('01-dimensions-selected');

  const dimNext = await clickEnabledByIds(['ctl00_ContentPlaceHolder_dalej1','ctl00_ContentPlaceHolder_dalej2']);
  if (!dimNext) throw new Error('Dimension-stage Dalej is not enabled');
  await page.waitForTimeout(3000);
  result.geography.url = page.url();
  await snapshot('02-geography-initial');

  // Select two territories only. 2 × 2,790 = 5,580 cells, enough to force the large-selection path
  // while keeping the proof deliberately small.
  const unit0 = page.locator('#ctl00_ContentPlaceHolder_terytList_UnitsList_i0');
  const unit1 = page.locator('#ctl00_ContentPlaceHolder_terytList_UnitsList_i1');
  result.geography.firstTwoLabels = [(await unit0.innerText()).trim(), (await unit1.innerText()).trim()];
  await unit0.click();
  await unit1.click({modifiers:['Control']});
  await page.waitForTimeout(1600);
  result.geography.clientSelection = await page.evaluate(() => {
    const c = typeof window.$find === 'function' ? window.$find('ctl00_ContentPlaceHolder_terytList_UnitsList') : null;
    if (!c) return {clientFound:false};
    const selected = c.get_selectedItems?.() || [];
    return {clientFound:true, selectedCount:selected.length, selected:selected.map(x => x.get_text?.()).slice(0,20)};
  });
  await snapshot('03-geography-two-selected');

  const geoNextId = await clickEnabledByIds(['ctl00_ContentPlaceHolder_dalej1','ctl00_ContentPlaceHolder_dalej2']);
  result.geography.nextId = geoNextId;
  if (!geoNextId) throw new Error(`Geography Dalej did not enable after selecting two units; selection=${JSON.stringify(result.geography.clientSelection)}`);
  await page.waitForTimeout(5000);
  result.bulk.afterGeographyUrl = page.url();
  await snapshot('04-after-geography-next');

  const exportCandidates = page.locator('button:visible,input[type="button"]:visible,input[type="submit"]:visible,a:visible');
  const seen = [];
  let exportControl = null;
  for (let i=0; i<await exportCandidates.count(); i++) {
    const el=exportCandidates.nth(i);
    const label=`${await el.getAttribute('value')||''} ${await el.textContent()||''} ${await el.getAttribute('title')||''}`.trim();
    if (/pobierz|download|eksport|export/i.test(label)) {
      seen.push({index:i,label,id:await el.getAttribute('id'),enabled:await el.isEnabled().catch(()=>false)});
      if (!exportControl && await el.isEnabled().catch(()=>false)) exportControl=el;
    }
  }
  result.bulk.exportCandidates=seen;

  if (exportControl) {
    const beforeUrl=page.url();
    const dlPromise=page.waitForEvent('download',{timeout:45000}).catch(()=>null);
    await exportControl.click();
    await page.waitForTimeout(6000);
    const dl=await dlPromise;
    result.bulk.exportClick={beforeUrl,afterUrl:page.url()};
    if (dl) {
      const fn=dl.suggestedFilename().replace(/[^A-Za-z0-9._-]/g,'_');
      const target=path.join(outDir,`download-${fn}`);
      await dl.saveAs(target);
      result.bulk.directDownload={filename:fn,bytes:(await fs.stat(target)).size};
    } else result.bulk.directDownload=null;
    await snapshot('05-after-export-click');
  } else result.bulk.noEnabledExportControl=true;

  await page.goto('https://bdl.stat.gov.pl/bdl/start', {waitUntil:'networkidle',timeout:60000});
  await page.waitForTimeout(2500);
  const startBody=safe(await page.locator('body').innerText().catch(()=>''));
  result.bulk.downloadedSubgroupsVisible=/Pobrane podgrupy/i.test(startBody);
  result.bulk.testSubgroupVisible=/Ludność wg pojedynczych roczników wieku i płci/i.test(startBody);
  result.bulk.bulkLinks=await page.locator('a').evaluateAll(as=>as.map(a=>({text:(a.textContent||'').trim(),href:a.href||''})).filter(x=>/pobran|podgrup|1341|roczników wieku/i.test(`${x.text} ${x.href}`)).slice(0,100));
  await snapshot('06-start-after-export');

  result.completedAt=new Date().toISOString(); await saveResult(); console.log(JSON.stringify(result,null,2));
} catch(e) {
  result.error=safe(e?.stack||e?.message||e); result.failedAt=new Date().toISOString(); await saveResult(); console.error(result.error); process.exitCode=1;
} finally { await browser.close(); }
