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
  const controls = await page.locator('button,input,a,li').evaluateAll(els => els.map(el => ({
    id: el.id || null, tag: el.tagName, type: el.getAttribute('type'), text: (el.textContent || '').trim().replace(/\s+/g,' ').slice(0,260),
    value: el.getAttribute('value'), title: el.getAttribute('title'), href: el.getAttribute('href'), disabled: 'disabled' in el ? !!el.disabled : null,
    cls: typeof el.className === 'string' ? el.className : '', visible: !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length)
  })).filter(x => /dalej|pobierz|download|zaznacz|wybran|select|podgrup|export|csv|xls|zip|menu/i.test(`${x.id} ${x.text} ${x.value} ${x.title} ${x.href} ${x.cls}`)).slice(0,1000));
  await write(`${name}.json`, {url:page.url(), title:await page.title(), body, controls});
}
async function saveResult() { await write('result.json', result); await write('network-events.json', events.slice(-10000)); }
async function bodyState() {
  const body = await page.locator('body').innerText().catch(()=> '');
  const total = body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);
  const counters = [...body.matchAll(/Zaznaczonych:\s*([0-9]+)\/([0-9]+)/gi)].map(m => ({selected:Number(m[1]), total:Number(m[2])}));
  return { selectedInformation: total ? Number(total[1].replace(/\s/g,'')) : null, counters };
}
async function clickSelectAll(buttonId) {
  const button = page.locator(`#${buttonId}`); const before = await bodyState();
  if (!await button.isVisible().catch(()=>false)) return {ok:false, reason:'not visible', before};
  if (!await button.isEnabled().catch(()=>false)) return {ok:false, reason:'disabled', before};
  const beforePosts = events.filter(x => x.kind==='request' && x.method==='POST').length;
  await button.click(); await page.waitForTimeout(2200);
  return {ok:true, before, after:await bodyState(), postRequests:events.filter(x => x.kind==='request' && x.method==='POST').length-beforePosts};
}
async function clickEnabledByIds(ids) {
  for (const id of ids) { const loc=page.locator(`#${id}`); if (await loc.isVisible().catch(()=>false) && await loc.isEnabled().catch(()=>false)) { await loc.click(); return id; } }
  return null;
}
async function listSelectionState() {
  return await page.evaluate(() => {
    const state = {};
    for (const id of ['ctl00_ContentPlaceHolder_terytList_UnitsList','ctl00_ContentPlaceHolder_terytList_SelectedUnits']) {
      const c = typeof window.$find === 'function' ? window.$find(id) : null;
      if (!c) { state[id]={found:false}; continue; }
      const items=c.get_items?.(); const selected=c.get_selectedItems?.() || [];
      state[id]={found:true,itemCount:items?.get_count?.() ?? null,selectedCount:selected.length,selected:selected.map(x=>x.get_text?.()).slice(0,20)};
    }
    return state;
  });
}

try {
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie',{waitUntil:'networkidle',timeout:60000});
  await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
  await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
  await page.locator('#ctl00_ContentPlaceHolder_SignIn').click(); await page.waitForTimeout(2400);
  const loginBody=safe(await page.locator('body').innerText().catch(()=>''));
  result.login={urlAfter:page.url(),success:!/Użytkownik:\s*Gość/i.test(loginBody)&&/Użytkownik:/i.test(loginBody)};
  if(!result.login.success) throw new Error('Authenticated BDL login failed');

  await page.goto('https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/3/7/1341',{waitUntil:'networkidle',timeout:60000});
  for(const [buttonId,name] of [['ctl00_ContentPlaceHolder_lata_SelectAll','years'],['ctl00_ContentPlaceHolder_wym1_SelectAll','sex'],['ctl00_ContentPlaceHolder_wym2_SelectAll','age']]){
    const state=await clickSelectAll(buttonId); result.dimensions.push({name,buttonId,...state}); if(!state.ok) throw new Error(`Could not select ${name}`);
  }
  result.bulk.selectedInformation=(await bodyState()).selectedInformation;
  const dimNext=await clickEnabledByIds(['ctl00_ContentPlaceHolder_dalej1','ctl00_ContentPlaceHolder_dalej2']);
  if(!dimNext) throw new Error('Dimension-stage Dalej not enabled'); await page.waitForTimeout(3000);

  const unit0=page.locator('#ctl00_ContentPlaceHolder_terytList_UnitsList_i0');
  const unit1=page.locator('#ctl00_ContentPlaceHolder_terytList_UnitsList_i1');
  result.geography.firstTwoLabels=[(await unit0.innerText()).trim(),(await unit1.innerText()).trim()];
  await unit0.click(); await unit1.click({modifiers:['Control']}); await page.waitForTimeout(500);
  result.geography.beforeTransfer=await listSelectionState();

  // The BDL UI requires transferring highlighted candidates into SelectedUnits via the native Zaznacz context menu.
  const menuButton=page.locator('#ctl00_ContentPlaceHolder_terytList_MenuButton');
  await menuButton.click(); await page.waitForTimeout(700);
  const visibleMenuItems=page.locator('.RadMenu:visible .rmItem:visible, .RadContextMenu:visible .rmItem:visible, [role="menuitem"]:visible');
  result.geography.menuItems=[];
  for(let i=0;i<await visibleMenuItems.count();i++){
    const el=visibleMenuItems.nth(i); result.geography.menuItems.push({index:i,text:(await el.innerText().catch(()=>'' )).trim(),id:await el.getAttribute('id'),cls:await el.getAttribute('class')});
  }
  await snapshot('01-geography-menu-open');

  let transferItem=null;
  for(let i=0;i<await visibleMenuItems.count();i++){
    const el=visibleMenuItems.nth(i); const t=(await el.innerText().catch(()=>'' )).trim();
    if(/wybran|zaznaczon|z listy|bieżąc/i.test(t)){transferItem=el;break;}
  }
  if(!transferItem && await visibleMenuItems.count()===1) transferItem=visibleMenuItems.first();
  if(!transferItem) throw new Error(`Could not identify BDL transfer menu item: ${JSON.stringify(result.geography.menuItems)}`);
  result.geography.transferMenuText=(await transferItem.innerText()).trim();
  await transferItem.click(); await page.waitForTimeout(2200);
  result.geography.afterTransfer=await listSelectionState();
  await snapshot('02-geography-after-transfer');

  const geoNext=await clickEnabledByIds(['ctl00_ContentPlaceHolder_dalej1','ctl00_ContentPlaceHolder_dalej2']);
  result.geography.nextId=geoNext;
  if(!geoNext) throw new Error(`Dalej still disabled after transfer: ${JSON.stringify(result.geography.afterTransfer)}`);
  await page.waitForTimeout(5000); result.bulk.afterGeographyUrl=page.url(); await snapshot('03-after-geography-next');

  const candidates=page.locator('button:visible,input[type="button"]:visible,input[type="submit"]:visible,a:visible');
  const seen=[]; let exportControl=null;
  for(let i=0;i<await candidates.count();i++){
    const el=candidates.nth(i); const label=`${await el.getAttribute('value')||''} ${await el.textContent()||''} ${await el.getAttribute('title')||''}`.trim();
    if(/pobierz|download|eksport|export/i.test(label)){const enabled=await el.isEnabled().catch(()=>false);seen.push({index:i,label,id:await el.getAttribute('id'),enabled});if(!exportControl&&enabled)exportControl=el;}
  }
  result.bulk.exportCandidates=seen;
  if(exportControl){
    const beforeUrl=page.url(); const dlPromise=page.waitForEvent('download',{timeout:45000}).catch(()=>null); await exportControl.click(); await page.waitForTimeout(7000); const dl=await dlPromise;
    result.bulk.exportClick={beforeUrl,afterUrl:page.url()};
    if(dl){const fn=dl.suggestedFilename().replace(/[^A-Za-z0-9._-]/g,'_');const target=path.join(outDir,`download-${fn}`);await dl.saveAs(target);result.bulk.directDownload={filename:fn,bytes:(await fs.stat(target)).size};}else result.bulk.directDownload=null;
    await snapshot('04-after-export-click');
  } else result.bulk.noEnabledExportControl=true;

  await page.goto('https://bdl.stat.gov.pl/bdl/start',{waitUntil:'networkidle',timeout:60000}); await page.waitForTimeout(2200);
  const startBody=safe(await page.locator('body').innerText().catch(()=>''));
  result.bulk.downloadedSubgroupsVisible=/Pobrane podgrupy/i.test(startBody); result.bulk.testSubgroupVisible=/Ludność wg pojedynczych roczników wieku i płci/i.test(startBody);
  result.bulk.bulkLinks=await page.locator('a').evaluateAll(as=>as.map(a=>({text:(a.textContent||'').trim(),href:a.href||''})).filter(x=>/pobran|podgrup|1341|roczników wieku/i.test(`${x.text} ${x.href}`)).slice(0,100));
  await snapshot('05-start-after-export');

  result.completedAt=new Date().toISOString(); await saveResult(); console.log(JSON.stringify(result,null,2));
}catch(e){result.error=safe(e?.stack||e?.message||e);result.failedAt=new Date().toISOString();await saveResult();console.error(result.error);process.exitCode=1;}finally{await browser.close();}
