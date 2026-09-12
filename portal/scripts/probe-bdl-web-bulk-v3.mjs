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
page.on('request', req => { try { const u=new URL(req.url()); if(u.hostname.endsWith('stat.gov.pl')) events.push({kind:'request',method:req.method(),path:u.pathname,type:req.resourceType()}); } catch {} });
page.on('response', res => { try { const u=new URL(res.url()); if(u.hostname.endsWith('stat.gov.pl')) events.push({kind:'response',status:res.status(),path:u.pathname}); } catch {} });
page.on('dialog', async d => { events.push({kind:'dialog',type:d.type(),message:safe(d.message()).slice(0,500)}); await d.accept(); });

async function write(name,data){await fs.writeFile(path.join(outDir,name),JSON.stringify(data,null,2));}
async function save(){await write('result.json',result);await write('network-events.json',events.slice(-12000));}
async function snapshot(name){
  const body=safe(await page.locator('body').innerText().catch(()=>''));
  const controls=await page.locator('button,input,a,li').evaluateAll(els=>els.map(el=>({id:el.id||null,tag:el.tagName,text:(el.textContent||'').trim().replace(/\s+/g,' ').slice(0,260),value:el.getAttribute('value'),title:el.getAttribute('title'),href:el.getAttribute('href'),disabled:'disabled' in el?!!el.disabled:null,cls:typeof el.className==='string'?el.className:'',visible:!!(el.offsetWidth||el.offsetHeight||el.getClientRects().length)})).filter(x=>/dalej|pobierz|download|zaznacz|wojew|podgrup|export|csv|xls|zip|menu/i.test(`${x.id} ${x.text} ${x.value} ${x.title} ${x.href} ${x.cls}`)).slice(0,1200));
  await write(`${name}.json`,{url:page.url(),title:await page.title(),body:body.slice(0,100000),controls});
}
async function bodyState(){const body=await page.locator('body').innerText().catch(()=>'');const m=body.match(/Wybrano\s+([0-9\s]+)\s+informacji/i);return {selectedInformation:m?Number(m[1].replace(/\s/g,'')):null,counters:[...body.matchAll(/Zaznaczonych:\s*([0-9]+)\/([0-9]+)/gi)].map(x=>({selected:+x[1],total:+x[2]}))};}
async function selectAll(id){const b=page.locator(`#${id}`);if(!await b.isEnabled().catch(()=>false))throw new Error(`${id} disabled`);await b.click();await page.waitForTimeout(2200);return bodyState();}
async function clickNext(){for(const id of ['ctl00_ContentPlaceHolder_dalej1','ctl00_ContentPlaceHolder_dalej2']){const b=page.locator(`#${id}`);if(await b.isVisible().catch(()=>false)&&await b.isEnabled().catch(()=>false)){await b.click();return id;}}return null;}
async function selectedUnitState(){return page.evaluate(()=>{const id='ctl00_ContentPlaceHolder_terytList_SelectedUnits';const c=typeof window.$find==='function'?window.$find(id):null;if(!c)return{found:false};const items=c.get_items?.();return{found:true,itemCount:items?.get_count?.()??null,items:Array.from({length:Math.min(items?.get_count?.()??0,30)},(_,i)=>items.getItem(i).get_text?.())};});}

try{
  await page.goto('https://bdl.stat.gov.pl/bdl/logowanie',{waitUntil:'networkidle',timeout:60000});
  await page.locator('#ctl00_ContentPlaceHolder_Email').fill(email);
  await page.locator('#ctl00_ContentPlaceHolder_Password').fill(password);
  await page.locator('#ctl00_ContentPlaceHolder_SignIn').click();await page.waitForTimeout(2400);
  const loginBody=safe(await page.locator('body').innerText().catch(()=>''));
  result.login={success:!/Użytkownik:\s*Gość/i.test(loginBody)&&/Użytkownik:/i.test(loginBody),urlAfter:page.url()};
  if(!result.login.success)throw new Error('BDL login failed');

  await page.goto('https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/3/7/1341',{waitUntil:'networkidle',timeout:60000});
  result.dimensions.push({name:'years',state:await selectAll('ctl00_ContentPlaceHolder_lata_SelectAll')});
  result.dimensions.push({name:'sex',state:await selectAll('ctl00_ContentPlaceHolder_wym1_SelectAll')});
  result.dimensions.push({name:'age',state:await selectAll('ctl00_ContentPlaceHolder_wym2_SelectAll')});
  result.bulk.selectedInformation=(await bodyState()).selectedInformation;
  if(!await clickNext())throw new Error('Dimension Dalej disabled');await page.waitForTimeout(3000);

  // Native bulk geographic selector: choose every voivodeship, which keeps the proof bounded (16 territories).
  await page.locator('#ctl00_ContentPlaceHolder_terytList_MenuButton').click();await page.waitForTimeout(600);
  const menuItems=page.locator('.RadMenu:visible .rmItem:visible, .RadContextMenu:visible .rmItem:visible, [role="menuitem"]:visible');
  result.geography.menuItems=[];
  let voivodeshipItem=null;
  for(let i=0;i<await menuItems.count();i++){const el=menuItems.nth(i);const text=(await el.innerText().catch(()=>'')).trim();result.geography.menuItems.push(text);if(/^Zaznacz województwa$/i.test(text))voivodeshipItem=el;}
  if(!voivodeshipItem)throw new Error(`Voivodeship selector not found: ${JSON.stringify(result.geography.menuItems)}`);
  await voivodeshipItem.click();await page.waitForTimeout(2500);
  result.geography.selectedUnits=await selectedUnitState();
  await snapshot('01-after-select-voivodeships');
  result.geography.nextId=await clickNext();
  if(!result.geography.nextId)throw new Error(`Dalej disabled after selecting voivodeships: ${JSON.stringify(result.geography.selectedUnits)}`);
  await page.waitForTimeout(6000);
  result.bulk.afterGeographyUrl=page.url();await snapshot('02-after-geography-next');

  const candidates=page.locator('button:visible,input[type="button"]:visible,input[type="submit"]:visible,a:visible');
  result.bulk.exportCandidates=[];let exportControl=null;
  for(let i=0;i<await candidates.count();i++){const el=candidates.nth(i);const label=`${await el.getAttribute('value')||''} ${await el.textContent()||''} ${await el.getAttribute('title')||''}`.trim();if(/pobierz|download|eksport|export/i.test(label)){const enabled=await el.isEnabled().catch(()=>false);result.bulk.exportCandidates.push({label,id:await el.getAttribute('id'),enabled});if(!exportControl&&enabled)exportControl=el;}}

  if(exportControl){
    const beforeUrl=page.url();const dlPromise=page.waitForEvent('download',{timeout:60000}).catch(()=>null);await exportControl.click();await page.waitForTimeout(8000);const dl=await dlPromise;result.bulk.exportClick={beforeUrl,afterUrl:page.url()};
    if(dl){const fn=dl.suggestedFilename().replace(/[^A-Za-z0-9._-]/g,'_');const target=path.join(outDir,`download-${fn}`);await dl.saveAs(target);result.bulk.directDownload={filename:fn,bytes:(await fs.stat(target)).size};}else result.bulk.directDownload=null;
    await snapshot('03-after-export-click');
  }else result.bulk.noEnabledExportControl=true;

  await page.goto('https://bdl.stat.gov.pl/bdl/start',{waitUntil:'networkidle',timeout:60000});await page.waitForTimeout(2500);
  const startBody=safe(await page.locator('body').innerText().catch(()=>''));
  result.bulk.downloadedSubgroupsVisible=/Pobrane podgrupy/i.test(startBody);
  result.bulk.testSubgroupVisible=/Ludność wg pojedynczych roczników wieku i płci/i.test(startBody);
  result.bulk.bulkLinks=await page.locator('a').evaluateAll(as=>as.map(a=>({text:(a.textContent||'').trim(),href:a.href||''})).filter(x=>/pobran|podgrup|1341|roczników wieku/i.test(`${x.text} ${x.href}`)).slice(0,100));
  await snapshot('04-start-after-export');
  result.completedAt=new Date().toISOString();await save();console.log(JSON.stringify(result,null,2));
}catch(e){result.error=safe(e?.stack||e?.message||e);result.failedAt=new Date().toISOString();await save();console.error(result.error);process.exitCode=1;}finally{await browser.close();}
