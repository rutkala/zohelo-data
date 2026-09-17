// Read one complete metadata table using BDL's ordinary website and pager.
// This performs no BDL API requests and never fetches observation values.
import { chromium } from '@playwright/test';
import fs from 'node:fs/promises';

const task = JSON.parse(process.env.BDL_CATALOGUE_TASK || '{}');
const output = process.env.BDL_CATALOGUE_OUTPUT;
const prefix = { categories: 'K', groups: 'G', subgroups: 'P' }[task.kind];
const expectedPath = task.kind === 'categories' ? '/bdl/metadane/kategorie'
  : task.kind === 'groups' ? `/bdl/metadane/grupy/${String(task.category_id).slice(1)}`
    : `/bdl/metadane/podgrupy/${String(task.group_id).slice(1)}`;
if (!prefix || !output || task.url !== `https://bdl.stat.gov.pl${expectedPath}`) {
  throw new Error('Invalid BDL Web catalogue task');
}
const result = { url: task.url, complete: false, records: [], pages: [], expected_count: null };
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ locale: 'pl-PL' });
page.setDefaultTimeout(30000);

async function settle() {
  await page.waitForLoadState('domcontentloaded');
  await page.waitForFunction(() => {
    const manager = window.Sys?.WebForms?.PageRequestManager?.getInstance?.();
    return !manager?.get_isInAsyncPostBack?.();
  }, null, { timeout: 60000 }).catch(() => {});
  await page.waitForTimeout(300);
  await page.waitForFunction(() => {
    const manager = window.Sys?.WebForms?.PageRequestManager?.getInstance?.();
    return !manager?.get_isInAsyncPostBack?.();
  }, null, { timeout: 60000 }).catch(() => {});
}

async function snapshot() {
  return page.evaluate((identityPrefix) => {
    const rows = [...document.querySelectorAll('tr')].flatMap((row) => {
      const cells = [...row.children].filter((el) => ['TD', 'TH'].includes(el.tagName));
      const values = cells.map((c) => (c.innerText || '').trim().replace(/\s+/g, ' '));
      const idAt = values.findIndex((value) => new RegExp(`^${identityPrefix}[0-9]+$`).test(value));
      if (idAt < 1) return [];
      // Exclude category/group selector tables containing a nested grid.
      if (cells.some((cell) => cell.querySelector('table'))) return [];
      const name = values[idAt - 1];
      if (!name) return [];
      return [{ id: values[idAt], name, cells: values }];
    });
    const text = document.body.innerText.replace(/\u00a0/g, ' ');
    const matches = [...text.matchAll(/Strona\s+(\d+)\s+z\s+(\d+),\s*elementy\s+od\s+(\d+)\s+do\s+(\d+)\s+z\s+([\d ]+)\./gi)];
    const pager = matches.length ? matches[0].slice(1).map((v) => Number(v.replace(/\s/g, ''))) : null;
    return { rows, pager, errorPage: /\/errors\//i.test(location.pathname) };
  }, prefix);
}

async function waitForReadySnapshot(expectedPage) {
  const deadline = Date.now() + 45000;
  let lastSnap = null;
  while (Date.now() < deadline) {
    await settle();
    lastSnap = await snapshot();
    if (lastSnap.errorPage) throw new Error('BDL metadata provider error page');
    if (lastSnap.pager) {
      const [number, totalPages, first, last, count] = lastSnap.pager;
      const expectedRows = last - first + 1;
      if (number === expectedPage && lastSnap.rows.length === expectedRows) {
        return lastSnap;
      }
    }
    await page.waitForTimeout(400);
  }
  return lastSnap || snapshot();
}

async function clickNextPage(currentPage) {
  const targetPage = currentPage + 1;
  const deadline = Date.now() + 20000;
  let clicked = false;
  while (Date.now() < deadline) {
    const nextBtn = page.locator('.rgPageNext:visible, button.rgPageNext, input.rgPageNext').first();
    if (await nextBtn.count() && await nextBtn.isVisible().catch(() => false) && await nextBtn.isEnabled().catch(() => false)) {
      await nextBtn.click();
      clicked = true;
      break;
    }
    const numLink = page.locator('.rgNumPart a').filter({ hasText: new RegExp(`^\\s*${targetPage}\\s*$`) }).first();
    if (await numLink.count() && await numLink.isVisible().catch(() => false)) {
      await numLink.click();
      clicked = true;
      break;
    }
    await page.waitForTimeout(400);
  }
  if (!clicked) throw new Error(`BDL metadata next-page control unavailable for page ${targetPage}`);
  await page.waitForFunction((oldPage) => {
    const match = document.body.innerText.match(/Strona\s+(\d+)\s+z\s+\d+/i);
    return match && Number(match[1]) === oldPage + 1;
  }, currentPage, { timeout: 45000 });
  await settle();
}

try {
  const response = await page.goto(task.url, { waitUntil: 'domcontentloaded', timeout: 90000 });
  if (!response || response.status() >= 400) throw new Error(`BDL metadata HTTP ${response?.status()}`);
  await page.waitForFunction(() => /Strona\s+\d+\s+z\s+\d+/.test(document.body.innerText), null, { timeout: 30000 });
  const seen = new Set();
  for (let expectedPage = 1; expectedPage <= 200; expectedPage++) {
    const current = await waitForReadySnapshot(expectedPage);
    if (current.errorPage || !current.pager) throw new Error('BDL metadata pager unavailable or provider error page');
    const [number, totalPages, first, last, count] = current.pager;
    if (number !== expectedPage || number > totalPages || last > count || first < 1) throw new Error('BDL metadata paging identity is inconsistent');
    if (result.expected_count !== null && result.expected_count !== count) throw new Error('BDL metadata count changed during paging');
    result.expected_count = count;
    if (current.rows.length !== last - first + 1) throw new Error(`BDL metadata rows do not match pager: found ${current.rows.length}, expected ${last - first + 1}`);
    for (const row of current.rows) {
      if (seen.has(row.id)) throw new Error(`Duplicate BDL metadata identity ${row.id}`);
      seen.add(row.id);
      result.records.push(row);
    }
    result.pages.push({ page: number, first, last, count });
    if (number === totalPages) {
      if (seen.size !== count || last !== count) throw new Error('BDL metadata table did not exhaust its declared rows');
      result.complete = true;
      break;
    }
    await clickNextPage(number);
  }
  if (!result.complete) throw new Error('BDL metadata safety bound reached; table remains incomplete');
} catch (error) {
  result.error = String(error?.message || error);
  process.exitCode = 1;
} finally {
  await fs.writeFile(output, JSON.stringify(result, null, 2));
  await browser.close();
}
