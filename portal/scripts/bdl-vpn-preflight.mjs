// Probe via the exact browser launch options used by BDL ingestion.
import { chromium } from 'playwright';
import { bdlBrowserOptions } from './bdl-web-proxy.mjs';

let browser;
try {
  browser = await chromium.launch(bdlBrowserOptions());
  const page = await browser.newPage();
  const response = await page.goto('https://api.ipify.org/', { waitUntil: 'domcontentloaded', timeout: 20000 });
  if (!response?.ok()) throw new Error('egress unavailable');
  const identity = (await page.locator('body').innerText()).trim();
  if (!/^[0-9a-fA-F:.]+$/.test(identity)) throw new Error('egress identity invalid');
  if (process.env.BDL_PREFLIGHT_DIRECT !== '1') {
    const bdl = await page.goto('https://bdl.stat.gov.pl/', { waitUntil: 'domcontentloaded', timeout: 25000 });
    if (!bdl || bdl.status() >= 400) throw new Error('BDL unavailable');
  }
  process.stdout.write(identity);
} catch {
  process.exitCode = 1;
} finally {
  await browser?.close();
}
