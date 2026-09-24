import { chromium } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

const outDir = path.resolve(process.argv[2] || 'test-results/teryt');
fs.mkdirSync(outDir, { recursive: true });

async function downloadTeryt() {
    const proxy = process.env.HTTP_PROXY || 'http://127.0.0.1:8081';
    console.log(`Launching stealth browser with proxy: ${proxy}`);
    const browser = await chromium.launch({
        headless: true,
        proxy: { server: proxy },
        args: [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox'
        ]
    });

    const context = await browser.newContext({
        acceptDownloads: true,
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        locale: 'pl-PL'
    });

    await context.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    });

    const page = await context.newPage();
    const url = 'https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/baza_teryt/uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx?contrast=default';
    console.log(`Navigating to ${url}...`);
    await page.goto(url, { waitUntil: 'networkidle', timeout: 45000 });
    console.log(`Page Title: ${await page.title()}`);

    const targets = [
        { id: '#body_BTERCUrzedowyPobierz', name: 'TERC_Urzedowy' },
        { id: '#body_BSIMCUrzedowyPobierz', name: 'SIMC_Urzedowy' },
        { id: '#body_BULICUrzedowyPobierz', name: 'ULIC_Urzedowy' }
    ];

    for (const target of targets) {
        console.log(`Triggering download for ${target.name} (${target.id})...`);
        const [ download ] = await Promise.all([
            page.waitForEvent('download', { timeout: 60000 }),
            page.click(target.id)
        ]);
        const filename = download.suggestedFilename();
        const targetPath = path.join(outDir, filename);
        await download.saveAs(targetPath);
        const stats = fs.statSync(targetPath);
        console.log(`Successfully downloaded ${filename} (${stats.size} bytes) -> ${targetPath}`);
    }

    await browser.close();
    console.log('All TERYT bulk downloads complete.');
}

downloadTeryt().catch(err => {
    console.error('TERYT download error:', err);
    process.exit(1);
});
