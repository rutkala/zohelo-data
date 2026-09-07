import { expect, test, type Page } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const fixturePath = path.join(here, "..", "dist", "docs", "index.html");
let wroteFixture = false;

async function ensureProfile(page: Page) {
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  if (!(await profile.isVisible().catch(() => false))) return;
  await profile.getByPlaceholder("Profile name").fill("catalog docs regression");
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(profile).toBeHidden();
}

async function openTechnicalDocs(page: Page) {
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Business catalogue", exact: true })
    .click();
  await page.getByRole("button", { name: "Technical dbt docs" }).click();
}

test.beforeAll(() => {
  if (fs.existsSync(fixturePath)) return;
  fs.mkdirSync(path.dirname(fixturePath), { recursive: true });
  fs.writeFileSync(
    fixturePath,
    "<!doctype html><html><head><title>dbt Docs</title></head><body><h1>Fixture dbt Catalog</h1></body></html>"
  );
  wroteFixture = true;
});

test.afterAll(() => {
  if (wroteFixture) fs.rmSync(path.dirname(fixturePath), { recursive: true, force: true });
});

test("a service worker serves dbt docs inside the technical catalogue view", async ({ page }) => {
  await page.goto("./");
  await page.evaluate(async () => {
    await navigator.serviceWorker.register(new URL("sw.js", location.href).href);
    await navigator.serviceWorker.ready;
  });
  await page.reload();
  await page.waitForFunction(() => navigator.serviceWorker.controller !== null);
  await ensureProfile(page);

  await openTechnicalDocs(page);

  const docsFrame = page.frameLocator('iframe[title="dbt Catalog & Lineage"]');
  await expect(docsFrame.locator("h1")).toHaveText("Fixture dbt Catalog");
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("a missing docs file is reported instead of embedding the portal fallback", async ({
  page,
}) => {
  await page.route("**/docs/index.html", async (route) => {
    await route.fulfill({ status: 404, contentType: "text/html", body: "Not found" });
  });
  await page.goto("./");
  await ensureProfile(page);

  await openTechnicalDocs(page);

  await expect(page.getByRole("alert")).toContainText(
    "Generated dbt documentation is unavailable."
  );
  await expect(page.getByRole("alert")).toContainText("The docs server returned 404.");
  await expect(page.locator('iframe[title="dbt Catalog & Lineage"]')).toHaveCount(0);
});
