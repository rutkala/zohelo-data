import { test, expect } from "@playwright/test";

test("public app information works without JavaScript, a profile or Google consent", async ({
  browser,
  baseURL,
}) => {
  const context = await browser.newContext({
    javaScriptEnabled: false,
    viewport: { width: 390, height: 844 },
  });
  const page = await context.newPage();
  const requests: string[] = [];
  page.on("request", (request) => requests.push(request.url()));
  const origin = new URL(baseURL!).origin;
  try {
    const response = await page.goto(new URL("about.html", baseURL!).href);
    expect(response?.status()).toBe(200);
    await expect(page.getByRole("heading", { level: 1 })).toHaveText(
      "A workspace for understanding data."
    );
    await page.getByRole("navigation").getByRole("link", { name: "Privacy", exact: true }).click();
    await expect(page.getByRole("heading", { level: 1 })).toHaveText("Privacy policy");
    await expect(
      page.getByText("Stopping access and removing copies", { exact: true })
    ).toBeVisible();
    await page.getByRole("navigation").getByRole("link", { name: "Terms", exact: true }).click();
    await expect(page.getByRole("heading", { level: 1 })).toHaveText("Terms of use");
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
      true
    );
    expect(requests.every((url) => new URL(url).origin === origin)).toBe(true);
    expect(requests.some((url) => /\.(js|wasm)(\?|$)/.test(url))).toBe(false);
  } finally {
    await context.close();
  }
});

test("an installed service worker keeps policy URLs out of the workspace fallback", async ({
  page,
}) => {
  await page.goto("./about.html");
  await page.evaluate(async () => {
    await navigator.serviceWorker.register(new URL("sw.js", location.href).href);
    await navigator.serviceWorker.ready;
  });
  await page.reload();
  await page.waitForFunction(() => navigator.serviceWorker.controller !== null);
  await page.getByRole("navigation").getByRole("link", { name: "Privacy", exact: true }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Privacy policy");
  await page.getByRole("navigation").getByRole("link", { name: "Terms", exact: true }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Terms of use");
  await expect(page.getByRole("dialog", { name: "Create Profile" })).toHaveCount(0);
});
