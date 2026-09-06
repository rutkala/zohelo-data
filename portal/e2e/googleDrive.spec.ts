import { test, expect } from "@playwright/test";

test.afterEach(async ({ page }, info) => {
  if (info.status !== info.expectedStatus) {
    // Only a fresh profile and synthetic Drive fixtures exist in this context.
    console.log("Drive regression page state:", await page.locator("body").innerText());
  }
});

test("Drive selection opens matching SQL results and a failed selection leaves them identifiable", async ({
  page,
}) => {
  page.on("pageerror", (error) => console.log("Browser error:", error.message));
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning") {
      console.log(`Browser ${message.type()}:`, message.text());
    }
  });
  // Network fixtures contain invented test labels, never real credentials or NBP values.
  await page.addInitScript(() => {
    sessionStorage.setItem("zohelo_gdrive_access_token", "synthetic-test-token");
  });
  await page.route("https://www.googleapis.com/drive/v3/files**", async (route) => {
    const url = new URL(route.request().url());
    const headers = { "access-control-allow-origin": "*" };
    if (url.searchParams.get("alt") === "media") {
      if (url.pathname.endsWith("/good-file")) {
        await route.fulfill({
          headers,
          contentType: "text/csv",
          body: "currency_code,mid_rate\nTEST,1.23\n",
        });
      } else {
        await route.fulfill({ headers, status: 403, body: "Synthetic access denied" });
      }
      return;
    }
    const q = url.searchParams.get("q") ?? "";
    let files: { id: string; name: string; mimeType?: string; size?: string }[] = [];
    if (q.includes("name='zohelo-data'")) files = [{ id: "fixture-root", name: "zohelo-data" }];
    else if (q.includes("name='02_bronze'")) files = [{ id: "fixture-bronze", name: "02_bronze" }];
    else if (q.includes("'fixture-bronze' in parents"))
      files = [
        { id: "good-folder", name: "fixture_rates" },
        { id: "bad-folder", name: "unavailable_rates" },
      ];
    else if (q.includes("'good-folder' in parents"))
      files = [{ id: "good-file", name: "data.csv", mimeType: "text/csv", size: "38" }];
    else if (q.includes("'bad-folder' in parents"))
      files = [{ id: "bad-file", name: "data.csv", mimeType: "text/csv", size: "38" }];
    await route.fulfill({ headers, json: { files } });
  });

  await page.goto("./");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.getByPlaceholder("Profile name").fill("Drive regression");
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(profile).toBeHidden();
  await expect(page.getByRole("status").filter({ hasText: "Catalog loaded" })).toBeVisible({
    timeout: 60000,
  });

  await page.getByText("fixture_rates", { exact: true }).click();
  const tab = page.getByRole("tab", { name: "02_bronze/fixture_rates", exact: true });
  await expect(tab).toHaveAttribute("aria-selected", "true");
  await expect(page.getByText("TEST", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("1.23", { exact: true }).first()).toBeVisible();
  await expect(page.locator(".monaco-editor .view-lines").first()).toContainText(
    '"02_bronze"."fixture_rates"'
  );
  // Exercise the owner's requested interaction: write SQL and inspect its result.
  const editor = page.locator(".monaco-editor .view-lines").first();
  await editor.click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText(
    'SELECT currency_code, mid_rate * 2 AS doubled_rate FROM "02_bronze"."fixture_rates";'
  );
  await expect(editor).toContainText("doubled_rate");
  await page.getByRole("button", { name: "Run Query", exact: true }).click();
  await expect(page.getByText("2.46", { exact: true }).first()).toBeVisible();
  const tabCount = await page.getByRole("tab").count();

  await page.getByText("unavailable_rates", { exact: true }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "Error loading 'unavailable_rates'" })
  ).toBeVisible();
  await expect(page.getByRole("tab")).toHaveCount(tabCount);
  await expect(tab).toHaveAttribute("aria-selected", "true");
  await expect(page.getByText("TEST", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("2.46", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("Drive Demo Mode", { exact: true })).toHaveCount(0);
});
