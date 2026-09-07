import { expect, test, type Locator, type Page } from "@playwright/test";
import { createHash } from "node:crypto";

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");
const datasets = [
  "bronze_nbp_exchange_rates_table_a",
  "bronze_nbp_exchange_rates_table_b",
  "bronze_nbp_exchange_rates_table_c",
  "bronze_nbp_gold_prices",
  "nbp_exchange_rates_table_a",
  "nbp_exchange_rates_table_b",
  "nbp_exchange_rates_table_c",
  "nbp_gold_prices",
  "nbp_change_events",
  "fact_fx_quotes",
  "fact_gold_prices",
  "dim_date",
  "dim_currency",
  "dim_source_table",
  "dim_commodity",
];
const layerFor = (dataset: string) =>
  dataset.startsWith("bronze_")
    ? "02_bronze"
    : dataset.startsWith("fact_") || dataset.startsWith("dim_")
      ? "04_gold"
      : "03_silver";

test.afterEach(async ({ page }, info) => {
  if (info.status !== info.expectedStatus) {
    const statuses = await page
      .getByRole("status")
      .allTextContents()
      .catch(() => [] as string[]);
    console.log("Join example status values:", statuses);
    console.log("Join example page state:", await page.locator("body").innerText());
  }
});

async function installReleaseFixture(page: Page) {
  const releaseId = "123e4567-e89b-42d3-a456-426614174001";
  const codeSha = "b".repeat(40);
  const files: Record<string, string> = {
    "fact-fx-file": [
      "source_table_key,effective_date,currency_key,quote_currency_key,mid,bid,ask",
      "A,2026-09-02,USD,PLN,3.92,,",
    ].join("\n"),
    "dim-currency-file": "currency_key,source_currency_name\nUSD,US dollar\nPLN,\n",
    "dim-date-file":
      "date_key,calendar_year,calendar_month,calendar_day,iso_weekday\n2026-09-02,2026,9,2,3\n",
  };
  const downloadedDataFileIds: string[] = [];
  const catalogue = JSON.stringify({
    format_version: 1,
    code_sha: codeSha,
    sources: ["A", "B", "C", "Gold"].map((suffix) => ({
      source_id: `nbp_${suffix.toLowerCase()}`,
      name: `NBP Table ${suffix}`,
      description: "Published source.",
      status: "published_snapshot",
      checked_through: "2026-09-02",
      latest_observation_date: "2026-09-02",
      last_successful_ingestion_at: "2026-09-02T00:00:00Z",
      last_attempt_at: "2026-09-02T00:01:00Z",
      raw_response_count: 1,
    })),
    lineage: { nodes: [], edges: [] },
    metrics: [],
  });
  const manifest = JSON.stringify({
    format_version: 2,
    release_id: releaseId,
    release_scope: "nbp_platform",
    status: "validated",
    code_sha: codeSha,
    created_at_utc: "2026-09-07T00:00:00Z",
    datasets: datasets.map((dataset_id) => {
      const fileId =
        dataset_id === "fact_fx_quotes"
          ? "fact-fx-file"
          : dataset_id === "dim_currency"
            ? "dim-currency-file"
            : dataset_id === "dim_date"
              ? "dim-date-file"
              : `${dataset_id}-file`;
      const body = files[fileId] ?? "unused";
      return {
        dataset_id,
        layer: layerFor(dataset_id),
        table_name: dataset_id.replace(/^bronze_/, ""),
        row_count: dataset_id === "nbp_change_events" ? 0 : 1,
        min_date: [
          "dim_currency",
          "dim_source_table",
          "dim_commodity",
          "nbp_change_events",
        ].includes(dataset_id)
          ? null
          : "2026-09-02",
        max_date: [
          "dim_currency",
          "dim_source_table",
          "dim_commodity",
          "nbp_change_events",
        ].includes(dataset_id)
          ? null
          : "2026-09-02",
        columns: [{ name: "id", type: "VARCHAR" }],
        files: [
          {
            id: fileId,
            name: `${dataset_id}.csv`,
            size: Buffer.byteLength(body),
            sha256: sha256(body),
          },
        ],
      };
    }),
    artifacts: [
      ["manifest.json", "artifact-manifest"],
      ["catalog.json", "artifact-catalog"],
      ["run_results.json", "artifact-run-results"],
      ["ingestion-state.json", "artifact-ingestion-state"],
      ["business-catalog.json", "business-catalogue-id"],
    ].map(([name, id]) => ({
      id,
      name,
      size: name === "business-catalog.json" ? Buffer.byteLength(catalogue) : 1,
      sha256: name === "business-catalog.json" ? sha256(catalogue) : "a".repeat(64),
    })),
    inputs: [],
    tests: { passed: true },
  });
  const pointer = JSON.stringify({
    format_version: 1,
    release_id: releaseId,
    manifest_file_id: "manifest-id",
    manifest_sha256: sha256(manifest),
    updated_at_utc: "2026-09-07T00:01:00Z",
  });

  await page.addInitScript(() => {
    sessionStorage.setItem("zohelo_gdrive_access_token", "synthetic-test-token");
  });
  await page.route("https://www.googleapis.com/drive/v3/files**", async (route) => {
    const url = new URL(route.request().url());
    const headers = { "access-control-allow-origin": "*" };
    if (url.searchParams.get("alt") === "media") {
      const fileId = url.pathname.split("/").at(-1) ?? "";
      if (fileId in files) downloadedDataFileIds.push(fileId);
      const body =
        fileId === "pointer-id"
          ? pointer
          : fileId === "business-catalogue-id"
            ? catalogue
            : (files[fileId] ?? manifest);
      await route.fulfill({ headers, contentType: "application/json", body });
      return;
    }
    if (url.pathname.endsWith("/manifest-id")) {
      await route.fulfill({
        headers,
        json: {
          id: "manifest-id",
          name: "release.json",
          size: String(Buffer.byteLength(manifest)),
        },
      });
      return;
    }
    const query = url.searchParams.get("q") ?? "";
    const response = query.includes("name='zohelo-data'")
      ? [{ id: "fixture-root", name: "zohelo-data" }]
      : query.includes("name='current-release.json'")
        ? [
            {
              id: "pointer-id",
              name: "current-release.json",
              size: String(Buffer.byteLength(pointer)),
            },
          ]
        : [];
    await route.fulfill({ headers, json: { files: response } });
  });

  return downloadedDataFileIds;
}

test("a generated catalogue query lazily loads its referenced gold tables when run", async ({
  page,
}) => {
  const downloadedDataFileIds = await installReleaseFixture(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("./");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.getByPlaceholder("Profile name").fill("Join example regression");
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(profile).toBeHidden();

  await expect(page.getByRole("status").filter({ hasText: "nbp_platform" }).first()).toBeVisible({
    timeout: 60_000,
  });
  await page.getByRole("button", { name: "Tables", exact: true }).click();
  const dataExplorer = page.getByLabel("Data Explorer");
  await dataExplorer.getByText("04_gold", { exact: true }).click();
  await dataExplorer.getByText("fact_fx_quotes", { exact: true }).click();
  await expect(
    dataExplorer.getByRole("status").filter({ hasText: "Loaded 'fact_fx_quotes'" })
  ).toBeVisible();
  await dataExplorer.getByRole("button", { name: "Close", exact: true }).click();

  const editor = page.locator(".monaco-editor .view-lines:visible").first();
  await expect(editor).toContainText('"04_gold"."fact_fx_quotes"');
  await editor.click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.evaluate(
    (sql) => navigator.clipboard.writeText(sql),
    `WITH rates AS (
  SELECT effective_date, currency_key, mid
  FROM "04_gold"."fact_fx_quotes"
)
SELECT rates.effective_date, rates.mid, currency.source_currency_name, dates.calendar_year
FROM rates
JOIN "04_gold"."dim_currency" AS currency ON currency.currency_key = rates.currency_key
JOIN "04_gold"."dim_date" AS dates ON dates.date_key = rates.effective_date;`
  );
  // A real paste avoids Monaco treating a multiline insertion as typed text
  // and adding an extra auto-closing parenthesis after the CTE.
  await page.keyboard.press("ControlOrMeta+V");
  await expect(editor).toContainText('JOIN "04_gold"."dim_currency"');
  expect(downloadedDataFileIds).toEqual(["fact-fx-file"]);
  await expect(page.getByText("US dollar", { exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("cell", { name: "US dollar", exact: true })).toBeVisible();
  expect(new Set(downloadedDataFileIds)).toEqual(
    new Set(["fact-fx-file", "dim-currency-file", "dim-date-file"])
  );
});

test("keeps the light and dark layout palettes consistent across desktop and mobile", async ({
  page,
}) => {
  await installReleaseFixture(page);
  await page.addInitScript(() => localStorage.setItem("vite-ui-theme", "dark"));
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto("./");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.getByPlaceholder("Profile name").fill("Theme regression");
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(profile).toBeHidden();
  await expect(page.locator("html")).toHaveClass(/dark/);
  await expect(page.getByRole("status").filter({ hasText: "nbp_platform" }).first()).toBeVisible();
  await page.getByRole("button", { name: "New SQL query", exact: true }).click();

  // This test compares final palette values, not animation frames. Disable
  // transitions only in this isolated page so a theme switch cannot capture
  // an interpolated desktop colour before the mobile comparison.
  await page.addStyleTag({
    content: "*, *::before, *::after { transition: none !important; animation: none !important; }",
  });

  const palette = (locator: Locator) =>
    locator.evaluate((element) => {
      const style = getComputedStyle(element);
      return {
        background: style.backgroundColor,
        foreground: style.color,
        border: style.borderColor,
      };
    });
  const themeTokens = () =>
    page.locator("html").evaluate((element) => {
      const style = getComputedStyle(element);
      return {
        background: style.getPropertyValue("--background"),
        foreground: style.getPropertyValue("--foreground"),
        input: style.getPropertyValue("--input"),
      };
    });

  const expectResponsivePalette = async (theme: "light" | "dark") => {
    await page.setViewportSize({ width: 1440, height: 1000 });
    await expect(page.locator("html")).toHaveClass(new RegExp(theme));
    const desktopTokens = await themeTokens();
    const desktopNavigation = await palette(
      page.getByRole("navigation", { name: "Main navigation" })
    );
    const desktopRun = await palette(
      page.getByRole("button", { name: "Run Query", exact: true }).filter({ visible: true })
    );

    await page.setViewportSize({ width: 390, height: 844 });
    await expect(page.locator("html")).toHaveClass(new RegExp(theme));
    const tables = page.getByRole("button", { name: "Tables", exact: true });
    const mobileRun = page.getByRole("button", { name: "Run", exact: true });
    await expect(tables).toBeVisible();
    await expect(mobileRun).toBeVisible();

    expect(await themeTokens()).toEqual(desktopTokens);
    expect(await palette(tables.locator(".."))).toEqual(desktopNavigation);
    expect(await palette(mobileRun)).toEqual(desktopRun);
  };

  await expectResponsivePalette("dark");
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.getByRole("button", { name: "Switch to light mode" }).click();
  await expectResponsivePalette("light");
});

for (const mobile of [false, true]) {
  test(`table menus create and query a view without loading its source first (${mobile ? "phone" : "desktop"})`, async ({
    page,
  }) => {
    const downloads = await installReleaseFixture(page);
    await page.setViewportSize(
      mobile ? { width: 390, height: 844 } : { width: 1440, height: 1000 }
    );
    await page.goto("./");
    const profile = page.getByRole("dialog", { name: "Create Profile" });
    await profile.getByPlaceholder("Profile name").fill("Table actions regression");
    await profile.getByRole("button", { name: "Create Profile" }).click();
    await expect(profile).toBeHidden();
    await expect(
      page.getByRole("status").filter({ hasText: "nbp_platform" }).first()
    ).toBeVisible();
    await page.getByRole("button", { name: "New SQL query", exact: true }).click();
    const editor = page.locator(".monaco-editor .view-lines:visible").first();
    const pasteSql = async (sql: string) => {
      await editor.click();
      await page.keyboard.press("ControlOrMeta+A");
      await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);
      await page.evaluate((text) => navigator.clipboard.writeText(text), sql);
      await page.keyboard.press("ControlOrMeta+V");
    };
    await pasteSql("SELECT 42 AS keep_my_draft;");
    if (mobile) await page.getByRole("button", { name: "Tables", exact: true }).click();
    const explorer = mobile ? page.getByLabel("Data Explorer") : page.locator("main");
    await explorer.getByText("04_gold", { exact: true }).click();
    await explorer.getByRole("button", { name: "Actions for dim_currency", exact: true }).click();
    await page.getByRole("menuitem", { name: "Query as SELECT", exact: true }).click();
    if (mobile) await expect(page.getByLabel("Data Explorer")).toBeHidden();
    await expect(editor).toContainText('"04_gold"."dim_currency"');
    expect(downloads).toEqual([]);

    // The menu created a new query without destroying the existing draft.
    await page
      .getByRole("tab", { name: "Untitled Query", exact: true })
      .filter({ visible: true })
      .click();
    await expect(editor).toContainText("keep_my_draft");
    await page
      .getByRole("tab", { name: "dim_currency", exact: true })
      .filter({ visible: true })
      .click();
    await page
      .getByRole("button", { name: "Create view", exact: true })
      .filter({ visible: true })
      .click();
    const dialog = page.getByRole("dialog", { name: "Create a view", exact: true });
    await expect(dialog).toContainText("database session");
    await dialog.getByRole("textbox", { name: "View name", exact: true }).fill("my_currency_view");
    await dialog.getByRole("button", { name: "Create view", exact: true }).click();
    await expect(dialog).toBeHidden();
    await expect(editor).toContainText('CREATE VIEW "main"."my_currency_view"');
    await expect(
      page.getByText("View main.my_currency_view created in Browser workspace", { exact: true })
    ).toBeVisible();
    expect(downloads).toEqual(["dim-currency-file"]);

    // A name collision must leave the existing view and its values intact.
    await pasteSql("SELECT 'must_not_replace' AS source_currency_name;");
    await page
      .getByRole("button", { name: "Create view", exact: true })
      .filter({ visible: true })
      .click();
    await dialog.getByRole("textbox", { name: "View name", exact: true }).fill("my_currency_view");
    await dialog.getByRole("button", { name: "Create view", exact: true }).click();
    await expect(page.getByRole("alert").filter({ hasText: "Query Error" })).toContainText(
      /already exists/i
    );

    if (mobile) await page.getByRole("button", { name: "Tables", exact: true }).click();
    await explorer.getByRole("treeitem", { name: "Browser workspace", exact: true }).click();
    await expect(
      explorer.getByRole("treeitem").filter({ hasText: "my_currency_view" }).last()
    ).toBeVisible();
    await explorer
      .getByRole("button", { name: "Actions for my_currency_view", exact: true })
      .click();
    await page.getByRole("menuitem", { name: "Query as SELECT", exact: true }).click();
    if (mobile) await expect(page.getByLabel("Data Explorer")).toBeHidden();
    await expect(editor).toContainText('"my_currency_view"');
    await page
      .getByRole("button", { name: mobile ? "Run" : "Run Query", exact: true })
      .filter({ visible: true })
      .click();
    await expect(page.getByRole("cell", { name: "US dollar", exact: true })).toBeVisible();

    // Empty-canvas insertion supports touch; desktop also exercises real drag data.
    await pasteSql("");
    if (mobile) {
      await page.getByRole("button", { name: "Tables", exact: true }).click();
      await explorer.getByRole("button", { name: "Actions for dim_date", exact: true }).click();
      await page.getByRole("menuitem", { name: "Insert in SQL editor", exact: true }).click();
      await expect(page.getByLabel("Data Explorer")).toBeHidden();
    } else {
      const source = explorer.locator('[draggable="true"]').filter({ hasText: "dim_date" }).first();
      await source.dragTo(page.getByTestId("sql-drop-target").filter({ visible: true }), {
        targetPosition: { x: 100, y: 50 },
      });
    }
    await expect(editor).toContainText('SELECT * FROM "04_gold"."dim_date" LIMIT 100;');
    await page
      .getByRole("button", { name: mobile ? "Run" : "Run Query", exact: true })
      .filter({ visible: true })
      .click();
    await expect(page.getByRole("cell", { name: "2026", exact: true })).toBeVisible();
    await expect(page.locator(".monaco-editor")).toHaveCount(1);
    if (mobile) {
      // One editor survives both sides of the responsive breakpoint.
      await pasteSql("SELECT 987 AS responsive_draft;");
      await page.setViewportSize({ width: 768, height: 1024 });
      await expect(editor).toContainText("responsive_draft");
      await expect(page.locator(".monaco-editor")).toHaveCount(1);
      // The expanded Gold folder is shared across explorer layouts.
      const action = page
        .getByRole("button", { name: "Actions for dim_date", exact: true })
        .filter({ visible: true });
      const actionBox = await action.boundingBox();
      const panelBox = await page.locator("[data-panel]").filter({ has: action }).boundingBox();
      expect(actionBox).not.toBeNull();
      expect(panelBox).not.toBeNull();
      expect(actionBox!.x + actionBox!.width).toBeLessThanOrEqual(
        panelBox!.x + panelBox!.width + 1
      );
      await page.setViewportSize({ width: 390, height: 844 });
      await expect(editor).toContainText("responsive_draft");
      await expect(page.locator(".monaco-editor")).toHaveCount(1);
    }
    expect(downloads).toEqual(["dim-currency-file", "dim-date-file"]);
    const size = await page.evaluate(() => ({
      width: document.documentElement.clientWidth,
      scroll: document.documentElement.scrollWidth,
    }));
    expect(size.scroll).toBeLessThanOrEqual(size.width + 1);
    await page.screenshot({
      path: test.info().outputPath(`table-actions-${mobile ? "phone" : "desktop"}.png`),
    });
  });
}
