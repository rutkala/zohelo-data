import { test, expect } from "@playwright/test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

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
      files = [{ id: "good-file", name: "data.csv", mimeType: "text/csv", size: "33" }];
    else if (q.includes("'bad-folder' in parents"))
      files = [{ id: "bad-file", name: "data.csv", mimeType: "text/csv", size: "33" }];
    await route.fulfill({ headers, json: { files } });
  });

  await page.goto("./");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.getByPlaceholder("Profile name").fill("Drive regression");
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(profile).toBeHidden();
  await expect(
    page.getByRole("status").filter({ hasText: "Legacy/unversioned catalog loaded" }).first()
  ).toBeVisible({
    timeout: 60000,
  });

  await page.getByText("fixture_rates", { exact: true }).click();
  const tab = page.getByRole("tab", { name: "02_bronze/fixture_rates", exact: true });
  await expect(tab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator(":text-is('TEST'):visible").first()).toBeVisible();
  await expect(page.getByText("1.23", { exact: true }).first()).toBeVisible();
  await expect(page.locator(".monaco-editor .view-lines:visible").first()).toContainText(
    '"02_bronze"."fixture_rates"'
  );
  // Exercise the owner's requested interaction: write SQL and inspect its result.
  const editor = page.locator(".monaco-editor .view-lines:visible").first();
  await editor.click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText(
    'SELECT currency_code, mid_rate * 2 AS doubled_rate FROM "02_bronze"."fixture_rates";'
  );
  await expect(editor).toContainText("doubled_rate");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByText("2.46", { exact: true }).first()).toBeVisible();
  const tabCount = await page.getByRole("tab").count();

  await page.getByText("unavailable_rates", { exact: true }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "Error loading 'unavailable_rates'" })
  ).toBeVisible();
  await expect(page.getByRole("tab")).toHaveCount(tabCount);
  await expect(tab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator(":text-is('TEST'):visible").first()).toBeVisible();
  await expect(page.getByText("2.46", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("Drive Demo Mode", { exact: true })).toHaveCount(0);
});

const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");

const platformDatasetIds = [
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
const platformLayer = (id: string) =>
  id.startsWith("bronze_")
    ? "02_bronze"
    : id.startsWith("fact_") || id.startsWith("dim_")
      ? "04_gold"
      : "03_silver";

test("v2 NBP and source Landing snapshots are queryable together", async ({ page }) => {
  const releaseId = "123e4567-e89b-42d3-a456-426614174000";
  const codeSha = "a".repeat(40);
  const goldCsv = "currency_code,mid_rate\nTEST,1.23\n";
  const catalogue = JSON.stringify({
    format_version: 1,
    code_sha: codeSha,
    sources: ["A", "B", "C", "Gold"].map((suffix, index) => ({
      source_id: `nbp_${suffix.toLowerCase()}`,
      name: `NBP Table ${suffix}`,
      description: `Published source ${suffix}.`,
      status: "published_snapshot",
      checked_through: "2026-09-01",
      latest_observation_date: index === 0 ? "2026-08-30" : "2026-08-31",
      last_successful_ingestion_at: "2026-09-01T01:00:00Z",
      last_attempt_at: "2026-09-01T01:05:00Z",
      raw_response_count: index + 1,
    })),
    lineage: {
      nodes: Array.from({ length: 31 }, (_, index) => ({
        id: `node_${index}`,
        label: `Lineage node ${index}`,
        kind: "model",
        layer: "03_silver",
        description: "Published dependency.",
      })),
      edges: [{ from: "node_0", to: "node_1" }],
    },
    metrics: [],
  });
  const manifest = JSON.stringify({
    format_version: 2,
    release_id: releaseId,
    release_scope: "nbp_platform",
    status: "validated",
    code_sha: codeSha,
    created_at_utc: "2026-09-06T00:00:00Z",
    datasets: platformDatasetIds.map((dataset_id) => {
      const nullBounds =
        dataset_id === "dim_currency" ||
        dataset_id === "dim_source_table" ||
        dataset_id === "dim_commodity" ||
        dataset_id === "nbp_change_events";
      const isGoldQuotes = dataset_id === "fact_fx_quotes";
      return {
        dataset_id,
        layer: platformLayer(dataset_id),
        table_name: dataset_id,
        row_count: dataset_id === "nbp_change_events" ? 0 : 1,
        min_date: nullBounds ? null : "2024-01-01",
        max_date: nullBounds ? null : "2024-01-01",
        columns: [{ name: "id", type: "INTEGER" }],
        files: [
          {
            id: isGoldQuotes ? "gold-file" : `${dataset_id}-file`,
            name: isGoldQuotes ? "fact_fx_quotes.csv" : `${dataset_id}.parquet`,
            size: isGoldQuotes ? Buffer.byteLength(goldCsv) : 1,
            sha256: isGoldQuotes ? sha256(goldCsv) : "a".repeat(64),
          },
        ],
      };
    }),
    artifacts: [
      "manifest.json",
      "catalog.json",
      "run_results.json",
      "ingestion-state.json",
      "business-catalog.json",
    ].map((name) => ({
      id:
        name === "business-catalog.json"
          ? "business-catalogue-id"
          : `artifact-${name.replace(/\./g, "-")}`,
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
    updated_at_utc: "2026-09-06T00:01:00Z",
  });
  const landingSnapshotId = "223e4567-e89b-42d3-a456-426614174000";
  const landingFragmentName = "fragment-323e4567-e89b-42d3-a456-426614174000.parquet";
  const landingParquet = readFileSync(
    new URL("./fixtures/landing-response.parquet", import.meta.url)
  );
  const landingManifest = JSON.stringify({
    format_version: 1,
    kind: "landing_snapshot",
    source_id: "world_bank_wdi",
    snapshot_id: landingSnapshotId,
    created_at_utc: "2026-09-08T12:00:00+00:00",
    code_sha: codeSha,
    status: "validated",
    layer: "01_landing",
    table_name: "world_bank_wdi_responses",
    row_count: 1,
    coverage_status: "incomplete",
    files: [
      {
        id: "landing-parquet-id",
        name: landingFragmentName,
        size: landingParquet.byteLength,
        sha256: sha256(landingParquet),
      },
    ],
    columns: [
      ["source_id", "VARCHAR"],
      ["task_id", "VARCHAR"],
      ["lane", "VARCHAR"],
      ["task_kind", "VARCHAR"],
      ["retrieved_at_utc", "TIMESTAMP"],
      ["record_count", "BIGINT"],
      ["raw_sha256", "VARCHAR"],
      ["raw_size_bytes", "BIGINT"],
      ["request_json", "VARCHAR"],
      ["metadata_json", "VARCHAR"],
      ["payload_utf8", "VARCHAR"],
      ["content_type", "VARCHAR"],
    ].map(([name, type]) => ({ name, type })),
    accepted_response_count: 1,
    published_response_count: 1,
    pending_publication_count: 0,
    receipt_checkpoint_sha256: "d".repeat(64),
    tests: { passed: true },
  });
  const landingPointer = JSON.stringify({
    format_version: 1,
    source_id: "world_bank_wdi",
    snapshot_id: landingSnapshotId,
    manifest_file_id: "landing-manifest-id",
    manifest_file_name: `manifest-${landingSnapshotId}.json`,
    manifest_sha256: sha256(landingManifest),
    manifest_size_bytes: Buffer.byteLength(landingManifest),
  });

  await page.setViewportSize({ width: 390, height: 844 });
  await page.addInitScript(() => {
    sessionStorage.setItem("zohelo_gdrive_access_token", "synthetic-test-token");
  });
  await page.route("https://www.googleapis.com/drive/v3/files**", async (route) => {
    const url = new URL(route.request().url());
    const headers = { "access-control-allow-origin": "*" };
    if (url.searchParams.get("alt") === "media") {
      if (url.pathname.endsWith("/landing-parquet-id")) {
        await route.fulfill({
          headers,
          contentType: "application/octet-stream",
          body: landingParquet,
        });
        return;
      }
      const body = url.pathname.endsWith("/pointer-id")
        ? pointer
        : url.pathname.endsWith("/landing-pointer-id")
          ? landingPointer
          : url.pathname.endsWith("/landing-manifest-id")
            ? landingManifest
            : url.pathname.endsWith("/business-catalogue-id")
              ? catalogue
              : url.pathname.endsWith("/gold-file")
                ? goldCsv
                : manifest;
      await route.fulfill({ headers, contentType: "application/json", body });
      return;
    }
    if (url.pathname.endsWith("/landing-manifest-id")) {
      await route.fulfill({
        headers,
        json: {
          id: "landing-manifest-id",
          name: `manifest-${landingSnapshotId}.json`,
          size: String(Buffer.byteLength(landingManifest)),
        },
      });
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
    const files = query.includes("name='zohelo-data'")
      ? [{ id: "fixture-root", name: "zohelo-data" }]
      : query.includes("name='06_control'")
        ? [{ id: "fixture-control", name: "06_control" }]
        : query.includes("name='source_campaigns'")
          ? [{ id: "fixture-campaigns", name: "source_campaigns" }]
          : query.includes("name='world_bank_wdi'")
            ? [{ id: "fixture-wdi", name: "world_bank_wdi" }]
            : query.includes("name='current-landing.json'")
              ? [
                  {
                    id: "landing-pointer-id",
                    name: "current-landing.json",
                    size: String(Buffer.byteLength(landingPointer)),
                  },
                ]
              : query.includes("name='current-release.json'")
                ? [
                    {
                      id: "pointer-id",
                      name: "current-release.json",
                      size: String(Buffer.byteLength(pointer)),
                    },
                  ]
                : [];
    await route.fulfill({ headers, json: { files } });
  });

  await page.goto("./");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.getByPlaceholder("Profile name").fill("Platform catalogue regression");
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(profile).toBeHidden();
  await page.getByRole("button", { name: "Tables", exact: true }).click();
  const dataExplorer = page.getByLabel("Data Explorer");
  await expect(dataExplorer).toBeVisible();
  await expect(dataExplorer.getByRole("status").filter({ hasText: "nbp_platform" })).toBeVisible({
    timeout: 60000,
  });
  await expect(dataExplorer.getByRole("link", { name: "setup guide" })).toHaveAttribute(
    "href",
    /docs\/source-accounts\.md$/
  );
  await expect(
    dataExplorer.getByRole("link", { name: "GitHub encrypted secrets" })
  ).toHaveAttribute("href", "https://github.com/rutkala/zohelo-data/settings/secrets/actions");
  await expect(dataExplorer.getByText(/one accepted source response per row/i)).toBeVisible();

  await expect(dataExplorer.getByRole("button", { name: "Business catalogue" })).toHaveCount(0);

  const landingActions = dataExplorer.getByRole("button", {
    name: "Actions for world_bank_wdi_responses",
  });
  await expect(landingActions).toBeVisible();
  await landingActions.click();
  await page.getByRole("menuitem", { name: "Query as SELECT" }).click();
  const editor = page.locator(".monaco-editor .view-lines:visible").first();
  await expect(editor).toContainText('"01_landing"."world_bank_wdi_responses"');
  await editor.click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText(
    'SELECT n.currency_code, l.task_id, l.payload_utf8 FROM "04_gold"."fact_fx_quotes" n CROSS JOIN "01_landing"."world_bank_wdi_responses" l;'
  );
  await page.getByRole("button", { name: "Run Query", exact: true }).click();
  await expect(page.locator(":text-is('TEST'):visible").first()).toBeVisible();
  await expect(page.locator(":text-is('wdi-country-metadata'):visible").first()).toBeVisible();
  await expect(page.getByText(/Poland/).first()).toBeVisible();
  await expect(
    page.getByRole("tab", { name: "world_bank_wdi_responses", exact: true })
  ).toHaveAttribute("aria-selected", "true");
});
