import { expect, test, type Page } from "@playwright/test";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const templatePath = path.join(here, "..", "dist", "docs", "viewer-template.html");
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
const releaseId = "123e4567-e89b-42d3-a456-426614174000";
const codeSha = "a".repeat(40);
const errors = new WeakMap<Page, string[]>();

const feeds = [
  ["nbp_exchange_rates_table_a", "br_nbp_table_a", "NBP Table A"],
  ["nbp_exchange_rates_table_b", "br_nbp_table_b", "NBP Table B"],
  ["nbp_exchange_rates_table_c", "br_nbp_table_c", "NBP Table C"],
  ["nbp_gold_prices", "br_nbp_gold_prices", "NBP Gold Prices"],
] as const;
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

function model(name: string, description: string, dependencies: string[] = []) {
  const unique_id = `model.zohelo_data.${name}`;
  return [
    unique_id,
    {
      database: "catalogue_fixture",
      schema: "main",
      name,
      resource_type: "model",
      package_name: "zohelo_data",
      path: `${name}.sql`,
      original_file_path: `models/${name}.sql`,
      unique_id,
      fqn: ["zohelo_data", name],
      alias: name,
      config: {
        enabled: true,
        materialized: "view",
        meta: { layer: "02_bronze" },
        tags: [],
        docs: { show: true, node_color: null },
      },
      tags: [],
      description,
      columns: {
        currency_code: {
          name: "currency_code",
          description: "Published currency code.",
          data_type: "TEXT",
          meta: {},
          tags: [],
          quote: null,
        },
        mid_rate: {
          name: "mid_rate",
          description: "Published mid exchange rate.",
          data_type: "DECIMAL",
          meta: {},
          tags: [],
          quote: null,
        },
      },
      meta: {},
      group: null,
      docs: { show: true, node_color: null },
      patch_path: "zohelo_data://models/schema.yml",
      relation_name: `"catalogue_fixture"."main"."${name}"`,
      raw_code: `select currency_code, mid_rate from ${name}`,
      compiled: true,
      compiled_code: `select currency_code, mid_rate from "catalogue_fixture"."main"."${name}"`,
      language: "sql",
      refs: [],
      sources: [],
      metrics: [],
      depends_on: { macros: [], nodes: dependencies },
    },
  ] as const;
}

function dbtArtifacts() {
  const entries = feeds.map(([, name]) => model(name, `Native model for ${name}.`));
  const tableA = "model.zohelo_data.br_nbp_table_a";
  const downstream = "model.zohelo_data.sl_nbp_table_a";
  entries.push(model("sl_nbp_table_a", "Native downstream lineage model.", [tableA]));
  const nodes = Object.fromEntries(entries) as Record<string, { name: string }>;
  const nodeIds = Object.keys(nodes);
  const manifest = JSON.stringify({
    metadata: {
      dbt_schema_version: "https://schemas.getdbt.com/dbt/manifest/v12.json",
      dbt_version: "1.12.3",
      generated_at: "2026-09-07T00:00:00.000000Z",
      invocation_id: "native-docs-browser-fixture",
      project_name: "zohelo_data",
      project_id: "native-docs-browser-fixture",
      adapter_type: "duckdb",
    },
    nodes,
    sources: {},
    macros: {},
    docs: {
      "doc.zohelo_data.__overview__": {
        name: "__overview__",
        resource_type: "doc",
        package_name: "zohelo_data",
        path: "docs/overview.md",
        original_file_path: "docs/overview.md",
        unique_id: "doc.zohelo_data.__overview__",
        block_contents:
          "# Native dbt Docs overview\n\nThis content is rendered by dbt Docs, not the portal shell.",
      },
    },
    exposures: {},
    metrics: {},
    groups: {},
    selectors: {},
    disabled: {},
    parent_map: Object.fromEntries(nodeIds.map((id) => [id, id === downstream ? [tableA] : []])),
    child_map: Object.fromEntries(nodeIds.map((id) => [id, id === tableA ? [downstream] : []])),
    group_map: {},
    saved_queries: {},
    semantic_models: {},
    unit_tests: {},
    functions: {},
  });
  const catalogNode = (name: string) => ({
    metadata: {
      name,
      type: "VIEW",
      schema: "main",
      database: "catalogue_fixture",
      comment: `Native dbt catalog metadata for ${name}.`,
      owner: null,
    },
    stats: {},
    columns: {
      currency_code: {
        name: "currency_code",
        index: 1,
        type: "VARCHAR",
        comment: "Published currency code.",
      },
      mid_rate: {
        name: "mid_rate",
        index: 2,
        type: "DECIMAL(18,6)",
        comment: "Published mid exchange rate.",
      },
    },
  });
  const catalog = JSON.stringify({
    metadata: {
      dbt_schema_version: "https://schemas.getdbt.com/dbt/catalog/v1.json",
      dbt_version: "1.12.3",
      generated_at: "2026-09-07T00:00:00.000000Z",
      invocation_id: "native-docs-browser-fixture",
      adapter_type: "duckdb",
      project_name: "zohelo_data",
    },
    nodes: Object.fromEntries(nodeIds.map((id) => [id, catalogNode(nodes[id].name)])),
    sources: {},
    errors: null,
  });
  return { manifest, catalog };
}

function releaseFixture(options: { catalogHash?: string } = {}) {
  const { manifest: dbtManifest, catalog: dbtCatalog } = dbtArtifacts();
  const businessCatalog = JSON.stringify({
    format_version: 1,
    code_sha: codeSha,
    sources: feeds.map(([source_id, , name]) => ({
      source_id,
      name,
      description: `${name} is a published fixture feed.`,
      status: "published_snapshot",
      checked_through: "2026-09-06",
      latest_observation_date: "2026-09-05",
      last_successful_ingestion_at: "2026-09-07T00:00:00Z",
      last_attempt_at: "2026-09-07T00:01:00Z",
      raw_response_count: 2,
    })),
    lineage: {
      nodes: [
        {
          id: "br-a",
          label: "br_nbp_table_a",
          kind: "model",
          layer: "02_bronze",
          description: "Bronze feed.",
        },
        {
          id: "sl-a",
          label: "sl_nbp_table_a",
          kind: "model",
          layer: "03_silver",
          description: "Silver model.",
        },
      ],
      edges: [{ from: "br-a", to: "sl-a" }],
    },
    metrics: [],
  });
  const fakeHash = "a".repeat(64);
  const artifacts = [
    ["dbt-manifest", "manifest.json", dbtManifest, hash(dbtManifest)],
    ["dbt-catalog", "catalog.json", dbtCatalog, options.catalogHash ?? hash(dbtCatalog)],
    ["run-results", "run_results.json", "x", fakeHash],
    ["ingestion-state", "ingestion-state.json", "x", fakeHash],
    ["business-catalog", "business-catalog.json", businessCatalog, hash(businessCatalog)],
  ] as const;
  const noDates = new Set([
    "nbp_change_events",
    "dim_currency",
    "dim_source_table",
    "dim_commodity",
  ]);
  const layer = (id: string) =>
    id.startsWith("bronze_")
      ? "02_bronze"
      : id.startsWith("fact_") || id.startsWith("dim_")
        ? "04_gold"
        : "03_silver";
  const releaseManifest = JSON.stringify({
    format_version: 2,
    release_id: releaseId,
    release_scope: "nbp_platform",
    status: "validated",
    code_sha: codeSha,
    created_at_utc: "2026-09-07T00:00:00Z",
    datasets: datasets.map((dataset_id) => ({
      dataset_id,
      layer: layer(dataset_id),
      table_name: dataset_id,
      row_count: dataset_id === "nbp_change_events" ? 0 : 1,
      min_date: noDates.has(dataset_id) ? null : "2026-09-05",
      max_date: noDates.has(dataset_id) ? null : "2026-09-05",
      columns: [{ name: "currency_code", type: "VARCHAR" }],
      files: [
        { id: `file-${dataset_id}`, name: `${dataset_id}.parquet`, size: 1, sha256: fakeHash },
      ],
    })),
    artifacts: artifacts.map(([id, name, body, sha256]) => ({
      id,
      name,
      size: Buffer.byteLength(body),
      sha256,
    })),
    inputs: [],
    tests: { passed: true },
  });
  const pointer = JSON.stringify({
    format_version: 1,
    release_id: releaseId,
    manifest_file_id: "release-manifest",
    manifest_sha256: hash(releaseManifest),
    updated_at_utc: "2026-09-07T00:01:00Z",
  });
  return {
    pointer,
    releaseManifest,
    files: new Map<string, string>([
      ["pointer", pointer],
      ["release-manifest", releaseManifest],
      ...artifacts.map(([id, , body]) => [id, body]),
    ]),
  };
}

async function installReleaseFixture(page: Page, options: { catalogHash?: string } = {}) {
  const fixture = releaseFixture(options);
  await page.addInitScript(() =>
    sessionStorage.setItem("zohelo_gdrive_access_token", "synthetic-catalogue-token")
  );
  await page.route("https://www.googleapis.com/drive/v3/files**", async (route) => {
    const url = new URL(route.request().url());
    const headers = { "access-control-allow-origin": "*" };
    if (url.searchParams.get("alt") === "media") {
      const body = fixture.files.get(url.pathname.split("/").at(-1)!);
      await route.fulfill(
        body === undefined
          ? { headers, status: 404, body: "Synthetic file not found" }
          : { headers, contentType: "application/json", body }
      );
      return;
    }
    if (url.pathname.endsWith("/release-manifest")) {
      await route.fulfill({
        headers,
        json: {
          id: "release-manifest",
          name: "release.json",
          size: String(Buffer.byteLength(fixture.releaseManifest)),
        },
      });
      return;
    }
    const query = url.searchParams.get("q") ?? "";
    const files = query.includes("name='zohelo-data'")
      ? [
          {
            id: "fixture-root",
            name: "zohelo-data",
            mimeType: "application/vnd.google-apps.folder",
          },
        ]
      : query.includes("name='current-release.json'")
        ? [
            {
              id: "pointer",
              name: "current-release.json",
              size: String(Buffer.byteLength(fixture.pointer)),
            },
          ]
        : [];
    await route.fulfill({ headers, json: { files } });
  });
}

async function ensureProfile(page: Page) {
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await expect(profile).toBeVisible();
  await profile.getByPlaceholder("Profile name").fill("Native dbt Docs regression");
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(profile).toBeHidden();
}

async function openCatalogue(page: Page) {
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("button", { name: "Data catalogue" })
    .click();
}

test.beforeAll(() => {
  expect(
    fs.existsSync(templatePath),
    "CI must stage the installed dbt Docs template before browser tests"
  ).toBe(true);
  const template = fs.readFileSync(templatePath, "utf8");
  expect(template).toContain("<title>dbt Docs</title>");
  expect(template.match(/MANIFEST\.JSON INLINE DATA/g)).toHaveLength(1);
  expect(template.match(/CATALOG\.JSON INLINE DATA/g)).toHaveLength(1);
});

test.beforeEach(async ({ page }) => {
  const messages: string[] = [];
  errors.set(page, messages);
  page.on("pageerror", (error) => messages.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning")
      messages.push(`console ${message.type()}: ${message.text()}`);
  });
});

test.afterEach(async ({ page }, info) => {
  if (info.status !== info.expectedStatus) {
    console.log("Native dbt Docs browser errors:", errors.get(page) ?? []);
    console.log(
      "Native dbt Docs page text:",
      await page
        .locator("body")
        .innerText()
        .catch(() => "<unavailable>")
    );
  }
});

test("renders a verified release in native dbt Docs", async ({ page }) => {
  await installReleaseFixture(page);
  await page.goto("./");
  await ensureProfile(page);
  await openCatalogue(page);

  const docs = page.frameLocator('iframe[title="Data catalogue — dbt Docs"]:visible');
  await expect(docs.locator("body")).toContainText("Native dbt Docs overview");
  await expect(docs.locator("body")).toContainText(`Release ID: ${releaseId}`);
  for (const [, , name] of feeds) await expect(docs.locator("body")).toContainText(name);
  await expect(docs.locator("body")).toContainText("published_snapshot");
  await expect(docs.locator("body")).toContainText("No approved metric definitions are published");
  await expect(docs.locator("body")).not.toContainText("Create Profile");

  await docs.getByRole("link", { name: "NBP Table A" }).click();
  await expect(docs.locator("body")).toContainText("br_nbp_table_a");
  await expect(docs.locator("body")).toContainText("currency_code");
  await expect(docs.locator("body")).toContainText("mid_rate");
  await expect(docs.locator("body")).toContainText("sl_nbp_table_a");
  await page.screenshot({ path: "test-results/catalogue.png", fullPage: true });
});

test("reports a release artifact hash mismatch without embedding docs", async ({ page }) => {
  await installReleaseFixture(page, { catalogHash: "b".repeat(64) });
  await page.goto("./");
  await ensureProfile(page);
  await openCatalogue(page);
  await expect(page.locator('[role="alert"]:visible')).toContainText("Data catalogue unavailable");
  await expect(page.locator('[role="alert"]:visible')).toContainText(
    "catalog.json does not match its release SHA-256."
  );
  await expect(page.locator('iframe[title="Data catalogue — dbt Docs"]')).toHaveCount(0);
});

test("reports a missing generic viewer template", async ({ page }) => {
  await installReleaseFixture(page);
  await page.route("**/docs/viewer-template.html", (route) =>
    route.fulfill({ status: 404, contentType: "text/html", body: "Not found" })
  );
  await page.goto("./");
  await ensureProfile(page);
  await openCatalogue(page);
  await expect(page.locator('[role="alert"]:visible')).toContainText("Data catalogue unavailable");
  await expect(page.locator('[role="alert"]:visible')).toContainText(
    "The catalogue viewer could not be loaded."
  );
  await expect(page.locator('iframe[title="Data catalogue — dbt Docs"]')).toHaveCount(0);
});
