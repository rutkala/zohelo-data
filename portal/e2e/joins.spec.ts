import { expect, test } from "@playwright/test";
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

test("join example loads its explicit gold tables and leaves SQL ready to run", async ({
  page,
}) => {
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
        table_name: dataset_id,
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

  await page.goto("./");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.getByPlaceholder("Profile name").fill("Join example regression");
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(profile).toBeHidden();

  await expect(page.getByRole("status").filter({ hasText: "nbp_platform" }).first()).toBeVisible({
    timeout: 60_000,
  });
  const navigation = page.getByRole("navigation", { name: "Main navigation" });
  await navigation.getByRole("button", { name: "Business catalogue", exact: true }).click();
  await page.getByRole("button", { name: "Open join example", exact: true }).click();

  await expect(
    page.getByRole("status").filter({ hasText: "Loaded 3 dataset(s)" }).first()
  ).toBeVisible();
  const editor = page.locator(".monaco-editor .view-lines:visible").first();
  await expect(editor).toContainText('JOIN "04_gold"."dim_currency"');
  await expect(page.getByText("US dollar", { exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "Run Query", exact: true }).click();
  await expect(page.getByRole("cell", { name: "US dollar", exact: true })).toBeVisible();
});
