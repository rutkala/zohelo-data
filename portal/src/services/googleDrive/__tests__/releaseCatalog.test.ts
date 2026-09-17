import { beforeEach, describe, expect, it, vi } from "vitest";
import { sha256Hex, createDriveDownloadBudget, resolveReleaseCatalog } from "../releaseCatalog";
import {
  fetchDriveFileBuffer,
  findFoldersByName,
  findNamedFilesInFolder,
  findNamedFilesInFolderById,
} from "../driveApi";

vi.mock("../driveApi", () => ({
  fetchDriveFileBuffer: vi.fn(),
  findFoldersByName: vi.fn(),
  findNamedFilesInFolder: vi.fn(),
  findNamedFilesInFolderById: vi.fn(),
}));

const encoder = new TextEncoder();
const datasetIds = [
  "nbp_exchange_rates_table_a",
  "nbp_exchange_rates_table_b",
  "nbp_exchange_rates_table_c",
  "nbp_gold_prices",
];
const bytes = (value: unknown) => encoder.encode(JSON.stringify(value));

async function fixture(manifestOverride: Record<string, unknown> = {}, pointerHash?: string) {
  const manifest = {
    format_version: 1,
    release_id: "123e4567-e89b-42d3-a456-426614174000",
    release_scope: "nbp_silver",
    status: "validated",
    code_sha: "a".repeat(40),
    created_at_utc: "2026-09-06T00:00:00Z",
    datasets: datasetIds.map((dataset_id) => ({
      dataset_id,
      layer: "03_silver",
      table_name: dataset_id,
      row_count: 1,
      min_date: "2024-01-01",
      max_date: "2024-01-01",
      columns: [{ name: "effective_date", type: "DATE" }],
      files: [
        {
          id: `${dataset_id}-file`,
          name: `${dataset_id}.parquet`,
          size: 3,
          sha256: "a".repeat(64),
        },
      ],
    })),
    artifacts: [],
    inputs: [],
    tests: { passed: true },
    ...manifestOverride,
  };
  const manifestBytes = bytes(manifest);
  const pointerBytes = bytes({
    format_version: 1,
    release_id: "123e4567-e89b-42d3-a456-426614174000",
    manifest_file_id: "manifest-id",
    manifest_sha256: pointerHash ?? (await sha256Hex(manifestBytes)),
    updated_at_utc: "2026-09-06T00:01:00Z",
  });
  vi.mocked(findFoldersByName).mockResolvedValue([{ id: "root-id", name: "zohelo-data" }]);
  vi.mocked(findNamedFilesInFolder).mockResolvedValue([
    { id: "pointer-id", name: "current-release.json", size: pointerBytes.byteLength },
  ]);
  vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
    id: "manifest-id",
    name: "123e4567-e89b-42d3-a456-426614174000.json",
    size: manifestBytes.byteLength,
  });
  vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) =>
    id === "pointer-id" ? pointerBytes : manifestBytes
  );
}

beforeEach(() => vi.resetAllMocks());

describe("immutable release catalog", () => {
  it("resolves and pins a valid manifest by ID without a folder fallback", async () => {
    await fixture();
    const catalog = await resolveReleaseCatalog("token", createDriveDownloadBudget());
    expect(catalog).toMatchObject({ kind: "release", manifestFileId: "manifest-id" });
    expect(catalog.kind === "release" && catalog.manifest.datasets).toHaveLength(4);
    expect(findNamedFilesInFolderById).toHaveBeenCalledWith("manifest-id", "token");
  });

  it("uses legacy only when the direct root pointer is absent", async () => {
    vi.mocked(findFoldersByName).mockResolvedValue([{ id: "root-id", name: "zohelo-data" }]);
    vi.mocked(findNamedFilesInFolder).mockResolvedValue([]);
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).resolves.toEqual({
      kind: "legacy",
    });
  });

  it("does not fall back when the pointer is malformed", async () => {
    vi.mocked(findFoldersByName).mockResolvedValue([{ id: "root-id", name: "zohelo-data" }]);
    vi.mocked(findNamedFilesInFolder).mockResolvedValue([
      { id: "pointer-id", name: "current-release.json", size: 2 },
    ]);
    vi.mocked(fetchDriveFileBuffer).mockResolvedValue(encoder.encode("{}"));
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /unsupported format_version/
    );
    expect(findNamedFilesInFolderById).not.toHaveBeenCalled();
  });

  it("rejects a pointer whose manifest SHA-256 does not match", async () => {
    await fixture({}, "0".repeat(64));
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /pointer SHA-256/
    );
  });

  it("rejects publisher-invalid code SHA and duplicate release file IDs", async () => {
    await fixture({ code_sha: "a".repeat(39) });
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /invalid code_sha/
    );

    vi.resetAllMocks();
    await fixture({
      datasets: datasetIds.map((dataset_id) => ({
        dataset_id,
        layer: "03_silver",
        table_name: dataset_id,
        row_count: 1,
        min_date: "2024-01-01",
        max_date: "2024-01-01",
        columns: [{ name: "id", type: "INTEGER" }],
        files: [
          { id: "reused-file", name: `${dataset_id}.parquet`, size: 1, sha256: "a".repeat(64) },
        ],
      })),
    });
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /reuses a file ID/
    );
  });

  it("rejects a release missing a required NBP dataset", async () => {
    await fixture({
      datasets: [
        {
          dataset_id: "nbp_exchange_rates_table_a",
          layer: "03_silver",
          table_name: "nbp_exchange_rates_table_a",
          row_count: 1,
          min_date: "2024-01-01",
          max_date: "2024-01-01",
          columns: [{ name: "id", type: "INTEGER" }],
          files: [{ id: "file", name: "data.parquet", size: 1, sha256: "a".repeat(64) }],
        },
      ],
    });
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /exactly the four required/
    );
  });
});

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
] as const;
const platformLayer = (id: string) =>
  id.startsWith("bronze_")
    ? "02_bronze"
    : id.startsWith("dim_") || id.startsWith("fact_")
      ? "04_gold"
      : "03_silver";

async function platformFixture(
  manifestOverride: Record<string, unknown> = {},
  catalogueOverride: Record<string, unknown> = {},
  catalogueHashOverride?: string
) {
  const releaseId = "123e4567-e89b-42d3-a456-426614174000";
  const codeSha = "b".repeat(40);
  const catalogue = {
    format_version: 1,
    code_sha: codeSha,
    sources: [
      {
        source_id: "nbp_table_a",
        name: "NBP Table A",
        description: "Daily exchange rates published by Narodowy Bank Polski.",
        status: "published",
        checked_through: "2026-09-01",
        latest_observation_date: "2026-08-31",
        last_successful_ingestion_at: "2026-09-01T01:00:00Z",
        last_attempt_at: "2026-09-01T01:00:00Z",
        raw_response_count: 3,
      },
    ],
    lineage: {
      nodes: [
        {
          id: "source",
          label: "NBP",
          kind: "source",
          layer: "01_landing",
          description: "Source response",
        },
        {
          id: "silver",
          label: "Rates",
          kind: "model",
          layer: "03_silver",
          description: "Validated rates",
        },
      ],
      edges: [{ from: "source", to: "silver" }],
    },
    metrics: [],
    ...catalogueOverride,
  };
  const catalogueBytes = bytes(catalogue);
  const manifest = {
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
            id: `${dataset_id}-file`,
            name: `${dataset_id}.parquet`,
            size: 1,
            sha256: "a".repeat(64),
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
      size: name === "business-catalog.json" ? catalogueBytes.byteLength : 1,
      sha256: name === "business-catalog.json" ? (catalogueHashOverride ?? "") : "a".repeat(64),
    })),
    inputs: [],
    tests: { passed: true },
    ...manifestOverride,
  };
  const expectedCatalogueHash = catalogueHashOverride ?? (await sha256Hex(catalogueBytes));
  (manifest.artifacts as Array<Record<string, unknown>>).find(
    (artifact) => artifact.name === "business-catalog.json"
  )!.sha256 = expectedCatalogueHash;
  const manifestBytes = bytes(manifest);
  const pointerBytes = bytes({
    format_version: 1,
    release_id: releaseId,
    manifest_file_id: "manifest-id",
    manifest_sha256: await sha256Hex(manifestBytes),
    updated_at_utc: "2026-09-06T00:01:00Z",
  });
  vi.mocked(findFoldersByName).mockResolvedValue([{ id: "root-id", name: "zohelo-data" }]);
  vi.mocked(findNamedFilesInFolder).mockResolvedValue([
    { id: "pointer-id", name: "current-release.json", size: pointerBytes.byteLength },
  ]);
  vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
    id: "manifest-id",
    name: "release.json",
    size: manifestBytes.byteLength,
  });
  vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) => {
    if (id === "pointer-id") return pointerBytes;
    if (id === "business-catalogue-id") return catalogueBytes;
    return manifestBytes;
  });
}

describe("complete platform release catalogue", () => {
  it("resolves all fifteen datasets and a code-bound business catalogue", async () => {
    await platformFixture();
    const catalog = await resolveReleaseCatalog("token", createDriveDownloadBudget());
    expect(catalog).toMatchObject({ kind: "release" });
    expect(catalog.kind === "release" && catalog.manifest).toMatchObject({
      format_version: 2,
      release_scope: "nbp_platform",
    });
    expect(catalog.kind === "release" && catalog.manifest.datasets).toHaveLength(15);
    expect(
      catalog.kind === "release" && catalog.businessCatalogue?.sources[0].checked_through
    ).toBe("2026-09-01");
  });

  it("rejects a tampered catalogue and a catalogue bound to other code", async () => {
    await platformFixture({}, {}, "0".repeat(64));
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /business-catalog.json does not match its release SHA-256/
    );

    vi.resetAllMocks();
    await platformFixture({}, { code_sha: "c".repeat(40) });
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /does not match the selected release code SHA/
    );
  });

  it("allows zero change events and null dimension bounds, while rejecting invalid v2 bounds", async () => {
    await platformFixture();
    await expect(
      resolveReleaseCatalog("token", createDriveDownloadBudget())
    ).resolves.toMatchObject({
      kind: "release",
    });

    vi.resetAllMocks();
    await platformFixture({
      datasets: platformDatasetIds.map((dataset_id) => ({
        dataset_id,
        layer: platformLayer(dataset_id),
        table_name: dataset_id,
        row_count: dataset_id === "dim_currency" ? 0 : dataset_id === "nbp_change_events" ? 0 : 1,
        min_date:
          dataset_id === "dim_currency" || dataset_id === "nbp_change_events" ? null : "2024-01-01",
        max_date:
          dataset_id === "dim_currency" || dataset_id === "nbp_change_events" ? null : "2024-01-01",
        columns: [{ name: "id", type: "INTEGER" }],
        files: [
          {
            id: `${dataset_id}-file`,
            name: `${dataset_id}.parquet`,
            size: 1,
            sha256: "a".repeat(64),
          },
        ],
      })),
    });
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /dim_currency.*zero rows/
    );
  });
});

const bdlDatasetIds = [
  "bronze_bdl_variables",
  "bronze_bdl_subjects",
  "bronze_bdl_units",
  "bronze_bdl_dictionary_entries",
  "bronze_bdl_years",
  "bronze_bdl_observations",
  "bdl_variables",
  "bdl_subjects",
  "bdl_units",
  "bdl_dictionary_entries",
  "bdl_observation_revisions",
  "bdl_observations",
  "dim_bdl_period",
  "dim_bdl_subject",
  "dim_bdl_variable",
  "dim_bdl_unit",
  "fact_bdl_observations",
  "mart_bdl_coverage",
] as const;

const bdlLayer = (id: string) =>
  id.startsWith("bronze_") ? "02_bronze" : id.startsWith("bdl_") ? "03_silver" : "04_gold";

const bdlDated = new Set(["dim_bdl_period", "fact_bdl_observations", "mart_bdl_coverage"]);

async function bdlPlatformFixture(
  manifestOverride: Record<string, unknown> = {},
  catalogueOverride: Record<string, unknown> = {}
) {
  const releaseId = "223e4567-e89b-42d3-a456-426614174000";
  const codeSha = "c".repeat(40);
  const catalogue = {
    format_version: 1,
    code_sha: codeSha,
    sources: [
      {
        source_id: "gus_bdl",
        name: "GUS BDL (Local Data Bank)",
        description: "Local Data Bank published by Statistics Poland.",
        status: "published",
        checked_through: "2026-09-10",
        latest_observation_date: "2026-09-10",
        last_successful_ingestion_at: "2026-09-10T01:00:00Z",
        last_attempt_at: "2026-09-10T01:00:00Z",
        raw_response_count: 5,
      },
    ],
    lineage: {
      nodes: [
        {
          id: "source",
          label: "GUS BDL",
          kind: "source",
          layer: "01_landing",
          description: "Source response",
        },
        {
          id: "silver",
          label: "Variables",
          kind: "model",
          layer: "03_silver",
          description: "Validated variables",
        },
      ],
      edges: [{ from: "source", to: "silver" }],
    },
    metrics: [{ name: "bdl_source_universe_total" }],
    ...catalogueOverride,
  };
  const catalogueBytes = bytes(catalogue);
  const manifest = {
    format_version: 2,
    release_id: releaseId,
    release_scope: "bdl_platform",
    status: "validated",
    code_sha: codeSha,
    created_at_utc: "2026-09-10T00:00:00Z",
    datasets: bdlDatasetIds.map((dataset_id) => {
      const isDated = bdlDated.has(dataset_id);
      return {
        dataset_id,
        layer: bdlLayer(dataset_id),
        table_name: dataset_id.replace(/^bronze_/, ""),
        row_count: 1,
        min_date: isDated ? "2026-09-01" : null,
        max_date: isDated ? "2026-09-10" : null,
        columns: [
          { name: isDated ? "period_start_date" : "id", type: isDated ? "DATE" : "VARCHAR" },
        ],
        files: [
          {
            id: `${dataset_id}-file`,
            name: `${dataset_id}.parquet`,
            size: 1,
            sha256: "d".repeat(64),
          },
        ],
      };
    }),
    artifacts: [
      {
        id: "manifest-id",
        name: "manifest.json",
        size: 1,
        sha256: "e".repeat(64),
      },
      {
        id: "catalog-id",
        name: "catalog.json",
        size: 1,
        sha256: "e".repeat(64),
      },
      {
        id: "run-results-id",
        name: "run_results.json",
        size: 1,
        sha256: "e".repeat(64),
      },
      {
        id: "ingestion-state-id",
        name: "ingestion-state.json",
        size: 1,
        sha256: "e".repeat(64),
      },
      {
        id: "business-catalogue-id",
        name: "business-catalog.json",
        size: catalogueBytes.byteLength,
        sha256: await sha256Hex(catalogueBytes),
      },
    ],
    inputs: [],
    tests: { passed: true },
    ...manifestOverride,
  };
  const manifestBytes = bytes(manifest);
  const pointerBytes = bytes({
    format_version: 1,
    release_id: releaseId,
    manifest_file_id: "bdl-manifest-id",
    manifest_sha256: await sha256Hex(manifestBytes),
    updated_at_utc: "2026-09-10T00:01:00Z",
  });

  vi.mocked(findFoldersByName).mockImplementation(async (name) => {
    if (name === "zohelo-data") return [{ id: "root-id", name: "zohelo-data" }];
    if (name === "bdl-platform") return [{ id: "bdl-folder-id", name: "bdl-platform" }];
    return [];
  });
  vi.mocked(findNamedFilesInFolder).mockImplementation(async (name, parentId) => {
    if (parentId === "bdl-folder-id" && name === "current-release.json") {
      return [
        { id: "bdl-pointer-id", name: "current-release.json", size: pointerBytes.byteLength },
      ];
    }
    return [];
  });
  vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
    id: "bdl-manifest-id",
    name: "release.json",
    size: manifestBytes.byteLength,
  });
  vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) => {
    if (id === "bdl-pointer-id") return pointerBytes;
    if (id === "business-catalogue-id") return catalogueBytes;
    return manifestBytes;
  });
}

describe("BDL platform release resolution", () => {
  it("resolves all eighteen BDL datasets and their business catalogue from bdl-platform folder", async () => {
    await bdlPlatformFixture();
    const catalog = await resolveReleaseCatalog("token", createDriveDownloadBudget());
    expect(catalog).toMatchObject({ kind: "release" });
    expect(catalog.kind === "release" && catalog.manifest).toMatchObject({
      format_version: 2,
      release_scope: "bdl_platform",
    });
    expect(catalog.kind === "release" && catalog.manifest.datasets).toHaveLength(18);
    expect(catalog.kind === "release" && catalog.businessCatalogue?.sources[0].source_id).toBe(
      "gus_bdl"
    );
    expect(catalog.kind === "release" && catalog.releases).toHaveLength(1);
  });

  it("rejects BDL dataset that omits date bounds on a dated dataset", async () => {
    await bdlPlatformFixture({
      datasets: bdlDatasetIds.map((dataset_id) => ({
        dataset_id,
        layer: bdlLayer(dataset_id),
        table_name: dataset_id.replace(/^bronze_/, ""),
        row_count: 1,
        min_date: null,
        max_date: null,
        columns: [{ name: "id", type: "VARCHAR" }],
        files: [{ id: "file-id", name: "f.parquet", size: 1, sha256: "a".repeat(64) }],
      })),
    });
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /dim_bdl_period.*may not omit date bounds/
    );
  });

  it("rejects BDL dataset that declares date bounds on a non-dated dataset", async () => {
    await bdlPlatformFixture({
      datasets: bdlDatasetIds.map((dataset_id) => ({
        dataset_id,
        layer: bdlLayer(dataset_id),
        table_name: dataset_id.replace(/^bronze_/, ""),
        row_count: 1,
        min_date: "2026-09-01",
        max_date: "2026-09-10",
        columns: [{ name: "id", type: "VARCHAR" }],
        files: [{ id: "file-id", name: "f.parquet", size: 1, sha256: "a".repeat(64) }],
      })),
    });
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /bronze_bdl_variables.*must not declare date bounds/
    );
  });

  it("resolves both NBP and BDL releases when both pointers exist", async () => {
    const nbpReleaseId = "123e4567-e89b-42d3-a456-426614174000";
    const bdlReleaseId = "223e4567-e89b-42d3-a456-426614174000";

    const nbpCatalogueBytes = bytes({
      format_version: 1,
      code_sha: "b".repeat(40),
      sources: [
        {
          source_id: "nbp_table_a",
          name: "NBP Table A",
          description: "Daily NBP exchange rates.",
          status: "published",
          checked_through: null,
          latest_observation_date: null,
          last_successful_ingestion_at: null,
          last_attempt_at: null,
          raw_response_count: 0,
        },
      ],
      lineage: { nodes: [], edges: [] },
      metrics: [],
    });
    const nbpManifestBytes = bytes({
      format_version: 2,
      release_id: nbpReleaseId,
      release_scope: "nbp_platform",
      status: "validated",
      code_sha: "b".repeat(40),
      created_at_utc: "2026-09-06T00:00:00Z",
      datasets: platformDatasetIds.map((dataset_id) => ({
        dataset_id,
        layer: platformLayer(dataset_id),
        table_name: dataset_id,
        row_count: dataset_id === "nbp_change_events" ? 0 : 1,
        min_date:
          dataset_id === "dim_currency" ||
          dataset_id === "dim_source_table" ||
          dataset_id === "dim_commodity" ||
          dataset_id === "nbp_change_events"
            ? null
            : "2024-01-01",
        max_date:
          dataset_id === "dim_currency" ||
          dataset_id === "dim_source_table" ||
          dataset_id === "dim_commodity" ||
          dataset_id === "nbp_change_events"
            ? null
            : "2024-01-01",
        columns: [{ name: "id", type: "INTEGER" }],
        files: [
          {
            id: `nbp-${dataset_id}-file`,
            name: `${dataset_id}.parquet`,
            size: 1,
            sha256: "a".repeat(64),
          },
        ],
      })),
      artifacts: [
        { id: "nbp-manifest-art", name: "manifest.json", size: 1, sha256: "a".repeat(64) },
        { id: "nbp-catalog-art", name: "catalog.json", size: 1, sha256: "a".repeat(64) },
        { id: "nbp-results-art", name: "run_results.json", size: 1, sha256: "a".repeat(64) },
        { id: "nbp-state-art", name: "ingestion-state.json", size: 1, sha256: "a".repeat(64) },
        {
          id: "nbp-business-art",
          name: "business-catalog.json",
          size: nbpCatalogueBytes.byteLength,
          sha256: await sha256Hex(nbpCatalogueBytes),
        },
      ],
      inputs: [],
      tests: { passed: true },
    });
    const nbpPointerBytes = bytes({
      format_version: 1,
      release_id: nbpReleaseId,
      manifest_file_id: "nbp-manifest-file-id",
      manifest_sha256: await sha256Hex(nbpManifestBytes),
      updated_at_utc: "2026-09-06T00:01:00Z",
    });

    const bdlCatalogueBytes = bytes({
      format_version: 1,
      code_sha: "c".repeat(40),
      sources: [
        {
          source_id: "gus_bdl",
          name: "GUS BDL",
          description: "Local Data Bank.",
          status: "published",
          checked_through: null,
          latest_observation_date: null,
          last_successful_ingestion_at: null,
          last_attempt_at: null,
          raw_response_count: 0,
        },
      ],
      lineage: { nodes: [], edges: [] },
      metrics: [],
    });
    const bdlManifestBytes = bytes({
      format_version: 2,
      release_id: bdlReleaseId,
      release_scope: "bdl_platform",
      status: "validated",
      code_sha: "c".repeat(40),
      created_at_utc: "2026-09-10T00:00:00Z",
      datasets: bdlDatasetIds.map((dataset_id) => {
        const isDated = bdlDated.has(dataset_id);
        return {
          dataset_id,
          layer: bdlLayer(dataset_id),
          table_name: dataset_id.replace(/^bronze_/, ""),
          row_count: 1,
          min_date: isDated ? "2026-09-01" : null,
          max_date: isDated ? "2026-09-10" : null,
          columns: [{ name: "id", type: "VARCHAR" }],
          files: [
            {
              id: `bdl-${dataset_id}-file`,
              name: `${dataset_id}.parquet`,
              size: 1,
              sha256: "b".repeat(64),
            },
          ],
        };
      }),
      artifacts: [
        { id: "bdl-manifest-art", name: "manifest.json", size: 1, sha256: "b".repeat(64) },
        { id: "bdl-catalog-art", name: "catalog.json", size: 1, sha256: "b".repeat(64) },
        { id: "bdl-results-art", name: "run_results.json", size: 1, sha256: "b".repeat(64) },
        { id: "bdl-state-art", name: "ingestion-state.json", size: 1, sha256: "b".repeat(64) },
        {
          id: "bdl-business-art",
          name: "business-catalog.json",
          size: bdlCatalogueBytes.byteLength,
          sha256: await sha256Hex(bdlCatalogueBytes),
        },
      ],
      inputs: [],
      tests: { passed: true },
    });
    const bdlPointerBytes = bytes({
      format_version: 1,
      release_id: bdlReleaseId,
      manifest_file_id: "bdl-manifest-file-id",
      manifest_sha256: await sha256Hex(bdlManifestBytes),
      updated_at_utc: "2026-09-10T00:01:00Z",
    });

    vi.mocked(findFoldersByName).mockImplementation(async (name) => {
      if (name === "zohelo-data") return [{ id: "root-id", name: "zohelo-data" }];
      if (name === "bdl-platform") return [{ id: "bdl-folder-id", name: "bdl-platform" }];
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockImplementation(async (name, parentId) => {
      if (parentId === "root-id" && name === "current-release.json") {
        return [
          { id: "nbp-pointer-id", name: "current-release.json", size: nbpPointerBytes.byteLength },
        ];
      }
      if (parentId === "bdl-folder-id" && name === "current-release.json") {
        return [
          { id: "bdl-pointer-id", name: "current-release.json", size: bdlPointerBytes.byteLength },
        ];
      }
      return [];
    });
    vi.mocked(findNamedFilesInFolderById).mockImplementation(async (id) => {
      if (id === "nbp-manifest-file-id") {
        return {
          id: "nbp-manifest-file-id",
          name: "release.json",
          size: nbpManifestBytes.byteLength,
        };
      }
      if (id === "bdl-manifest-file-id") {
        return {
          id: "bdl-manifest-file-id",
          name: "release.json",
          size: bdlManifestBytes.byteLength,
        };
      }
      return null;
    });
    vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) => {
      if (id === "nbp-pointer-id") return nbpPointerBytes;
      if (id === "nbp-manifest-file-id") return nbpManifestBytes;
      if (id === "nbp-business-art") return nbpCatalogueBytes;
      if (id === "bdl-pointer-id") return bdlPointerBytes;
      if (id === "bdl-manifest-file-id") return bdlManifestBytes;
      if (id === "bdl-business-art") return bdlCatalogueBytes;
      throw new Error(`Unexpected fetch for ${id}`);
    });

    const catalog = await resolveReleaseCatalog("token", createDriveDownloadBudget());
    expect(catalog).toMatchObject({ kind: "release" });
    if (catalog.kind !== "release") throw new Error();
    expect(catalog.releases).toHaveLength(2);
    expect(catalog.releases![0].manifest.release_scope).toBe("nbp_platform");
    expect(catalog.releases![1].manifest.release_scope).toBe("bdl_platform");
    expect(catalog.fingerprint).toContain(";");
  });
});

const wdiDatasetIds = [
  "bronze_wdi_country",
  "bronze_wdi_country_series",
  "bronze_wdi_data",
  "bronze_wdi_footnote",
  "bronze_wdi_series",
  "bronze_wdi_series_time",
  "wdi_countries",
  "wdi_indicators",
  "wdi_observations",
  "dim_wdi_geography",
  "dim_wdi_indicator",
  "dim_wdi_year",
  "fact_wdi_observations",
  "mart_wdi_coverage",
];
const wdiDated = new Set([
  "wdi_observations",
  "dim_wdi_year",
  "fact_wdi_observations",
  "mart_wdi_coverage",
]);
const wdiLayer = (datasetId: string) =>
  datasetId.startsWith("bronze_")
    ? "02_bronze"
    : datasetId.startsWith("wdi_")
      ? "03_silver"
      : "04_gold";

async function wdiPlatformFixture() {
  const releaseId = "323e4567-e89b-42d3-a456-426614174000";
  const codeSha = "d".repeat(40);
  const catalogueBytes = bytes({
    format_version: 1,
    code_sha: codeSha,
    sources: [
      {
        source_id: "world_bank_wdi",
        name: "World Development Indicators",
        description: "Official World Bank WDI archive.",
        status: "published_snapshot",
        checked_through: "2026-09-13",
        latest_observation_date: "2025-01-01",
        last_successful_ingestion_at: "2026-09-13T12:00:00Z",
        last_attempt_at: "2026-09-13T12:00:00Z",
        raw_response_count: 1,
      },
    ],
    lineage: { nodes: [], edges: [] },
    metrics: [{ name: "wdi_modeled_value_coverage_ratio" }],
  });
  const manifest = {
    format_version: 2,
    release_id: releaseId,
    release_scope: "wdi_platform",
    status: "validated",
    code_sha: codeSha,
    created_at_utc: "2026-09-13T12:00:00Z",
    datasets: wdiDatasetIds.map((dataset_id) => {
      const dated = wdiDated.has(dataset_id);
      const files = [
        {
          id: `${dataset_id}-file-1`,
          name: `${dataset_id}-part-1.parquet`,
          size: 1,
          sha256: "a".repeat(64),
        },
      ];
      if (dataset_id === "fact_wdi_observations") {
        files.push({
          id: `${dataset_id}-file-2`,
          name: `${dataset_id}-part-2.parquet`,
          size: 1,
          sha256: "b".repeat(64),
        });
      }
      return {
        dataset_id,
        layer: wdiLayer(dataset_id),
        table_name: dataset_id.replace(/^bronze_/, ""),
        row_count: 1,
        min_date: dated ? "1960-01-01" : null,
        max_date: dated ? "2025-01-01" : null,
        columns: [{ name: dated ? "observation_date" : "id", type: dated ? "DATE" : "VARCHAR" }],
        files,
      };
    }),
    artifacts: [
      { id: "wdi-manifest-art", name: "manifest.json", size: 1, sha256: "c".repeat(64) },
      { id: "wdi-catalog-art", name: "catalog.json", size: 1, sha256: "c".repeat(64) },
      { id: "wdi-results-art", name: "run_results.json", size: 1, sha256: "c".repeat(64) },
      { id: "wdi-state-art", name: "ingestion-state.json", size: 1, sha256: "c".repeat(64) },
      {
        id: "wdi-business-art",
        name: "business-catalog.json",
        size: catalogueBytes.byteLength,
        sha256: await sha256Hex(catalogueBytes),
      },
    ],
    inputs: [],
    tests: { passed: true },
  };
  const manifestBytes = bytes(manifest);
  const pointerBytes = bytes({
    format_version: 1,
    release_id: releaseId,
    manifest_file_id: "wdi-manifest-file-id",
    manifest_sha256: await sha256Hex(manifestBytes),
    updated_at_utc: "2026-09-13T12:01:00Z",
  });
  vi.mocked(findFoldersByName).mockImplementation(async (name) => {
    if (name === "zohelo-data") return [{ id: "root-id", name: "zohelo-data" }];
    if (name === "wdi-platform") return [{ id: "wdi-folder-id", name: "wdi-platform" }];
    return [];
  });
  vi.mocked(findNamedFilesInFolder).mockImplementation(async (name, parentId) => {
    if (name === "current-release.json" && parentId === "wdi-folder-id") {
      return [{ id: "wdi-pointer-id", name, size: pointerBytes.byteLength }];
    }
    return [];
  });
  vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
    id: "wdi-manifest-file-id",
    name: "release.json",
    size: manifestBytes.byteLength,
  });
  vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) => {
    if (id === "wdi-pointer-id" || id === "wdi-canonical-ptr" || id === "wdi-legacy-ptr")
      return pointerBytes;
    if (id === "wdi-business-art") return catalogueBytes;
    return manifestBytes;
  });
  return { pointerBytes, manifestBytes };
}

describe("WDI platform release resolution", () => {
  it("discovers the independent complete-archive release and all bounded table parts", async () => {
    await wdiPlatformFixture();
    const catalog = await resolveReleaseCatalog("token", createDriveDownloadBudget());
    expect(catalog.kind).toBe("release");
    if (catalog.kind !== "release") throw new Error();
    expect(catalog.manifest.release_scope).toBe("wdi_platform");
    expect(catalog.manifest.datasets).toHaveLength(14);
    expect(catalog.businessCatalogue?.sources[0].source_id).toBe("world_bank_wdi");
    expect(
      catalog.manifest.datasets.find((item) => item.dataset_id === "fact_wdi_observations")?.files
    ).toHaveLength(2);
  });
});

describe("Canonical consolidated layout release resolution", () => {
  it("resolves canonical releases/wdi while genuinely absent source folders stay optional", async () => {
    const { pointerBytes } = await wdiPlatformFixture();
    vi.mocked(findFoldersByName).mockImplementation(async (name, parentId) => {
      if (name === "zohelo-data" && parentId === "root")
        return [{ id: "root-id", name: "zohelo-data" }];
      if (name === "releases" && parentId === "root-id")
        return [{ id: "releases-root-id", name: "releases" }];
      if (name === "wdi" && parentId === "releases-root-id")
        return [{ id: "wdi-rel-id", name: "wdi" }];
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockImplementation(async (name, parentId) => {
      if (parentId === "wdi-rel-id" && name === "current-release.json") {
        return [{ id: "wdi-pointer-id", name, size: pointerBytes.byteLength }];
      }
      return [];
    });
    const catalog = await resolveReleaseCatalog("token", createDriveDownloadBudget());
    expect(catalog.kind).toBe("release");
    if (catalog.kind !== "release") throw new Error();
    expect(catalog.manifest.release_scope).toBe("wdi_platform");
  });

  it("fails closed when conflicting pointers exist in canonical and legacy locations", async () => {
    const { pointerBytes } = await wdiPlatformFixture();
    vi.mocked(findFoldersByName).mockImplementation(async (name, parentId) => {
      if (name === "zohelo-data" && parentId === "root")
        return [{ id: "root-id", name: "zohelo-data" }];
      if (name === "releases" && parentId === "root-id")
        return [{ id: "releases-root-id", name: "releases" }];
      if (name === "wdi" && parentId === "releases-root-id")
        return [{ id: "wdi-rel-id", name: "wdi" }];
      if (name === "wdi-platform" && parentId === "root-id")
        return [{ id: "wdi-folder-id", name: "wdi-platform" }];
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockImplementation(async (name, parentId) => {
      if (parentId === "wdi-rel-id" && name === "current-release.json") {
        return [{ id: "wdi-canonical-ptr", name, size: pointerBytes.byteLength }];
      }
      if (parentId === "wdi-folder-id" && name === "current-release.json") {
        return [{ id: "wdi-legacy-ptr", name, size: pointerBytes.byteLength }];
      }
      return [];
    });
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /Conflicting current-release pointers found for WDI/
    );
  });
});

const eurostatDatasetIds = [
  "bronze_eurostat_observations",
  "eurostat_observation_revisions",
  "eurostat_observations",
  "dim_eurostat_dataset",
  "dim_eurostat_geography",
  "dim_eurostat_period",
  "fact_eurostat_observations",
  "mart_eurostat_coverage",
] as const;
const eurostatDated = new Set([
  "dim_eurostat_period",
  "fact_eurostat_observations",
  "mart_eurostat_coverage",
]);
const eurostatLayer = (datasetId: string) =>
  datasetId.startsWith("bronze_")
    ? "02_bronze"
    : datasetId.startsWith("eurostat_")
      ? "03_silver"
      : "04_gold";

describe("Eurostat progressive platform release resolution", () => {
  it("discovers canonical releases/eurostat and exposes all eight modeled tables", async () => {
    const releaseId = "423e4567-e89b-42d3-a456-426614174000";
    const codeSha = "e".repeat(40);
    const catalogueBytes = bytes({
      format_version: 1,
      code_sha: codeSha,
      sources: [
        {
          source_id: "eurostat",
          name: "Eurostat",
          description: "Progressive source-shaped Eurostat API release.",
          status: "published_snapshot",
          checked_through: "2026-09-14",
          latest_observation_date: "2026-08-31",
          last_successful_ingestion_at: "2026-09-14T18:51:00Z",
          last_attempt_at: "2026-09-14T18:51:00Z",
          raw_response_count: 1108,
        },
      ],
      lineage: { nodes: [], edges: [] },
      metrics: [{ name: "eurostat_modeled_cell_total" }],
    });
    const manifest = {
      format_version: 2,
      release_id: releaseId,
      release_scope: "eurostat_progressive_api_platform",
      status: "validated",
      code_sha: codeSha,
      created_at_utc: "2026-09-14T19:00:00Z",
      datasets: eurostatDatasetIds.map((dataset_id) => ({
        dataset_id,
        layer: eurostatLayer(dataset_id),
        table_name: dataset_id.replace(/^bronze_/, ""),
        row_count: 1,
        min_date: eurostatDated.has(dataset_id) ? "2023-01-01" : null,
        max_date: eurostatDated.has(dataset_id) ? "2026-08-31" : null,
        columns: [
          {
            name: eurostatDated.has(dataset_id) ? "period_start_date" : "id",
            type: eurostatDated.has(dataset_id) ? "DATE" : "VARCHAR",
          },
        ],
        files: [
          {
            id: `${dataset_id}-file`,
            name: `${dataset_id}.parquet`,
            size: 1,
            sha256: "a".repeat(64),
          },
        ],
      })),
      artifacts: [
        { id: "eurostat-manifest-art", name: "manifest.json", size: 1, sha256: "c".repeat(64) },
        { id: "eurostat-catalog-art", name: "catalog.json", size: 1, sha256: "c".repeat(64) },
        { id: "eurostat-results-art", name: "run_results.json", size: 1, sha256: "c".repeat(64) },
        { id: "eurostat-state-art", name: "ingestion-state.json", size: 1, sha256: "c".repeat(64) },
        {
          id: "eurostat-business-art",
          name: "business-catalog.json",
          size: catalogueBytes.byteLength,
          sha256: await sha256Hex(catalogueBytes),
        },
      ],
      inputs: [],
      tests: { passed: true },
    };
    const manifestBytes = bytes(manifest);
    const pointerBytes = bytes({
      format_version: 1,
      release_id: releaseId,
      manifest_file_id: "eurostat-manifest-file",
      manifest_sha256: await sha256Hex(manifestBytes),
      updated_at_utc: "2026-09-14T19:01:00Z",
    });
    vi.mocked(findFoldersByName).mockImplementation(async (name, parentId) => {
      if (name === "zohelo-data" && parentId === "root") return [{ id: "root-id", name }];
      if (name === "releases" && parentId === "root-id") return [{ id: "releases-id", name }];
      if (name === "eurostat" && parentId === "releases-id") return [{ id: "eurostat-id", name }];
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockImplementation(async (name, parentId) =>
      name === "current-release.json" && parentId === "eurostat-id"
        ? [{ id: "eurostat-pointer", name, size: pointerBytes.byteLength }]
        : []
    );
    vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
      id: "eurostat-manifest-file",
      name: "release.json",
      size: manifestBytes.byteLength,
    });
    vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) => {
      if (id === "eurostat-pointer") return pointerBytes;
      if (id === "eurostat-business-art") return catalogueBytes;
      return manifestBytes;
    });

    const catalog = await resolveReleaseCatalog("token", createDriveDownloadBudget());
    expect(catalog.kind).toBe("release");
    if (catalog.kind !== "release") throw new Error();
    expect(catalog.manifest.release_scope).toBe("eurostat_progressive_api_platform");
    expect(catalog.manifest.datasets).toHaveLength(8);
    expect(catalog.businessCatalogue?.sources[0].source_id).toBe("eurostat");
  });
});

describe("Release discovery mutation safety", () => {
  it.each([
    { canonical: true, label: "releases/nbp/current-release.json" },
    { canonical: false, label: "current-release.json" },
  ])("rejects a folder named current-release.json at $label", async ({ canonical }) => {
    vi.mocked(findFoldersByName).mockImplementation(async (name, parentId) => {
      if (name === "zohelo-data" && parentId === "root") return [{ id: "root-id", name }];
      if (canonical && name === "releases" && parentId === "root-id") {
        return [{ id: "releases-root-id", name }];
      }
      if (canonical && name === "nbp" && parentId === "releases-root-id") {
        return [{ id: "nbp-rel-id", name }];
      }
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockImplementation(async (name, parentId) => {
      if (name === "current-release.json" && parentId === (canonical ? "nbp-rel-id" : "root-id")) {
        return [
          {
            id: "pointer-folder",
            name,
            mimeType: "application/vnd.google-apps.folder",
          },
        ];
      }
      return [];
    });
    await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
      /ambiguous or not a file/
    );
  });

  it("retries a source once when its pointer moves into the canonical folder", async () => {
    const { pointerBytes } = await wdiPlatformFixture();
    let canonicalReads = 0;
    vi.mocked(findFoldersByName).mockImplementation(async (name, parentId) => {
      if (name === "zohelo-data" && parentId === "root") return [{ id: "root-id", name }];
      if (name === "releases" && parentId === "root-id") {
        return [{ id: "releases-root-id", name }];
      }
      if (name === "wdi" && parentId === "releases-root-id") {
        return [{ id: "wdi-rel-id", name }];
      }
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockImplementation(async (name, parentId) => {
      if (name === "current-release.json" && parentId === "wdi-rel-id") {
        canonicalReads += 1;
        return canonicalReads === 1
          ? []
          : [{ id: "wdi-pointer-id", name, size: pointerBytes.byteLength }];
      }
      return [];
    });
    const catalog = await resolveReleaseCatalog("token", createDriveDownloadBudget());
    expect(catalog.kind).toBe("release");
    expect(canonicalReads).toBe(2);
  });

  it.each(["legacy", "canonical"] as const)(
    "fails clearly when an established %s source remains pointerless",
    async (layout) => {
      vi.mocked(findFoldersByName).mockImplementation(async (name, parentId) => {
        if (name === "zohelo-data" && parentId === "root") {
          return [{ id: "root-id", name }];
        }
        if (layout === "canonical" && name === "releases" && parentId === "root-id") {
          return [{ id: "releases-root-id", name }];
        }
        if (layout === "canonical" && name === "wdi" && parentId === "releases-root-id") {
          return [{ id: "canonical-wdi", name }];
        }
        if (layout === "legacy" && name === "wdi-platform" && parentId === "root-id") {
          return [{ id: "legacy-wdi", name }];
        }
        return [];
      });
      vi.mocked(findNamedFilesInFolder).mockResolvedValue([]);
      await expect(resolveReleaseCatalog("token", createDriveDownloadBudget())).rejects.toThrow(
        /Established source 'wdi'.*moving or incomplete/
      );
    }
  );
});
