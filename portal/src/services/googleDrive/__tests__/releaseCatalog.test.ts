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
