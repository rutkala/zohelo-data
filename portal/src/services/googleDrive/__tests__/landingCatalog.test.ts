import { beforeEach, describe, expect, it, vi } from "vitest";
import { resolveLandingCatalog } from "../landingCatalog";
import { createDriveDownloadBudget, sha256Hex } from "../releaseCatalog";
import {
  fetchDriveFileBuffer,
  findFoldersByName,
  findNamedFilesInFolder,
  findNamedFilesInFolderById,
} from "../driveApi";

vi.mock("../driveApi", async (original) => ({
  ...(await original<typeof import("../driveApi")>()),
  fetchDriveFileBuffer: vi.fn(),
  findFoldersByName: vi.fn(),
  findNamedFilesInFolder: vi.fn(),
  findNamedFilesInFolderById: vi.fn(),
}));

const encoder = new TextEncoder();
const bytes = (value: unknown) => encoder.encode(JSON.stringify(value));
const columns = [
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
].map(([name, type]) => ({ name, type }));
const bulkColumns = [
  ["dataset_id", "VARCHAR"],
  ["source_id", "VARCHAR"],
  ["version", "VARCHAR"],
  ["kind", "VARCHAR"],
  ["retrieved_at_utc", "TIMESTAMP"],
  ["raw_file_id", "VARCHAR"],
  ["raw_file_name", "VARCHAR"],
  ["raw_size_bytes", "BIGINT"],
  ["raw_sha256", "VARCHAR"],
  ["request_json", "VARCHAR"],
  ["inspection_json", "VARCHAR"],
].map(([name, type]) => ({ name, type }));

async function sourceFixture(sourceId = "world_bank_wdi") {
  const snapshotId = "123e4567-e89b-42d3-a456-426614174000";
  const manifest = {
    format_version: 1,
    kind: "landing_snapshot",
    source_id: sourceId,
    snapshot_id: snapshotId,
    created_at_utc: "2026-09-08T12:00:00Z",
    code_sha: "a".repeat(40),
    status: "validated",
    layer: "01_landing",
    table_name: `${sourceId}_responses`,
    row_count: 1,
    coverage_status: "incomplete",
    files: [
      {
        id: `${sourceId}-parquet-id`,
        name: "fragment-223e4567-e89b-42d3-a456-426614174000.parquet",
        size: 123,
        sha256: "b".repeat(64),
      },
    ],
    columns,
    accepted_response_count: 2,
    published_response_count: 1,
    pending_publication_count: 1,
    receipt_checkpoint_sha256: "c".repeat(64),
    tests: { passed: true },
  };
  const manifestBytes = bytes(manifest);
  const pointer = {
    format_version: 1,
    source_id: sourceId,
    snapshot_id: snapshotId,
    manifest_file_id: `${sourceId}-manifest-id`,
    manifest_file_name: `manifest-${snapshotId}.json`,
    manifest_sha256: await sha256Hex(manifestBytes),
    manifest_size_bytes: manifestBytes.byteLength,
  };
  return { manifest, manifestBytes, pointer, pointerBytes: bytes(pointer) };
}

async function bulkFixture(sourceId = "eurostat_bulk") {
  const snapshotId = "323e4567-e89b-42d3-a456-426614174000";
  const manifest = {
    format_version: 2,
    kind: "full_distribution_index",
    source_id: sourceId,
    snapshot_id: snapshotId,
    created_at_utc: "2026-09-08T12:00:00Z",
    code_sha: "d".repeat(40),
    status: "validated",
    layer: "01_landing",
    table_name: "eurostat_distributions",
    row_count: 2,
    coverage_status: "complete_current_catalogue",
    files: [
      {
        id: "eurostat-bulk-parquet-id",
        name: "fragment-423e4567-e89b-42d3-a456-426614174000.parquet",
        size: 456,
        sha256: "e".repeat(64),
      },
    ],
    columns: bulkColumns,
    accepted_distribution_count: 2,
    published_distribution_count: 2,
    pending_publication_count: 0,
    receipt_checkpoint_sha256: "f".repeat(64),
    tests: { passed: true },
  };
  const manifestBytes = bytes(manifest);
  const pointer = {
    format_version: 1,
    source_id: sourceId,
    snapshot_id: snapshotId,
    manifest_file_id: "eurostat-bulk-manifest-id",
    manifest_file_name: `manifest-${snapshotId}.json`,
    manifest_sha256: await sha256Hex(manifestBytes),
    manifest_size_bytes: manifestBytes.byteLength,
  };
  return { manifest, manifestBytes, pointer, pointerBytes: bytes(pointer) };
}

beforeEach(() => vi.resetAllMocks());

describe("source-scoped Landing catalog", () => {
  it("resolves only the bounded sources and validates a pointer-selected manifest", async () => {
    const fixture = await sourceFixture();
    vi.mocked(findFoldersByName).mockImplementation(async (name) => {
      if (name === "zohelo-data") return [{ id: "root-id", name }];
      if (name === "06_control") return [{ id: "control-id", name }];
      if (name === "source_campaigns") return [{ id: "campaigns-id", name }];
      if (name === "world_bank_wdi") return [{ id: "wdi-id", name }];
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockResolvedValue([
      {
        id: "wdi-pointer-id",
        name: "current-landing.json",
        size: fixture.pointerBytes.byteLength,
      },
    ]);
    vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
      id: fixture.pointer.manifest_file_id,
      name: fixture.pointer.manifest_file_name,
      size: fixture.manifestBytes.byteLength,
    });
    vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) =>
      id === "wdi-pointer-id" ? fixture.pointerBytes : fixture.manifestBytes
    );

    const result = await resolveLandingCatalog("token", createDriveDownloadBudget());

    expect(result.issues).toEqual([]);
    expect(result.snapshots).toHaveLength(1);
    expect(result.snapshots[0].manifest).toMatchObject({
      source_id: "world_bank_wdi",
      table_name: "world_bank_wdi_responses",
      row_count: 1,
      coverage_status: "incomplete",
    });
    expect(result.snapshots[0].manifest.files[0]).toMatchObject({
      tableName: "world_bank_wdi_responses",
      layer: "01_landing",
    });
    expect(findFoldersByName).toHaveBeenCalledWith("world_bank_wdi", "campaigns-id", "token");
    expect(findFoldersByName).toHaveBeenCalledWith("gus_bdl", "campaigns-id", "token");
    expect(findFoldersByName).toHaveBeenCalledWith("eurostat", "campaigns-id", "token");
    expect(findFoldersByName).toHaveBeenCalledWith("world_bank_wdi_bulk", "campaigns-id", "token");
    expect(findFoldersByName).toHaveBeenCalledWith("eurostat_bulk", "campaigns-id", "token");
    expect(findFoldersByName).toHaveBeenCalledWith("opendata_org_bulk", "campaigns-id", "token");
  });

  it("discovers a strict metadata-only full-distribution index", async () => {
    const fixture = await bulkFixture();
    vi.mocked(findFoldersByName).mockImplementation(async (name) => {
      if (name === "zohelo-data") return [{ id: "root-id", name }];
      if (name === "06_control") return [{ id: "control-id", name }];
      if (name === "source_campaigns") return [{ id: "campaigns-id", name }];
      if (name === "eurostat_bulk") return [{ id: "bulk-id", name }];
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockResolvedValue([
      {
        id: "bulk-pointer-id",
        name: "current-landing.json",
        size: fixture.pointerBytes.byteLength,
      },
    ]);
    vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
      id: fixture.pointer.manifest_file_id,
      name: fixture.pointer.manifest_file_name,
      size: fixture.manifestBytes.byteLength,
    });
    vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) =>
      id === "bulk-pointer-id" ? fixture.pointerBytes : fixture.manifestBytes
    );

    const result = await resolveLandingCatalog("token", createDriveDownloadBudget());

    expect(result.issues).toEqual([]);
    expect(result.snapshots).toHaveLength(1);
    expect(result.snapshots[0].manifest).toMatchObject({
      format_version: 2,
      kind: "full_distribution_index",
      source_id: "eurostat_bulk",
      table_name: "eurostat_distributions",
      row_count: 2,
      accepted_distribution_count: 2,
      published_distribution_count: 2,
    });
    expect(result.snapshots[0].manifest.files[0]).toMatchObject({
      tableName: "eurostat_distributions",
      layer: "01_landing",
    });
    expect(result.snapshots[0].manifest.columns.map(({ name }) => name)).not.toContain(
      "payload_utf8"
    );
  });

  it("treats an absent old pointer as unpublished without an error", async () => {
    vi.mocked(findFoldersByName).mockImplementation(async (name) => {
      if (name === "zohelo-data") return [{ id: "root-id", name }];
      if (name === "06_control") return [{ id: "control-id", name }];
      if (name === "source_campaigns") return [{ id: "campaigns-id", name }];
      if (name === "gus_bdl") return [{ id: "bdl-id", name }];
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockResolvedValue([]);

    await expect(resolveLandingCatalog("token", createDriveDownloadBudget())).resolves.toEqual({
      snapshots: [],
      issues: [],
      fingerprint: "none",
    });
  });

  it("reports a malformed existing source pointer without hiding a healthy source", async () => {
    const fixture = await sourceFixture();
    const invalidPointer = bytes({ format_version: 1, source_id: "gus_bdl" });
    vi.mocked(findFoldersByName).mockImplementation(async (name) => {
      if (name === "zohelo-data") return [{ id: "root-id", name }];
      if (name === "06_control") return [{ id: "control-id", name }];
      if (name === "source_campaigns") return [{ id: "campaigns-id", name }];
      if (name === "world_bank_wdi") return [{ id: "wdi-id", name }];
      if (name === "gus_bdl") return [{ id: "bdl-id", name }];
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockImplementation(async (_name, folderId) => {
      if (folderId === "wdi-id") {
        return [
          {
            id: "wdi-pointer-id",
            name: "current-landing.json",
            size: fixture.pointerBytes.byteLength,
          },
        ];
      }
      return [
        { id: "bdl-pointer-id", name: "current-landing.json", size: invalidPointer.byteLength },
      ];
    });
    vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
      id: fixture.pointer.manifest_file_id,
      name: fixture.pointer.manifest_file_name,
      size: fixture.manifestBytes.byteLength,
    });
    vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) => {
      if (id === "wdi-pointer-id") return fixture.pointerBytes;
      if (id === "bdl-pointer-id") return invalidPointer;
      return fixture.manifestBytes;
    });

    const result = await resolveLandingCatalog("token", createDriveDownloadBudget());

    expect(result.snapshots.map((snapshot) => snapshot.manifest.source_id)).toEqual([
      "world_bank_wdi",
    ]);
    expect(result.issues).toHaveLength(1);
    expect(result.issues[0]).toMatchObject({ source_id: "gus_bdl" });
    expect(result.issues[0].message).toMatch(/current-landing\.json/);
  });

  it("rejects inconsistent response counts and a tampered manifest as source errors", async () => {
    const fixture = await sourceFixture();
    const inconsistentBytes = bytes({
      ...fixture.manifest,
      pending_publication_count: 0,
    });
    vi.mocked(findFoldersByName).mockImplementation(async (name) => {
      if (name === "zohelo-data") return [{ id: "root-id", name }];
      if (name === "06_control") return [{ id: "control-id", name }];
      if (name === "source_campaigns") return [{ id: "campaigns-id", name }];
      if (name === "world_bank_wdi") return [{ id: "wdi-id", name }];
      return [];
    });
    vi.mocked(findNamedFilesInFolder).mockResolvedValue([
      {
        id: "wdi-pointer-id",
        name: "current-landing.json",
        size: fixture.pointerBytes.byteLength,
      },
    ]);
    vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
      id: fixture.pointer.manifest_file_id,
      name: fixture.pointer.manifest_file_name,
      size: fixture.manifestBytes.byteLength,
    });
    vi.mocked(fetchDriveFileBuffer).mockImplementation(async (id) =>
      id === "wdi-pointer-id" ? fixture.pointerBytes : inconsistentBytes
    );

    const result = await resolveLandingCatalog("token", createDriveDownloadBudget());
    expect(result.snapshots).toEqual([]);
    expect(result.issues[0].message).toMatch(/pointer SHA-256/);
  });
});
