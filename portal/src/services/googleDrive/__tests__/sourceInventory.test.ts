import { beforeEach, describe, expect, it, vi } from "vitest";
import { resolveSourceInventory } from "../sourceInventory";
import { sha256Hex } from "../releaseCatalog";
import {
  fetchDriveFileBuffer,
  findFoldersByName,
  findNamedFilesInFolder,
  findNamedFilesInFolderById,
  listFolderChildrenMetadata,
} from "../driveApi";

vi.mock("../driveApi", async (original) => ({
  ...(await original<typeof import("../driveApi")>()),
  fetchDriveFileBuffer: vi.fn(),
  findFoldersByName: vi.fn(),
  findNamedFilesInFolder: vi.fn(),
  findNamedFilesInFolderById: vi.fn(),
  listFolderChildrenMetadata: vi.fn(),
}));

const encoder = new TextEncoder();
const folders = new Map([
  ["root/zohelo-data", "root-id"],
  ["root-id/01_landing", "landing-id"],
  ["root-id/02_bronze", "bronze-id"],
  ["landing-id/gus_bdl", "bdl-id"],
  ["bdl-id/web_bulk", "bdl-web-id"],
  ["bdl-web-id/_control", "bdl-control-id"],
  ["landing-id/gus_teryt", "teryt-id"],
  ["teryt-id/native", "teryt-native-id"],
  ["teryt-native-id/bulk", "teryt-bulk-id"],
  ["bronze-id/gus_teryt", "teryt-bronze-id"],
]);

beforeEach(async () => {
  vi.resetAllMocks();
  vi.mocked(findFoldersByName).mockImplementation(async (name, parent) => {
    const id = folders.get(`${parent}/${name}`);
    return id ? [{ id, name }] : [];
  });
  vi.mocked(listFolderChildrenMetadata).mockImplementation(async (id, _token, onPage) => {
    onPage?.();
    if (id === "teryt-bulk-id") {
      return [
        {
          id: "native-file",
          name: "TERC.zip",
          size: 12,
          modifiedTime: "2026-09-20T10:00:00Z",
        },
        {
          id: "control-file",
          name: "checkpoint.json",
          size: 999,
          modifiedTime: "2026-09-21T12:00:00Z",
        },
      ];
    }
    if (id === "teryt-bronze-id") {
      return [
        {
          id: "bronze-file",
          name: "br_teryt_terc.parquet",
          size: 8,
          modifiedTime: "2026-09-20T11:00:00Z",
        },
      ];
    }
    return [];
  });
  const checkpoint = {
    format_version: 1,
    source_id: "gus_bdl",
    candidates: { P1: {}, P2: {}, P3: {} },
    selection_plans: {
      P1: { summary: { files: 2, bytes: 75, complete: true } },
      P2: { summary: { files: 1, bytes: 25, complete: false } },
    },
  };
  const checkpointBytes = encoder.encode(JSON.stringify(checkpoint));
  const checkpointSha = await sha256Hex(checkpointBytes);
  vi.mocked(findNamedFilesInFolder).mockResolvedValue([
    {
      id: "checkpoint-id",
      name: "web-queue-v1.json",
      size: checkpointBytes.byteLength,
      version: "7",
      sha256Checksum: checkpointSha,
      md5Checksum: "b".repeat(32),
      modifiedTime: "2026-09-21T10:00:00Z",
    },
  ]);
  vi.mocked(fetchDriveFileBuffer).mockResolvedValue(checkpointBytes);
  vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
    id: "checkpoint-id",
    name: "web-queue-v1.json",
    size: checkpointBytes.byteLength,
    version: "7",
    sha256Checksum: checkpointSha,
    md5Checksum: "b".repeat(32),
    modifiedTime: "2026-09-21T10:00:00Z",
  });
});

describe("Files on Drive inventory", () => {
  it("reports physical stage metadata and counts every BDL selection plan once", async () => {
    const result = await resolveSourceInventory("token");

    expect(result.entries[0]).toMatchObject({
      source_id: "gus_bdl_web",
      state: "in_progress",
      stages: [
        {
          basis: "mutable_checkpoint",
          file_count: 3,
          byte_count: 100,
          selection_complete_count: 1,
          selection_total_count: 3,
        },
      ],
    });
    expect(result.entries.find((entry) => entry.source_id === "gus_teryt")).toMatchObject({
      state: "retained",
      stages: [
        { stage: "Landing", file_count: 1, byte_count: 12 },
        { stage: "Bronze", file_count: 1, byte_count: 8 },
      ],
    });
    expect(result.entries.find((entry) => entry.source_id === "imgw_pib")?.state).toBe("absent");
  });

  it("retries once when a BDL checkpoint changes and accepts the next stable read", async () => {
    vi.mocked(findNamedFilesInFolderById).mockResolvedValueOnce({
      id: "checkpoint-id",
      name: "web-queue-v1.json",
      size: 100,
      version: "8",
      sha256Checksum: "b".repeat(64),
      md5Checksum: "c".repeat(32),
      modifiedTime: "2026-09-21T10:01:00Z",
    });

    const result = await resolveSourceInventory("token");

    expect(result.entries[0]).toMatchObject({
      source_id: "gus_bdl_web",
      state: "in_progress",
      stages: [{ file_count: 3, byte_count: 100 }],
    });
    expect(fetchDriveFileBuffer).toHaveBeenCalledTimes(2);
  });

  it("reports an actively changing BDL checkpoint without accepting totals", async () => {
    vi.mocked(findNamedFilesInFolderById).mockResolvedValue({
      id: "checkpoint-id",
      name: "web-queue-v1.json",
      size: 100,
      version: "8",
      sha256Checksum: "b".repeat(64),
      md5Checksum: "c".repeat(32),
      modifiedTime: "2026-09-21T10:01:00Z",
    });

    const result = await resolveSourceInventory("token");

    expect(result.entries[0]).toMatchObject({
      source_id: "gus_bdl_web",
      state: "updating",
      stages: [],
      message: "The collection checkpoint changed during refresh. Refresh again for stable totals.",
    });
    expect(fetchDriveFileBuffer).toHaveBeenCalledTimes(2);
    expect(result.entries.find((entry) => entry.source_id === "gus_teryt")?.state).toBe("retained");
  });

  it("rejects unsafe or missing file sizes per source", async () => {
    vi.mocked(listFolderChildrenMetadata).mockImplementation(async (id, _token, onPage) => {
      onPage?.();
      return id === "teryt-bulk-id" ? [{ id: "bad", name: "bad.zip" }] : [];
    });

    const result = await resolveSourceInventory("token");

    expect(result.entries.find((entry) => entry.source_id === "gus_teryt")).toMatchObject({
      state: "error",
      error: "Drive file has no safe byte size.",
    });
  });
});
