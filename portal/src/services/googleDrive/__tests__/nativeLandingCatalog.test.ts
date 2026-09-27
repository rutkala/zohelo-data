import { beforeEach, describe, expect, it, vi } from "vitest";
import { createHash } from "node:crypto";
import { createExecution, type LocalDuckSession } from "@/services/engine";
import {
  resolveNativeLandingRoot,
  listNativeFolder,
  freshNativeFile,
  nativeDriveLink,
  nativePreviewFormat,
  previewNativeFile,
  disposeAllNativePreviews,
  safeDriveLink,
  nativeMetadataNames,
  scanNativeSourceMetadata,
  publishNativeMetadata,
  type NativeLandingFile,
} from "../nativeLandingCatalog";
import {
  findFoldersByName,
  getNativeFileMetadata,
  listNativeChildrenPage,
  listNativeMetadataBatchPage,
  buildNativeMetadataBatchUrl,
  fetchDriveFileBuffer,
} from "../driveApi";
import { DriveDownloadBudget } from "../releaseCatalog";

vi.mock("../driveApi", async (original) => ({
  ...(await original<typeof import("../driveApi")>()),
  findFoldersByName: vi.fn(),
  getNativeFileMetadata: vi.fn(),
  listNativeChildrenPage: vi.fn(),
  listNativeMetadataBatchPage: vi.fn(),
  fetchDriveFileBuffer: vi.fn(),
}));
const root = {
  id: "landing",
  name: "01_landing",
  parentId: "project",
  parents: ["project"],
  mimeType: "application/vnd.google-apps.folder" as const,
  version: "1",
  modifiedTime: "2026-09-26T00:00:00Z",
};
const child = { ...root, id: "deep", name: "BDL", parentId: root.id, parents: [root.id] };
const bytes = new TextEncoder().encode("id,value\n1,2\n");
const digest = createHash("sha256").update(bytes).digest("hex");
const file: NativeLandingFile = {
  id: "file",
  name: "source.csv",
  parentId: child.id,
  parents: [child.id],
  size: bytes.length,
  version: "2",
  modifiedTime: "2026-09-26T01:00:00Z",
  mimeType: "text/csv",
  sha256Checksum: digest,
  capabilities: { canDownload: true },
  webViewLink: "https://drive.google.com/file/d/file/view",
  webContentLink: "https://drive.google.com/uc?id=file&export=download",
};
const folders = { [root.id]: root, [child.id]: child };
const makeSession = (db: object) => {
  let open = true;
  const listeners = new Set<() => void>();
  const statements: string[] = [];
  const shared = {
    query: vi.fn(() => {
      throw new Error("Preview DDL used the catalogue connection");
    }),
  };
  const session = {
    get isOpen() {
      return open;
    },
    local: { db, connection: shared },
    execute: vi.fn(({ sql }: { sql: string }) =>
      createExecution({
        sql,
        produce: async function* () {
          statements.push(sql);
          yield { kind: "chunk", rows: 0, chunk: { encoding: "rows", rows: [] } };
        },
      })
    ),
    onClose: (listener: () => void) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    close: async () => {
      open = false;
      for (const listener of listeners) listener();
    },
  } as unknown as LocalDuckSession;
  return { session, statements, shared };
};
beforeEach(() => {
  vi.resetAllMocks();
  vi.mocked(findFoldersByName).mockImplementation(async (name, parent) =>
    name === "zohelo-data" && parent === "root"
      ? [{ id: "project", name }]
      : name === "01_landing" && parent === "project"
        ? [{ id: root.id, name }]
        : []
  );
  vi.mocked(getNativeFileMetadata).mockImplementation(async (id) => {
    const value = { landing: root, deep: child, file }[id as keyof typeof folders | "file"];
    if (!value) throw new Error("404");
    return value;
  });
  vi.mocked(fetchDriveFileBuffer).mockResolvedValue(bytes);
});

describe("physical Landing discovery", () => {
  it("names all sources deterministically, including duplicate and punctuation-heavy names", () => {
    const sources = [
      { ...child, id: "one", name: `BDL "A"` },
      { ...child, id: "two", name: `BDL "A"` },
      { ...child, id: "three", name: `BDL "A"_files__one` },
    ];
    const names = nativeMetadataNames(sources);
    expect(new Set([...names.values()].map((name) => name.toLowerCase())).size).toBe(3);
    expect(nativeMetadataNames(sources.reverse())).toEqual(names);
    expect(names.get("one")).toContain("__one");
  });

  it("scans every nested metadata page with no content downloads, excluding shortcuts", async () => {
    const nested = {
      ...child,
      id: "nested",
      name: "division",
      parentId: child.id,
      parents: [child.id],
    };
    vi.mocked(getNativeFileMetadata).mockImplementation(
      async (id) =>
        ({ landing: root, deep: child, nested })[id as "landing" | "deep" | "nested"] ?? file
    );
    vi.mocked(listNativeMetadataBatchPage).mockImplementation(async (ids, kind, _token, page) => {
      if (kind === "folders") return { files: ids.includes(child.id) ? [nested] : [], nextPageToken: null };
      if (ids.includes(child.id) && !page) return { files: [], nextPageToken: "second" };
      if (ids.includes(child.id))
        return {
          files: [
            {
              ...file,
              id: "large",
              name: "a.zip",
              size: 3_000_000_000,
              createdTime: "2026-09-25T00:00:00Z",
              parents: [child.id],
            },
            { ...file, parents: [nested.id], id: "deep-file", name: "value.csv" },
            { ...file, parents: [nested.id], id: "shortcut",
              mimeType: "application/vnd.google-apps.shortcut" },
          ],
          nextPageToken: null,
        };
      return { files: [], nextPageToken: null };
    });
    const rows = await scanNativeSourceMetadata(child, root, "token", () => true);
    expect(rows.map((row) => row.relative_path)).toEqual(["a.zip", "division/value.csv"]);
    expect(rows[0]).toMatchObject({
      file_id: "large",
      size_bytes: 3_000_000_000,
      created_at_utc: "2026-09-25T00:00:00Z",
      source_folder_id: child.id,
    });
    expect(rows[0].metadata_refreshed_at_utc).not.toEqual(rows[0].modified_at_utc);
    expect(fetchDriveFileBuffer).not.toHaveBeenCalled();
    expect(listNativeMetadataBatchPage).toHaveBeenCalledTimes(5);
    expect(vi.mocked(listNativeMetadataBatchPage).mock.calls.some((call) => call[3] === "second")).toBe(true);
  });

  it("bounds metadata requests linearly across a deep and wide source tree", async () => {
    const all = [root, child];
    let parent = child;
    for (let depth = 0; depth < 12; depth++) {
      const next = { ...child, id: `chain-${depth}`, name: `level-${depth}`,
        parentId: parent.id, parents: [parent.id] };
      all.push(next);
      parent = next;
      for (let width = 0; width < 20; width++) all.push({ ...child,
        id: `side-${depth}-${width}`, name: `item-${width}`,
        parentId: parent.id, parents: [parent.id] });
    }
    for (let width = 0; width < 2250; width++) all.push({ ...child,
      id: `wide-${width}`, name: `wide-${width}`,
      parentId: child.id, parents: [child.id] });
    const byId = new Map(all.map(folder => [folder.id, folder]));
    vi.mocked(getNativeFileMetadata).mockImplementation(async id => byId.get(id)!);
    vi.mocked(listNativeMetadataBatchPage).mockImplementation(async (ids, kind, _token, page) => {
      const items = kind === "folders" ? all.filter(folder => ids.includes(folder.parentId)) : [];
      const offset = page ? Number(page) : 0;
      return { files: items.slice(offset, offset + 1000),
        nextPageToken: offset + 1000 < items.length ? String(offset + 1000) : null };
    });
    const result = await scanNativeSourceMetadata(child, root, "token", () => true);
    expect(result).toEqual([]);
    expect(all.length - 2).toBeGreaterThan(2500);
    expect(listNativeMetadataBatchPage).toHaveBeenCalledTimes(92);
    expect(getNativeFileMetadata).toHaveBeenCalledTimes(4);
    expect(findFoldersByName).toHaveBeenCalledTimes(2);
    expect(fetchDriveFileBuffer).not.toHaveBeenCalled();
  });

  it("packs long parent IDs by their actual encoded URL before any listing", async () => {
    const nested = Array.from({ length: 180 }, (_, index) => ({ ...child,
      id: `folder-${index}`.padEnd(88, "x"), name: `folder-${index}`,
      parentId: child.id, parents: [child.id] }));
    vi.mocked(listNativeMetadataBatchPage).mockImplementation(async (ids, kind) => ({
      files: kind === "folders" && ids.includes(child.id) ? nested : [], nextPageToken: null,
    }));
    await scanNativeSourceMetadata(child, root, "token", () => true);
    const longBatches = vi.mocked(listNativeMetadataBatchPage).mock.calls
      .filter(([ids]) => ids.some((id) => id.length === 88));
    expect(longBatches.length).toBeGreaterThan(3);
    for (const [ids, kind] of longBatches) {
      expect(ids.length).toBeLessThan(100);
      expect(buildNativeMetadataBatchUrl(ids, kind).length).toBeLessThanOrEqual(6000);
    }
  });

  it("splits a candidate beyond the hard URL limit when each folder fits alone", async () => {
    const nested = Array.from({ length: 70 }, (_, index) => ({ ...child,
      id: `folder-${index}`.padEnd(60, "x"), name: `folder-${index}`,
      parentId: child.id, parents: [child.id] }));
    nested.push({ ...child, id: "L".repeat(2000), name: "long",
      parentId: child.id, parents: [child.id] });
    expect(buildNativeMetadataBatchUrl(nested.slice(0, -1).map((item) => item.id), "folders").length)
      .toBeLessThanOrEqual(6000);
    expect(() => buildNativeMetadataBatchUrl(nested.map((item) => item.id), "folders"))
      .toThrow("7800-character limit");
    vi.mocked(listNativeMetadataBatchPage).mockImplementation(async (ids, kind) => ({
      files: kind === "folders" && ids.includes(child.id) ? nested : [], nextPageToken: null,
    }));
    await expect(scanNativeSourceMetadata(child, root, "token", () => true)).resolves.toEqual([]);
    for (const [ids, kind] of vi.mocked(listNativeMetadataBatchPage).mock.calls)
      expect(buildNativeMetadataBatchUrl(ids, kind).length).toBeLessThanOrEqual(6000);
  });

  it("fails a broken nested page before any SQL relation is published", async () => {
    vi.mocked(getNativeFileMetadata).mockImplementation(async (id) =>
      id === "nested"
        ? { ...child, id: "nested", parents: [child.id] }
        : id === root.id
          ? root
          : child
    );
    vi.mocked(listNativeMetadataBatchPage)
      .mockResolvedValueOnce({
        files: [{ ...child, id: "nested", parents: [child.id] }],
        nextPageToken: null,
      })
      .mockRejectedValueOnce(new Error("Drive page failed"));
    await expect(scanNativeSourceMetadata(child, root, "token", () => true)).rejects.toThrow(
      "Drive page failed"
    );
    expect(fetchDriveFileBuffer).not.toHaveBeenCalled();
  });

  it.each([
    [{ ...file, parents: ["elsewhere"] }, "out-of-scope"],
    [{ ...file, parents: [child.id, "elsewhere"] }, "out-of-scope"],
  ])("rejects an invalid file parent before returning rows", async (invalid, message) => {
    vi.mocked(listNativeMetadataBatchPage).mockImplementation(async (_ids, kind) => ({
      files: kind === "files" ? [invalid] : [], nextPageToken: null,
    }));
    await expect(scanNativeSourceMetadata(child, root, "token", () => true)).rejects.toThrow(message);
  });

  it("rejects changed, duplicated, and missing folders on final verification", async () => {
    const nested = { ...child, id: "nested", parentId: child.id, parents: [child.id] };
    const changed = { ...nested, version: "99" };
    for (const final of [[changed], [nested, nested], []]) {
      let folderPhase = 0;
      vi.mocked(listNativeMetadataBatchPage).mockImplementation(async (_ids, kind) => ({
        files: kind === "files" ? [] : ++folderPhase === 1 ? [nested] : folderPhase === 2 ? [] : final,
        nextPageToken: null,
      }));
      await expect(scanNativeSourceMetadata(child, root, "token", () => true)).rejects.toThrow();
    }
    expect(fetchDriveFileBuffer).not.toHaveBeenCalled();
  });

  it("aborts a held project lookup and never issues a folder list", async () => {
    const controller = new AbortController();
    vi.mocked(findFoldersByName).mockImplementationOnce(async (_name, _parent, _token,
      _onPage, signal) => new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")),
          { once: true });
      }));
    const task = scanNativeSourceMetadata(child, root, "token", () => true, undefined,
      controller.signal);
    controller.abort();
    await expect(task).rejects.toThrow("Aborted");
    expect(listNativeMetadataBatchPage).not.toHaveBeenCalled();
  });

  it("publishes only a completed metadata table and rolls back a superseded commit", async () => {
    const { session, statements } = makeSession({});
    const rows = [
      {
        file_id: "a'b",
        source_folder_id: "deep",
        source_folder: "BDL",
        relative_path: "a'b.zip",
        file_name: "a'b.zip",
        parent_folder_id: "deep",
        size_bytes: null,
        mime_type: null,
        created_at_utc: null,
        modified_at_utc: null,
        metadata_refreshed_at_utc: "2026-09-27T00:00:00Z",
        drive_url: null,
        sha256_checksum: null,
      },
    ];
    await publishNativeMetadata(session, `BDL "files"`, rows, () => true);
    expect(statements.find((sql) => sql.startsWith("INSERT"))).toContain("a''b.zip");
    expect(statements.find((sql) => sql.startsWith("CREATE OR REPLACE TABLE"))).toContain(
      '"BDL ""files"""'
    );
    expect(statements.find((sql) => sql.startsWith("COMMENT ON TABLE"))).toContain(
      "zohelo-native-landing-file-metadata:v1"
    );
    expect(statements.indexOf("COMMIT;")).toBeGreaterThan(
      statements.findIndex((sql) => sql.startsWith("COMMENT ON TABLE"))
    );
    let checks = 0;
    await expect(publishNativeMetadata(session, "other", rows, () => ++checks < 4)).rejects.toThrow(
      "superseded"
    );
    expect(statements).toContain("ROLLBACK;");
  });
  it("resolves one unambiguous root and refuses duplicates", async () => {
    expect((await resolveNativeLandingRoot("token")).id).toBe(root.id);
    vi.mocked(findFoldersByName).mockResolvedValue([
      { id: "a", name: "zohelo-data" },
      { id: "b", name: "zohelo-data" },
    ]);
    await expect(resolveNativeLandingRoot("token")).rejects.toThrow("unambiguous");
  });

  it("exhausts pages, retains duplicate names, controls and huge archives, then rechecks ancestry", async () => {
    const many = Array.from({ length: 1000 }, (_, i) => ({
      ...file,
      id: `id-${i}`,
      name: "same.zip",
      size: 2_000_000_000,
      parents: [root.id],
    }));
    vi.mocked(listNativeChildrenPage)
      .mockResolvedValueOnce({ files: many, nextPageToken: "second" })
      .mockResolvedValueOnce({ files: [{ ...child, name: "_control" }], nextPageToken: null });
    const found = await listNativeFolder(root, folders, root.id, "token", () => true);
    expect(found).toHaveLength(1001);
    expect(found.filter((f) => f.name === "same.zip")).toHaveLength(1000);
    expect(vi.mocked(listNativeChildrenPage).mock.calls[1][2]).toBe("second");
    expect(
      vi.mocked(getNativeFileMetadata).mock.calls.filter(([id]) => id === root.id)
    ).toHaveLength(2);
    vi.mocked(listNativeChildrenPage).mockResolvedValue({ files: [], nextPageToken: null });
    vi.mocked(getNativeFileMetadata).mockImplementation(async (id) =>
      id === root.id ? { ...root, parents: ["elsewhere"] } : child
    );
    await expect(listNativeFolder(root, folders, root.id, "token", () => true)).rejects.toThrow(
      "root changed"
    );
  });

  it("checks parent chain, file identity and safe Drive-managed URLs", async () => {
    expect((await freshNativeFile(file, folders, root.id, "token", () => true)).id).toBe(file.id);
    expect(nativeDriveLink(file, "token", "download")).toBe(file.webContentLink);
    expect(safeDriveLink("https://drive.google.com/uc?access_token=secret", "token")).toBeNull();
    expect(safeDriveLink("https://evil.google.com/uc", "token")).toBeNull();
    expect(nativePreviewFormat({ ...file, name: "archive.zip" })).toBeNull();
    expect(nativePreviewFormat({ ...file, size: 2_000_000_000 })).toBeNull();
    vi.mocked(getNativeFileMetadata).mockImplementation(async (id) =>
      id === file.id ? { ...file, version: "3" } : id === root.id ? root : child
    );
    await expect(freshNativeFile(file, folders, root.id, "token", () => true)).rejects.toThrow(
      "file changed"
    );
  });

  it("verifies bounded bytes before a disposable SQL view and removes them", async () => {
    const db = {
      registerFileBuffer: vi.fn().mockResolvedValue(undefined),
      dropFile: vi.fn().mockResolvedValue(undefined),
    };
    const { session, statements, shared } = makeSession(db);
    const target = await previewNativeFile(
      session,
      file,
      folders,
      root.id,
      "token",
      () => true,
      new DriveDownloadBudget()
    );
    expect(target).toMatch(/^temp."native_preview_/);
    expect(statements[0]).toContain("read_csv_auto");
    expect(shared.query).not.toHaveBeenCalled();
    await disposeAllNativePreviews();
    expect(statements[1]).toContain("DROP VIEW IF EXISTS");
    expect(db.dropFile).toHaveBeenCalledTimes(1);
    vi.mocked(fetchDriveFileBuffer).mockResolvedValue(new TextEncoder().encode("altered"));
    await expect(
      previewNativeFile(
        session,
        file,
        folders,
        root.id,
        "token",
        () => true,
        new DriveDownloadBudget()
      )
    ).rejects.toThrow("bytes do not match");
  });

  it("keeps repeated same-file preview buffers separate when an old request is superseded", async () => {
    let finishRegistration!: () => void;
    const registerFileBuffer = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise<void>((resolve) => {
            finishRegistration = resolve;
          })
      )
      .mockResolvedValue(undefined);
    const dropFile = vi.fn().mockResolvedValue(undefined);
    const db = { registerFileBuffer, dropFile };
    const { session } = makeSession(db);
    let firstCurrent = true;
    const first = previewNativeFile(
      session,
      file,
      folders,
      root.id,
      "token",
      () => firstCurrent,
      new DriveDownloadBudget()
    );
    await vi.waitFor(() => expect(registerFileBuffer).toHaveBeenCalledTimes(1));
    firstCurrent = false;
    const second = await previewNativeFile(
      session,
      file,
      folders,
      root.id,
      "token",
      () => true,
      new DriveDownloadBudget()
    );
    finishRegistration();
    await expect(first).rejects.toThrow("superseded");
    expect(second).toMatch(/^temp."native_preview_/);
    const oldPath = registerFileBuffer.mock.calls[0][0];
    const newPath = registerFileBuffer.mock.calls[1][0];
    expect(oldPath).not.toBe(newPath);
    expect(dropFile).toHaveBeenCalledWith(oldPath);
    expect(dropFile).not.toHaveBeenCalledWith(newPath);
    await disposeAllNativePreviews();
    expect(dropFile).toHaveBeenCalledWith(newPath);
  });

  it("retires preview bookkeeping when its local engine closes", async () => {
    const db = {
      registerFileBuffer: vi.fn().mockResolvedValue(undefined),
      dropFile: vi.fn().mockResolvedValue(undefined),
    };
    const { session } = makeSession(db);
    await previewNativeFile(
      session,
      file,
      folders,
      root.id,
      "token",
      () => true,
      new DriveDownloadBudget()
    );
    await session.close();
    await expect(disposeAllNativePreviews()).resolves.toBeUndefined();
    expect(db.dropFile).not.toHaveBeenCalled(); // Closing the engine discards its temporary bytes.
  });

  it("does not let an OPFS close racing a queued disposal block future browsing", async () => {
    const db = {
      registerFileBuffer: vi.fn().mockResolvedValue(undefined),
      dropFile: vi.fn().mockResolvedValue(undefined),
    };
    const { session } = makeSession(db);
    await previewNativeFile(
      session,
      file,
      folders,
      root.id,
      "token",
      () => true,
      new DriveDownloadBudget()
    );
    const original = session.execute.bind(session);
    vi.spyOn(session, "execute").mockImplementation((request) => {
      if (request.sql.startsWith("DROP VIEW")) {
        void session.close();
        return createExecution({
          sql: request.sql,
          produce: async function* () {
            yield { kind: "chunk", rows: 0, chunk: { encoding: "rows", rows: [] } };
            throw new Error("Connection is closed");
          },
        });
      }
      return original(request);
    });
    await expect(disposeAllNativePreviews()).resolves.toBeUndefined();
    await expect(disposeAllNativePreviews()).resolves.toBeUndefined();
  });

  it("retains a live engine preview after cleanup failure so the error is retryable", async () => {
    const db = {
      registerFileBuffer: vi.fn().mockResolvedValue(undefined),
      dropFile: vi
        .fn()
        .mockRejectedValueOnce(new Error("file is busy"))
        .mockResolvedValue(undefined),
    };
    const { session } = makeSession(db);
    await previewNativeFile(
      session,
      file,
      folders,
      root.id,
      "token",
      () => true,
      new DriveDownloadBudget()
    );
    await expect(disposeAllNativePreviews()).rejects.toThrow("file is busy");
    await expect(disposeAllNativePreviews()).resolves.toBeUndefined();
    expect(db.dropFile).toHaveBeenCalledTimes(2);
  });
});
