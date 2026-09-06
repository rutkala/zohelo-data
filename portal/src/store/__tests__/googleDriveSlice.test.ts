import { beforeEach, describe, expect, it, vi } from "vitest";
import { createStore } from "zustand/vanilla";
import { devtools } from "zustand/middleware";
import type { DuckStoreState } from "../types";
import { createGoogleDriveSlice } from "../slices/googleDriveSlice";
import {
  loadTableIntoDuckDB,
  loadFileIntoDuckDB,
  resolveLayerFolderId,
  listSubfolders,
  GoogleDriveAuthError,
} from "@/services/googleDrive";

vi.mock("sonner", () => ({ toast: { error: vi.fn(), success: vi.fn() } }));
vi.mock("@/services/engine", () => ({ asLocalDuckSession: (session: unknown) => session }));
vi.mock("@/services/googleDrive", async (original) => ({
  ...(await original<typeof import("@/services/googleDrive")>()),
  getStoredToken: () => null,
  clearStoredToken: vi.fn(),
  loadTableIntoDuckDB: vi.fn(),
  loadFileIntoDuckDB: vi.fn(),
  resolveLayerFolderId: vi.fn(),
  listSubfolders: vi.fn(),
}));

const target = '"02_bronze"."rates"';
function makeStore() {
  const store = createStore<DuckStoreState>()(
    devtools(
      (set, get, api) =>
        ({
          currentSession: { local: { db: {}, connection: {} } },
          fetchDatabasesAndTablesInfo: vi.fn().mockResolvedValue(undefined),
          ...createGoogleDriveSlice(set, get, api),
        }) as unknown as DuckStoreState,
      { enabled: false }
    )
  );
  store.setState({
    googleAuth: {
      token: "fixture-token",
      isAuthenticated: true,
      authSource: "manual",
      error: null,
    },
    lakehouseCatalog: [
      {
        type: "layer",
        name: "02_bronze",
        id: "bronze",
        expanded: true,
        loaded: true,
        children: [
          {
            type: "table",
            name: "rates",
            id: "folder",
            layer: "02_bronze",
            expanded: true,
            loaded: true,
            children: [
              { id: "file", name: "data.parquet", tableName: "rates", layer: "02_bronze" },
            ],
          },
        ],
      },
    ],
  });
  return store;
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(loadTableIntoDuckDB).mockResolvedValue({ loadedFiles: ["file"], queryTarget: target });
  vi.mocked(loadFileIntoDuckDB).mockResolvedValue({
    filePath: "file",
    queryTarget: target + "__file",
  });
});

describe("Drive selection state", () => {
  it("publishes the selected dataset and exact SQL target only after loading succeeds", async () => {
    const store = makeStore();
    let complete!: (value: { loadedFiles: string[]; queryTarget: string }) => void;
    vi.mocked(loadTableIntoDuckDB).mockReturnValueOnce(
      new Promise((resolve) => {
        complete = resolve;
      })
    );
    const pending = store.getState().selectLakehouseDataset("02_bronze", "rates");
    expect(store.getState().activeLakehouseDataset).toBeNull();
    complete({ loadedFiles: ["file"], queryTarget: target });
    expect(await pending).toBe(target);
    expect(store.getState().activeLakehouseDataset).toBe("rates");
    expect(store.getState().activeLakehouseLayer).toBe("02_bronze");
  });

  it("returns no target on failure and keeps the previous selection", async () => {
    const store = makeStore();
    store.setState({ activeLakehouseDataset: "previous", activeLakehouseLayer: "03_silver" });
    vi.mocked(loadTableIntoDuckDB).mockRejectedValueOnce(new Error("Download unavailable"));
    expect(await store.getState().selectLakehouseDataset("02_bronze", "rates")).toBeNull();
    expect(store.getState().activeLakehouseDataset).toBe("previous");
    expect(store.getState().lakehouseStatusMessage).toContain("Download unavailable");
    expect(store.getState().isLakehouseLoading).toBe(false);
  });

  it("clears an expired token after a 401 while preserving loaded results", async () => {
    const store = makeStore();
    store.setState({ activeLakehouseDataset: "previous", activeLakehouseLayer: "03_silver" });
    vi.mocked(loadTableIntoDuckDB).mockRejectedValueOnce(
      new GoogleDriveAuthError("Google Drive authorization expired or was revoked. Sign in again.")
    );
    expect(await store.getState().selectLakehouseDataset("02_bronze", "rates")).toBeNull();
    expect(store.getState().googleAuth.isAuthenticated).toBe(false);
    expect(store.getState().googleAuth.token).toBeNull();
    expect(store.getState().activeLakehouseDataset).toBe("previous");
    expect(store.getState().lakehouseStatusMessage).toContain("Sign in again");
  });

  it("does not clear a newer token when an older request receives a 401", async () => {
    const store = makeStore();
    let reject!: (error: Error) => void;
    vi.mocked(loadTableIntoDuckDB).mockReturnValueOnce(
      new Promise((_, fail) => {
        reject = fail;
      })
    );
    const pending = store.getState().selectLakehouseDataset("02_bronze", "rates");
    store.setState({
      googleAuth: {
        token: "new-token",
        isAuthenticated: true,
        authSource: "google_identity",
        error: null,
      },
    });
    reject(
      new GoogleDriveAuthError("Google Drive authorization expired or was revoked. Sign in again.")
    );
    expect(await pending).toBeNull();
    expect(store.getState().googleAuth.token).toBe("new-token");
    expect(store.getState().googleAuth.isAuthenticated).toBe(true);
  });

  it("does not use another dataset when the requested catalog entry is missing", async () => {
    const store = makeStore();
    expect(await store.getState().selectLakehouseDataset("02_bronze", "missing")).toBeNull();
    expect(await store.getState().selectLakehouseFile("02_bronze", "rates", "missing")).toBeNull();
    expect(loadTableIntoDuckDB).not.toHaveBeenCalled();
    expect(loadFileIntoDuckDB).not.toHaveBeenCalled();
  });

  it("does not return a SQL target after a failed single-file load", async () => {
    const store = makeStore();
    vi.mocked(loadFileIntoDuckDB).mockRejectedValueOnce(new Error("Forbidden"));
    expect(await store.getState().selectLakehouseFile("02_bronze", "rates", "file")).toBeNull();
    expect(store.getState().activeLakehouseDataset).toBeNull();
    expect(store.getState().lakehouseStatusMessage).toContain("Forbidden");
  });

  it("selects the requested Drive ID when two files have the same name", async () => {
    const store = makeStore();
    const table = store.getState().lakehouseCatalog[0].children[0];
    const second = { ...table.children[0], id: "second-file" };
    table.children.push(second);
    await store.getState().selectLakehouseFile("02_bronze", "rates", second.id);
    expect(vi.mocked(loadFileIntoDuckDB).mock.calls[0][3]).toEqual(second);
  });

  it("serializes rapid selections", async () => {
    const store = makeStore();
    let complete!: (value: { loadedFiles: string[]; queryTarget: string }) => void;
    vi.mocked(loadTableIntoDuckDB).mockReturnValueOnce(
      new Promise((resolve) => {
        complete = resolve;
      })
    );
    const first = store.getState().selectLakehouseDataset("02_bronze", "rates");
    expect(await store.getState().selectLakehouseDataset("02_bronze", "rates")).toBeNull();
    expect(loadTableIntoDuckDB).toHaveBeenCalledTimes(1);
    complete({ loadedFiles: ["file"], queryTarget: target });
    expect(await first).toBe(target);
  });

  it("ignores a completed load if Drive was disconnected while waiting", async () => {
    const store = makeStore();
    let complete!: (value: { loadedFiles: string[]; queryTarget: string }) => void;
    vi.mocked(loadTableIntoDuckDB).mockReturnValueOnce(
      new Promise((resolve) => {
        complete = resolve;
      })
    );
    const pending = store.getState().selectLakehouseDataset("02_bronze", "rates");
    store.getState().disconnectGoogleDrive();
    complete({ loadedFiles: ["file"], queryTarget: target });
    expect(await pending).toBeNull();
    expect(store.getState().activeLakehouseDataset).toBeNull();
    expect(store.getState().lakehouseStatusMessage).toMatch(/Disconnected/);
    expect(store.getState().lakehouseCatalog.every((layer) => layer.children.length === 0)).toBe(
      true
    );
  });

  it("preserves the last catalog and reports refresh failure", async () => {
    const store = makeStore();
    const previous = store.getState().lakehouseCatalog;
    vi.mocked(resolveLayerFolderId).mockRejectedValueOnce(new Error("Access denied"));
    await expect(store.getState().refreshLakehouseCatalog()).rejects.toThrow("Access denied");
    expect(store.getState().lakehouseCatalog).toBe(previous);
    expect(store.getState().lakehouseStatusMessage).toContain("Catalog refresh error");
  });

  it("does not automatically load an arbitrary default after refreshing the catalog", async () => {
    const store = makeStore();
    vi.mocked(resolveLayerFolderId).mockResolvedValue("bronze");
    vi.mocked(listSubfolders).mockResolvedValue([{ id: "new-folder", name: "nbp_gold_prices" }]);
    await store.getState().refreshLakehouseCatalog();
    expect(loadTableIntoDuckDB).not.toHaveBeenCalled();
    expect(store.getState().activeLakehouseDataset).toBeNull();
    expect(store.getState().lakehouseStatusMessage).toContain("Select a dataset");
  });
});
