import { beforeEach, describe, expect, it, vi } from "vitest";
import { createStore } from "zustand/vanilla";
import { devtools } from "zustand/middleware";
import type { DuckStoreState } from "../types";
import { createGoogleDriveSlice } from "../slices/googleDriveSlice";
import {
  loadTableIntoDuckDB,
  loadFileIntoDuckDB,
  loadTablesIntoDuckDB,
  resolvePublishedTableReferences,
  resolveLayerFolderId,
  listSubfolders,
  resolveLandingCatalog,
  resolveReleaseCatalog,
  GoogleDriveAuthError,
  type LandingSnapshotResolution,
} from "@/services/googleDrive";

vi.mock("sonner", () => ({ toast: { error: vi.fn(), success: vi.fn() } }));
vi.mock("@/services/engine", () => ({ asLocalDuckSession: (session: unknown) => session }));
vi.mock("@/services/googleDrive", async (original) => ({
  ...(await original<typeof import("@/services/googleDrive")>()),
  getStoredToken: () => null,
  clearStoredToken: vi.fn(),
  loadTableIntoDuckDB: vi.fn(),
  loadFileIntoDuckDB: vi.fn(),
  loadTablesIntoDuckDB: vi.fn(),
  resolvePublishedTableReferences: vi.fn(),
  resolveLayerFolderId: vi.fn(),
  listSubfolders: vi.fn(),
  resolveLandingCatalog: vi.fn(),
  resolveReleaseCatalog: vi.fn(),
}));

const target = '"02_bronze"."rates"';
function makeStore() {
  const store = createStore<DuckStoreState>()(
    devtools(
      (set, get, api) =>
        ({
          currentSession: {
            local: {
              db: {},
              connection: { query: vi.fn().mockResolvedValue({ toArray: () => [] }) },
            },
          },
          fetchDatabasesAndTablesInfo: vi.fn().mockResolvedValue(undefined),
          createTab: vi.fn(() => "prepared-join-tab"),
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
  vi.mocked(loadTablesIntoDuckDB).mockResolvedValue({
    loadedFiles: ["file"],
    queryTargets: [target],
  });
  vi.mocked(resolvePublishedTableReferences).mockResolvedValue([]);
  vi.mocked(resolveReleaseCatalog).mockResolvedValue({ kind: "legacy" });
  vi.mocked(resolveLandingCatalog).mockResolvedValue({
    snapshots: [],
    issues: [],
    fingerprint: "none",
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

describe("immutable release selection", () => {
  const release = (id: string) => ({
    kind: "release" as const,
    pointer: {
      format_version: 1 as const,
      release_id: id,
      manifest_file_id: `${id}-manifest`,
      manifest_sha256: "a".repeat(64),
      updated_at_utc: "2026-09-06T00:00:00Z",
    },
    manifestFileId: `${id}-manifest`,
    fingerprint: `${id}:${id}-manifest:${"a".repeat(64)}`,
    manifest: {
      format_version: 1 as const,
      release_id: id,
      release_scope: "nbp_silver" as const,
      status: "validated" as const,
      code_sha: "code",
      created_at_utc: "2026-09-06T00:00:00Z",
      artifacts: [],
      inputs: [],
      tests: { passed: true as const },
      datasets: [
        "nbp_exchange_rates_table_a",
        "nbp_exchange_rates_table_b",
        "nbp_exchange_rates_table_c",
        "nbp_gold_prices",
      ].map((dataset_id) => ({
        dataset_id,
        layer: "03_silver" as const,
        table_name: dataset_id,
        row_count: 1,
        min_date: "2024-01-01",
        max_date: "2024-01-01",
        columns: [{ name: "id", type: "INTEGER" }],
        files: [
          {
            id: `${id}-${dataset_id}`,
            name: `${dataset_id}.parquet`,
            size: 1,
            sha256: "a".repeat(64),
            tableName: dataset_id,
            layer: "03_silver",
          },
        ],
      })),
    },
  });

  const landing = (snapshotId: string) =>
    ({
      snapshots: [
        {
          fingerprint: `world_bank_wdi:${snapshotId}:manifest:${"b".repeat(64)}`,
          pointer: { snapshot_id: snapshotId },
          manifest: {
            source_id: "world_bank_wdi",
            snapshot_id: snapshotId,
            layer: "01_landing",
            table_name: "world_bank_wdi_responses",
            columns: [{ name: "payload_utf8", type: "VARCHAR" }],
            files: [
              {
                id: `${snapshotId}-file`,
                name: "part-00000.parquet",
                size: 10,
                sha256: "c".repeat(64),
                tableName: "world_bank_wdi_responses",
                layer: "01_landing",
              },
            ],
          },
        },
      ],
      issues: [],
      fingerprint: `world_bank_wdi:${snapshotId}`,
    }) as unknown as DuckStoreState["lakehouseLanding"] & {};

  it("merges an independently pinned Landing table without changing the NBP manifest", async () => {
    const store = makeStore();
    vi.mocked(resolveReleaseCatalog).mockResolvedValueOnce(release("release-1"));
    vi.mocked(resolveLandingCatalog).mockResolvedValueOnce(landing("snapshot-1"));

    await store.getState().refreshLakehouseCatalog();

    const selectedRelease = store.getState().lakehouseRelease;
    expect(selectedRelease?.kind === "release" && selectedRelease.manifest.datasets).toHaveLength(
      4
    );
    expect(
      store
        .getState()
        .lakehouseCatalog.find((layer) => layer.name === "01_landing")
        ?.children.map((table) => table.name)
    ).toEqual(["world_bank_wdi_responses"]);
    expect(store.getState().lakehouseStatusMessage).toContain("1 Landing source snapshot");
  });

  it("invalidates only a changed Landing view on refresh and retains the NBP pin", async () => {
    const store = makeStore();
    const firstLanding = landing("snapshot-1");
    vi.mocked(resolveReleaseCatalog).mockResolvedValue(release("release-1"));
    vi.mocked(resolveLandingCatalog).mockResolvedValueOnce(firstLanding);
    await store.getState().refreshLakehouseCatalog();
    await store.getState().selectLakehouseDataset("01_landing", "world_bank_wdi_responses");

    const connection = (
      store.getState().currentSession as unknown as {
        local: { connection: { query: ReturnType<typeof vi.fn> } };
      }
    ).local.connection;
    connection.query.mockClear();
    vi.mocked(resolveLandingCatalog).mockResolvedValueOnce(landing("snapshot-2"));

    await expect(store.getState().refreshLakehouseCatalog()).resolves.toBeUndefined();

    expect(connection.query).toHaveBeenCalledWith(
      'DROP VIEW IF EXISTS "01_landing"."world_bank_wdi_responses";'
    );
    expect(store.getState().lakehouseRelease).toMatchObject({
      kind: "release",
      manifest: { release_id: "release-1" },
    });
    expect(store.getState().activeLakehouseDataset).toBeNull();
    expect(loadTableIntoDuckDB).toHaveBeenCalledTimes(1);
  });

  it("pins the loaded release and refuses an explicit refresh to a different release", async () => {
    const store = makeStore();
    vi.mocked(resolveReleaseCatalog).mockResolvedValueOnce(release("release-1"));
    await store.getState().refreshLakehouseCatalog();
    expect(store.getState().lakehouseRelease).toMatchObject({ kind: "release" });
    await store.getState().selectLakehouseDataset("03_silver", "nbp_exchange_rates_table_a");

    vi.mocked(resolveReleaseCatalog).mockResolvedValueOnce(release("release-2"));
    await expect(store.getState().refreshLakehouseCatalog()).rejects.toThrow(
      /fresh DuckDB session/
    );
    expect(store.getState().lakehouseRelease).toMatchObject({
      kind: "release",
      manifest: { release_id: "release-1" },
    });
  });

  it("allows a different release after the current DuckDB engine is replaced", async () => {
    const store = makeStore();
    vi.mocked(resolveReleaseCatalog).mockResolvedValueOnce(release("release-1"));
    await store.getState().refreshLakehouseCatalog();
    await store.getState().selectLakehouseDataset("03_silver", "nbp_exchange_rates_table_a");

    store.setState({
      currentSession: {
        local: { db: {}, connection: { query: vi.fn().mockResolvedValue({ toArray: () => [] }) } },
      },
    } as unknown as Partial<DuckStoreState>);
    vi.mocked(resolveReleaseCatalog).mockResolvedValueOnce(release("release-2"));
    await expect(store.getState().refreshLakehouseCatalog()).resolves.toBeUndefined();
    expect(store.getState().lakehouseRelease).toMatchObject({
      kind: "release",
      manifest: { release_id: "release-2" },
    });
  });

  it("pins an engine even when its load completes after the session changes", async () => {
    const store = makeStore();
    vi.mocked(resolveReleaseCatalog).mockResolvedValueOnce(release("release-1"));
    await store.getState().refreshLakehouseCatalog();
    let complete!: (value: { loadedFiles: string[]; queryTarget: string }) => void;
    vi.mocked(loadTableIntoDuckDB).mockReturnValueOnce(
      new Promise((resolve) => (complete = resolve))
    );
    const pending = store
      .getState()
      .selectLakehouseDataset("03_silver", "nbp_exchange_rates_table_a");
    const originalEngine = (store.getState().currentSession as unknown as { local: { db: object } })
      .local.db;
    store.setState({
      currentSession: {
        local: { db: {}, connection: { query: vi.fn().mockResolvedValue({ toArray: () => [] }) } },
      },
    } as unknown as Partial<DuckStoreState>);
    complete({ loadedFiles: ["file"], queryTarget: target });
    await expect(pending).resolves.toBeNull();

    // Switching back to the engine where the request published its view remains blocked.
    store.setState({
      currentSession: { local: { db: originalEngine, connection: {} } },
    } as unknown as Partial<DuckStoreState>);
    vi.mocked(resolveReleaseCatalog).mockResolvedValueOnce(release("release-2"));
    await expect(store.getState().refreshLakehouseCatalog()).rejects.toThrow(
      /fresh DuckDB session/
    );
  });

  it("populates lakehouseCatalog with tables from multiple releases across bronze, silver, and gold", async () => {
    const store = makeStore();
    const nbpRel = release("nbp-release-1");
    const bdlRel = {
      kind: "release" as const,
      pointer: {
        format_version: 1 as const,
        release_id: "bdl-release-1",
        manifest_file_id: "bdl-manifest",
        manifest_sha256: "b".repeat(64),
        updated_at_utc: "2026-09-10T00:00:00Z",
      },
      manifestFileId: "bdl-manifest",
      fingerprint: `bdl-release-1:bdl-manifest:${"b".repeat(64)}`,
      manifest: {
        format_version: 2 as const,
        release_id: "bdl-release-1",
        release_scope: "bdl_platform" as const,
        status: "validated" as const,
        code_sha: "bdl-code",
        created_at_utc: "2026-09-10T00:00:00Z",
        artifacts: [],
        inputs: [],
        tests: { passed: true as const },
        datasets: [
          {
            dataset_id: "bronze_bdl_variables",
            layer: "02_bronze" as const,
            table_name: "bdl_variables",
            row_count: 10,
            min_date: null,
            max_date: null,
            columns: [{ name: "id", type: "INTEGER" }],
            files: [
              {
                id: "bdl-br-file",
                name: "bdl_variables.parquet",
                size: 10,
                sha256: "c".repeat(64),
                tableName: "bdl_variables",
                layer: "02_bronze" as const,
              },
            ],
          },
          {
            dataset_id: "bdl_variables",
            layer: "03_silver" as const,
            table_name: "bdl_variables",
            row_count: 10,
            min_date: null,
            max_date: null,
            columns: [{ name: "id", type: "INTEGER" }],
            files: [
              {
                id: "bdl-si-file",
                name: "bdl_variables.parquet",
                size: 10,
                sha256: "c".repeat(64),
                tableName: "bdl_variables",
                layer: "03_silver" as const,
              },
            ],
          },
          {
            dataset_id: "dim_bdl_variable",
            layer: "04_gold" as const,
            table_name: "dim_bdl_variable",
            row_count: 10,
            min_date: null,
            max_date: null,
            columns: [{ name: "id", type: "INTEGER" }],
            files: [
              {
                id: "bdl-gd-file",
                name: "dim_bdl_variable.parquet",
                size: 10,
                sha256: "c".repeat(64),
                tableName: "dim_bdl_variable",
                layer: "04_gold" as const,
              },
            ],
          },
        ],
      },
    };

    const multiRelease = {
      ...nbpRel,
      releases: [nbpRel, bdlRel],
    };

    vi.mocked(resolveReleaseCatalog).mockResolvedValueOnce(multiRelease);
    await store.getState().refreshLakehouseCatalog();

    const catalog = store.getState().lakehouseCatalog;
    const bronzeLayer = catalog.find((l) => l.name === "02_bronze");
    const silverLayer = catalog.find((l) => l.name === "03_silver");
    const goldLayer = catalog.find((l) => l.name === "04_gold");

    expect(bronzeLayer?.children.map((t) => t.name)).toContain("bdl_variables");
    expect(silverLayer?.children.map((t) => t.name)).toContain("nbp_exchange_rates_table_a");
    expect(silverLayer?.children.map((t) => t.name)).toContain("bdl_variables");
    expect(goldLayer?.children.map((t) => t.name)).toContain("dim_bdl_variable");
    expect(store.getState().lakehouseStatusMessage).toContain("nbp_silver");
    expect(store.getState().lakehouseStatusMessage).toContain("bdl_platform");
  });

  it("populates lakehouseCatalog with bronze campaign tables in 02_bronze", async () => {
    const store = makeStore();
    store.setState({
      googleAuth: { token: "token", isAuthenticated: true, authSource: "manual", error: null },
    });

    const bronzeCampaignSnapshot = {
      pointer: {
        format_version: 1,
        source_id: "opendata_org_bronze",
        snapshot_id: "snap-bronze",
        manifest_file_id: "man-bronze-id",
        manifest_file_name: "manifest-snap-bronze.json",
        manifest_sha256: "sha-bronze",
        manifest_size_bytes: 1234,
      },
      manifest: {
        format_version: 1,
        kind: "bronze_snapshot",
        source_id: "opendata_org_bronze",
        snapshot_id: "snap-bronze",
        created_at_utc: "2026-09-11T12:00:00Z",
        code_sha: "c".repeat(40),
        status: "validated",
        layer: "02_bronze",
        table_name: "br_opendata_organizations",
        row_count: 867322,
        coverage_status: "incomplete",
        files: [
          {
            id: "opendata-file-0",
            name: "br_opendata_organizations_bq_organization_000000000000.parquet",
            size: 9672157,
            sha256: "d".repeat(64),
            tableName: "br_opendata_organizations",
            layer: "02_bronze",
          },
        ],
        columns: [{ name: "record_id", type: "VARCHAR" }],
        accepted_file_count: 1,
        published_file_count: 1,
        pending_publication_count: 0,
        receipt_checkpoint_sha256: "e".repeat(64),
        tests: { passed: true },
      },
      fingerprint: "snap-bronze-fingerprint",
    };

    vi.mocked(resolveReleaseCatalog).mockResolvedValueOnce(release("release-1"));
    vi.mocked(resolveLandingCatalog).mockResolvedValueOnce({
      snapshots: [bronzeCampaignSnapshot as unknown as LandingSnapshotResolution],
      issues: [],
      fingerprint: "bronze-fingerprint",
    });

    await store.getState().refreshLakehouseCatalog();

    const catalog = store.getState().lakehouseCatalog;
    const bronzeLayer = catalog.find((l) => l.name === "02_bronze");
    expect(bronzeLayer?.children.map((t) => t.name)).toContain("br_opendata_organizations");
  });
});

describe("preparing a multi-table SQL query", () => {
  const joinedTables = ["fact_fx_quotes", "dim_currency", "dim_date"];
  const configurePinnedGoldRelease = (store: ReturnType<typeof makeStore>) => {
    store.setState({
      lakehouseRelease: {
        kind: "release",
        fingerprint: "pinned-release",
        pointer: {},
        manifestFileId: "manifest",
        manifest: {
          datasets: joinedTables.map((tableName) => ({
            dataset_id: tableName,
            layer: "04_gold",
            table_name: tableName,
            row_count: 1,
            min_date: null,
            max_date: null,
            columns: [],
            files: [
              {
                id: `${tableName}-file`,
                name: `${tableName}.parquet`,
                tableName,
                layer: "04_gold",
              },
            ],
          })),
        },
      } as unknown as DuckStoreState["lakehouseRelease"],
      lakehouseCatalog: [
        {
          type: "layer",
          name: "04_gold",
          id: null,
          expanded: true,
          loaded: true,
          children: joinedTables.map((name) => ({
            type: "table" as const,
            name,
            id: null,
            layer: "04_gold",
            expanded: false,
            loaded: true,
            children: [
              { id: `${name}-file`, name: `${name}.parquet`, tableName: name, layer: "04_gold" },
            ],
          })),
        },
      ],
    });
  };

  it("loads only explicit tables from the pinned release and opens unexecuted SQL", async () => {
    const store = makeStore();
    configurePinnedGoldRelease(store);
    const sql = 'SELECT * FROM "04_gold"."fact_fx_quotes"';

    await expect(
      store.getState().prepareLakehouseQuery(
        joinedTables.map((tableName) => ({ layerName: "04_gold", tableName })),
        sql,
        "FX join"
      )
    ).resolves.toBe("prepared-join-tab");

    expect(loadTablesIntoDuckDB).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      joinedTables.map((datasetName) =>
        expect.objectContaining({ datasetName, layerName: "04_gold" })
      ),
      "fixture-token",
      expect.anything(),
      expect.any(Function)
    );
    expect(store.getState().createTab).toHaveBeenCalledWith("sql", sql, "FX join");
    expect(store.getState().lakehouseStatusMessage).toContain("SQL is ready to run");
  });

  it("does not open SQL when Drive authorization changes during the grouped load", async () => {
    const store = makeStore();
    configurePinnedGoldRelease(store);
    let complete!: (result: { loadedFiles: string[]; queryTargets: string[] }) => void;
    vi.mocked(loadTablesIntoDuckDB).mockReturnValueOnce(
      new Promise((resolve) => (complete = resolve))
    );

    const pending = store.getState().prepareLakehouseQuery(
      joinedTables.map((tableName) => ({ layerName: "04_gold", tableName })),
      "SELECT 1",
      "FX join"
    );
    store.setState({
      googleAuth: { token: "new-token", isAuthenticated: true, authSource: "manual", error: null },
    });
    complete({ loadedFiles: ["file"], queryTargets: [target] });

    await expect(pending).resolves.toBeNull();
    expect(store.getState().createTab).not.toHaveBeenCalled();
  });
});

describe("lazy published-query loading", () => {
  const referencedTables = ["fact_fx_quotes", "dim_currency", "dim_date"];

  const configurePinnedGoldRelease = (store: ReturnType<typeof makeStore>) => {
    store.setState({
      lakehouseRelease: {
        kind: "release",
        fingerprint: "pinned-release",
        pointer: {},
        manifestFileId: "manifest",
        manifest: {
          datasets: referencedTables.map((tableName) => ({
            dataset_id: tableName,
            layer: "04_gold",
            table_name: tableName,
            row_count: 1,
            min_date: null,
            max_date: null,
            columns: [],
            files: [
              {
                id: `${tableName}-file`,
                name: `${tableName}.parquet`,
                tableName,
                layer: "04_gold",
              },
            ],
          })),
        },
      } as unknown as DuckStoreState["lakehouseRelease"],
    });
  };

  it("loads only parser-referenced pinned tables without opening a tab or executing SQL", async () => {
    const store = makeStore();
    configurePinnedGoldRelease(store);
    const getTableNames = vi
      .fn()
      .mockResolvedValue([
        '"04_gold"."fact_fx_quotes"',
        '"04_gold"."dim_currency"',
        '"04_gold"."dim_date"',
        "local_cte",
      ]);
    store.setState({
      currentSession: {
        local: {
          db: {},
          connection: { getTableNames, query: vi.fn().mockResolvedValue({ toArray: () => [] }) },
        },
      },
    } as unknown as Partial<DuckStoreState>);
    vi.mocked(resolvePublishedTableReferences).mockResolvedValue(
      referencedTables.map((datasetName) => ({
        datasetName,
        layerName: "04_gold" as const,
        files: [],
      }))
    );

    await store.getState().preparePublishedTablesForQuery("WITH local_cte AS (SELECT 1) SELECT 1");

    expect(resolvePublishedTableReferences).toHaveBeenCalledWith(
      expect.objectContaining({ getTableNames }),
      "WITH local_cte AS (SELECT 1) SELECT 1",
      expect.any(Array)
    );
    expect(loadTablesIntoDuckDB).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      referencedTables.map((datasetName) =>
        expect.objectContaining({ datasetName, layerName: "04_gold" })
      ),
      "fixture-token",
      expect.anything(),
      expect.any(Function)
    );
    expect(store.getState().createTab).not.toHaveBeenCalled();
  });

  it("autoloads an independently pinned Landing response table for SQL", async () => {
    const store = makeStore();
    const landingFile = {
      id: "landing-file",
      name: "part-00000.parquet",
      size: 10,
      sha256: "d".repeat(64),
      tableName: "world_bank_wdi_responses",
      layer: "01_landing",
    };
    store.setState({
      lakehouseRelease: { kind: "legacy" },
      lakehouseLanding: {
        snapshots: [
          {
            fingerprint: "landing-fingerprint",
            pointer: {},
            manifest: {
              source_id: "world_bank_wdi",
              table_name: "world_bank_wdi_responses",
              layer: "01_landing",
              columns: [{ name: "payload_utf8", type: "VARCHAR" }],
              files: [landingFile],
            },
          },
        ],
        issues: [],
        fingerprint: "landing-fingerprint",
      } as unknown as DuckStoreState["lakehouseLanding"],
      currentSession: {
        local: { db: {}, connection: { query: vi.fn().mockResolvedValue({ toArray: () => [] }) } },
      },
    } as unknown as Partial<DuckStoreState>);
    vi.mocked(resolvePublishedTableReferences).mockResolvedValue([
      {
        datasetName: "world_bank_wdi_responses",
        layerName: "01_landing",
        files: [landingFile],
      },
    ]);

    await store
      .getState()
      .preparePublishedTablesForQuery(
        'SELECT payload_utf8 FROM "01_landing"."world_bank_wdi_responses"'
      );

    expect(resolvePublishedTableReferences).toHaveBeenCalledWith(
      expect.anything(),
      expect.any(String),
      [
        expect.objectContaining({
          layer: "01_landing",
          table_name: "world_bank_wdi_responses",
        }),
      ]
    );
    expect(loadTablesIntoDuckDB).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      [expect.objectContaining({ layerName: "01_landing" })],
      "fixture-token",
      expect.anything(),
      expect.any(Function)
    );
  });

  it("does not replace an existing selection when referenced-table loading fails", async () => {
    const store = makeStore();
    configurePinnedGoldRelease(store);
    store.setState({
      activeLakehouseDataset: "previous",
      activeLakehouseLayer: "03_silver",
      currentSession: {
        local: { db: {}, connection: { query: vi.fn().mockResolvedValue({ toArray: () => [] }) } },
      },
    } as unknown as Partial<DuckStoreState>);
    vi.mocked(resolvePublishedTableReferences).mockResolvedValue([
      { datasetName: "dim_currency", layerName: "04_gold", files: [] },
    ]);
    vi.mocked(loadTablesIntoDuckDB).mockRejectedValueOnce(new Error("Download unavailable"));

    await expect(
      store.getState().preparePublishedTablesForQuery("SELECT * FROM dim_currency")
    ).rejects.toThrow("Download unavailable");
    expect(store.getState().activeLakehouseDataset).toBe("previous");
    expect(store.getState().activeLakehouseLayer).toBe("03_silver");
  });
  it("keeps existing local relations and reloads a release relation after it is dropped", async () => {
    const store = makeStore();
    configurePinnedGoldRelease(store);
    const query = vi
      .fn()
      .mockResolvedValueOnce({
        toArray: () => [{ table_schema: "04_gold", table_name: "dim_currency" }],
      })
      .mockResolvedValueOnce({ toArray: () => [] });
    store.setState({
      currentSession: { local: { db: {}, connection: { query } } },
    } as unknown as Partial<DuckStoreState>);
    vi.mocked(resolvePublishedTableReferences).mockResolvedValue([
      { datasetName: "dim_currency", layerName: "04_gold", files: [] },
    ]);
    await store.getState().preparePublishedTablesForQuery('SELECT * FROM "04_gold".dim_currency');
    expect(loadTablesIntoDuckDB).not.toHaveBeenCalled();
    await store.getState().preparePublishedTablesForQuery('SELECT * FROM "04_gold".dim_currency');
    expect(loadTablesIntoDuckDB).toHaveBeenCalledTimes(1);
  });
});
