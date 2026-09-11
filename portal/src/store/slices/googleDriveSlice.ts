import type { StateCreator } from "zustand";
import { toast } from "sonner";
import { asLocalDuckSession } from "@/services/engine";
import {
  clearStoredToken,
  clearStoredTokenIfCurrent,
  createDefaultLakehouseTree,
  createDriveDownloadBudget,
  getStoredToken,
  listDataFilesInFolder,
  listSubfolders,
  loadFileIntoDuckDB,
  loadTableIntoDuckDB,
  loadTablesIntoDuckDB,
  requestGoogleAccessToken,
  resolveLandingCatalog,
  resolvePublishedTableReferences,
  resolveLayerFolderId,
  resolveReleaseCatalog,
  isGoogleDriveAuthError,
  setStoredToken,
  type LakehouseLayer,
  type LakehouseTable,
  type LandingCatalogResolution,
  type LandingSourceId,
  type PublishedDataset,
  type ReleaseCatalogResolution,
} from "@/services/googleDrive";
import type { DuckStoreState, GoogleDriveSlice } from "../types";

const messageOf = (error: unknown) => (error instanceof Error ? error.message : "Unknown error");
const releaseMessage = (release: Extract<ReleaseCatalogResolution, { kind: "release" }>) => {
  const rels =
    release.releases && release.releases.length > 0 ? release.releases : [release];
  return rels
    .map(
      (r) =>
        `Release ${r.manifest.release_id} · ${r.manifest.release_scope} · ${r.manifest.status}.`
    )
    .join(" ");
};
const releaseFingerprint = (release: ReleaseCatalogResolution | null) => {
  if (!release) return null;
  if (release.kind === "legacy") return "legacy";
  if (release.releases && release.releases.length > 0) {
    return release.releases.map((r) => r.fingerprint).sort().join(";");
  }
  return release.fingerprint;
};

const landingDatasets = (landing: LandingCatalogResolution | null): PublishedDataset[] =>
  (landing?.snapshots ?? []).map(({ manifest }) => ({
    dataset_id: manifest.table_name,
    layer: manifest.layer,
    table_name: manifest.table_name,
    columns: manifest.columns,
    files: manifest.files,
  }));

const publishedDatasets = (
  release: ReleaseCatalogResolution | null,
  landing: LandingCatalogResolution | null
): PublishedDataset[] => [
  ...(release?.kind === "release"
    ? release.releases && release.releases.length > 0
      ? release.releases.flatMap((r) => r.manifest.datasets)
      : release.manifest.datasets
    : []),
  ...landingDatasets(landing),
];

const landingStatus = (landing: LandingCatalogResolution) => {
  const available = `${landing.snapshots.length} Landing source snapshot(s)`;
  if (landing.issues.length === 0) return available;
  const errors = landing.issues.map((issue) => `${issue.source_id}: ${issue.message}`).join("; ");
  return `${available}. Landing metadata error — ${errors}`;
};

const handleDriveAuthFailure = (
  set: (state: Partial<DuckStoreState>) => void,
  get: () => DuckStoreState,
  token: string,
  error: unknown
): boolean => {
  if (!isGoogleDriveAuthError(error) || get().googleAuth.token !== token) return false;
  clearStoredTokenIfCurrent(token);
  set({
    googleAuth: { token: null, isAuthenticated: false, authSource: "none", error: error.message },
    lakehouseStatusMessage: "Google Drive authorization expired or was revoked. Sign in again.",
  });
  return true;
};

async function loadTableFiles(table: LakehouseTable, token: string): Promise<LakehouseTable> {
  if (!table.id) throw new Error(`Dataset '${table.name}' has no Drive folder.`);
  const files = await listDataFilesInFolder(table.id, token);
  return {
    ...table,
    loaded: true,
    children: files.map((file) => ({ ...file, tableName: table.name, layer: table.layer })),
  };
}

async function loadLayer(layer: LakehouseLayer, token: string): Promise<LakehouseLayer> {
  const id = await resolveLayerFolderId(layer.name, token);
  if (!id) return { ...layer, id: null, loaded: true, children: [] };
  const folders = await listSubfolders(id, token);
  folders.sort((a, b) => a.name.localeCompare(b.name));
  const children = await Promise.all(
    folders.map(async (folder) => {
      const previous = layer.children.find((table) => table.id === folder.id);
      const table: LakehouseTable = {
        type: "table",
        name: folder.name,
        id: folder.id,
        layer: layer.name,
        expanded: previous?.expanded ?? false,
        loaded: false,
        children: [],
      };
      return table.expanded ? loadTableFiles(table, token) : table;
    })
  );
  return { ...layer, id, loaded: true, children };
}

function treeFromPublished(
  release: Extract<ReleaseCatalogResolution, { kind: "release" }>,
  landing: LandingCatalogResolution
): LakehouseLayer[] {
  const datasets = publishedDatasets(release, landing);
  return createDefaultLakehouseTree().map((layer) => {
    return {
      ...layer,
      id: null,
      expanded:
        layer.name === "01_landing" ? landing.snapshots.length > 0 : layer.name === "03_silver",
      loaded: true,
      children: datasets
        .filter((dataset) => dataset.layer === layer.name)
        .map((dataset) => ({
          type: "table" as const,
          name: dataset.table_name,
          id: null,
          layer: dataset.layer,
          expanded: false,
          loaded: true,
          children: dataset.files.map((file) => ({ ...file, tableName: dataset.table_name })),
        })),
    };
  });
}

function mergeLandingIntoTree(
  tree: LakehouseLayer[],
  landing: LandingCatalogResolution
): LakehouseLayer[] {
  const datasets = landingDatasets(landing);
  return tree.map((layer) => {
    const matching = datasets
      .filter((d) => d.layer === layer.name)
      .map((dataset) => ({
        type: "table" as const,
        name: dataset.table_name,
        id: null,
        layer: dataset.layer,
        expanded: false,
        loaded: true,
        children: dataset.files.map((file) => ({ ...file, tableName: dataset.table_name })),
      }));
    if (matching.length === 0) return layer;
    return {
      ...layer,
      id: null,
      expanded: matching.length > 0 || layer.expanded,
      loaded: true,
      children: [
        ...layer.children.filter((c) => !matching.some((m) => m.name === c.name)),
        ...matching,
      ],
    };
  });
}

export const createGoogleDriveSlice: StateCreator<
  DuckStoreState,
  [["zustand/devtools", never]],
  [],
  GoogleDriveSlice
> = (set, get) => {
  let busy = false;
  // DuckDB views survive disconnect; record their release per engine, not globally.
  const loadedReleaseFingerprints = new WeakMap<object, string>();
  const loadedLandingFingerprints = new WeakMap<object, Map<LandingSourceId, string>>();
  const downloadBudgets = new WeakMap<object, ReturnType<typeof createDriveDownloadBudget>>();
  const budgetForEngine = (db: object) => {
    let budget = downloadBudgets.get(db);
    if (!budget) {
      budget = createDriveDownloadBudget();
      downloadBudgets.set(db, budget);
    }
    return budget;
  };
  const tableKey = (layerName: string, tableName: string) => `${layerName}\u0000${tableName}`;
  const markLoadedTable = (
    db: object,
    layerName: string,
    tableName: string,
    release: ReleaseCatalogResolution | null,
    landing: LandingCatalogResolution | null
  ) => {
    if (layerName === "01_landing") {
      const snapshot = landing?.snapshots.find(({ manifest }) => manifest.table_name === tableName);
      if (!snapshot) return;
      let fingerprints = loadedLandingFingerprints.get(db);
      if (!fingerprints) {
        fingerprints = new Map();
        loadedLandingFingerprints.set(db, fingerprints);
      }
      fingerprints.set(snapshot.manifest.source_id, snapshot.fingerprint);
      return;
    }
    const fingerprint = releaseFingerprint(release);
    if (fingerprint) loadedReleaseFingerprints.set(db, fingerprint);
  };

  const select = async (
    layerName: string,
    tableName: string,
    fileId?: string
  ): Promise<string | null> => {
    if (busy) return null;
    const session = get().currentSession;
    const local = asLocalDuckSession(session)?.local;
    const token = get().googleAuth.token;
    const source = get().lakehouseRelease;
    const landing = get().lakehouseLanding;
    const table = get()
      .lakehouseCatalog.find((layer) => layer.name === layerName)
      ?.children.find((item) => item.name === tableName);
    const file =
      fileId === undefined ? undefined : table?.children.find((item) => item.id === fileId);
    const label = file?.name ?? tableName;
    const current = () =>
      get().currentSession === session &&
      get().googleAuth.token === token &&
      get().lakehouseRelease === source &&
      get().lakehouseLanding === landing;
    if (!local) {
      set({
        lakehouseStatusMessage:
          "DuckDB is still initializing. Please wait, then select the dataset again.",
      });
      return null;
    }
    busy = true;
    set({
      isLakehouseLoading: true,
      lakehouseStatusMessage: `Loading '${label}' from Google Drive...`,
    });
    try {
      if (!table) throw new Error(`Dataset '${tableName}' was not found in the catalog.`);
      let queryTarget: string;
      if (fileId !== undefined) {
        if (!file) throw new Error("The requested file was not found in the catalog.");
        ({ queryTarget } = await loadFileIntoDuckDB(
          local.db,
          local.connection,
          tableName,
          file,
          token ?? "",
          budgetForEngine(local.db)
        ));
      } else {
        ({ queryTarget } = await loadTableIntoDuckDB(
          local.db,
          local.connection,
          tableName,
          table.id,
          table.children,
          token ?? "",
          layerName,
          budgetForEngine(local.db)
        ));
      }
      // publishViews has completed at this point. Keep this engine pinned even if the
      // caller changed token/session while the request was in flight.
      markLoadedTable(local.db, layerName, tableName, source, landing);
      if (!current()) return null;
      let schemaWarning = "";
      try {
        await get().fetchDatabasesAndTablesInfo();
      } catch (error) {
        schemaWarning = ` Schema refresh failed: ${messageOf(error)}`;
      }
      if (!current()) return null;
      set({
        activeLakehouseDataset: tableName,
        activeLakehouseLayer: layerName,
        lakehouseStatusMessage: `Loaded '${label}'. Query ${queryTarget}.${schemaWarning}`,
      });
      toast.success(`Loaded '${label}'`);
      return queryTarget;
    } catch (error) {
      const authFailure = handleDriveAuthFailure(set, get, token ?? "", error);
      if (current()) {
        const message = authFailure
          ? "Google Drive authorization expired or was revoked. Sign in again."
          : `Error loading '${label}': ${messageOf(error)}`;
        set({ lakehouseStatusMessage: message });
        toast.error(message);
      }
      return null;
    } finally {
      busy = false;
      set({ isLakehouseLoading: false });
    }
  };

  const prepareLakehouseQuery = async (
    requestedTables: ReadonlyArray<{ layerName: string; tableName: string }>,
    sql: string,
    title: string
  ): Promise<string | null> => {
    if (busy) return null;
    const session = get().currentSession;
    const local = asLocalDuckSession(session)?.local;
    const token = get().googleAuth.token;
    const source = get().lakehouseRelease;
    const landing = get().lakehouseLanding;
    const current = () =>
      get().currentSession === session &&
      get().googleAuth.token === token &&
      get().lakehouseRelease === source &&
      get().lakehouseLanding === landing;
    if (!local) {
      set({
        lakehouseStatusMessage:
          "DuckDB is still initializing. Please wait, then open the join example again.",
      });
      return null;
    }
    if (!token) {
      set({ lakehouseStatusMessage: "Sign in to Google Drive before preparing a SQL query." });
      return null;
    }
    if (source?.kind !== "release" && (landing?.snapshots.length ?? 0) === 0) {
      set({
        lakehouseStatusMessage:
          "Refresh the Google Drive lakehouse to pin a release before preparing a multi-table query.",
      });
      return null;
    }
    if (!sql.trim() || !title.trim() || requestedTables.length === 0) {
      set({
        lakehouseStatusMessage:
          "The join example must include SQL, a title, and at least one dataset.",
      });
      return null;
    }

    let selections: Array<{
      datasetName: string;
      tableFolderId: string | null;
      files: LakehouseTable["children"];
      layerName: string;
    }>;
    try {
      selections = requestedTables.map(({ layerName, tableName }) => {
        const table = get()
          .lakehouseCatalog.find((layer) => layer.name === layerName)
          ?.children.find((item) => item.name === tableName);
        if (!table) {
          throw new Error(
            `Dataset '${layerName}.${tableName}' was not found in the pinned release.`
          );
        }
        return {
          datasetName: table.name,
          tableFolderId: table.id,
          files: table.children,
          layerName: table.layer,
        };
      });
    } catch (error) {
      set({ lakehouseStatusMessage: `Error preparing SQL query: ${messageOf(error)}` });
      return null;
    }
    const label = selections.map((table) => table.datasetName).join(", ");
    busy = true;
    set({
      isLakehouseLoading: true,
      lakehouseStatusMessage: `Loading ${selections.length} selected dataset(s) from Google Drive...`,
    });
    try {
      await loadTablesIntoDuckDB(
        local.db,
        local.connection,
        selections,
        token,
        budgetForEngine(local.db),
        () => {
          if (!current()) {
            throw new Error(
              "Google Drive session changed before the selected tables could be prepared."
            );
          }
        }
      );
      // Views may exist on the old engine if a session or token changed while a
      // download was pending, but never open a tab whose SQL was not prepared
      // against the still-current pinned release.
      for (const table of selections) {
        markLoadedTable(local.db, table.layerName, table.datasetName, source, landing);
      }
      if (!current()) return null;
      let schemaWarning = "";
      try {
        await get().fetchDatabasesAndTablesInfo();
      } catch (error) {
        schemaWarning = ` Schema refresh failed: ${messageOf(error)}`;
      }
      if (!current()) return null;
      const tabId = get().createTab("sql", sql, title);
      const lastSelection = selections[selections.length - 1];
      set({
        activeLakehouseDataset: lastSelection?.datasetName ?? null,
        activeLakehouseLayer: lastSelection?.layerName ?? null,
        lakehouseStatusMessage: `Loaded ${selections.length} dataset(s): ${label}. SQL is ready to run.${schemaWarning}`,
      });
      toast.success("Join SQL is ready to run");
      return tabId;
    } catch (error) {
      const authFailure = handleDriveAuthFailure(set, get, token, error);
      if (current()) {
        const message = authFailure
          ? "Google Drive authorization expired or was revoked. Sign in again."
          : `Error preparing SQL query: ${messageOf(error)}`;
        set({ lakehouseStatusMessage: message });
        toast.error(message);
      }
      return null;
    } finally {
      busy = false;
      set({ isLakehouseLoading: false });
    }
  };

  /**
   * Loads precisely the pinned-release datasets a SQL statement references.
   * This runs before the engine execution begins; it never opens a tab or
   * submits the statement itself.
   */
  const preparePublishedTablesForQuery = async (sql: string): Promise<void> => {
    const session = get().currentSession;
    const local = asLocalDuckSession(session)?.local;
    const token = get().googleAuth.token;
    const source = get().lakehouseRelease;
    const landing = get().lakehouseLanding;
    // Remote engines, ordinary local SQL, and legacy catalogs keep their
    // existing execution path. Only a resolved immutable release participates.
    const datasets = publishedDatasets(source, landing);
    if (!local || datasets.length === 0) return;
    const current = () =>
      get().currentSession === session &&
      get().googleAuth.token === token &&
      get().lakehouseRelease === source &&
      get().lakehouseLanding === landing;
    if (busy) {
      throw new Error(
        "Google Drive is already loading data. Wait for it to finish, then run the query again."
      );
    }

    busy = true;
    try {
      const referenced = await resolvePublishedTableReferences(local.connection, sql, datasets);
      if (!current()) {
        throw new Error("Google Drive session changed before SQL dependencies could be resolved.");
      }
      if (referenced.length === 0) return;
      const loadedFingerprint = loadedReleaseFingerprints.get(local.db);
      const selectedReleaseFingerprint = releaseFingerprint(source);
      if (
        loadedFingerprint &&
        selectedReleaseFingerprint &&
        loadedFingerprint !== selectedReleaseFingerprint
      ) {
        throw new Error(
          "This DuckDB session has views from a different release. Start a fresh DuckDB session before querying this release."
        );
      }

      // Check actual relations, not a remembered "loaded" flag. This preserves
      // user-created relations and lets a dropped release view be loaded again.
      const relations = await local.connection.query(
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = current_database()"
      );
      if (!current())
        throw new Error("The active data session changed before loading could start.");
      const existing = new Set(
        relations
          .toArray()
          .map((row) =>
            tableKey(String(row.table_schema).toLowerCase(), String(row.table_name).toLowerCase())
          )
      );
      const pending = referenced.filter(
        (table) =>
          !existing.has(tableKey(table.layerName.toLowerCase(), table.datasetName.toLowerCase()))
      );
      if (pending.length === 0) return;
      if (!token)
        throw new Error("Sign in to Google Drive before querying published release data.");

      const label = pending.map((table) => `${table.layerName}.${table.datasetName}`).join(", ");
      set({
        isLakehouseLoading: true,
        lakehouseStatusMessage: `Loading ${pending.length} published dataset(s) referenced by this SQL query...`,
      });
      await loadTablesIntoDuckDB(
        local.db,
        local.connection,
        pending.map((table) => ({
          datasetName: table.datasetName,
          tableFolderId: null,
          files: table.files,
          layerName: table.layerName,
        })),
        token,
        budgetForEngine(local.db),
        () => {
          if (!current()) {
            throw new Error(
              "Google Drive session changed before the referenced tables could be loaded."
            );
          }
        }
      );
      if (!current()) {
        throw new Error("Google Drive session changed before the query could run.");
      }
      for (const table of pending) {
        markLoadedTable(local.db, table.layerName, table.datasetName, source, landing);
      }
      // Refresh visible workspace relations after lazy loading; a metadata
      // refresh failure must not discard verified data already available.
      await get()
        .fetchDatabasesAndTablesInfo()
        .catch(() => undefined);
      if (!current())
        throw new Error("The active data session changed before this query could run.");
      set({
        lakehouseStatusMessage: `Loaded ${pending.length} published dataset(s) for this query: ${label}.`,
      });
    } catch (error) {
      const authFailure = token ? handleDriveAuthFailure(set, get, token, error) : false;
      if (current()) {
        set({
          lakehouseStatusMessage: authFailure
            ? "Google Drive authorization expired or was revoked. Sign in again."
            : `Could not load published query data: ${messageOf(error)}`,
        });
      }
      throw error;
    } finally {
      busy = false;
      set({ isLakehouseLoading: false });
    }
  };

  const token = getStoredToken();
  return {
    googleAuth: {
      token,
      isAuthenticated: !!token,
      authSource: token ? "google_identity" : "none",
      error: null,
    },
    lakehouseCatalog: createDefaultLakehouseTree(),
    lakehouseRelease: null,
    lakehouseLanding: null,
    isLakehouseLoading: false,
    lakehouseStatusMessage: "Sign in to browse Google Drive datasets.",
    activeLakehouseDataset: null,
    activeLakehouseLayer: null,

    signInWithGoogle: async (promptConsent = false) => {
      try {
        set({
          isLakehouseLoading: true,
          lakehouseStatusMessage: "Requesting Google Sign-In authorization...",
        });
        const nextToken = await requestGoogleAccessToken({ promptConsent });
        set({
          googleAuth: {
            token: nextToken,
            isAuthenticated: true,
            authSource: "google_identity",
            error: null,
          },
        });
        await get().refreshLakehouseCatalog();
        return true;
      } catch (error) {
        const message = messageOf(error);
        set({ lakehouseStatusMessage: `Google Drive connection error: ${message}` });
        toast.error(message);
        return false;
      } finally {
        set({ isLakehouseLoading: false });
      }
    },

    setManualGoogleToken: async (nextToken) => {
      const trimmed = nextToken.trim();
      if (!trimmed) {
        toast.error("Please enter a valid Google OAuth token");
        return false;
      }
      setStoredToken(trimmed);
      set({
        googleAuth: { token: trimmed, isAuthenticated: true, authSource: "manual", error: null },
      });
      try {
        await get().refreshLakehouseCatalog();
        return true;
      } catch {
        return false;
      }
    },

    disconnectGoogleDrive: () => {
      clearStoredToken();
      set({
        googleAuth: { token: null, isAuthenticated: false, authSource: "none", error: null },
        lakehouseCatalog: createDefaultLakehouseTree(),
        lakehouseRelease: null,
        lakehouseLanding: null,
        activeLakehouseDataset: null,
        activeLakehouseLayer: null,
        isLakehouseLoading: false,
        lakehouseStatusMessage: "Disconnected from Google Drive.",
      });
    },

    refreshLakehouseCatalog: async () => {
      if (busy) return;
      const activeToken = get().googleAuth.token;
      const activeSession = get().currentSession;
      if (!activeToken) {
        set({ lakehouseStatusMessage: "Sign in to browse Google Drive datasets." });
        return;
      }
      busy = true;
      set({
        isLakehouseLoading: true,
        lakehouseStatusMessage: "Resolving the Google Drive release...",
      });
      try {
        // This is the only pointer resolution path. Layer toggles use the pinned result.
        const local = asLocalDuckSession(activeSession)?.local;
        const budget = local ? budgetForEngine(local.db) : createDriveDownloadBudget();
        const release = await resolveReleaseCatalog(activeToken, budget);
        const landing = await resolveLandingCatalog(activeToken, budget);
        if (get().googleAuth.token !== activeToken || get().currentSession !== activeSession)
          return;
        const candidateFingerprint = releaseFingerprint(release);
        const loadedReleaseFingerprint = local
          ? loadedReleaseFingerprints.get(local.db)
          : undefined;
        if (loadedReleaseFingerprint && loadedReleaseFingerprint !== candidateFingerprint) {
          throw new Error(
            "A different release is available, but this DuckDB session still has loaded views. Start a fresh DuckDB session before switching releases."
          );
        }
        // Landing sources are pinned independently of NBP. On explicit refresh,
        // invalidate only a changed source view; its next preview/query downloads
        // and verifies the newly selected fragments. Unchanged NBP views and
        // registered files remain available in this engine.
        if (local) {
          const loadedLanding = loadedLandingFingerprints.get(local.db);
          if (loadedLanding) {
            for (const [sourceId, loadedFingerprint] of loadedLanding) {
              const selected = landing.snapshots.find(
                ({ manifest }) => manifest.source_id === sourceId
              );
              if (selected?.fingerprint === loadedFingerprint) continue;
              const targetLayer =
                selected?.manifest.layer ??
                (sourceId.endsWith("_bronze") ? "02_bronze" : "01_landing");
              const tableName =
                selected?.manifest.table_name ??
                (sourceId === "opendata_org_bronze"
                  ? "br_opendata_organizations"
                  : sourceId === "opendata_org_locations_bronze"
                    ? "br_opendata_locations"
                    : sourceId === "opendata_org_people_bronze"
                      ? "br_opendata_people"
                      : sourceId.endsWith("_bulk")
                        ? `${sourceId.slice(0, -"_bulk".length)}_distributions`
                        : `${sourceId}_responses`);
              await local.connection.query(
                `DROP VIEW IF EXISTS "${targetLayer}"."${tableName}";`
              );
              loadedLanding.delete(sourceId);
              if (
                get().activeLakehouseLayer === targetLayer &&
                get().activeLakehouseDataset === tableName
              ) {
                set({ activeLakehouseLayer: null, activeLakehouseDataset: null });
              }
            }
          }
        }
        if (get().googleAuth.token !== activeToken || get().currentSession !== activeSession)
          return;
        if (release.kind === "release") {
          set({
            lakehouseCatalog: treeFromPublished(release, landing),
            lakehouseRelease: release,
            lakehouseLanding: landing,
            lakehouseStatusMessage: `${releaseMessage(release)} ${landingStatus(landing)}. Select a dataset to query.`,
          });
          return;
        }
        const tree: LakehouseLayer[] = [];
        for (const layer of get().lakehouseCatalog) {
          tree.push(
            layer.expanded
              ? await loadLayer(layer, activeToken)
              : { ...layer, id: null, loaded: false, children: [] }
          );
        }
        if (get().googleAuth.token !== activeToken) return;
        set({
          lakehouseCatalog: mergeLandingIntoTree(tree, landing),
          lakehouseRelease: release,
          lakehouseLanding: landing,
          lakehouseStatusMessage: `Legacy/unversioned catalog loaded. ${landingStatus(landing)}. Select a dataset to query.`,
        });
      } catch (error) {
        const authFailure = handleDriveAuthFailure(set, get, activeToken, error);
        if (get().googleAuth.token === activeToken && get().currentSession === activeSession) {
          const message = authFailure
            ? "Google Drive authorization expired or was revoked. Sign in again."
            : `Catalog refresh error: ${messageOf(error)}`;
          set({ lakehouseStatusMessage: message });
          toast.error(message);
        }
        throw error;
      } finally {
        busy = false;
        set({ isLakehouseLoading: false });
      }
    },

    toggleLakehouseLayer: async (layerName) => {
      if (busy) return;
      const layer = get().lakehouseCatalog.find((item) => item.name === layerName);
      if (!layer) return;
      let updated = { ...layer, expanded: !layer.expanded };
      const replace = () =>
        set({
          lakehouseCatalog: get().lakehouseCatalog.map((item) =>
            item.name === layerName ? updated : item
          ),
        });
      replace();
      const activeToken = get().googleAuth.token;
      if (
        get().lakehouseRelease?.kind === "release" ||
        !updated.expanded ||
        updated.loaded ||
        !activeToken
      )
        return;
      busy = true;
      set({ isLakehouseLoading: true, lakehouseStatusMessage: `Loading '${layerName}'...` });
      try {
        updated = await loadLayer(updated, activeToken);
        if (get().googleAuth.token !== activeToken) return;
        replace();
        set({
          lakehouseStatusMessage: `Found ${updated.children.length} legacy/unversioned dataset(s) in '${layerName}'.`,
        });
      } catch (error) {
        const authFailure = handleDriveAuthFailure(set, get, activeToken, error);
        if (get().googleAuth.token === activeToken) {
          set({
            lakehouseStatusMessage: authFailure
              ? "Google Drive authorization expired or was revoked. Sign in again."
              : `Error loading '${layerName}': ${messageOf(error)}`,
          });
        }
      } finally {
        busy = false;
        set({ isLakehouseLoading: false });
      }
    },

    toggleLakehouseTable: async (layerName, tableName) => {
      if (busy) return;
      const table = get()
        .lakehouseCatalog.find((item) => item.name === layerName)
        ?.children.find((item) => item.name === tableName);
      if (!table) return;
      let updated = { ...table, expanded: !table.expanded };
      const replace = () =>
        set({
          lakehouseCatalog: get().lakehouseCatalog.map((layer) =>
            layer.name === layerName
              ? {
                  ...layer,
                  children: layer.children.map((item) =>
                    item.name === tableName ? updated : item
                  ),
                }
              : layer
          ),
        });
      replace();
      const activeToken = get().googleAuth.token;
      if (
        get().lakehouseRelease?.kind === "release" ||
        !updated.expanded ||
        updated.loaded ||
        !activeToken
      )
        return;
      busy = true;
      set({
        isLakehouseLoading: true,
        lakehouseStatusMessage: `Loading files for '${tableName}'...`,
      });
      try {
        updated = await loadTableFiles(updated, activeToken);
        if (get().googleAuth.token !== activeToken) return;
        replace();
        set({
          lakehouseStatusMessage: `Found ${updated.children.length} legacy/unversioned file(s) in '${tableName}'.`,
        });
      } catch (error) {
        const authFailure = handleDriveAuthFailure(set, get, activeToken, error);
        if (get().googleAuth.token === activeToken) {
          set({
            lakehouseStatusMessage: authFailure
              ? "Google Drive authorization expired or was revoked. Sign in again."
              : `Error loading '${tableName}': ${messageOf(error)}`,
          });
        }
      } finally {
        busy = false;
        set({ isLakehouseLoading: false });
      }
    },
    selectLakehouseDataset: (layerName, tableName) => select(layerName, tableName),
    selectLakehouseFile: (layerName, tableName, fileId) => select(layerName, tableName, fileId),
    prepareLakehouseQuery,
    preparePublishedTablesForQuery,
  };
};
