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
  requestGoogleAccessToken,
  resolveLayerFolderId,
  resolveReleaseCatalog,
  isGoogleDriveAuthError,
  setStoredToken,
  type LakehouseLayer,
  type LakehouseTable,
  type ReleaseCatalogResolution,
} from "@/services/googleDrive";
import type { DuckStoreState, GoogleDriveSlice } from "../types";

const messageOf = (error: unknown) => (error instanceof Error ? error.message : "Unknown error");
const releaseMessage = (release: Extract<ReleaseCatalogResolution, { kind: "release" }>) =>
  `Release ${release.manifest.release_id} · ${release.manifest.release_scope} · ${release.manifest.status}.`;
const releaseFingerprint = (release: ReleaseCatalogResolution | null) =>
  release?.kind === "release" ? release.fingerprint : release?.kind === "legacy" ? "legacy" : null;

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

function treeFromRelease(
  release: Extract<ReleaseCatalogResolution, { kind: "release" }>
): LakehouseLayer[] {
  return createDefaultLakehouseTree().map((layer) => {
    if (layer.name !== "03_silver") return { ...layer, expanded: false, loaded: true };
    return {
      ...layer,
      id: null,
      expanded: true,
      loaded: true,
      children: release.manifest.datasets.map((dataset) => ({
        type: "table" as const,
        name: dataset.dataset_id,
        id: null,
        layer: "03_silver",
        expanded: false,
        loaded: true,
        children: dataset.files.map((file) => ({ ...file, tableName: dataset.dataset_id })),
      })),
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
  const loadedCatalogFingerprints = new WeakMap<object, string>();
  const downloadBudgets = new WeakMap<object, ReturnType<typeof createDriveDownloadBudget>>();
  const noEngineBudget = createDriveDownloadBudget();
  const budgetForEngine = (db: object) => {
    let budget = downloadBudgets.get(db);
    if (!budget) {
      budget = createDriveDownloadBudget();
      downloadBudgets.set(db, budget);
    }
    return budget;
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
    const table = get()
      .lakehouseCatalog.find((layer) => layer.name === layerName)
      ?.children.find((item) => item.name === tableName);
    const file =
      fileId === undefined ? undefined : table?.children.find((item) => item.id === fileId);
    const label = file?.name ?? tableName;
    const current = () =>
      get().currentSession === session &&
      get().googleAuth.token === token &&
      get().lakehouseRelease === source;
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
      const fingerprint = releaseFingerprint(source);
      if (fingerprint) loadedCatalogFingerprints.set(local.db, fingerprint);
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
        activeLakehouseDataset: null,
        activeLakehouseLayer: null,
        isLakehouseLoading: false,
        lakehouseStatusMessage: "Disconnected from Google Drive.",
      });
    },

    refreshLakehouseCatalog: async () => {
      if (busy) return;
      const activeToken = get().googleAuth.token;
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
        const local = asLocalDuckSession(get().currentSession)?.local;
        const budget = local ? budgetForEngine(local.db) : noEngineBudget;
        const release = await resolveReleaseCatalog(activeToken, budget);
        if (get().googleAuth.token !== activeToken) return;
        const candidateFingerprint = releaseFingerprint(release);
        const loadedCatalogFingerprint = local
          ? loadedCatalogFingerprints.get(local.db)
          : undefined;
        if (loadedCatalogFingerprint && loadedCatalogFingerprint !== candidateFingerprint) {
          throw new Error(
            "A different release is available, but this DuckDB session still has loaded views. Start a fresh DuckDB session before switching releases."
          );
        }
        if (release.kind === "release") {
          set({
            lakehouseCatalog: treeFromRelease(release),
            lakehouseRelease: release,
            lakehouseStatusMessage: `${releaseMessage(release)} Select a dataset to query.`,
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
          lakehouseCatalog: tree,
          lakehouseRelease: release,
          lakehouseStatusMessage: "Legacy/unversioned catalog loaded. Select a dataset to query.",
        });
      } catch (error) {
        const authFailure = handleDriveAuthFailure(set, get, activeToken, error);
        if (get().googleAuth.token === activeToken) {
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
  };
};
