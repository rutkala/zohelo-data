import type { StateCreator } from "zustand";
import { toast } from "sonner";
import { asLocalDuckSession } from "@/services/engine";
import {
  clearStoredToken,
  createDefaultLakehouseTree,
  getStoredToken,
  listDataFilesInFolder,
  listSubfolders,
  loadFileIntoDuckDB,
  loadTableIntoDuckDB,
  requestGoogleAccessToken,
  resolveLayerFolderId,
  setStoredToken,
  type LakehouseLayer,
  type LakehouseTable,
} from "@/services/googleDrive";
import type { DuckStoreState, GoogleDriveSlice } from "../types";

const messageOf = (error: unknown) => (error instanceof Error ? error.message : "Unknown error");

async function loadTableFiles(table: LakehouseTable, token: string): Promise<LakehouseTable> {
  if (!table.id) throw new Error(`Dataset '${table.name}' has no Drive folder.`);
  const files = await listDataFilesInFolder(table.id, token);
  return {
    ...table,
    loaded: true,
    children: files.map((file) => ({
      ...file,
      tableName: table.name,
      layer: table.layer,
    })),
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

export const createGoogleDriveSlice: StateCreator<
  DuckStoreState,
  [["zustand/devtools", never]],
  [],
  GoogleDriveSlice
> = (set, get) => {
  // Serialize catalog/load operations, including rapid clicks before React renders.
  let busy = false;
  const select = async (
    layerName: string,
    tableName: string,
    fileName?: string
  ): Promise<string | null> => {
    if (busy) return null;
    const session = get().currentSession;
    const local = asLocalDuckSession(session)?.local;
    const token = get().googleAuth.token;
    const label = fileName ?? tableName;
    const current = () => get().currentSession === session && get().googleAuth.token === token;
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
      const table = get()
        .lakehouseCatalog.find((layer) => layer.name === layerName)
        ?.children.find((item) => item.name === tableName);
      if (!table) throw new Error(`Dataset '${tableName}' was not found in the catalog.`);
      let queryTarget: string;
      if (fileName !== undefined) {
        const file = table.children.find((item) => item.name === fileName);
        if (!file) throw new Error(`File '${fileName}' was not found in the catalog.`);
        ({ queryTarget } = await loadFileIntoDuckDB(
          local.db,
          local.connection,
          tableName,
          file,
          token ?? ""
        ));
      } else {
        ({ queryTarget } = await loadTableIntoDuckDB(
          local.db,
          local.connection,
          tableName,
          table.id,
          table.children,
          token ?? "",
          layerName
        ));
      }
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
      if (current()) {
        const message = `Error loading '${label}': ${messageOf(error)}`;
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
        const token = await requestGoogleAccessToken({ promptConsent });
        set({
          googleAuth: { token, isAuthenticated: true, authSource: "google_identity", error: null },
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

    setManualGoogleToken: async (token) => {
      const trimmed = token.trim();
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
        activeLakehouseDataset: null,
        activeLakehouseLayer: null,
        isLakehouseLoading: false,
        lakehouseStatusMessage: "Disconnected from Google Drive.",
      });
    },

    refreshLakehouseCatalog: async () => {
      if (busy) return;
      const token = get().googleAuth.token;
      if (!token) {
        set({ lakehouseStatusMessage: "Sign in to browse Google Drive datasets." });
        return;
      }
      busy = true;
      set({ isLakehouseLoading: true, lakehouseStatusMessage: "Loading Google Drive catalog..." });
      try {
        const tree: LakehouseLayer[] = [];
        for (const layer of get().lakehouseCatalog) {
          tree.push(
            layer.expanded
              ? await loadLayer(layer, token)
              : { ...layer, id: null, loaded: false, children: [] }
          );
        }
        if (get().googleAuth.token !== token) return;
        set({
          lakehouseCatalog: tree,
          lakehouseStatusMessage: "Catalog loaded. Select a dataset to query.",
        });
      } catch (error) {
        if (get().googleAuth.token === token) {
          const message = `Catalog refresh error: ${messageOf(error)}`;
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
      const token = get().googleAuth.token;
      if (!updated.expanded || updated.loaded || !token) return;
      busy = true;
      set({ isLakehouseLoading: true, lakehouseStatusMessage: `Loading '${layerName}'...` });
      try {
        updated = await loadLayer(updated, token);
        if (get().googleAuth.token !== token) return;
        replace();
        set({
          lakehouseStatusMessage: `Loaded ${updated.children.length} dataset(s) in '${layerName}'.`,
        });
      } catch (error) {
        if (get().googleAuth.token === token)
          set({ lakehouseStatusMessage: `Error loading '${layerName}': ${messageOf(error)}` });
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
      const token = get().googleAuth.token;
      if (!updated.expanded || updated.loaded || !token) return;
      busy = true;
      set({
        isLakehouseLoading: true,
        lakehouseStatusMessage: `Loading files for '${tableName}'...`,
      });
      try {
        updated = await loadTableFiles(updated, token);
        if (get().googleAuth.token !== token) return;
        replace();
        set({
          lakehouseStatusMessage: `Found ${updated.children.length} file(s) in '${tableName}'.`,
        });
      } catch (error) {
        if (get().googleAuth.token === token)
          set({ lakehouseStatusMessage: `Error loading '${tableName}': ${messageOf(error)}` });
      } finally {
        busy = false;
        set({ isLakehouseLoading: false });
      }
    },
    selectLakehouseDataset: (layerName, tableName) => select(layerName, tableName),
    selectLakehouseFile: (layerName, tableName, fileName) => select(layerName, tableName, fileName),
  };
};
