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
  type LakehouseFile,
  type LakehouseLayer,
  type LakehouseTable,
} from "@/services/googleDrive";
import type { DuckStoreState, GoogleDriveSlice } from "../types";

export const createGoogleDriveSlice: StateCreator<
  DuckStoreState,
  [["zustand/devtools", never]],
  [],
  GoogleDriveSlice
> = (set, get) => ({
  googleAuth: {
    token: getStoredToken(),
    isAuthenticated: !!getStoredToken(),
    authSource: getStoredToken() ? "google_identity" : "none",
    error: null,
  },
  lakehouseCatalog: createDefaultLakehouseTree(),
  isLakehouseLoading: false,
  lakehouseStatusMessage: "Ready. Sign in to browse Google Drive lakehouse datasets.",
  activeLakehouseDataset: "nbp_exchange_rates_table_a",
  activeLakehouseLayer: "02_bronze",

  signInWithGoogle: async (promptConsent = false) => {
    try {
      set({
        isLakehouseLoading: true,
        lakehouseStatusMessage: "Requesting Google Sign-In authorization...",
      });

      const token = await requestGoogleAccessToken({ promptConsent });
      set({
        googleAuth: {
          token,
          isAuthenticated: true,
          authSource: "google_identity",
          error: null,
        },
        lakehouseStatusMessage: "Google Drive connected. Synchronizing catalog...",
      });

      toast.success("Signed in with Google Drive");
      await get().refreshLakehouseCatalog();
      return true;
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Google Sign-In failed";
      set({
        googleAuth: {
          token: null,
          isAuthenticated: false,
          authSource: "none",
          error: msg,
        },
        lakehouseStatusMessage: `Authentication error: ${msg}`,
      });
      toast.error(msg);
      return false;
    } finally {
      set({ isLakehouseLoading: false });
    }
  },

  setManualGoogleToken: async (token: string) => {
    const trimmed = token.trim();
    if (!trimmed) {
      toast.error("Please enter a valid Google OAuth token");
      return false;
    }

    setStoredToken(trimmed);
    set({
      googleAuth: {
        token: trimmed,
        isAuthenticated: true,
        authSource: "manual",
        error: null,
      },
      lakehouseStatusMessage: "Manual token applied. Refreshing catalog...",
    });

    toast.success("Manual Google Token applied");
    try {
      await get().refreshLakehouseCatalog();
      return true;
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Catalog sync failed";
      toast.error(msg);
      return false;
    }
  },

  disconnectGoogleDrive: () => {
    clearStoredToken();
    set({
      googleAuth: {
        token: null,
        isAuthenticated: false,
        authSource: "none",
        error: null,
      },
      lakehouseCatalog: createDefaultLakehouseTree(),
      lakehouseStatusMessage: "Disconnected from Google Drive.",
    });
    toast.info("Disconnected from Google Drive");
  },

  refreshLakehouseCatalog: async () => {
    const token = get().googleAuth.token;
    if (!token) {
      set({
        lakehouseStatusMessage: "Sign in with Google to browse private lakehouse datasets.",
      });
      return;
    }

    set({
      isLakehouseLoading: true,
      lakehouseStatusMessage: "Scanning Google Drive lakehouse layers...",
    });

    try {
      const currentTree = [...get().lakehouseCatalog];
      const updatedTree: LakehouseLayer[] = [];

      for (const layerNode of currentTree) {
        let layerId = layerNode.id;
        try {
          layerId = await resolveLayerFolderId(layerNode.name, token);
        } catch {
          layerId = null;
        }

        const newLayer: LakehouseLayer = {
          ...layerNode,
          id: layerId,
          children: [...layerNode.children],
        };

        // If layer folder found and is expanded or is 02_bronze, load datasets
        if (layerId && (layerNode.expanded || layerNode.name === "02_bronze")) {
          try {
            const subfolders = await listSubfolders(layerId, token);
            subfolders.sort((a, b) => a.name.localeCompare(b.name));

            newLayer.children = await Promise.all(
              subfolders.map(async (folder) => {
                const existingTable = layerNode.children.find((t) => t.name === folder.name);
                let files: LakehouseFile[] = existingTable?.children || [];

                if (existingTable?.expanded || folder.name === "nbp_exchange_rates_table_a") {
                  const driveFiles = await listDataFilesInFolder(folder.id, token);
                  files = driveFiles.map((f) => ({
                    id: f.id,
                    name: f.name,
                    mimeType: f.mimeType,
                    size: f.size,
                    tableName: folder.name,
                    layer: layerNode.name,
                  }));
                }

                const table: LakehouseTable = {
                  type: "table",
                  name: folder.name,
                  id: folder.id,
                  layer: layerNode.name,
                  expanded: existingTable?.expanded ?? (folder.name === "nbp_exchange_rates_table_a"),
                  loaded: true,
                  children: files,
                };
                return table;
              })
            );
            newLayer.loaded = true;
          } catch (err) {
            console.error(`Error loading datasets for layer ${layerNode.name}:`, err);
          }
        }

        updatedTree.push(newLayer);
      }

      set({
        lakehouseCatalog: updatedTree,
        lakehouseStatusMessage: "Lakehouse catalog synchronized with Google Drive.",
      });

      // Auto-load bronze default dataset if available
      const bronzeLayer = updatedTree.find((l) => l.name === "02_bronze");
      const defaultTable =
        bronzeLayer?.children.find((t) => t.name === "nbp_exchange_rates_table_a") ||
        bronzeLayer?.children[0];

      if (defaultTable) {
        await get().selectLakehouseDataset("02_bronze", defaultTable.name);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to refresh catalog";
      set({ lakehouseStatusMessage: `Catalog refresh error: ${msg}` });
      console.error("[GoogleDrive] Catalog refresh error:", err);
    } finally {
      set({ isLakehouseLoading: false });
    }
  },

  toggleLakehouseLayer: async (layerName: string) => {
    const catalog = [...get().lakehouseCatalog];
    const layer = catalog.find((l) => l.name === layerName);
    if (!layer) return;

    layer.expanded = !layer.expanded;
    set({ lakehouseCatalog: catalog });

    const token = get().googleAuth.token;
    if (layer.expanded && !layer.loaded && token) {
      try {
        set({ isLakehouseLoading: true, lakehouseStatusMessage: `Loading ${layerName}...` });
        const layerId = layer.id || (await resolveLayerFolderId(layer.name, token));
        layer.id = layerId;

        if (layerId) {
          const subfolders = await listSubfolders(layerId, token);
          subfolders.sort((a, b) => a.name.localeCompare(b.name));
          layer.children = subfolders.map((f) => ({
            type: "table",
            name: f.name,
            id: f.id,
            layer: layer.name,
            expanded: false,
            loaded: false,
            children: [],
          }));
          layer.loaded = true;
          set({
            lakehouseCatalog: [...catalog],
            lakehouseStatusMessage: `Loaded ${subfolders.length} dataset(s) in ${layerName}.`,
          });
        }
      } catch (err) {
        console.error(`Failed to expand layer ${layerName}:`, err);
      } finally {
        set({ isLakehouseLoading: false });
      }
    }
  },

  toggleLakehouseTable: async (layerName: string, tableName: string) => {
    const catalog = [...get().lakehouseCatalog];
    const layer = catalog.find((l) => l.name === layerName);
    const table = layer?.children.find((t) => t.name === tableName);
    if (!table) return;

    table.expanded = !table.expanded;
    set({ lakehouseCatalog: [...catalog] });

    const token = get().googleAuth.token;
    if (table.expanded && (!table.loaded || table.children.length === 0) && table.id && token) {
      try {
        const driveFiles = await listDataFilesInFolder(table.id, token);
        table.children = driveFiles.map((f) => ({
          id: f.id,
          name: f.name,
          mimeType: f.mimeType,
          size: f.size,
          tableName: table.name,
          layer: layerName,
        }));
        table.loaded = true;
        set({ lakehouseCatalog: [...catalog] });
      } catch (err) {
        console.error(`Failed to expand table ${tableName}:`, err);
      }
    }
  },

  selectLakehouseDataset: async (layerName: string, tableName: string) => {
    const local = asLocalDuckSession(get().currentSession)?.local;
    if (!local) {
      toast.error("DuckDB engine is still initializing. Please wait...");
      return;
    }

    const token = get().googleAuth.token;
    const catalog = get().lakehouseCatalog;
    const layer = catalog.find((l) => l.name === layerName);
    const table = layer?.children.find((t) => t.name === tableName);

    set({
      isLakehouseLoading: true,
      activeLakehouseDataset: tableName,
      activeLakehouseLayer: layerName,
      lakehouseStatusMessage: `Fetching '${tableName}' Parquet from Google Drive and registering in DuckDB-WASM...`,
    });

    try {
      const { loadedFiles } = await loadTableIntoDuckDB(
        local.db,
        local.connection,
        tableName,
        table?.id || null,
        table?.children || [],
        token || ""
      );

      // Refresh DuckDB schema introspection so DataExplorer knows about active_layer and the new table
      await get().fetchDatabasesAndTablesInfo();

      set({
        lakehouseStatusMessage:
          loadedFiles.length > 0
            ? `Registered '${tableName}' (${loadedFiles.length} file(s)) as active_layer.`
            : `Using demo data for '${tableName}'.`,
      });

      toast.success(`Loaded dataset "${tableName}" into DuckDB-WASM`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to load dataset";
      set({ lakehouseStatusMessage: `Error loading '${tableName}': ${msg}` });
      toast.error(`Error loading dataset "${tableName}": ${msg}`);
      console.error("[GoogleDrive] Load dataset error:", err);
    } finally {
      set({ isLakehouseLoading: false });
    }
  },

  selectLakehouseFile: async (layerName: string, tableName: string, fileName: string) => {
    const local = asLocalDuckSession(get().currentSession)?.local;
    if (!local) {
      toast.error("DuckDB engine is still initializing. Please wait...");
      return;
    }

    const token = get().googleAuth.token;
    const catalog = get().lakehouseCatalog;
    const layer = catalog.find((l) => l.name === layerName);
    const table = layer?.children.find((t) => t.name === tableName);
    const file = table?.children.find((f) => f.name === fileName);

    if (!file) return;

    set({
      isLakehouseLoading: true,
      activeLakehouseDataset: tableName,
      activeLakehouseLayer: layerName,
      lakehouseStatusMessage: `Fetching '${fileName}' from Google Drive into DuckDB-WASM...`,
    });

    try {
      await loadFileIntoDuckDB(local.db, local.connection, tableName, file, token || "");
      await get().fetchDatabasesAndTablesInfo();

      set({
        lakehouseStatusMessage: `Registered file '${fileName}' as active_layer.`,
      });

      toast.success(`Loaded file "${fileName}"`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to load file";
      set({ lakehouseStatusMessage: `Error loading file '${fileName}': ${msg}` });
      toast.error(`Error loading file "${fileName}": ${msg}`);
    } finally {
      set({ isLakehouseLoading: false });
    }
  },
});
