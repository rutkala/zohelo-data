/**
 * Google Drive REST API Client for Lakehouse Catalog & File Storage
 */
import { DRIVE_ROOT, LAKEHOUSE_LAYERS } from "./auth";
import type { LakehouseLayer } from "./types";

export const driveRequest = async (url: string, token: string): Promise<Response> => {
  if (!token) {
    throw new Error("No Google Drive OAuth token available");
  }

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const errorBody = await response.text().catch(() => "");
    throw new Error(`Google Drive API error (${response.status}): ${errorBody || response.statusText}`);
  }

  return response;
};

export const findFolderIdByName = async (
  name: string,
  parentId = "root",
  token: string
): Promise<string | null> => {
  if (name.includes("'")) {
    throw new Error("Folder names containing single quotes are not supported.");
  }

  const query = [
    `mimeType='application/vnd.google-apps.folder'`,
    `name='${name}'`,
    `'${parentId}' in parents`,
    "trashed=false",
  ].join(" and ");

  const url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(query)}&fields=files(id,name)&pageSize=1`;
  const response = await driveRequest(url, token);
  const payload = await response.json();
  return payload.files?.[0]?.id || null;
};

export const resolveLayerFolderId = async (
  layerName: string,
  token: string
): Promise<string | null> => {
  const rootId = await findFolderIdByName(DRIVE_ROOT, "root", token);
  if (!rootId) {
    throw new Error(`Master Lakehouse folder '${DRIVE_ROOT}' not found in Google Drive root.`);
  }
  return findFolderIdByName(layerName, rootId, token);
};

export const listSubfolders = async (
  folderId: string,
  token: string
): Promise<Array<{ id: string; name: string }>> => {
  const folders: Array<{ id: string; name: string }> = [];
  let pageToken: string | null = null;

  do {
    const query = `'${folderId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`;
    let url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(
      query
    )}&fields=files(id,name),nextPageToken&pageSize=200`;
    if (pageToken) {
      url += `&pageToken=${encodeURIComponent(pageToken)}`;
    }
    const response = await driveRequest(url, token);
    const payload = await response.json();
    for (const item of payload.files || []) {
      folders.push({ id: item.id, name: item.name });
    }
    pageToken = payload.nextPageToken || null;
  } while (pageToken);

  return folders;
};

export const listDataFilesInFolder = async (
  folderId: string,
  token: string
): Promise<Array<{ id: string; name: string; mimeType?: string; size?: number }>> => {
  const files: Array<{ id: string; name: string; mimeType?: string; size?: number }> = [];
  let pageToken: string | null = null;

  do {
    const query = `'${folderId}' in parents and trashed=false`;
    let url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(
      query
    )}&fields=files(id,name,mimeType,size),nextPageToken&pageSize=200`;
    if (pageToken) {
      url += `&pageToken=${encodeURIComponent(pageToken)}`;
    }
    const response = await driveRequest(url, token);
    const payload = await response.json();
    for (const item of payload.files || []) {
      if (item.mimeType !== "application/vnd.google-apps.folder") {
        const lc = item.name.toLowerCase();
        if (
          lc.endsWith(".parquet") ||
          lc.endsWith(".json") ||
          lc.endsWith(".csv") ||
          lc.endsWith(".duckdb")
        ) {
          files.push({
            id: item.id,
            name: item.name,
            mimeType: item.mimeType,
            size: item.size ? Number(item.size) : undefined,
          });
        }
      }
    }
    pageToken = payload.nextPageToken || null;
  } while (pageToken);

  return files;
};

export const fetchDriveFileBuffer = async (
  fileId: string,
  token: string
): Promise<Uint8Array> => {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?alt=media`;
  const response = await driveRequest(url, token);
  const arrayBuffer = await response.arrayBuffer();
  return new Uint8Array(arrayBuffer);
};

export const createDefaultLakehouseTree = (): LakehouseLayer[] => {
  return LAKEHOUSE_LAYERS.map((layerName) => ({
    type: "layer",
    name: layerName,
    id: null,
    expanded: layerName === "02_bronze",
    loaded: false,
    children:
      layerName === "02_bronze"
        ? [
            {
              type: "table",
              name: "nbp_exchange_rates_table_a",
              id: null,
              layer: "02_bronze",
              expanded: true,
              loaded: true,
              children: [
                {
                  id: "demo_file",
                  name: "data.parquet",
                  layer: "02_bronze",
                  tableName: "nbp_exchange_rates_table_a",
                },
              ],
            },
          ]
        : [],
  }));
};
