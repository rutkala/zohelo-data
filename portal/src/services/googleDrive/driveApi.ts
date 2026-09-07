/**
 * Google Drive REST API Client for Lakehouse Catalog & File Storage
 */
import { DRIVE_ROOT, isStoredTokenExpired, LAKEHOUSE_LAYERS } from "./auth";
import type { LakehouseLayer } from "./types";

export class GoogleDriveAuthError extends Error {
  readonly status = 401;

  constructor(message: string) {
    super(message);
    this.name = "GoogleDriveAuthError";
  }
}

export const isGoogleDriveAuthError = (error: unknown): error is GoogleDriveAuthError =>
  error instanceof GoogleDriveAuthError;

export const driveRequest = async (url: string, token: string): Promise<Response> => {
  if (!token) {
    throw new Error("No Google Drive OAuth token available");
  }
  if (isStoredTokenExpired(token)) {
    throw new GoogleDriveAuthError(
      "Google Drive authorization expired or was revoked. Sign in again."
    );
  }

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    const errorBody = await response.text().catch(() => "");
    if (response.status === 401) {
      throw new GoogleDriveAuthError(
        "Google Drive authorization expired or was revoked. Sign in again."
      );
    }
    throw new Error(
      `Google Drive API error (${response.status}): ${errorBody || response.statusText}`
    );
  }

  return response;
};

export interface DriveFileMetadata {
  id: string;
  name: string;
  mimeType?: string;
  size?: number;
}

const listFilesForQuery = async (query: string, token: string): Promise<DriveFileMetadata[]> => {
  const files: DriveFileMetadata[] = [];
  let pageToken: string | null = null;
  do {
    let url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(
      query
    )}&fields=files(id,name,mimeType,size),nextPageToken&pageSize=200`;
    if (pageToken) url += `&pageToken=${encodeURIComponent(pageToken)}`;
    const response = await driveRequest(url, token);
    const payload = await response.json();
    for (const item of payload.files || []) {
      files.push({
        id: item.id,
        name: item.name,
        mimeType: item.mimeType,
        size: item.size === undefined ? undefined : Number(item.size),
      });
    }
    pageToken = payload.nextPageToken || null;
  } while (pageToken);
  return files;
};

export const findFoldersByName = async (
  name: string,
  parentId = "root",
  token: string
): Promise<Array<{ id: string; name: string }>> => {
  if (name.includes("'"))
    throw new Error("Folder names containing single quotes are not supported.");
  const query = [
    `mimeType='application/vnd.google-apps.folder'`,
    `name='${name}'`,
    `'${parentId}' in parents`,
    "trashed=false",
  ].join(" and ");
  return (await listFilesForQuery(query, token)).map(({ id, name: folderName }) => ({
    id,
    name: folderName,
  }));
};

export const findNamedFilesInFolder = async (
  name: string,
  parentId: string,
  token: string
): Promise<DriveFileMetadata[]> => {
  if (name.includes("'")) throw new Error("File names containing single quotes are not supported.");
  const query = [`name='${name}'`, `'${parentId}' in parents`, "trashed=false"].join(" and ");
  return listFilesForQuery(query, token);
};

/** Metadata by immutable ID; no folder listing is used for release manifests. */
export const findNamedFilesInFolderById = async (
  fileId: string,
  token: string
): Promise<DriveFileMetadata | null> => {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(
    fileId
  )}?fields=id,name,mimeType,size`;
  try {
    const response = await driveRequest(url, token);
    const item = await response.json();
    if (!item?.id || item.mimeType === "application/vnd.google-apps.folder") return null;
    return {
      id: item.id,
      name: item.name,
      mimeType: item.mimeType,
      size: item.size === undefined ? undefined : Number(item.size),
    };
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("Google Drive API error (404)"))
      return null;
    throw error;
  }
};

export const findFolderIdByName = async (
  name: string,
  parentId = "root",
  token: string
): Promise<string | null> => {
  if (name.includes("'")) {
    throw new Error("Folder names containing single quotes are not supported.");
  }

  const folders = await findFoldersByName(name, parentId, token);
  return folders[0]?.id || null;
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
  token: string,
  maxBytes?: number
): Promise<Uint8Array> => {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?alt=media`;
  const response = await driveRequest(url, token);
  const length = response.headers.get("content-length");
  if (maxBytes !== undefined && length && Number(length) > maxBytes) {
    throw new Error(`Drive download exceeds its declared ${maxBytes} byte limit.`);
  }
  if (!response.body) {
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (maxBytes !== undefined && bytes.byteLength > maxBytes) {
      throw new Error(`Drive download exceeds its declared ${maxBytes} byte limit.`);
    }
    return bytes;
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      total += next.value.byteLength;
      if (maxBytes !== undefined && total > maxBytes) {
        await reader.cancel();
        throw new Error(`Drive download exceeds its declared ${maxBytes} byte limit.`);
      }
      chunks.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
};

export const createDefaultLakehouseTree = (): LakehouseLayer[] => {
  return LAKEHOUSE_LAYERS.map((layerName) => ({
    type: "layer",
    name: layerName,
    id: null,
    expanded: layerName === "02_bronze",
    loaded: false,
    children: [],
  }));
};
