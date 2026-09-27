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

/** Preserve the existing error text while allowing scanner-only bounded retries. */
export class GoogleDriveApiError extends Error {
  constructor(message: string, readonly status: number, readonly reason: string | null) {
    super(message);
    this.name = "GoogleDriveApiError";
  }
}

export const isGoogleDriveAuthError = (error: unknown): error is GoogleDriveAuthError =>
  error instanceof GoogleDriveAuthError;

export const driveRequest = async (url: string, token: string, signal?: AbortSignal): Promise<Response> => {
  if (!token) {
    throw new Error("No Google Drive OAuth token available");
  }
  if (isStoredTokenExpired(token)) {
    throw new GoogleDriveAuthError(
      "Google Drive authorization expired or was revoked. Sign in again."
    );
  }

  const response = await fetch(url, {
    signal,
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
    let reason: string | null = null;
    try {
      const parsed = JSON.parse(errorBody);
      const value = parsed?.error?.errors?.[0]?.reason ?? parsed?.error?.status;
      if (typeof value === "string") reason = value;
    } catch { /* Non-JSON errors retain the existing message. */ }
    throw new GoogleDriveApiError(
      `Google Drive API error (${response.status}): ${errorBody || response.statusText}`,
      response.status, reason
    );
  }

  return response;
};

export interface DriveFileMetadata {
  id: string;
  name: string;
  mimeType?: string;
  size?: number;
  modifiedTime?: string;
  createdTime?: string;
  version?: string;
  md5Checksum?: string;
  sha256Checksum?: string;
  parents?: string[];
  trashed?: boolean;
  webViewLink?: string;
  webContentLink?: string;
  capabilities?: { canDownload?: boolean };
  shortcutDetails?: { targetId?: string; targetMimeType?: string };
}

const NATIVE_FIELDS =
  "id,name,mimeType,size,createdTime,modifiedTime,version,md5Checksum,sha256Checksum,parents,trashed,webViewLink,webContentLink,capabilities(canDownload),shortcutDetails(targetId,targetMimeType)";

export const NATIVE_METADATA_PARENT_BATCH = 100;
export const NATIVE_METADATA_PLAN_URL_LIMIT = 6000;
export const NATIVE_METADATA_URL_LIMIT = 7800;
export class NativeMetadataUrlTooLongError extends Error {}
const NATIVE_METADATA_FOLDER_FIELDS = "id,name,mimeType,parents,trashed,version,modifiedTime";
const NATIVE_METADATA_FILE_FIELDS =
  "id,name,mimeType,size,createdTime,modifiedTime,parents,trashed,webViewLink,sha256Checksum";

const nativeMetadata = (
  item: DriveFileMetadata & { size?: string | number }
): DriveFileMetadata => {
  if (!item || typeof item.id !== "string" || typeof item.name !== "string") {
    throw new Error("Drive returned invalid native file metadata.");
  }
  const size = item.size === undefined ? undefined : Number(item.size);
  if (size !== undefined && (!Number.isSafeInteger(size) || size < 0)) {
    throw new Error("Drive returned an invalid native file size.");
  }
  return { ...item, size };
};

/** One explicitly paginated folder page. Never silently turn an API failure into an empty folder. */
export const listNativeChildrenPage = async (
  folderId: string,
  token: string,
  pageToken?: string
): Promise<{ files: DriveFileMetadata[]; nextPageToken: string | null }> => {
  const query = `'${folderId.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}' in parents and trashed=false`;
  const url = new URL("https://www.googleapis.com/drive/v3/files");
  url.searchParams.set("q", query);
  url.searchParams.set("fields", `nextPageToken,files(${NATIVE_FIELDS})`);
  url.searchParams.set("pageSize", "1000");
  if (pageToken) url.searchParams.set("pageToken", pageToken);
  const payload = await (await driveRequest(url.toString(), token)).json();
  if (
    !Array.isArray(payload?.files) ||
    (payload.nextPageToken !== undefined && typeof payload.nextPageToken !== "string")
  ) {
    throw new Error("Drive returned an incomplete native folder page.");
  }
  return { files: payload.files.map(nativeMetadata), nextPageToken: payload.nextPageToken || null };
};

/** Scanner-only batched parent search. Keep interactive folder navigation unchanged. */
export const buildNativeMetadataBatchUrl = (
  parentIds: readonly string[], kind: "folders" | "files", pageToken?: string
): string => {
  if (!parentIds.length || parentIds.length > NATIVE_METADATA_PARENT_BATCH)
    throw new Error("Invalid Landing metadata parent batch.");
  const parents = parentIds.map((id) => `'${id.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}' in parents`).join(" or ");
  const mime = kind === "folders" ? "=" : "!=";
  const q = `(${parents}) and mimeType ${mime} 'application/vnd.google-apps.folder' and trashed=false`;
  const url = new URL("https://www.googleapis.com/drive/v3/files");
  url.searchParams.set("q", q);
  const fields = kind === "folders" ? NATIVE_METADATA_FOLDER_FIELDS : NATIVE_METADATA_FILE_FIELDS;
  url.searchParams.set("fields", `nextPageToken,incompleteSearch,files(${fields})`);
  url.searchParams.set("pageSize", "1000");
  if (pageToken) url.searchParams.set("pageToken", pageToken);
  const encoded = url.toString();
  if (encoded.length > NATIVE_METADATA_URL_LIMIT)
    throw new NativeMetadataUrlTooLongError(
      "Drive metadata request URL exceeds the 7800-character limit. No incomplete table was published."
    );
  return encoded;
};

/** Retry only transient scanner requests; the same deadline covers response headers and body. */
type NativeMetadataPagePayload = {
  files?: unknown;
  nextPageToken?: unknown;
  incompleteSearch?: unknown;
} | null;
const scannerPage = async (
  url: string, token: string, signal?: AbortSignal
): Promise<NativeMetadataPagePayload> => {
  const controller = new AbortController();
  const cancel = () => controller.abort();
  if (signal?.aborted) cancel();
  else signal?.addEventListener("abort", cancel, { once: true });
  const deadline = setTimeout(cancel, 90000);
  const active = controller.signal;
  const abortError = () => new Error(signal?.aborted
    ? "Landing metadata request was cancelled."
    : "Landing metadata page timed out after 90 seconds.");
  try {
    for (let attempt = 0; attempt < 4; attempt++) {
      if (active.aborted) throw abortError();
      try { return await (await driveRequest(url, token, active)).json() as NativeMetadataPagePayload; }
      catch (error) {
        if (active.aborted) throw abortError();
        const transient = error instanceof GoogleDriveApiError && (
          error.status === 429 || error.status >= 500 ||
          (error.status === 403 && ["rateLimitExceeded", "userRateLimitExceeded",
            "RATE_LIMIT_EXCEEDED"].includes(error.reason ?? ""))
        );
        if (!transient || attempt === 3) throw error;
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(() => { active.removeEventListener("abort", abort); resolve(); },
            250 * 2 ** attempt + Math.floor(Math.random() * 250));
          const abort = () => { clearTimeout(timer); reject(abortError()); };
          active.addEventListener("abort", abort, { once: true });
          if (active.aborted) abort();
        });
      }
    }
    throw new Error("Landing metadata request exceeded the retry limit.");
  } finally {
    clearTimeout(deadline);
    signal?.removeEventListener("abort", cancel);
  }
};

export const listNativeMetadataBatchPage = async (
  parentIds: readonly string[], kind: "folders" | "files", token: string, pageToken?: string,
  signal?: AbortSignal
): Promise<{ files: DriveFileMetadata[]; nextPageToken: string | null }> => {
  const url = buildNativeMetadataBatchUrl(parentIds, kind, pageToken);
  const payload = await scannerPage(url, token, signal);
  if (payload?.incompleteSearch === true) throw new Error("Drive returned an incomplete Landing metadata search.");
  if (!Array.isArray(payload?.files) ||
      (payload.nextPageToken !== undefined && typeof payload.nextPageToken !== "string") ||
      (payload.incompleteSearch !== undefined && typeof payload.incompleteSearch !== "boolean"))
    throw new Error("Drive returned an incomplete Landing metadata page.");
  return { files: payload.files.map(nativeMetadata), nextPageToken: payload.nextPageToken || null };
};

/** Fresh metadata by ID, including current parent and original Drive-managed links. */
export const getNativeFileMetadata = async (
  fileId: string,
  token: string,
  signal?: AbortSignal
): Promise<DriveFileMetadata> => {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?fields=${encodeURIComponent(NATIVE_FIELDS)}`;
  return nativeMetadata(await (await driveRequest(url, token, signal)).json());
};

const listFilesForQuery = async (
  query: string,
  token: string,
  onPage?: () => void,
  signal?: AbortSignal
): Promise<DriveFileMetadata[]> => {
  const files: DriveFileMetadata[] = [];
  let pageToken: string | null = null;
  do {
    let url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(
      query
    )}&fields=files(id,name,mimeType,size,modifiedTime,version,md5Checksum,sha256Checksum),nextPageToken&pageSize=1000`;
    if (pageToken) url += `&pageToken=${encodeURIComponent(pageToken)}`;
    onPage?.();
    const response = await driveRequest(url, token, signal);
    const payload = await response.json();
    for (const item of payload.files || []) {
      files.push({
        id: item.id,
        name: item.name,
        mimeType: item.mimeType,
        size: item.size === undefined ? undefined : Number(item.size),
        modifiedTime: item.modifiedTime,
        version: item.version,
        md5Checksum: item.md5Checksum,
        sha256Checksum: item.sha256Checksum,
      });
    }
    pageToken = payload.nextPageToken || null;
  } while (pageToken);
  return files;
};

export const findFoldersByName = async (
  name: string,
  parentId = "root",
  token: string,
  onPage?: () => void,
  signal?: AbortSignal
): Promise<Array<{ id: string; name: string }>> => {
  if (name.includes("'"))
    throw new Error("Folder names containing single quotes are not supported.");
  const query = [
    `mimeType='application/vnd.google-apps.folder'`,
    `name='${name}'`,
    `'${parentId}' in parents`,
    "trashed=false",
  ].join(" and ");
  return (await listFilesForQuery(query, token, onPage, signal)).map(({ id, name: folderName }) => ({
    id,
    name: folderName,
  }));
};

export const findNamedFilesInFolder = async (
  name: string,
  parentId: string,
  token: string,
  onPage?: () => void
): Promise<DriveFileMetadata[]> => {
  if (name.includes("'")) throw new Error("File names containing single quotes are not supported.");
  const query = [`name='${name}'`, `'${parentId}' in parents`, "trashed=false"].join(" and ");
  return listFilesForQuery(query, token, onPage);
};

/** Metadata by immutable ID; no folder listing is used for release manifests. */
export const findNamedFilesInFolderById = async (
  fileId: string,
  token: string
): Promise<DriveFileMetadata | null> => {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(
    fileId
  )}?fields=id,name,mimeType,size,modifiedTime,version,md5Checksum,sha256Checksum`;
  try {
    const response = await driveRequest(url, token);
    const item = await response.json();
    if (!item?.id || item.mimeType === "application/vnd.google-apps.folder") return null;
    return {
      id: item.id,
      name: item.name,
      mimeType: item.mimeType,
      size: item.size === undefined ? undefined : Number(item.size),
      modifiedTime: item.modifiedTime,
      version: item.version,
      md5Checksum: item.md5Checksum,
      sha256Checksum: item.sha256Checksum,
    };
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("Google Drive API error (404)"))
      return null;
    throw error;
  }
};

export const listFolderChildrenMetadata = async (
  folderId: string,
  token: string,
  onPage?: () => void
): Promise<DriveFileMetadata[]> => {
  const files: DriveFileMetadata[] = [];
  let pageToken: string | null = null;
  do {
    const query = `'${folderId}' in parents and trashed=false`;
    let url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(
      query
    )}&fields=files(id,name,mimeType,size,modifiedTime,version,md5Checksum,sha256Checksum),nextPageToken&pageSize=1000`;
    if (pageToken) url += `&pageToken=${encodeURIComponent(pageToken)}`;
    onPage?.();
    const response = await driveRequest(url, token);
    const payload = await response.json();
    for (const item of payload.files || []) {
      files.push({
        id: item.id,
        name: item.name,
        mimeType: item.mimeType,
        size: item.size === undefined ? undefined : Number(item.size),
        modifiedTime: item.modifiedTime,
        version: item.version,
        md5Checksum: item.md5Checksum,
        sha256Checksum: item.sha256Checksum,
      });
    }
    pageToken = payload.nextPageToken || null;
  } while (pageToken);
  return files;
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
    if (
      maxBytes !== undefined &&
      (!length || !Number.isSafeInteger(Number(length)) || Number(length) > maxBytes)
    ) {
      throw new Error("Drive did not provide a bounded stream or a safe content length.");
    }
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
