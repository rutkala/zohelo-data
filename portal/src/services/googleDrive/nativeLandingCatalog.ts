/** Metadata-only browser for the actual retained 01_landing tree. */
import type * as duckdb from "@duckdb/duckdb-wasm";
import { runQuery, type LocalDuckSession } from "@/services/engine";
import { sqlEscapeIdentifier, sqlEscapeString } from "@/lib/sqlSanitize";
import { DRIVE_ROOT } from "./auth";
import {
  fetchDriveFileBuffer, findFoldersByName, getNativeFileMetadata,
  listNativeChildrenPage, type DriveFileMetadata,
} from "./driveApi";
import { DriveDownloadBudget, sha256Hex } from "./releaseCatalog";

export const NATIVE_FOLDER_MIME = "application/vnd.google-apps.folder";
export const NATIVE_SHORTCUT_MIME = "application/vnd.google-apps.shortcut";
export const NATIVE_PREVIEW_LIMIT_BYTES = 8 * 1024 * 1024;
export type NativeLandingFile = DriveFileMetadata & { parentId: string };
export type NativeLandingFolder = NativeLandingFile & { mimeType: typeof NATIVE_FOLDER_MIME };

export const isNativeFolder = (file: NativeLandingFile): file is NativeLandingFolder =>
  file.mimeType === NATIVE_FOLDER_MIME;

export async function resolveNativeLandingRoot(token: string): Promise<NativeLandingFolder> {
  const projects = await findFoldersByName(DRIVE_ROOT, "root", token);
  if (projects.length !== 1) throw new Error(`Project folder '${DRIVE_ROOT}' must be unambiguous.`);
  const roots = await findFoldersByName("01_landing", projects[0].id, token);
  if (roots.length !== 1) throw new Error("Native Landing folder '01_landing' must be unambiguous.");
  const fresh = await getNativeFileMetadata(roots[0].id, token);
  if (fresh.mimeType !== NATIVE_FOLDER_MIME || fresh.trashed ||
      !fresh.parents?.includes(projects[0].id)) {
    throw new Error("Native Landing root changed during discovery.");
  }
  return { ...fresh, parentId: projects[0].id, mimeType: NATIVE_FOLDER_MIME };
}

export async function listNativeFolder(
  folder: NativeLandingFolder,
  folders: Readonly<Record<string, NativeLandingFolder>>,
  rootId: string,
  token: string,
  isCurrent: () => boolean
): Promise<NativeLandingFile[]> {
  await verifyNativeFolder(folder, folders, rootId, token, isCurrent);
  const result: NativeLandingFile[] = [];
  const ids = new Set<string>();
  const seenPages = new Set<string>();
  let next: string | null = null;
  do {
    if (!isCurrent()) throw new Error("Native Landing request was superseded.");
    const page = await listNativeChildrenPage(folder.id, token, next ?? undefined);
    if (!isCurrent()) throw new Error("Native Landing request was superseded.");
    for (const file of page.files) {
      if (file.trashed || !file.parents?.includes(folder.id) || ids.has(file.id)) {
        throw new Error("Native Landing folder membership changed during listing.");
      }
      ids.add(file.id);
      result.push({ ...file, parentId: folder.id });
    }
    next = page.nextPageToken;
    if (next && seenPages.has(next)) throw new Error("Drive repeated a native folder page token.");
    if (next) seenPages.add(next);
  } while (next);
  await verifyNativeFolder(folder, folders, rootId, token, isCurrent);
  return result.sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
}

async function verifyNativeFolder(
  folder: NativeLandingFolder,
  folders: Readonly<Record<string, NativeLandingFolder>>,
  rootId: string,
  token: string,
  isCurrent: () => boolean
): Promise<void> {
  if (folder.id !== rootId) {
    await freshNativeFile(folder, folders, rootId, token, isCurrent);
    return;
  }
  const fresh = await getNativeFileMetadata(rootId, token);
  if (!isCurrent() || fresh.trashed || fresh.mimeType !== NATIVE_FOLDER_MIME ||
      !fresh.parents?.includes(folder.parentId) || fresh.version !== folder.version ||
      fresh.modifiedTime !== folder.modifiedTime) {
    throw new Error("Native Landing root changed. Refresh before browsing it.");
  }
  await verifyProjectParent(folder.parentId, token, isCurrent);
}

async function verifyProjectParent(projectId: string, token: string, isCurrent: () => boolean): Promise<void> {
  const matches = await findFoldersByName(DRIVE_ROOT, "root", token);
  if (!isCurrent() || matches.length !== 1 || matches[0].id !== projectId) {
    throw new Error("Native Landing project location changed. Refresh before browsing it.");
  }
}

export function safeDriveLink(link: string | undefined, token: string): string | null {
  if (!link) return null;
  try {
    const url = new URL(link);
    if (url.protocol !== "https:" || url.username || url.password || url.port ||
        !["drive.google.com", "drive.usercontent.google.com", "docs.google.com"].includes(url.hostname) ||
        (token && decodeURIComponent(url.toString()).includes(token))) return null;
    for (const name of url.searchParams.keys()) {
      if (/^(?:access_token|oauth_token|id_token|token|code|key|authorization)$/i.test(name)) return null;
    }
    return url.toString();
  } catch {
    return null;
  }
}

/** Re-read every ancestor, including the selected file, before any action. */
export async function freshNativeFile(
  selected: NativeLandingFile,
  folders: Readonly<Record<string, NativeLandingFolder>>,
  landingRootId: string,
  token: string,
  isCurrent: () => boolean
): Promise<NativeLandingFile> {
  let expectedId = selected.parentId;
  const seen = new Set<string>();
  while (expectedId !== landingRootId) {
    if (!isCurrent() || seen.has(expectedId)) throw new Error("Native Landing location changed.");
    seen.add(expectedId);
    const selectedFolder = folders[expectedId];
    if (!selectedFolder) throw new Error("Native Landing ancestor is not in the loaded tree.");
    const fresh = await getNativeFileMetadata(expectedId, token);
    if (!isCurrent() || fresh.trashed || fresh.mimeType !== NATIVE_FOLDER_MIME ||
        !fresh.parents?.includes(selectedFolder.parentId) ||
        fresh.version !== selectedFolder.version || fresh.modifiedTime !== selectedFolder.modifiedTime) {
      throw new Error("Native Landing ancestor changed. Refresh before opening this file.");
    }
    expectedId = selectedFolder.parentId;
  }
  const freshRoot = await getNativeFileMetadata(landingRootId, token);
  if (!isCurrent() || freshRoot.trashed || freshRoot.mimeType !== NATIVE_FOLDER_MIME ||
      freshRoot.version !== folders[landingRootId]?.version ||
      freshRoot.modifiedTime !== folders[landingRootId]?.modifiedTime ||
      !freshRoot.parents?.includes(folders[landingRootId]?.parentId)) {
    throw new Error("Native Landing root changed. Refresh before opening this file.");
  }
  await verifyProjectParent(folders[landingRootId].parentId, token, isCurrent);
  const fresh = await getNativeFileMetadata(selected.id, token);
  if (!isCurrent() || fresh.trashed || !fresh.parents?.includes(selected.parentId) ||
      fresh.id !== selected.id || fresh.name !== selected.name ||
      fresh.size !== selected.size || fresh.version !== selected.version ||
      fresh.modifiedTime !== selected.modifiedTime ||
      fresh.sha256Checksum !== selected.sha256Checksum ||
      fresh.md5Checksum !== selected.md5Checksum) {
    throw new Error("Native Landing file changed. Refresh before opening it.");
  }
  return { ...fresh, parentId: selected.parentId };
}

export function nativeDriveLink(file: NativeLandingFile, token: string, action: "open" | "download"): string {
  if (action === "download") {
    if (file.capabilities?.canDownload !== true) throw new Error("Drive does not permit this file to be downloaded.");
    const download = safeDriveLink(file.webContentLink, token);
    if (!download) throw new Error("Drive did not provide a safe managed download link.");
    return download;
  }
  const returned = safeDriveLink(file.webViewLink, token);
  if (returned) return returned;
  throw new Error("Drive did not provide a safe view link.");
}

export type NativePreviewFormat = "csv" | "json" | "jsonl" | "parquet";
const activeNativePreviews = new WeakMap<object, { path: string; view: string; session: LocalDuckSession; unsubscribe: () => void }>();
const activeNativeEngines = new Set<duckdb.AsyncDuckDB>();
const pendingNativeDisposals = new WeakMap<object, Promise<void>>();
const retireNativePreview = (db: duckdb.AsyncDuckDB) => {
  const active = activeNativePreviews.get(db);
  active?.unsubscribe();
  activeNativePreviews.delete(db);
  activeNativeEngines.delete(db);
};
const runNativeDdl = async (session: LocalDuckSession, sql: string) => {
  const result = await runQuery(session, sql);
  if (result.error) throw new Error(result.error);
};
export function disposeNativePreview(db: duckdb.AsyncDuckDB): Promise<void> {
  const pending = pendingNativeDisposals.get(db);
  if (pending) return pending;
  const active = activeNativePreviews.get(db);
  if (!active) return Promise.resolve();
  // A closed local engine has discarded its TEMP namespace and registered
  // buffers. No SQL can be sent to it, and it must not block future browsing.
  if (!active.session.isOpen) { retireNativePreview(db); return Promise.resolve(); }
  const task = (async () => {
    try {
      await runNativeDdl(active.session, `DROP VIEW IF EXISTS temp.${sqlEscapeIdentifier(active.view)};`);
      await db.dropFile(active.path);
      retireNativePreview(db);
    } catch (error) {
      // Closing an OPFS session destroys its temporary namespace and buffer.
      // A concurrent close can invalidate DDL already queued by refresh.
      if (!active.session.isOpen) { retireNativePreview(db); return; }
      throw error;
    } finally {
      pendingNativeDisposals.delete(db);
    }
  })();
  pendingNativeDisposals.set(db, task);
  return task;
}
export async function disposeAllNativePreviews(): Promise<void> {
  await Promise.all([...activeNativeEngines].map(disposeNativePreview));
}
export function nativePreviewFormat(file: NativeLandingFile): NativePreviewFormat | null {
  if (file.mimeType === NATIVE_FOLDER_MIME || file.mimeType === NATIVE_SHORTCUT_MIME ||
      file.capabilities?.canDownload !== true || !file.size || file.size > NATIVE_PREVIEW_LIMIT_BYTES ||
      !file.version || !file.modifiedTime || !Number.isFinite(Date.parse(file.modifiedTime)) ||
      !/^[a-fA-F0-9]{64}$/.test(file.sha256Checksum ?? "")) return null;
  const parts = file.name.toLowerCase().split(".");
  const suffix = parts[parts.length - 1];
  if (suffix === "csv" || suffix === "json" || suffix === "jsonl" || suffix === "parquet") return suffix;
  return null;
}

/** Verify native bytes before creating a temporary, non-publication SQL view. */
export async function previewNativeFile(
  session: LocalDuckSession,
  selected: NativeLandingFile,
  folders: Readonly<Record<string, NativeLandingFolder>>,
  rootId: string,
  token: string,
  isCurrent: () => boolean,
  budget: DriveDownloadBudget
): Promise<string> {
  const db = session.local.db;
  if (!session.isOpen || typeof session.onClose !== "function")
    throw new Error("Native Landing preview needs an open local session.");
  const before = await freshNativeFile(selected, folders, rootId, token, isCurrent);
  const format = nativePreviewFormat(before);
  if (!format) throw new Error("Preview requires a supported file of at most 8 MiB with a verifiable Drive SHA-256. Open in Drive instead.");
  budget.reserve(before.size!, before.name);
  const bytes = await fetchDriveFileBuffer(before.id, token, NATIVE_PREVIEW_LIMIT_BYTES);
  if (!isCurrent()) throw new Error("Native Landing preview was superseded.");
  budget.consume(bytes.byteLength, before.name);
  if (bytes.byteLength !== before.size || (await sha256Hex(bytes)).toLowerCase() !== before.sha256Checksum!.toLowerCase()) {
    throw new Error("Native Landing bytes do not match Drive metadata.");
  }
  const after = await freshNativeFile(selected, folders, rootId, token, isCurrent);
  if (after.version !== before.version || after.modifiedTime !== before.modifiedTime ||
      after.size !== before.size || after.sha256Checksum !== before.sha256Checksum) {
    throw new Error("Native Landing file changed during preview.");
  }
  const view = `native_preview_${crypto.randomUUID().replace(/-/g, "")}`;
  const path = `/native-preview/${encodeURIComponent(before.id)}/${before.sha256Checksum}/${encodeURIComponent(before.version!)}/${view}/${encodeURIComponent(before.name)}`;
  await disposeNativePreview(db);
  if (!isCurrent()) throw new Error("Native Landing preview was superseded.");
  await db.registerFileBuffer(path, bytes);
  const reader = format === "parquet" ? "read_parquet" : format === "csv" ? "read_csv_auto" : format === "jsonl" ? "read_ndjson_auto" : "read_json_auto";
  const target = `temp.${sqlEscapeIdentifier(view)}`;
  try {
    await runNativeDdl(session, `CREATE TEMP VIEW ${sqlEscapeIdentifier(view)} AS SELECT * FROM ${reader}('${sqlEscapeString(path)}');`);
  } catch (error) {
    // A failed streamed DDL result can still have created the TEMP view.
    if (session.isOpen) await runNativeDdl(session, `DROP VIEW IF EXISTS ${target};`).catch(() => undefined);
    await db.dropFile(path).catch(() => undefined);
    throw error;
  }
  if (!isCurrent()) {
    if (session.isOpen) await runNativeDdl(session, `DROP VIEW IF EXISTS ${target};`).catch(() => undefined);
    if (session.isOpen) await db.dropFile(path).catch(() => undefined);
    throw new Error("Native Landing preview was superseded.");
  }
  const unsubscribe = session.onClose(() => {
    if (activeNativePreviews.get(db)?.view === view) retireNativePreview(db);
  });
  if (!session.isOpen) throw new Error("Native Landing preview session closed.");
  activeNativePreviews.set(db, { path, view, session, unsubscribe });
  activeNativeEngines.add(db);
  return target;
}
