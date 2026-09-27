/** Metadata-only browser for the actual retained 01_landing tree. */
import type * as duckdb from "@duckdb/duckdb-wasm";
import { runQuery, type LocalDuckSession } from "@/services/engine";
import { sqlEscapeIdentifier, sqlEscapeString } from "@/lib/sqlSanitize";
import { NATIVE_METADATA_COMMENT } from "@/lib/nativeMetadataOwnership";
import { DRIVE_ROOT } from "./auth";
import {
  fetchDriveFileBuffer,
  findFoldersByName,
  getNativeFileMetadata,
  listNativeChildrenPage,
  listNativeMetadataBatchPage,
  type DriveFileMetadata,
} from "./driveApi";
import { DriveDownloadBudget, sha256Hex } from "./releaseCatalog";

export const NATIVE_FOLDER_MIME = "application/vnd.google-apps.folder";
export const NATIVE_SHORTCUT_MIME = "application/vnd.google-apps.shortcut";
export const NATIVE_PREVIEW_LIMIT_BYTES = 8 * 1024 * 1024;
export const NATIVE_METADATA_MAX_FILES = 100000;
export const NATIVE_METADATA_MAX_FOLDERS = 20000;
export type NativeLandingFile = DriveFileMetadata & { parentId: string };
export type NativeLandingFolder = NativeLandingFile & { mimeType: typeof NATIVE_FOLDER_MIME };

export const isNativeFolder = (file: NativeLandingFile): file is NativeLandingFolder =>
  file.mimeType === NATIVE_FOLDER_MIME;

/** A stable, SQL-safe relation name. Duplicate Drive folder names stay distinct. */
export function nativeMetadataNames(folders: readonly NativeLandingFolder[]): Map<string, string> {
  const bases = folders
    .map((folder) => ({ folder, base: `${folder.name}_files` }))
    .sort((a, b) => a.folder.id.localeCompare(b.folder.id));
  const counts = new Map<string, number>();
  for (const { base } of bases)
    counts.set(base.toLowerCase(), (counts.get(base.toLowerCase()) ?? 0) + 1);
  const used = new Set<string>();
  return new Map(
    bases.map(({ folder, base }) => {
      const candidate = (counts.get(base.toLowerCase()) ?? 0) > 1 ? `${base}__${folder.id}` : base;
      let name = candidate;
      for (let suffix = 2; used.has(name.toLowerCase()); suffix++) name = `${candidate}__${suffix}`;
      used.add(name.toLowerCase());
      return [folder.id, name];
    })
  );
}

export interface NativeMetadataRow {
  file_id: string;
  source_folder_id: string;
  source_folder: string;
  relative_path: string;
  file_name: string;
  parent_folder_id: string;
  size_bytes: number | null;
  mime_type: string | null;
  created_at_utc: string | null;
  modified_at_utc: string | null;
  metadata_refreshed_at_utc: string;
  drive_url: string | null;
  sha256_checksum: string | null;
}

export interface NativeMetadataProgress {
  phase: "folders" | "files" | "verifying" | "publishing" | "cancelling";
  folders: number;
  files: number;
  listPages: number;
  startedAtMs: number;
}

export const NATIVE_METADATA_PARENT_BATCH = 25;
export const NATIVE_METADATA_WORKERS = 4;

const parentBatches = (ids: readonly string[]): string[][] => {
  const batches: string[][] = [];
  let batch: string[] = [];
  for (const id of ids) {
    // Leave ample space for fields, MIME filter and encoded URL delimiters.
    if (encodeURIComponent(id).length > 1500)
      throw new Error("Drive folder ID exceeds the Landing metadata query limit.");
    const candidate = [...batch, id];
    const length = candidate.reduce((sum, value) => sum + encodeURIComponent(value).length + 35, 0);
    if (batch.length && (candidate.length > NATIVE_METADATA_PARENT_BATCH || length > 2000)) {
      batches.push(batch);
      batch = [id];
    } else batch = candidate;
  }
  if (batch.length) batches.push(batch);
  return batches;
};

/** Complete metadata-only recursion. A failed page/limit/cancellation never returns partial rows. */
export async function scanNativeSourceMetadata(
  source: NativeLandingFolder,
  root: NativeLandingFolder,
  token: string,
  isCurrent: () => boolean,
  onProgress?: (progress: NativeMetadataProgress) => void,
  signal?: AbortSignal
): Promise<NativeMetadataRow[]> {
  if (source.id === root.id || source.parentId !== root.id)
    throw new Error("Landing metadata source must be an immediate folder under 01_landing.");
  const folders = new Map<string, NativeLandingFolder>([[source.id, source]]);
  const prefixes = new Map<string, string>([[source.id, ""]]);
  const files = new Set<string>();
  const rows: NativeMetadataRow[] = [];
  const scanned = new Date().toISOString();
  const progress: NativeMetadataProgress = { phase: "folders", folders: 1, files: 0,
    listPages: 0, startedAtMs: Date.now() };
  const controller = new AbortController();
  const onAbort = () => controller.abort();
  signal?.addEventListener("abort", onAbort, { once: true });
  if (signal?.aborted) controller.abort();
  const ensureCurrent = () => {
    if (!isCurrent() || controller.signal.aborted)
      throw new Error("Landing metadata scan was cancelled or superseded.");
  };
  const report = () => { ensureCurrent(); onProgress?.({ ...progress }); };
  const verifyMember = async (folder: NativeLandingFolder) => {
    ensureCurrent();
    const fresh = await getNativeFileMetadata(folder.id, token, controller.signal);
    if (!isCurrent() || fresh.trashed || fresh.id !== folder.id || fresh.name !== folder.name ||
        fresh.mimeType !== NATIVE_FOLDER_MIME || !fresh.parents?.includes(folder.parentId) ||
        fresh.version !== folder.version || fresh.modifiedTime !== folder.modifiedTime)
      throw new Error("Landing folder changed during metadata scan. Refresh and retry.");
    ensureCurrent();
  };
  /** Four workers, complete pagination per batch, globally unique IDs per phase. */
  const collect = async (parentIds: readonly string[], kind: "folders" | "files",
    handle: (item: DriveFileMetadata, parentId: string) => void) => {
    const batches = parentBatches(parentIds);
    const seenIds = new Set<string>();
    let cursor = 0;
    const worker = async () => {
      while (cursor < batches.length) {
        ensureCurrent();
        const ids = batches[cursor++];
        const requested = new Set(ids);
        const tokens = new Set<string>();
        let next: string | null = null;
        do {
          ensureCurrent();
          const page = await listNativeMetadataBatchPage(ids, kind, token, next ?? undefined, controller.signal);
          ensureCurrent();
          progress.listPages++;
          for (const item of page.files) {
            if (item.trashed || item.parents?.length !== 1 || !requested.has(item.parents[0]) ||
                seenIds.has(item.id) || (kind === "folders") !== (item.mimeType === NATIVE_FOLDER_MIME))
              throw new Error("Drive returned changed, duplicated or out-of-scope Landing metadata.");
            seenIds.add(item.id);
            handle(item, item.parents[0]);
          }
          report();
          next = page.nextPageToken;
          if (next && tokens.has(next)) throw new Error("Drive repeated a Landing metadata page token.");
          if (next) tokens.add(next);
        } while (next);
      }
    };
    try { await Promise.all(Array.from({ length: Math.min(NATIVE_METADATA_WORKERS, batches.length) }, worker)); }
    catch (error) { controller.abort(); throw error; }
  };
  try {
    await verifyProjectParent(root.parentId, token, () => isCurrent() && !controller.signal.aborted,
      controller.signal);
    await verifyMember(root);
    await verifyMember(source);
    report();
    let frontier = [source.id];
    while (frontier.length) {
      const next: string[] = [];
      await collect(frontier, "folders", (item, parentId) => {
        if (folders.has(item.id)) throw new Error("Landing folder appears twice in the source tree.");
        if (folders.size >= NATIVE_METADATA_MAX_FOLDERS)
          throw new Error(`Source exceeds the ${NATIVE_METADATA_MAX_FOLDERS}-folder metadata scan limit.`);
        const folder = { ...item, parentId, mimeType: NATIVE_FOLDER_MIME } as NativeLandingFolder;
        folders.set(item.id, folder);
        prefixes.set(item.id, `${prefixes.get(parentId)}${item.name}/`);
        next.push(item.id);
        progress.folders = folders.size;
      });
      frontier = next;
    }
    progress.phase = "files";
    report();
    await collect([...folders.keys()], "files", (item, parentId) => {
      if (folders.has(item.id) || files.has(item.id))
        throw new Error("Landing file appears twice in the source tree.");
      files.add(item.id);
      if (item.mimeType === NATIVE_SHORTCUT_MIME) return;
      if (rows.length >= NATIVE_METADATA_MAX_FILES)
        throw new Error(`Source exceeds the ${NATIVE_METADATA_MAX_FILES}-file metadata scan limit.`);
      rows.push({ file_id: item.id, source_folder_id: source.id, source_folder: source.name,
        relative_path: `${prefixes.get(parentId)}${item.name}`, file_name: item.name,
        parent_folder_id: parentId, size_bytes: item.size ?? null, mime_type: item.mimeType ?? null,
        created_at_utc: item.createdTime ?? null, modified_at_utc: item.modifiedTime ?? null,
        metadata_refreshed_at_utc: scanned, drive_url: safeDriveLink(item.webViewLink, token),
        sha256_checksum: item.sha256Checksum ?? null });
      progress.files = rows.length;
    });
    progress.phase = "verifying";
    report();
    const verified = new Set<string>();
    await collect([...folders.keys()], "folders", (item, parentId) => {
      const original = folders.get(item.id);
      if (!original || original.parentId !== parentId || original.name !== item.name ||
          original.version !== item.version || original.modifiedTime !== item.modifiedTime)
        throw new Error("Landing folder changed during metadata scan. Refresh and retry.");
      verified.add(item.id);
    });
    // Source is the only folder absent from searches of its descendants.
    if (verified.size !== folders.size - 1)
      throw new Error("Landing folder disappeared during metadata scan. Refresh and retry.");
    await verifyMember(source);
    await verifyMember(root);
    await verifyProjectParent(root.parentId, token, () => isCurrent() && !controller.signal.aborted,
      controller.signal);
    ensureCurrent();
    progress.phase = "publishing";
    report();
    return rows;
  } finally {
    signal?.removeEventListener("abort", onAbort);
  }
}

const metadataColumns = [
  "file_id",
  "source_folder_id",
  "source_folder",
  "relative_path",
  "file_name",
  "parent_folder_id",
  "size_bytes",
  "mime_type",
  "created_at_utc",
  "modified_at_utc",
  "metadata_refreshed_at_utc",
  "drive_url",
  "sha256_checksum",
] as const;
const metadataLiteral = (value: string | number | null) =>
  value === null
    ? "NULL"
    : typeof value === "number"
      ? String(value)
      : `'${sqlEscapeString(value)}'`;

/** Publish the completed inventory atomically into this browser's local DuckDB session. */
export async function publishNativeMetadata(
  session: LocalDuckSession,
  tableName: string,
  rows: readonly NativeMetadataRow[],
  isCurrent: () => boolean
): Promise<string> {
  const target = `${sqlEscapeIdentifier("01_landing")}.${sqlEscapeIdentifier(tableName)}`;
  const staging = sqlEscapeIdentifier(`native_metadata_${crypto.randomUUID().replace(/-/g, "")}`);
  if (!isCurrent()) throw new Error("Landing metadata scan was superseded.");
  await runNativeDdl(session, `CREATE SCHEMA IF NOT EXISTS ${sqlEscapeIdentifier("01_landing")};`);
  await runNativeDdl(
    session,
    `CREATE TEMP TABLE ${staging} (
    file_id VARCHAR, source_folder_id VARCHAR, source_folder VARCHAR, relative_path VARCHAR,
    file_name VARCHAR, parent_folder_id VARCHAR, size_bytes BIGINT, mime_type VARCHAR,
    created_at_utc TIMESTAMP, modified_at_utc TIMESTAMP, metadata_refreshed_at_utc TIMESTAMP,
    drive_url VARCHAR, sha256_checksum VARCHAR
  );`
  );
  try {
    for (let i = 0; i < rows.length; i += 100) {
      if (!isCurrent()) throw new Error("Landing metadata scan was superseded.");
      const values = rows
        .slice(i, i + 100)
        .map((row) => `(${metadataColumns.map((key) => metadataLiteral(row[key])).join(",")})`)
        .join(",");
      await runNativeDdl(session, `INSERT INTO ${staging} VALUES ${values};`);
    }
    if (!isCurrent()) throw new Error("Landing metadata scan was superseded.");
    await runNativeDdl(session, "BEGIN TRANSACTION;");
    try {
      if (!isCurrent()) throw new Error("Landing metadata scan was superseded.");
      await runNativeDdl(session, `CREATE OR REPLACE TABLE ${target} AS SELECT * FROM ${staging};`);
      await runNativeDdl(
        session,
        `COMMENT ON TABLE ${target} IS '${sqlEscapeString(NATIVE_METADATA_COMMENT)}';`
      );
      if (!isCurrent()) throw new Error("Landing metadata scan was superseded.");
      await runNativeDdl(session, "COMMIT;");
    } catch (error) {
      await runNativeDdl(session, "ROLLBACK;").catch(() => undefined);
      throw error;
    }
    return target;
  } finally {
    if (session.isOpen)
      await runNativeDdl(session, `DROP TABLE IF EXISTS ${staging};`).catch(() => undefined);
  }
}

export async function resolveNativeLandingRoot(token: string): Promise<NativeLandingFolder> {
  const projects = await findFoldersByName(DRIVE_ROOT, "root", token);
  if (projects.length !== 1) throw new Error(`Project folder '${DRIVE_ROOT}' must be unambiguous.`);
  const roots = await findFoldersByName("01_landing", projects[0].id, token);
  if (roots.length !== 1)
    throw new Error("Native Landing folder '01_landing' must be unambiguous.");
  const fresh = await getNativeFileMetadata(roots[0].id, token);
  if (
    fresh.mimeType !== NATIVE_FOLDER_MIME ||
    fresh.trashed ||
    !fresh.parents?.includes(projects[0].id)
  ) {
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
  if (
    !isCurrent() ||
    fresh.trashed ||
    fresh.mimeType !== NATIVE_FOLDER_MIME ||
    !fresh.parents?.includes(folder.parentId) ||
    fresh.version !== folder.version ||
    fresh.modifiedTime !== folder.modifiedTime
  ) {
    throw new Error("Native Landing root changed. Refresh before browsing it.");
  }
  await verifyProjectParent(folder.parentId, token, isCurrent);
}

async function verifyProjectParent(
  projectId: string,
  token: string,
  isCurrent: () => boolean,
  signal?: AbortSignal
): Promise<void> {
  const matches = await findFoldersByName(DRIVE_ROOT, "root", token, undefined, signal);
  if (!isCurrent() || matches.length !== 1 || matches[0].id !== projectId) {
    throw new Error("Native Landing project location changed. Refresh before browsing it.");
  }
}

export function safeDriveLink(link: string | undefined, token: string): string | null {
  if (!link) return null;
  try {
    const url = new URL(link);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.port ||
      !["drive.google.com", "drive.usercontent.google.com", "docs.google.com"].includes(
        url.hostname
      ) ||
      (token && decodeURIComponent(url.toString()).includes(token))
    )
      return null;
    for (const name of url.searchParams.keys()) {
      if (/^(?:access_token|oauth_token|id_token|token|code|key|authorization)$/i.test(name))
        return null;
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
    if (
      !isCurrent() ||
      fresh.trashed ||
      fresh.mimeType !== NATIVE_FOLDER_MIME ||
      !fresh.parents?.includes(selectedFolder.parentId) ||
      fresh.version !== selectedFolder.version ||
      fresh.modifiedTime !== selectedFolder.modifiedTime
    ) {
      throw new Error("Native Landing ancestor changed. Refresh before opening this file.");
    }
    expectedId = selectedFolder.parentId;
  }
  const freshRoot = await getNativeFileMetadata(landingRootId, token);
  if (
    !isCurrent() ||
    freshRoot.trashed ||
    freshRoot.mimeType !== NATIVE_FOLDER_MIME ||
    freshRoot.version !== folders[landingRootId]?.version ||
    freshRoot.modifiedTime !== folders[landingRootId]?.modifiedTime ||
    !freshRoot.parents?.includes(folders[landingRootId]?.parentId)
  ) {
    throw new Error("Native Landing root changed. Refresh before opening this file.");
  }
  await verifyProjectParent(folders[landingRootId].parentId, token, isCurrent);
  const fresh = await getNativeFileMetadata(selected.id, token);
  if (
    !isCurrent() ||
    fresh.trashed ||
    !fresh.parents?.includes(selected.parentId) ||
    fresh.id !== selected.id ||
    fresh.name !== selected.name ||
    fresh.size !== selected.size ||
    fresh.version !== selected.version ||
    fresh.modifiedTime !== selected.modifiedTime ||
    fresh.sha256Checksum !== selected.sha256Checksum ||
    fresh.md5Checksum !== selected.md5Checksum
  ) {
    throw new Error("Native Landing file changed. Refresh before opening it.");
  }
  return { ...fresh, parentId: selected.parentId };
}

export function nativeDriveLink(
  file: NativeLandingFile,
  token: string,
  action: "open" | "download"
): string {
  if (action === "download") {
    if (file.capabilities?.canDownload !== true)
      throw new Error("Drive does not permit this file to be downloaded.");
    const download = safeDriveLink(file.webContentLink, token);
    if (!download) throw new Error("Drive did not provide a safe managed download link.");
    return download;
  }
  const returned = safeDriveLink(file.webViewLink, token);
  if (returned) return returned;
  throw new Error("Drive did not provide a safe view link.");
}

export type NativePreviewFormat = "csv" | "json" | "jsonl" | "parquet";
const activeNativePreviews = new WeakMap<
  object,
  { path: string; view: string; session: LocalDuckSession; unsubscribe: () => void }
>();
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
  if (!active.session.isOpen) {
    retireNativePreview(db);
    return Promise.resolve();
  }
  const task = (async () => {
    try {
      await runNativeDdl(
        active.session,
        `DROP VIEW IF EXISTS temp.${sqlEscapeIdentifier(active.view)};`
      );
      await db.dropFile(active.path);
      retireNativePreview(db);
    } catch (error) {
      // Closing an OPFS session destroys its temporary namespace and buffer.
      // A concurrent close can invalidate DDL already queued by refresh.
      if (!active.session.isOpen) {
        retireNativePreview(db);
        return;
      }
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
  if (
    file.mimeType === NATIVE_FOLDER_MIME ||
    file.mimeType === NATIVE_SHORTCUT_MIME ||
    file.capabilities?.canDownload !== true ||
    !file.size ||
    file.size > NATIVE_PREVIEW_LIMIT_BYTES ||
    !file.version ||
    !file.modifiedTime ||
    !Number.isFinite(Date.parse(file.modifiedTime)) ||
    !/^[a-fA-F0-9]{64}$/.test(file.sha256Checksum ?? "")
  )
    return null;
  const parts = file.name.toLowerCase().split(".");
  const suffix = parts[parts.length - 1];
  if (suffix === "csv" || suffix === "json" || suffix === "jsonl" || suffix === "parquet")
    return suffix;
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
  if (!format)
    throw new Error(
      "Preview requires a supported file of at most 8 MiB with a verifiable Drive SHA-256. Open in Drive instead."
    );
  budget.reserve(before.size!, before.name);
  const bytes = await fetchDriveFileBuffer(before.id, token, NATIVE_PREVIEW_LIMIT_BYTES);
  if (!isCurrent()) throw new Error("Native Landing preview was superseded.");
  budget.consume(bytes.byteLength, before.name);
  if (
    bytes.byteLength !== before.size ||
    (await sha256Hex(bytes)).toLowerCase() !== before.sha256Checksum!.toLowerCase()
  ) {
    throw new Error("Native Landing bytes do not match Drive metadata.");
  }
  const after = await freshNativeFile(selected, folders, rootId, token, isCurrent);
  if (
    after.version !== before.version ||
    after.modifiedTime !== before.modifiedTime ||
    after.size !== before.size ||
    after.sha256Checksum !== before.sha256Checksum
  ) {
    throw new Error("Native Landing file changed during preview.");
  }
  const view = `native_preview_${crypto.randomUUID().replace(/-/g, "")}`;
  const path = `/native-preview/${encodeURIComponent(before.id)}/${before.sha256Checksum}/${encodeURIComponent(before.version!)}/${view}/${encodeURIComponent(before.name)}`;
  await disposeNativePreview(db);
  if (!isCurrent()) throw new Error("Native Landing preview was superseded.");
  await db.registerFileBuffer(path, bytes);
  const reader =
    format === "parquet"
      ? "read_parquet"
      : format === "csv"
        ? "read_csv_auto"
        : format === "jsonl"
          ? "read_ndjson_auto"
          : "read_json_auto";
  const target = `temp.${sqlEscapeIdentifier(view)}`;
  try {
    await runNativeDdl(
      session,
      `CREATE TEMP VIEW ${sqlEscapeIdentifier(view)} AS SELECT * FROM ${reader}('${sqlEscapeString(path)}');`
    );
  } catch (error) {
    // A failed streamed DDL result can still have created the TEMP view.
    if (session.isOpen)
      await runNativeDdl(session, `DROP VIEW IF EXISTS ${target};`).catch(() => undefined);
    await db.dropFile(path).catch(() => undefined);
    throw error;
  }
  if (!isCurrent()) {
    if (session.isOpen)
      await runNativeDdl(session, `DROP VIEW IF EXISTS ${target};`).catch(() => undefined);
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
