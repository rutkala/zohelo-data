/** Load real Drive files into a disposable DuckDB session. */
import type * as duckdb from "@duckdb/duckdb-wasm";
import { sqlEscapeIdentifier, sqlEscapeString } from "@/lib/sqlSanitize";
import { fetchDriveFileBuffer, listDataFilesInFolder } from "./driveApi";
import {
  DRIVE_DOWNLOAD_LIMIT_BYTES,
  DriveDownloadBudget,
  createDriveDownloadBudget,
  sha256Hex,
} from "./releaseCatalog";
import type { LakehouseFile } from "./types";

// File registrations belong to an engine, never to the application globally.
const registeredFiles = new WeakMap<duckdb.AsyncDuckDB, Set<string>>();
const defaultDownloadBudgets = new WeakMap<duckdb.AsyncDuckDB, DriveDownloadBudget>();

const budgetFor = (db: duckdb.AsyncDuckDB) => {
  let budget = defaultDownloadBudgets.get(db);
  if (!budget) {
    budget = createDriveDownloadBudget();
    defaultDownloadBudgets.set(db, budget);
  }
  return budget;
};
const pathPart = (value: string) => encodeURIComponent(value).replace(/\*/g, "%2A");

async function registerFile(
  db: duckdb.AsyncDuckDB,
  file: LakehouseFile,
  token: string,
  downloadBudget: DriveDownloadBudget
) {
  if (!token) throw new Error("Sign in to Google Drive before loading data.");
  if (!file.id || file.id === "demo_file" || !file.name) {
    throw new Error("The catalog entry does not identify a real Drive file.");
  }
  // The digest is part of the cache identity. A reused Drive ID with different
  // declared release bytes must be downloaded and verified again.
  const path = `/google-drive/${pathPart(file.id)}/${pathPart(file.sha256 ?? "legacy")}/${pathPart(file.name)}`;
  let registered = registeredFiles.get(db);
  if (!registered) {
    registered = new Set();
    registeredFiles.set(db, registered);
  }
  if (!registered.has(path)) {
    if (file.size !== undefined) downloadBudget.reserve(file.size, file.name);
    const bytes = await fetchDriveFileBuffer(
      file.id,
      token,
      file.size ?? DRIVE_DOWNLOAD_LIMIT_BYTES
    );
    downloadBudget.consume(bytes.byteLength, file.name);
    if (file.size !== undefined && bytes.byteLength !== file.size) {
      throw new Error(`Downloaded '${file.name}' does not match its declared size.`);
    }
    if (file.sha256 && (await sha256Hex(bytes)) !== file.sha256) {
      throw new Error(`Downloaded '${file.name}' does not match its release SHA-256.`);
    }
    await db.registerFileBuffer(path, bytes);
    registered.add(path);
  }
  return path;
}

async function publishViews(
  conn: duckdb.AsyncDuckDBConnection,
  layerName: string,
  viewName: string,
  files: string[]
) {
  const target = `${sqlEscapeIdentifier(layerName)}.${sqlEscapeIdentifier(viewName)}`;
  // Use exactly these files. A wildcard can accidentally include an old selection.
  // Preserve source rows; deduplication and reshaping belong in dbt.
  const source = files
    .map((file) => `SELECT * FROM '${sqlEscapeString(file)}'`)
    .join(" UNION ALL BY NAME ");
  await conn.query("BEGIN TRANSACTION;");
  try {
    await conn.query(`CREATE SCHEMA IF NOT EXISTS ${sqlEscapeIdentifier(layerName)};`);
    await conn.query(`CREATE OR REPLACE VIEW ${target} AS ${source};`);
    await conn.query(`CREATE OR REPLACE VIEW active_layer AS SELECT * FROM ${target};`);
    await conn.query("COMMIT;");
    return target;
  } catch (error) {
    await conn.query("ROLLBACK;").catch(() => undefined);
    throw error;
  }
}

export const loadTableIntoDuckDB = async (
  db: duckdb.AsyncDuckDB,
  conn: duckdb.AsyncDuckDBConnection,
  datasetName: string,
  tableFolderId: string | null,
  existingFiles: LakehouseFile[],
  token: string,
  layerName: string,
  downloadBudget?: DriveDownloadBudget
): Promise<{ loadedFiles: string[]; queryTarget: string }> => {
  if (!token) throw new Error("Sign in to Google Drive before loading data.");
  let files = existingFiles;
  if (files.length === 0 && tableFolderId) {
    files = (await listDataFilesInFolder(tableFolderId, token)).map((file) => ({
      ...file,
      tableName: datasetName,
      layer: layerName,
    }));
  }
  if (files.length === 0) throw new Error(`No data files found for '${datasetName}'.`);
  const activeBudget = downloadBudget ?? budgetFor(db);
  const loadedFiles: string[] = [];
  for (const file of files) loadedFiles.push(await registerFile(db, file, token, activeBudget));
  const queryTarget = await publishViews(conn, layerName, datasetName, loadedFiles);
  return { loadedFiles, queryTarget };
};

export const loadFileIntoDuckDB = async (
  db: duckdb.AsyncDuckDB,
  conn: duckdb.AsyncDuckDBConnection,
  tableName: string,
  file: LakehouseFile,
  token: string,
  downloadBudget?: DriveDownloadBudget
): Promise<{ filePath: string; queryTarget: string }> => {
  const filePath = await registerFile(db, file, token, downloadBudget ?? budgetFor(db));
  // A file preview must not replace the view for the complete dataset.
  const queryTarget = await publishViews(conn, file.layer, `${tableName}__file_${file.id}`, [
    filePath,
  ]);
  return { filePath, queryTarget };
};
