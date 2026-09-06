/** Load real Drive files into a disposable DuckDB session. */
import type * as duckdb from "@duckdb/duckdb-wasm";
import { sqlEscapeIdentifier, sqlEscapeString } from "@/lib/sqlSanitize";
import { fetchDriveFileBuffer, listDataFilesInFolder } from "./driveApi";
import type { LakehouseFile } from "./types";

// File registrations belong to an engine, never to the application globally.
const registeredFiles = new WeakMap<duckdb.AsyncDuckDB, Set<string>>();
const pathPart = (value: string) => encodeURIComponent(value).replace(/\*/g, "%2A");

async function registerFile(db: duckdb.AsyncDuckDB, file: LakehouseFile, token: string) {
  if (!token) throw new Error("Sign in to Google Drive before loading data.");
  if (!file.id || file.id === "demo_file" || !file.name) {
    throw new Error("The catalog entry does not identify a real Drive file.");
  }
  const path = `/google-drive/${pathPart(file.id)}/${pathPart(file.name)}`;
  let registered = registeredFiles.get(db);
  if (!registered) {
    registered = new Set();
    registeredFiles.set(db, registered);
  }
  if (!registered.has(path)) {
    const bytes = await fetchDriveFileBuffer(file.id, token);
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
  layerName: string
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
  const loadedFiles: string[] = [];
  for (const file of files) loadedFiles.push(await registerFile(db, file, token));
  const queryTarget = await publishViews(conn, layerName, datasetName, loadedFiles);
  return { loadedFiles, queryTarget };
};

export const loadFileIntoDuckDB = async (
  db: duckdb.AsyncDuckDB,
  conn: duckdb.AsyncDuckDBConnection,
  tableName: string,
  file: LakehouseFile,
  token: string
): Promise<{ filePath: string; queryTarget: string }> => {
  const filePath = await registerFile(db, file, token);
  // A file preview must not replace the view for the complete dataset.
  const queryTarget = await publishViews(conn, file.layer, `${tableName}__file_${file.id}`, [
    filePath,
  ]);
  return { filePath, queryTarget };
};
