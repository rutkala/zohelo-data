/**
 * Bridge between Google Drive Lakehouse Storage and DuckDB-WASM
 */
import type * as duckdb from "@duckdb/duckdb-wasm";
import { sqlEscapeIdentifier } from "@/lib/sqlSanitize";
import { fetchDriveFileBuffer, listDataFilesInFolder } from "./driveApi";
import type { LakehouseFile } from "./types";

const registeredVirtualFiles = new Set<string>();

export const isFileRegistered = (path: string): boolean => {
  return registeredVirtualFiles.has(path);
};

export const clearRegisteredFiles = (): void => {
  registeredVirtualFiles.clear();
};

/**
 * Initializes fallback demo exchange rates tables in DuckDB-WASM
 */
export const ensureDemoRatesTable = async (
  conn: duckdb.AsyncDuckDBConnection
): Promise<void> => {
  try {
    await conn.query(`
      CREATE OR REPLACE TABLE demo_bronze_rates (
        table_no VARCHAR,
        effective_date DATE,
        currency VARCHAR,
        currency_code VARCHAR,
        mid_rate DOUBLE
      );
      INSERT INTO demo_bronze_rates VALUES
        ('168/A/NBP/2026', DATE '2026-08-28', 'Euro', 'EUR', 4.3250),
        ('168/A/NBP/2026', DATE '2026-08-28', 'US Dollar', 'USD', 3.9820),
        ('168/A/NBP/2026', DATE '2026-08-28', 'British Pound', 'GBP', 5.1240),
        ('168/A/NBP/2026', DATE '2026-08-28', 'Swiss Franc', 'CHF', 4.5610),
        ('168/A/NBP/2026', DATE '2026-08-28', 'Japanese Yen', 'JPY', 0.0275);

      CREATE OR REPLACE VIEW active_layer AS SELECT * FROM demo_bronze_rates;
      CREATE OR REPLACE VIEW nbp_exchange_rates_table_a AS SELECT * FROM demo_bronze_rates;
    `);
  } catch (err) {
    console.warn("[LakehouseBridge] Demo table initialization note:", err);
  }
};

/**
 * Registers all files of a Google Drive dataset/table in DuckDB-WASM and creates views.
 */
export const loadTableIntoDuckDB = async (
  db: duckdb.AsyncDuckDB,
  conn: duckdb.AsyncDuckDBConnection,
  datasetName: string,
  tableFolderId: string | null,
  existingFiles: LakehouseFile[],
  token: string
): Promise<{ loadedFiles: string[]; queryTarget: string }> => {
  let files = existingFiles;

  if ((!files || files.length === 0) && tableFolderId && token) {
    const driveFiles = await listDataFilesInFolder(tableFolderId, token);
    files = driveFiles.map((f) => ({
      id: f.id,
      name: f.name,
      mimeType: f.mimeType,
      size: f.size,
      tableName: datasetName,
      layer: "02_bronze",
    }));
  }

  const loadedFiles: string[] = [];

  if (token && files && files.length > 0) {
    for (const file of files) {
      if (!file.id || file.id === "demo_file") continue;
      const filePath = `/${datasetName}/${file.name}`;
      if (!registeredVirtualFiles.has(filePath)) {
        const buffer = await fetchDriveFileBuffer(file.id, token);
        await db.registerFileBuffer(filePath, buffer);
        registeredVirtualFiles.add(filePath);
      }
      loadedFiles.push(filePath);
    }
  }

  const sanitizedDataset = sqlEscapeIdentifier(datasetName);

  if (loadedFiles.length > 0) {
    // If files are loaded, create views pointing to virtual filesystem
    const sourcePath =
      loadedFiles.length === 1 ? loadedFiles[0] : `/${datasetName}/*`;

    await conn.query(
      `CREATE OR REPLACE VIEW active_layer AS SELECT * FROM '${sourcePath}';`
    );
    await conn.query(
      `CREATE OR REPLACE VIEW ${sanitizedDataset} AS SELECT * FROM '${sourcePath}';`
    );

    return { loadedFiles, queryTarget: `active_layer` };
  } else {
    // Fallback to demo tables if no remote files were downloaded
    await ensureDemoRatesTable(conn);
    await conn.query(
      `CREATE OR REPLACE VIEW ${sanitizedDataset} AS SELECT * FROM demo_bronze_rates;`
    );
    return { loadedFiles: [], queryTarget: `active_layer` };
  }
};

/**
 * Registers a single Google Drive file in DuckDB-WASM and creates views.
 */
export const loadFileIntoDuckDB = async (
  db: duckdb.AsyncDuckDB,
  conn: duckdb.AsyncDuckDBConnection,
  tableName: string,
  file: LakehouseFile,
  token: string
): Promise<{ filePath: string; queryTarget: string }> => {
  const filePath = `/${tableName}/${file.name}`;
  const sanitizedTable = sqlEscapeIdentifier(tableName);

  if (token && file.id && file.id !== "demo_file") {
    if (!registeredVirtualFiles.has(filePath)) {
      const buffer = await fetchDriveFileBuffer(file.id, token);
      await db.registerFileBuffer(filePath, buffer);
      registeredVirtualFiles.add(filePath);
    }

    await conn.query(
      `CREATE OR REPLACE VIEW active_layer AS SELECT * FROM '${filePath}';`
    );
    await conn.query(
      `CREATE OR REPLACE VIEW ${sanitizedTable} AS SELECT * FROM '${filePath}';`
    );
  } else {
    await ensureDemoRatesTable(conn);
    await conn.query(
      `CREATE OR REPLACE VIEW ${sanitizedTable} AS SELECT * FROM demo_bronze_rates;`
    );
  }

  return { filePath, queryTarget: "active_layer" };
};
