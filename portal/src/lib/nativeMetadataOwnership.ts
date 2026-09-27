/** Exact browser-local provenance for generated Landing inventory relations. */
import type * as duckdb from "@duckdb/duckdb-wasm";
import { sqlEscapeIdentifier, sqlEscapeString } from "./sqlSanitize";

export const NATIVE_METADATA_COMMENT = "zohelo-native-landing-file-metadata:v1";

export async function listOwnedNativeMetadataTables(
  connection: Pick<duckdb.AsyncDuckDBConnection, "query">
): Promise<string[]> {
  const result = await connection.query(
    `SELECT table_name FROM duckdb_tables() WHERE database_name = current_database() ` +
      `AND schema_name = '01_landing' AND comment = '${sqlEscapeString(NATIVE_METADATA_COMMENT)}'`
  );
  return result.toArray().map((row) => String(row.table_name));
}

/** Run before exposing a reopened OPFS engine to introspection or SQL. */
export async function cleanupOwnedNativeMetadataTables(
  connection: Pick<duckdb.AsyncDuckDBConnection, "query">
): Promise<void> {
  for (const name of await listOwnedNativeMetadataTables(connection)) {
    await connection.query(
      `DROP TABLE ${sqlEscapeIdentifier("01_landing")}.${sqlEscapeIdentifier(name)};`
    );
  }
}
