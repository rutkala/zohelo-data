/** Resolve published release relations using DuckDB's own SQL parser AST. */
import { sqlEscapeString } from "@/lib/sqlSanitize";
import type { ReleaseDataset } from "./types";

export interface DuckDbSqlParser {
  query(
    sql: string
  ): Promise<{ getChildAt(index: number): { get(index: number): unknown } | null }>;
}

export interface PublishedTableReference {
  datasetName: string;
  layerName: ReleaseDataset["layer"];
  files: ReleaseDataset["files"];
}

type JsonRecord = Record<string, unknown>;
const isRecord = (value: unknown): value is JsonRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const canonical = (value: string) => value.toLowerCase();
const relationLabel = (reference: PublishedTableReference) =>
  `${reference.layerName}.${reference.datasetName}`;

const ambiguousReference = (name: string, candidates: PublishedTableReference[]) =>
  new Error(
    `Published table '${name}' is ambiguous. Qualify it as one of: ${candidates
      .map((candidate) => relationLabel(candidate))
      .join(", ")}.`
  );

const ctesFor = (node: JsonRecord): Set<string> => {
  const cteMap = node.cte_map;
  if (!isRecord(cteMap) || !Array.isArray(cteMap.map)) return new Set();
  return new Set(
    cteMap.map.flatMap((entry) =>
      isRecord(entry) && typeof entry.key === "string" ? [canonical(entry.key)] : []
    )
  );
};

/**
 * Resolves every BASE_TABLE node emitted by DuckDB's `json_serialize_sql`.
 * The packaged WASM `getTableNames(query)` wrapper has no qualified flag and
 * omits views it expands. This parser-only operation neither binds nor runs
 * the submitted SQL, and application code does not parse SQL text itself.
 */
export const resolvePublishedTableReferences = async (
  parser: DuckDbSqlParser,
  sql: string,
  datasets: readonly ReleaseDataset[]
): Promise<PublishedTableReference[]> => {
  let serialized: Awaited<ReturnType<DuckDbSqlParser["query"]>>;
  try {
    serialized = await parser.query(
      `SELECT json_serialize_sql('${sqlEscapeString(sql)}') AS parsed_sql`
    );
  } catch {
    // json_serialize_sql is deliberately SELECT-only in this WASM build.
    // Let local DDL/DML and syntax errors follow the normal engine path.
    return [];
  }
  const rawAst = serialized.getChildAt(0)?.get(0);
  if (typeof rawAst !== "string") return [];
  let ast: unknown;
  try {
    ast = JSON.parse(rawAst);
  } catch {
    return [];
  }
  if (!isRecord(ast) || ast.error !== false) return [];

  const published = datasets.map((dataset) => ({
    datasetName: dataset.table_name,
    layerName: dataset.layer,
    files: dataset.files,
  }));
  const selected = new Map<string, PublishedTableReference>();
  const visit = (value: unknown, scopes: readonly Set<string>[]): void => {
    if (Array.isArray(value)) {
      value.forEach((item) => visit(item, scopes));
      return;
    }
    if (!isRecord(value)) return;
    const ctes = ctesFor(value);
    const nextScopes = ctes.size > 0 ? [...scopes, ctes] : scopes;
    if (value.type === "BASE_TABLE" && typeof value.table_name === "string") {
      const tableName = value.table_name;
      const schemaName = typeof value.schema_name === "string" ? value.schema_name : "";
      const catalogName = typeof value.catalog_name === "string" ? value.catalog_name : "";
      const isLocalCte =
        !schemaName && !catalogName && nextScopes.some((scope) => scope.has(canonical(tableName)));
      const isOtherCatalog =
        !!catalogName && !["memory", ":memory:"].includes(canonical(catalogName));
      // Bare identifiers remain ordinary local SQL. Generated catalogue SQL is
      // schema-qualified, and a bare published name must not shadow a local
      // table or view with the same name.
      if (!isLocalCte && !isOtherCatalog && schemaName) {
        const matches = published.filter(
          (dataset) =>
            canonical(dataset.datasetName) === canonical(tableName) &&
            canonical(dataset.layerName) === canonical(schemaName)
        );
        if (matches.length > 1) throw ambiguousReference(`${schemaName}.${tableName}`, matches);
        const dataset = matches[0];
        if (dataset) selected.set(relationLabel(dataset), dataset);
      }
    }
    Object.values(value).forEach((child) => visit(child, nextScopes));
  };
  visit(ast, []);

  return [...selected.values()];
};
