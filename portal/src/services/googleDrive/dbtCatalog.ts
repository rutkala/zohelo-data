/** Release-bound dbt Docs artifacts and display-only metadata projection. */
import { fetchDriveFileBuffer } from "./driveApi";
import { createDriveDownloadBudget, DriveDownloadBudget, sha256Hex } from "./releaseCatalog";
import type { BusinessCatalogueSource, ReleaseArtifact, ReleaseCatalogResolution } from "./types";

export const DBT_DOCUMENTATION_LIMIT_BYTES = 16 * 1024 * 1024;

type DbtArtifactName = "manifest.json" | "catalog.json";
const BRONZE_MODELS: Record<string, string> = {
  nbp_exchange_rates_table_a: "model.zohelo_data.br_nbp_table_a",
  nbp_exchange_rates_table_b: "model.zohelo_data.br_nbp_table_b",
  nbp_exchange_rates_table_c: "model.zohelo_data.br_nbp_table_c",
  nbp_gold_prices: "model.zohelo_data.br_nbp_gold_prices",
};

type JsonRecord = Record<string, unknown>;

export interface DbtManifest extends JsonRecord {
  metadata: JsonRecord;
  nodes: Record<string, JsonRecord>;
  sources: Record<string, JsonRecord>;
  docs?: Record<string, JsonRecord>;
}

export interface DbtCatalog extends JsonRecord {
  metadata: JsonRecord;
  nodes: Record<string, JsonRecord>;
  sources: Record<string, JsonRecord>;
}

export interface ReleaseDbtArtifacts {
  manifest: DbtManifest;
  catalog: DbtCatalog;
}

export type ReleaseArtifactFetcher = (
  fileId: string,
  token: string,
  maxBytes: number
) => Promise<Uint8Array>;

const isRecord = (value: unknown): value is JsonRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

function artifactFor(
  resolution: Extract<ReleaseCatalogResolution, { kind: "release" }>,
  name: DbtArtifactName
): ReleaseArtifact {
  const matches = resolution.manifest.artifacts.filter((artifact) => artifact.name === name);
  if (matches.length !== 1) {
    throw new Error(`The selected release must contain exactly one ${name} artifact.`);
  }
  return matches[0];
}

async function downloadArtifact(
  artifact: ReleaseArtifact,
  token: string,
  budget: DriveDownloadBudget,
  fetchArtifact: ReleaseArtifactFetcher
): Promise<Uint8Array> {
  budget.reserve(artifact.size, artifact.name);
  const bytes = await fetchArtifact(artifact.id, token, artifact.size);
  budget.consume(bytes.byteLength, artifact.name);
  if (bytes.byteLength !== artifact.size) {
    throw new Error(`${artifact.name} size does not match its release metadata.`);
  }
  if ((await sha256Hex(bytes)) !== artifact.sha256) {
    throw new Error(`${artifact.name} does not match its release SHA-256.`);
  }
  return bytes;
}

function parseJson(bytes: Uint8Array, name: string): JsonRecord {
  try {
    const value: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    if (!isRecord(value)) throw new Error();
    return value;
  } catch {
    throw new Error(`${name} is not valid JSON.`);
  }
}

function requiredRecord(
  document: JsonRecord,
  key: string,
  name: string
): Record<string, JsonRecord> {
  const value = document[key];
  if (!isRecord(value) || Object.values(value).some((entry) => !isRecord(entry))) {
    throw new Error(`${name} has an invalid ${key} section.`);
  }
  return value as Record<string, JsonRecord>;
}

function requiredMetadata(document: JsonRecord, name: string): JsonRecord {
  const metadata = document.metadata;
  if (
    !isRecord(metadata) ||
    typeof metadata.dbt_schema_version !== "string" ||
    !metadata.dbt_schema_version.trim()
  ) {
    throw new Error(`${name} is not a supported dbt artifact.`);
  }
  return metadata;
}

function parseDbtArtifacts(
  manifestBytes: Uint8Array,
  catalogBytes: Uint8Array
): ReleaseDbtArtifacts {
  const manifestRaw = parseJson(manifestBytes, "manifest.json");
  const catalogRaw = parseJson(catalogBytes, "catalog.json");
  const manifestMetadata = requiredMetadata(manifestRaw, "manifest.json");
  const catalogMetadata = requiredMetadata(catalogRaw, "catalog.json");
  const manifest: DbtManifest = {
    ...manifestRaw,
    metadata: manifestMetadata,
    nodes: requiredRecord(manifestRaw, "nodes", "manifest.json"),
    sources: requiredRecord(manifestRaw, "sources", "manifest.json"),
    ...(manifestRaw.docs === undefined
      ? {}
      : { docs: requiredRecord(manifestRaw, "docs", "manifest.json") }),
  };
  const catalog: DbtCatalog = {
    ...catalogRaw,
    metadata: catalogMetadata,
    nodes: requiredRecord(catalogRaw, "nodes", "catalog.json"),
    sources: requiredRecord(catalogRaw, "sources", "catalog.json"),
  };
  return { manifest, catalog };
}

/**
 * Fetch the exact dbt artifacts listed in the immutable release, checking both
 * declared size and SHA-256 before returning parsed JSON.
 */
export async function loadReleaseDbtArtifacts(
  resolution: ReleaseCatalogResolution,
  token: string,
  budget: DriveDownloadBudget = createDriveDownloadBudget(),
  fetchArtifact: ReleaseArtifactFetcher = fetchDriveFileBuffer
): Promise<ReleaseDbtArtifacts> {
  if (resolution.kind !== "release") {
    throw new Error("The connected data has no immutable release with dbt artifacts.");
  }
  const manifestArtifact = artifactFor(resolution, "manifest.json");
  const catalogArtifact = artifactFor(resolution, "catalog.json");
  if (manifestArtifact.size + catalogArtifact.size > DBT_DOCUMENTATION_LIMIT_BYTES) {
    throw new Error("This release exceeds the 16 MiB browser documentation limit.");
  }
  const [manifestBytes, catalogBytes] = await Promise.all([
    downloadArtifact(manifestArtifact, token, budget, fetchArtifact),
    downloadArtifact(catalogArtifact, token, budget, fetchArtifact),
  ]);
  return parseDbtArtifacts(manifestBytes, catalogBytes);
}

function cloneManifest(manifest: DbtManifest): DbtManifest {
  return JSON.parse(JSON.stringify(manifest)) as DbtManifest;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value : undefined;
}

function writeIngestionMeta(node: JsonRecord, source: BusinessCatalogueSource): JsonRecord {
  const config = isRecord(node.config) ? node.config : {};
  const existingMeta = isRecord(config.meta) ? config.meta : {};
  const nodeMeta = isRecord(node.meta) ? node.meta : {};
  const ingestionMeta = {
    ...source.provider_metadata,
    zohelo_source_id: source.source_id,
    zohelo_ingestion_status: source.status,
    zohelo_checked_through: source.checked_through,
    zohelo_latest_observation_date: source.latest_observation_date,
    zohelo_last_successful_ingestion_at: source.last_successful_ingestion_at,
    zohelo_last_attempt_at: source.last_attempt_at,
    zohelo_raw_response_count: source.raw_response_count,
  };
  return {
    ...node,
    // The bundled dbt Docs table-details directive reads node.meta directly.
    // Keep config.meta aligned for consumers of the raw dbt configuration.
    meta: { ...nodeMeta, ...ingestionMeta },
    config: {
      ...config,
      meta: { ...existingMeta, ...ingestionMeta },
    },
  };
}

function overviewEntry(
  manifest: DbtManifest,
  projectName: string
): [string, JsonRecord] | undefined {
  for (const [id, document] of Object.entries(manifest.docs ?? {})) {
    if (document.name === "__overview__" && document.package_name === projectName) {
      return [id, document];
    }
  }
  return undefined;
}

function statusLine(source: BusinessCatalogueSource, modelId: string): string {
  const checkedThrough = source.checked_through ?? "not recorded";
  const observation = source.latest_observation_date ?? "not recorded";
  return `- [${source.name}](#!/model/${encodeURIComponent(modelId)}): ${source.status}; checked through ${checkedThrough}; latest observation ${observation}.`;
}

/**
 * Return a display-only clone with release status placed on the existing bronze
 * models and appended to dbt's native overview. It adds no sources, graph edges,
 * models, or metrics.
 */
export function prepareDbtManifest(
  manifest: DbtManifest,
  resolution: ReleaseCatalogResolution
): DbtManifest {
  const prepared = cloneManifest(manifest);
  if (resolution.kind !== "release" || resolution.manifest.format_version !== 2) return prepared;
  const businessCatalogue = resolution.businessCatalogue;
  if (!businessCatalogue) return prepared;
  const sources = businessCatalogue.sources;
  const byId = new Map(sources.map((source) => [source.source_id, source]));
  const releasedSources = Object.entries(BRONZE_MODELS).map(([sourceId, modelId]) => ({
    source: byId.get(sourceId),
    modelId,
  }));
  if (releasedSources.some(({ source }) => source === undefined)) return prepared;

  for (const { source, modelId } of releasedSources) {
    const node = prepared.nodes[modelId];
    if (!node || !source) return prepared;
    prepared.nodes[modelId] = writeIngestionMeta(node, source);
  }

  const projectName =
    asString(prepared.metadata.project_name) ??
    asString(prepared.nodes[releasedSources[0].modelId]?.package_name) ??
    "zohelo_data";
  const docs = { ...(prepared.docs ?? {}) };
  const existing = overviewEntry(prepared, projectName);
  const existingContents = asString(existing?.[1].block_contents) ?? "";
  const releaseContents = [
    "## Connected data release",
    "",
    `Release ID: \`${resolution.manifest.release_id}\`  `,
    `Data producer SHA: \`${resolution.manifest.code_sha}\``,
    "",
    "### Released source status",
    "",
    ...releasedSources.map(({ source, modelId }) => statusLine(source!, modelId)),
    "",
    "### Metrics",
    "",
    businessCatalogue.metrics.length === 0
      ? "No approved metric definitions are published for this release."
      : `${businessCatalogue.metrics.length} governed metric definition${
          businessCatalogue.metrics.length === 1 ? " is" : "s are"
        } published for this release.`,
  ].join("\n");
  // dbt's bundled OverviewCtrl selects a document by name and package_name.
  // The conventional manifest key uses metadata.project_name, not a project ID.
  const id = existing?.[0] ?? `doc.${projectName}.__overview__`;
  docs[id] = {
    ...(existing?.[1] ?? {
      name: "__overview__",
      resource_type: "doc",
      package_name: projectName,
      path: "release-overview.md",
      original_file_path: "release-overview.md",
      unique_id: id,
    }),
    block_contents: [existingContents, releaseContents].filter(Boolean).join("\n\n---\n\n"),
  };
  prepared.docs = docs;
  return prepared;
}
