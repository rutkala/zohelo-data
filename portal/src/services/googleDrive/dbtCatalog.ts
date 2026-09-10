/** Release-bound dbt Docs artifacts and display-only metadata projection. */
import { fetchDriveFileBuffer } from "./driveApi";
import { createDriveDownloadBudget, DriveDownloadBudget, sha256Hex } from "./releaseCatalog";
import type {
  BusinessCatalogueSource,
  ReleaseArtifact,
  ReleaseCatalogResolution,
  SingleReleaseResolution,
} from "./types";

export const DBT_DOCUMENTATION_LIMIT_BYTES = 16 * 1024 * 1024;

type DbtArtifactName = "manifest.json" | "catalog.json";
const BRONZE_MODELS: Record<string, string> = {
  nbp_exchange_rates_table_a: "model.zohelo_data.br_nbp_table_a",
  nbp_exchange_rates_table_b: "model.zohelo_data.br_nbp_table_b",
  nbp_exchange_rates_table_c: "model.zohelo_data.br_nbp_table_c",
  nbp_gold_prices: "model.zohelo_data.br_nbp_gold_prices",
};

const BDL_BRONZE_MODELS: readonly string[] = [
  "model.zohelo_data.br_bdl_variables",
  "model.zohelo_data.br_bdl_subjects",
  "model.zohelo_data.br_bdl_units",
  "model.zohelo_data.br_bdl_dictionary_entries",
  "model.zohelo_data.br_bdl_years",
  "model.zohelo_data.br_bdl_observations",
];

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
  release:
    | Extract<ReleaseCatalogResolution, { kind: "release" }>
    | SingleReleaseResolution,
  name: DbtArtifactName
): ReleaseArtifact {
  const matches = release.manifest.artifacts.filter((artifact) => artifact.name === name);
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
  const releases =
    resolution.releases && resolution.releases.length > 0
      ? resolution.releases
      : [resolution];

  const totalSize = releases.reduce(
    (sum, r) =>
      sum + artifactFor(r, "manifest.json").size + artifactFor(r, "catalog.json").size,
    0
  );
  if (totalSize > DBT_DOCUMENTATION_LIMIT_BYTES) {
    throw new Error("This release exceeds the 16 MiB browser documentation limit.");
  }

  const loaded = await Promise.all(
    releases.map(async (r) => {
      const manifestArtifact = artifactFor(r, "manifest.json");
      const catalogArtifact = artifactFor(r, "catalog.json");
      const [manifestBytes, catalogBytes] = await Promise.all([
        downloadArtifact(manifestArtifact, token, budget, fetchArtifact),
        downloadArtifact(catalogArtifact, token, budget, fetchArtifact),
      ]);
      return parseDbtArtifacts(manifestBytes, catalogBytes);
    })
  );

  if (loaded.length === 1) return loaded[0];

  const mergedManifest: DbtManifest = {
    ...loaded[0].manifest,
    nodes: Object.assign({}, ...loaded.map((a) => a.manifest.nodes)),
    sources: Object.assign({}, ...loaded.map((a) => a.manifest.sources)),
    docs: Object.assign({}, ...loaded.map((a) => a.manifest.docs ?? {})),
  };
  const mergedCatalog: DbtCatalog = {
    ...loaded[0].catalog,
    nodes: Object.assign({}, ...loaded.map((a) => a.catalog.nodes)),
    sources: Object.assign({}, ...loaded.map((a) => a.catalog.sources)),
  };
  return { manifest: mergedManifest, catalog: mergedCatalog };
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
  if (resolution.kind !== "release") return prepared;
  const releases =
    resolution.releases && resolution.releases.length > 0
      ? resolution.releases
      : [resolution];
  const validReleases = releases.filter(
    (r) => r.manifest.format_version === 2 && r.businessCatalogue
  );
  if (validReleases.length === 0) return prepared;

  const releasedStatusLines: string[] = [];
  let totalMetrics = 0;
  const releaseHeaders: string[] = [];

  for (const rel of validReleases) {
    const businessCatalogue = rel.businessCatalogue!;
    totalMetrics += businessCatalogue.metrics.length;
    releaseHeaders.push(
      `Release ID: \`${rel.manifest.release_id}\`  \nData producer SHA: \`${rel.manifest.code_sha}\``
    );
    const byId = new Map(businessCatalogue.sources.map((source) => [source.source_id, source]));

    if (rel.manifest.release_scope === "nbp_platform") {
      const releasedSources = Object.entries(BRONZE_MODELS).map(([sourceId, modelId]) => ({
        source: byId.get(sourceId),
        modelId,
      }));
      if (releasedSources.some(({ source }) => source === undefined)) return prepared;

      for (const { source, modelId } of releasedSources) {
        const node = prepared.nodes[modelId];
        if (!node || !source) return prepared;
        prepared.nodes[modelId] = writeIngestionMeta(node, source);
        releasedStatusLines.push(statusLine(source, modelId));
      }
    } else if (rel.manifest.release_scope === "bdl_platform") {
      const bdlSource = byId.get("gus_bdl");
      if (!bdlSource) return prepared;
      for (const modelId of BDL_BRONZE_MODELS) {
        const node = prepared.nodes[modelId];
        if (node) {
          prepared.nodes[modelId] = writeIngestionMeta(node, bdlSource);
        }
      }
      releasedStatusLines.push(statusLine(bdlSource, BDL_BRONZE_MODELS[0]));
    }
  }

  if (releasedStatusLines.length === 0) return prepared;

  const projectName =
    asString(prepared.metadata.project_name) ??
    "zohelo_data";
  const docs = { ...(prepared.docs ?? {}) };
  const existing = overviewEntry(prepared, projectName);
  const existingContents = asString(existing?.[1].block_contents) ?? "";
  const releaseContents = [
    "## Connected data release",
    "",
    ...releaseHeaders,
    "",
    "### Released source status",
    "",
    ...releasedStatusLines,
    "",
    "### Metrics",
    "",
    totalMetrics === 0
      ? "No approved metric definitions are published for this release."
      : `${totalMetrics} governed metric definition${
          totalMetrics === 1 ? " is" : "s are"
        } published for this release.`,
  ].join("\n");
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
