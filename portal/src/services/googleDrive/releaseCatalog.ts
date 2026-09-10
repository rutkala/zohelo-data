/** Immutable NBP release resolution for browser readers. */
import { DRIVE_ROOT } from "./auth";
import {
  fetchDriveFileBuffer,
  findFoldersByName,
  findNamedFilesInFolder,
  findNamedFilesInFolderById,
} from "./driveApi";
import type {
  BdlPlatformReleaseManifest,
  BusinessCatalogue,
  BusinessCatalogueLineageNode,
  BusinessCatalogueSource,
  LakehouseFile,
  PlatformReleaseManifest,
  ReleaseArtifact,
  ReleaseCatalogResolution,
  ReleaseDataset,
  ReleaseLayer,
  ReleaseManifest,
  ReleasePointer,
  SilverReleaseManifest,
  SingleReleaseResolution,
} from "./types";

export const DRIVE_DOWNLOAD_LIMIT_BYTES = 64 * 1024 * 1024;
export const POINTER_MAX_BYTES = 64 * 1024;
export const MANIFEST_MAX_BYTES = 4 * 1024 * 1024;
export const BUSINESS_CATALOGUE_MAX_BYTES = 4 * 1024 * 1024;

const V1_DATASETS = [
  "nbp_exchange_rates_table_a",
  "nbp_exchange_rates_table_b",
  "nbp_exchange_rates_table_c",
  "nbp_gold_prices",
] as const;
const V2_DATASETS: Record<string, ReleaseLayer> = {
  bronze_nbp_exchange_rates_table_a: "02_bronze",
  bronze_nbp_exchange_rates_table_b: "02_bronze",
  bronze_nbp_exchange_rates_table_c: "02_bronze",
  bronze_nbp_gold_prices: "02_bronze",
  nbp_exchange_rates_table_a: "03_silver",
  nbp_exchange_rates_table_b: "03_silver",
  nbp_exchange_rates_table_c: "03_silver",
  nbp_gold_prices: "03_silver",
  nbp_change_events: "03_silver",
  fact_fx_quotes: "04_gold",
  fact_gold_prices: "04_gold",
  dim_date: "04_gold",
  dim_currency: "04_gold",
  dim_source_table: "04_gold",
  dim_commodity: "04_gold",
};
const V2_NULL_DATE_DATASETS = new Set(["dim_currency", "dim_source_table", "dim_commodity"]);

export const BDL_DATASETS: Record<string, ReleaseLayer> = {
  bronze_bdl_variables: "02_bronze",
  bronze_bdl_subjects: "02_bronze",
  bronze_bdl_units: "02_bronze",
  bronze_bdl_dictionary_entries: "02_bronze",
  bronze_bdl_years: "02_bronze",
  bronze_bdl_observations: "02_bronze",
  bdl_variables: "03_silver",
  bdl_subjects: "03_silver",
  bdl_units: "03_silver",
  bdl_dictionary_entries: "03_silver",
  bdl_observation_revisions: "03_silver",
  bdl_observations: "03_silver",
  dim_bdl_period: "04_gold",
  dim_bdl_subject: "04_gold",
  dim_bdl_variable: "04_gold",
  dim_bdl_unit: "04_gold",
  fact_bdl_observations: "04_gold",
  mart_bdl_coverage: "04_gold",
};
export const BDL_DATED_DATASETS = new Set([
  "dim_bdl_period",
  "fact_bdl_observations",
  "mart_bdl_coverage",
]);
const V2_REQUIRED_ARTIFACTS = new Set([
  "manifest.json",
  "catalog.json",
  "run_results.json",
  "ingestion-state.json",
  "business-catalog.json",
]);

export class DriveDownloadBudget {
  private planned = 0;
  private consumed = 0;

  reserve(bytes: number, label: string) {
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      throw new Error(`Drive did not provide a valid size for ${label}.`);
    }
    if (bytes > DRIVE_DOWNLOAD_LIMIT_BYTES) {
      throw new Error(`${label} exceeds the per-file download limit of 64 MiB.`);
    }
    if (this.planned + bytes > DRIVE_DOWNLOAD_LIMIT_BYTES) {
      throw new Error(
        "The selected release exceeds the 64 MiB browser download limit for this session."
      );
    }
    this.planned += bytes;
  }

  consume(bytes: number, label: string) {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > DRIVE_DOWNLOAD_LIMIT_BYTES) {
      throw new Error(`${label} exceeded the per-file download limit of 64 MiB.`);
    }
    if (this.consumed + bytes > DRIVE_DOWNLOAD_LIMIT_BYTES) {
      throw new Error(
        "Drive downloads exceeded the 64 MiB browser download limit for this session."
      );
    }
    this.consumed += bytes;
  }

  get consumedBytes() {
    return this.consumed;
  }
}

export const createDriveDownloadBudget = () => new DriveDownloadBudget();

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const asNonEmptyString = (value: unknown, field: string) => {
  if (typeof value !== "string" || !value.trim())
    throw new Error(`Release metadata has invalid ${field}.`);
  return value;
};
const asSha256 = (value: unknown, field: string) => {
  const hash = asNonEmptyString(value, field).toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(hash)) throw new Error(`Release metadata has invalid ${field}.`);
  return hash;
};
const asPositiveInteger = (value: unknown, field: string) => {
  if (!Number.isSafeInteger(value) || (value as number) <= 0) {
    throw new Error(`Release manifest has invalid ${field}.`);
  }
  return value as number;
};
const asNonNegativeInteger = (value: unknown, field: string) => {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new Error(`Release manifest has invalid ${field}.`);
  }
  return value as number;
};
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{1,255}$/;
const CODE_SHA_RE = /^[0-9a-f]{40}$/;
const TABLE_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]{0,199}$/;
const asCanonicalUuid = (value: unknown, field: string) => {
  const id = asNonEmptyString(value, field);
  if (!UUID_RE.test(id)) throw new Error(`Release metadata has invalid ${field}.`);
  return id;
};
const asDriveId = (value: unknown, field: string) => {
  const id = asNonEmptyString(value, field);
  if (!DRIVE_ID_RE.test(id)) throw new Error(`Release metadata has invalid ${field}.`);
  return id;
};
const asIsoDate = (value: unknown, field: string) => {
  const date = asNonEmptyString(value, field);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || Number.isNaN(Date.parse(`${date}T00:00:00Z`))) {
    throw new Error(`Release manifest has invalid ${field}.`);
  }
  if (new Date(`${date}T00:00:00Z`).toISOString().slice(0, 10) !== date) {
    throw new Error(`Release manifest has invalid ${field}.`);
  }
  return date;
};
const asNullableDate = (value: unknown, field: string) =>
  value === null ? null : asIsoDate(value, field);
const asNullableTimestamp = (value: unknown, field: string) => {
  if (value === null) return null;
  const timestamp = asNonEmptyString(value, field);
  if (Number.isNaN(Date.parse(timestamp))) {
    throw new Error(`Business catalogue has invalid ${field}.`);
  }
  return timestamp;
};

export const sha256Hex = async (bytes: Uint8Array): Promise<string> => {
  if (!globalThis.crypto?.subtle)
    throw new Error("This browser cannot verify SHA-256 release files.");
  const input = new Uint8Array(bytes.byteLength);
  input.set(bytes);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", input);
  return Array.from(new Uint8Array(digest), (value) => value.toString(16).padStart(2, "0")).join(
    ""
  );
};

function parsePointer(bytes: Uint8Array): ReleasePointer {
  let raw: unknown;
  try {
    raw = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error("current-release.json is not valid JSON.");
  }
  if (!isRecord(raw) || raw.format_version !== 1) {
    throw new Error("current-release.json has an unsupported format_version.");
  }
  const pointer: ReleasePointer = {
    format_version: 1,
    release_id: asCanonicalUuid(raw.release_id, "release_id"),
    manifest_file_id: asDriveId(raw.manifest_file_id, "manifest_file_id"),
    manifest_sha256: asSha256(raw.manifest_sha256, "manifest_sha256"),
    updated_at_utc: asNonEmptyString(raw.updated_at_utc, "updated_at_utc"),
  };
  if (raw.previous_manifest_file_id !== undefined) {
    pointer.previous_manifest_file_id = asDriveId(
      raw.previous_manifest_file_id,
      "previous_manifest_file_id"
    );
  }
  return pointer;
}

function parseFile(raw: unknown, datasetId: string, layer: ReleaseLayer): LakehouseFile {
  if (!isRecord(raw)) throw new Error(`Release manifest has invalid file for '${datasetId}'.`);
  return {
    id: asDriveId(raw.id, "file.id"),
    name: asNonEmptyString(raw.name, "file.name"),
    size: asPositiveInteger(raw.size, "file.size"),
    sha256: asSha256(raw.sha256, "file.sha256"),
    tableName: datasetId,
    layer,
  };
}

function parseDataset(
  raw: unknown,
  scope: "nbp_silver" | "nbp_platform" | "bdl_platform"
): ReleaseDataset {
  if (!isRecord(raw)) throw new Error("Release manifest has an invalid dataset.");
  const dataset_id = asNonEmptyString(raw.dataset_id, "dataset_id");
  const expectedLayer =
    scope === "nbp_silver"
      ? "03_silver"
      : scope === "nbp_platform"
        ? V2_DATASETS[dataset_id]
        : BDL_DATASETS[dataset_id];
  if (!expectedLayer || raw.layer !== expectedLayer) {
    throw new Error(`Release dataset '${dataset_id}' has an invalid layer.`);
  }
  if (!Array.isArray(raw.columns) || raw.columns.length === 0) {
    throw new Error(`Release dataset '${dataset_id}' has invalid columns.`);
  }
  const columnNames = new Set<string>();
  for (const column of raw.columns) {
    if (
      !isRecord(column) ||
      typeof column.name !== "string" ||
      !column.name.trim() ||
      typeof column.type !== "string" ||
      !column.type.trim() ||
      columnNames.has(column.name)
    ) {
      throw new Error(`Release dataset '${dataset_id}' has invalid columns.`);
    }
    columnNames.add(column.name);
  }
  if (!Array.isArray(raw.files) || raw.files.length === 0) {
    throw new Error(`Release dataset '${dataset_id}' has no files.`);
  }
  const row_count =
    scope === "nbp_platform"
      ? asNonNegativeInteger(raw.row_count, "row_count")
      : asPositiveInteger(raw.row_count, "row_count");
  if (scope === "nbp_platform" && row_count === 0 && dataset_id !== "nbp_change_events") {
    throw new Error(`Release dataset '${dataset_id}' may not have zero rows.`);
  }
  const min_date =
    scope === "nbp_silver"
      ? asIsoDate(raw.min_date, "min_date")
      : asNullableDate(raw.min_date, "min_date");
  const max_date =
    scope === "nbp_silver"
      ? asIsoDate(raw.max_date, "max_date")
      : asNullableDate(raw.max_date, "max_date");
  if ((min_date === null) !== (max_date === null)) {
    throw new Error(`Release dataset '${dataset_id}' must provide both date bounds or neither.`);
  }
  if (
    min_date === null &&
    scope === "nbp_platform" &&
    !V2_NULL_DATE_DATASETS.has(dataset_id) &&
    row_count !== 0
  ) {
    throw new Error(`Release dataset '${dataset_id}' may not omit date bounds.`);
  }
  if (
    scope === "bdl_platform" &&
    BDL_DATED_DATASETS.has(dataset_id) &&
    min_date === null
  ) {
    throw new Error(`Release dataset '${dataset_id}' may not omit date bounds.`);
  }
  if (
    scope === "bdl_platform" &&
    !BDL_DATED_DATASETS.has(dataset_id) &&
    min_date !== null
  ) {
    throw new Error(`Release dataset '${dataset_id}' must not declare date bounds.`);
  }
  if (min_date !== null && max_date !== null && min_date > max_date) {
    throw new Error(`Release dataset '${dataset_id}' has inverted date bounds.`);
  }
  const table_name = asNonEmptyString(raw.table_name, "table_name");
  if (!TABLE_NAME_RE.test(table_name)) {
    throw new Error(`Release dataset '${dataset_id}' has an invalid table_name.`);
  }
  return {
    dataset_id,
    layer: expectedLayer,
    table_name,
    row_count,
    min_date,
    max_date,
    columns: raw.columns.map((column) => ({
      name: column.name as string,
      type: column.type as string,
    })),
    files: raw.files.map((file) => parseFile(file, dataset_id, expectedLayer)),
  };
}

function parseArtifact(raw: unknown): ReleaseArtifact {
  if (!isRecord(raw)) throw new Error("The selected release manifest has an invalid artifact.");
  return {
    id: asDriveId(raw.id, "artifact.id"),
    name: asNonEmptyString(raw.name, "artifact.name"),
    size: asPositiveInteger(raw.size, "artifact.size"),
    sha256: asSha256(raw.sha256, "artifact.sha256"),
  };
}

function parseArtifacts(raw: unknown, formatVersion: 1 | 2) {
  if (!Array.isArray(raw)) {
    throw new Error("The selected release manifest has invalid artifacts.");
  }
  const artifacts = raw.map(parseArtifact);
  const names = new Set<string>();
  for (const artifact of artifacts) {
    if (names.has(artifact.name)) {
      throw new Error("The selected release manifest has duplicate artifact names.");
    }
    names.add(artifact.name);
  }
  if (formatVersion === 2 && [...V2_REQUIRED_ARTIFACTS].some((name) => !names.has(name))) {
    throw new Error("The selected platform release is missing required artifacts.");
  }
  return artifacts;
}

function parseManifest(bytes: Uint8Array, pointer: ReleasePointer): ReleaseManifest {
  let raw: unknown;
  try {
    raw = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error("The selected release manifest is not valid JSON.");
  }
  if (!isRecord(raw) || (raw.format_version !== 1 && raw.format_version !== 2)) {
    throw new Error("The selected release manifest has an unsupported format_version.");
  }
  if (asCanonicalUuid(raw.release_id, "release_id") !== pointer.release_id) {
    throw new Error("The selected release manifest does not match current-release.json.");
  }
  const format_version = raw.format_version;
  const rawScope = raw.release_scope;
  if (
    (format_version === 1 && rawScope !== "nbp_silver") ||
    (format_version === 2 && rawScope !== "nbp_platform" && rawScope !== "bdl_platform") ||
    raw.status !== "validated"
  ) {
    throw new Error(
      format_version === 1
        ? "The selected release is not a validated nbp_silver release."
        : "The selected release is not a validated platform release."
    );
  }
  const scope: "nbp_silver" | "nbp_platform" | "bdl_platform" = rawScope as
    | "nbp_silver"
    | "nbp_platform"
    | "bdl_platform";
  if (!isRecord(raw.tests) || raw.tests.passed !== true) {
    throw new Error("The selected release does not have passing tests.");
  }
  if (!Array.isArray(raw.datasets))
    throw new Error("The selected release manifest has no datasets.");
  const datasets = raw.datasets.map((dataset) => parseDataset(dataset, scope));
  const ids = new Set(datasets.map((dataset) => dataset.dataset_id));
  const requiredIds =
    scope === "nbp_silver"
      ? V1_DATASETS
      : scope === "nbp_platform"
        ? Object.keys(V2_DATASETS)
        : Object.keys(BDL_DATASETS);
  if (
    ids.size !== datasets.length ||
    ids.size !== requiredIds.length ||
    requiredIds.some((id) => !ids.has(id))
  ) {
    throw new Error(
      scope === "nbp_silver"
        ? "The selected release must contain exactly the four required NBP silver datasets."
        : scope === "nbp_platform"
          ? "The selected platform release must contain exactly the 15 required NBP datasets."
          : "The selected platform release must contain exactly the 18 required BDL datasets."
    );
  }
  const fileIds = new Set<string>();
  for (const dataset of datasets) {
    for (const file of dataset.files) {
      if (fileIds.has(file.id)) {
        throw new Error("The selected release reuses a file ID across datasets.");
      }
      fileIds.add(file.id);
    }
  }
  const artifacts = parseArtifacts(raw.artifacts, format_version);
  for (const artifact of artifacts) {
    if (fileIds.has(artifact.id)) {
      throw new Error("The selected release reuses a file ID across data and artifacts.");
    }
    fileIds.add(artifact.id);
  }
  if (!Array.isArray(raw.inputs)) {
    throw new Error("The selected release manifest has invalid inputs.");
  }
  const code_sha = asNonEmptyString(raw.code_sha, "code_sha");
  if (!CODE_SHA_RE.test(code_sha)) throw new Error("Release metadata has invalid code_sha.");
  const base = {
    release_id: pointer.release_id,
    status: "validated" as const,
    code_sha,
    created_at_utc: asNonEmptyString(raw.created_at_utc, "created_at_utc"),
    datasets,
    artifacts,
    inputs: raw.inputs,
    tests: { passed: true } as const,
  };
  if (format_version === 1) {
    return {
      ...base,
      format_version: 1,
      release_scope: "nbp_silver",
    } satisfies SilverReleaseManifest;
  }
  if (scope === "nbp_platform") {
    return {
      ...base,
      format_version: 2,
      release_scope: "nbp_platform",
    } satisfies PlatformReleaseManifest;
  }
  return {
    ...base,
    format_version: 2,
    release_scope: "bdl_platform",
  } satisfies BdlPlatformReleaseManifest;
}

function parseBusinessSource(raw: unknown): BusinessCatalogueSource {
  if (!isRecord(raw)) throw new Error("Business catalogue has an invalid source.");
  const providerMetadata: Record<string, string> = {};
  for (const field of [
    "provider_url",
    "documentation_url",
    "publication_schedule_url",
    "frequency",
    "coverage_start",
    "reuse_terms_url",
    "reuse_summary",
    "quote_unit",
    "methodology_notes",
  ]) {
    if (raw[field] !== undefined)
      providerMetadata[field] = asNonEmptyString(raw[field], `source.${field}`);
  }
  return {
    source_id: asNonEmptyString(raw.source_id, "source_id"),
    name: asNonEmptyString(raw.name, "source.name"),
    description: asNonEmptyString(raw.description, "source.description"),
    status: asNonEmptyString(raw.status, "source.status"),
    checked_through: asNullableDate(raw.checked_through, "source.checked_through"),
    latest_observation_date: asNullableDate(
      raw.latest_observation_date,
      "source.latest_observation_date"
    ),
    last_successful_ingestion_at: asNullableTimestamp(
      raw.last_successful_ingestion_at,
      "source.last_successful_ingestion_at"
    ),
    last_attempt_at: asNullableTimestamp(raw.last_attempt_at, "source.last_attempt_at"),
    raw_response_count: asNonNegativeInteger(raw.raw_response_count, "source.raw_response_count"),
    ...(Object.keys(providerMetadata).length ? { provider_metadata: providerMetadata } : {}),
  };
}

function parseLineageNode(raw: unknown): BusinessCatalogueLineageNode {
  if (!isRecord(raw)) throw new Error("Business catalogue has an invalid lineage node.");
  return {
    id: asNonEmptyString(raw.id, "lineage.node.id"),
    label: asNonEmptyString(raw.label, "lineage.node.label"),
    kind: asNonEmptyString(raw.kind, "lineage.node.kind"),
    layer: asNonEmptyString(raw.layer, "lineage.node.layer"),
    description: asNonEmptyString(raw.description, "lineage.node.description"),
  };
}

function parseBusinessCatalogue(
  bytes: Uint8Array,
  manifest: PlatformReleaseManifest | BdlPlatformReleaseManifest
): BusinessCatalogue {
  let raw: unknown;
  try {
    raw = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error("business-catalog.json is not valid JSON.");
  }
  if (!isRecord(raw) || raw.format_version !== 1) {
    throw new Error("business-catalog.json has an unsupported format_version.");
  }
  if (asNonEmptyString(raw.code_sha, "business catalogue code_sha") !== manifest.code_sha) {
    throw new Error("business-catalog.json does not match the selected release code SHA.");
  }
  if (!Array.isArray(raw.sources) || !isRecord(raw.lineage) || !Array.isArray(raw.metrics)) {
    throw new Error("business-catalog.json has invalid sources, lineage, or metrics.");
  }
  const sources = raw.sources.map(parseBusinessSource);
  const sourceIds = new Set<string>();
  for (const source of sources) {
    if (sourceIds.has(source.source_id))
      throw new Error("business-catalog.json has duplicate source IDs.");
    sourceIds.add(source.source_id);
  }
  if (!Array.isArray(raw.lineage.nodes) || !Array.isArray(raw.lineage.edges)) {
    throw new Error("business-catalog.json has invalid lineage.");
  }
  const nodes = raw.lineage.nodes.map(parseLineageNode);
  const nodeIds = new Set<string>();
  for (const node of nodes) {
    if (nodeIds.has(node.id))
      throw new Error("business-catalog.json has duplicate lineage node IDs.");
    nodeIds.add(node.id);
  }
  const edges = raw.lineage.edges.map((edge) => {
    if (!isRecord(edge)) throw new Error("business-catalog.json has an invalid lineage edge.");
    const from = asNonEmptyString(edge.from, "lineage.edge.from");
    const to = asNonEmptyString(edge.to, "lineage.edge.to");
    if (!nodeIds.has(from) || !nodeIds.has(to)) {
      throw new Error("business-catalog.json has an edge that does not reference a lineage node.");
    }
    return { from, to };
  });
  return {
    format_version: 1,
    code_sha: manifest.code_sha,
    sources,
    lineage: { nodes, edges },
    metrics: raw.metrics.map((metric) => {
      if (!isRecord(metric)) throw new Error("business-catalog.json has an invalid metric.");
      return metric;
    }),
  };
}

async function downloadExact(
  id: string,
  declaredSize: number | undefined,
  maxBytes: number,
  label: string,
  token: string,
  budget: DriveDownloadBudget
) {
  if (declaredSize === undefined || declaredSize > maxBytes) {
    throw new Error(`${label} is missing a safe declared size.`);
  }
  budget.reserve(declaredSize, label);
  const bytes = await fetchDriveFileBuffer(id, token, declaredSize);
  budget.consume(bytes.byteLength, label);
  if (bytes.byteLength !== declaredSize) {
    throw new Error(`${label} size does not match its Drive metadata.`);
  }
  return bytes;
}

async function resolveSingleRelease(
  pointerFile: { id: string; name: string; size?: number },
  pointerLabel: string,
  token: string,
  budget: DriveDownloadBudget
): Promise<SingleReleaseResolution> {
  const pointerBytes = await downloadExact(
    pointerFile.id,
    pointerFile.size,
    POINTER_MAX_BYTES,
    pointerLabel,
    token,
    budget
  );
  const pointer = parsePointer(pointerBytes);
  const manifestMetadata = await findNamedFilesInFolderById(pointer.manifest_file_id, token);
  const manifestBytes = await downloadExact(
    pointer.manifest_file_id,
    manifestMetadata?.size,
    MANIFEST_MAX_BYTES,
    "release manifest",
    token,
    budget
  );
  if ((await sha256Hex(manifestBytes)) !== pointer.manifest_sha256) {
    throw new Error("The selected release manifest does not match the pointer SHA-256.");
  }
  const manifest = parseManifest(manifestBytes, pointer);
  const resolved: SingleReleaseResolution = {
    pointer,
    manifest,
    manifestFileId: pointer.manifest_file_id,
    fingerprint: `${pointer.release_id}:${pointer.manifest_file_id}:${pointer.manifest_sha256}`,
  };
  if (manifest.format_version !== 2) return resolved;

  const catalogueArtifact = manifest.artifacts.find(
    (artifact) => artifact.name === "business-catalog.json"
  );
  if (!catalogueArtifact) {
    throw new Error("The selected platform release is missing business-catalog.json.");
  }
  const catalogueBytes = await downloadExact(
    catalogueArtifact.id,
    catalogueArtifact.size,
    BUSINESS_CATALOGUE_MAX_BYTES,
    "business-catalog.json",
    token,
    budget
  );
  if ((await sha256Hex(catalogueBytes)) !== catalogueArtifact.sha256) {
    throw new Error("business-catalog.json does not match its release SHA-256.");
  }
  return { ...resolved, businessCatalogue: parseBusinessCatalogue(catalogueBytes, manifest) };
}

export async function resolveReleaseCatalog(
  token: string,
  budget: DriveDownloadBudget
): Promise<ReleaseCatalogResolution> {
  const roots = await findFoldersByName(DRIVE_ROOT, "root", token);
  if (roots.length === 0) {
    throw new Error(`Master Lakehouse folder '${DRIVE_ROOT}' not found in Google Drive root.`);
  }
  if (roots.length !== 1) {
    throw new Error(`Master Lakehouse folder '${DRIVE_ROOT}' is ambiguous in Google Drive root.`);
  }
  const rootId = roots[0].id;
  const rootPointerFiles = await findNamedFilesInFolder("current-release.json", rootId, token);
  if (
    rootPointerFiles.length > 1 ||
    (rootPointerFiles.length === 1 &&
      rootPointerFiles[0].mimeType === "application/vnd.google-apps.folder")
  ) {
    throw new Error(
      "current-release.json is ambiguous or not a file; refusing to select a release."
    );
  }

  const bdlFolders = (await findFoldersByName("bdl-platform", rootId, token)).filter(
    (folder) => folder.name === "bdl-platform"
  );
  let bdlPointerFile: { id: string; name: string; size?: number; mimeType?: string } | undefined;
  if (bdlFolders.length === 1) {
    const bdlPointerFiles = await findNamedFilesInFolder(
      "current-release.json",
      bdlFolders[0].id,
      token
    );
    if (
      bdlPointerFiles.length === 1 &&
      bdlPointerFiles[0].mimeType !== "application/vnd.google-apps.folder"
    ) {
      bdlPointerFile = bdlPointerFiles[0];
    } else if (bdlPointerFiles.length > 1) {
      throw new Error(
        "bdl-platform/current-release.json is ambiguous; refusing to select a release."
      );
    }
  } else if (bdlFolders.length > 1) {
    throw new Error("Folder 'bdl-platform' is ambiguous; refusing to select a release.");
  }

  if (rootPointerFiles.length === 0 && !bdlPointerFile) return { kind: "legacy" };

  const releases: SingleReleaseResolution[] = [];
  if (rootPointerFiles.length === 1) {
    releases.push(
      await resolveSingleRelease(rootPointerFiles[0], "current-release.json", token, budget)
    );
  }
  if (bdlPointerFile) {
    releases.push(
      await resolveSingleRelease(
        bdlPointerFile,
        "bdl-platform/current-release.json",
        token,
        budget
      )
    );
  }

  const primary = releases[0];
  const fingerprint =
    releases.length === 1
      ? primary.fingerprint
      : releases.map((r) => r.fingerprint).sort().join(";");

  return {
    kind: "release",
    pointer: primary.pointer,
    manifest: primary.manifest,
    manifestFileId: primary.manifestFileId,
    fingerprint,
    businessCatalogue: primary.businessCatalogue,
    releases,
  };
}
