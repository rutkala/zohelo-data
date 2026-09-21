/** Strict, source-scoped Landing snapshot resolution for browser readers. */
import { DRIVE_ROOT } from "./auth";
import {
  fetchDriveFileBuffer,
  findFoldersByName,
  findNamedFilesInFolder,
  findNamedFilesInFolderById,
  isGoogleDriveAuthError,
} from "./driveApi";
import { POINTER_MAX_BYTES, DriveDownloadBudget, sha256Hex } from "./releaseCatalog";
import type {
  BronzeCampaignSourceId,
  BulkLandingSourceId,
  LandingCatalogIssue,
  LandingCatalogResolution,
  LandingSnapshotManifest,
  LandingSnapshotPointer,
  LandingSnapshotResolution,
  LandingSourceId,
  LakehouseFile,
  RetainedBronzeIndicator,
} from "./types";

export const LANDING_SOURCE_IDS = [
  "world_bank_wdi",
  "gus_bdl",
  "eurostat",
  "world_bank_wdi_bulk",
  "eurostat_bulk",
  "opendata_org_bulk",
  "opendata_org_bronze",
  "opendata_org_locations_bronze",
  "opendata_org_people_bronze",
  "gus_dbw_retained_bronze",
] as const;

const LANDING_COLUMNS = [
  ["source_id", "VARCHAR"],
  ["task_id", "VARCHAR"],
  ["lane", "VARCHAR"],
  ["task_kind", "VARCHAR"],
  ["retrieved_at_utc", "TIMESTAMP"],
  ["record_count", "BIGINT"],
  ["raw_sha256", "VARCHAR"],
  ["raw_size_bytes", "BIGINT"],
  ["request_json", "VARCHAR"],
  ["metadata_json", "VARCHAR"],
  ["payload_utf8", "VARCHAR"],
  ["content_type", "VARCHAR"],
] as const;

const BULK_INDEX_COLUMNS = [
  ["dataset_id", "VARCHAR"],
  ["source_id", "VARCHAR"],
  ["version", "VARCHAR"],
  ["kind", "VARCHAR"],
  ["retrieved_at_utc", "TIMESTAMP"],
  ["raw_file_id", "VARCHAR"],
  ["raw_file_name", "VARCHAR"],
  ["raw_size_bytes", "BIGINT"],
  ["raw_sha256", "VARCHAR"],
  ["request_json", "VARCHAR"],
  ["inspection_json", "VARCHAR"],
] as const;

const OPENDATA_ORGANIZATION_COLUMNS = [
  ["record_id", "VARCHAR"],
  ["data_source", "VARCHAR"],
  ["bq_dataset", "VARCHAR"],
  ["name_org", "VARCHAR"],
  ["name_type", "VARCHAR"],
  ["record_type", "VARCHAR"],
  ["addr_line1", "VARCHAR"],
  ["addr_city", "VARCHAR"],
  ["addr_state", "VARCHAR"],
  ["addr_postal_code", "VARCHAR"],
  ["addr_country", "VARCHAR"],
  ["addr_type", "VARCHAR"],
  ["geo_latitude", "DOUBLE"],
  ["geo_longitude", "DOUBLE"],
  ["placekey", "VARCHAR"],
  ["bq_id", "VARCHAR"],
  ["rel_anchor_domain", "VARCHAR"],
  ["rel_anchor_key", "VARCHAR"],
] as const;

const OPENDATA_LOCATION_COLUMNS = [
  ["record_id", "VARCHAR"],
  ["data_source", "VARCHAR"],
  ["bq_dataset", "VARCHAR"],
  ["name_full", "VARCHAR"],
  ["record_type", "VARCHAR"],
  ["addr_line1", "VARCHAR"],
  ["addr_city", "VARCHAR"],
  ["addr_state", "VARCHAR"],
  ["addr_postal_code", "VARCHAR"],
  ["addr_country", "VARCHAR"],
  ["geo_latitude", "DOUBLE"],
  ["geo_longitude", "DOUBLE"],
  ["placekey", "VARCHAR"],
  ["bq_id", "VARCHAR"],
] as const;

const OPENDATA_PEOPLE_COLUMNS = [
  ["record_id", "VARCHAR"],
  ["data_source", "VARCHAR"],
  ["bq_dataset", "VARCHAR"],
  ["name_full", "VARCHAR"],
  ["name_first", "VARCHAR"],
  ["name_last", "VARCHAR"],
  ["record_type", "VARCHAR"],
  ["addr_country", "VARCHAR"],
  ["group_assn_id_number", "VARCHAR"],
  ["group_assn_id_type", "VARCHAR"],
  ["rel_pointer_domain", "VARCHAR"],
  ["rel_pointer_key", "VARCHAR"],
  ["rel_pointer_role", "VARCHAR"],
  ["linkedin", "VARCHAR"],
] as const;

const DBW_OBSERVATION_COLUMNS = [
  ["indicator_id", "BIGINT"], ["przekroj_id", "BIGINT"],
  ...Array.from({ length: 9 }, (_, i) => [[`wymiar_${i + 1}`, "BIGINT"], [`pozycja_${i + 1}`, "BIGINT"]]).flat(),
  ["okres_id", "INTEGER"], ["sposob_prezentacji_miara_id", "INTEGER"],
  ["period_year", "INTEGER"], ["wartosc_raw", "VARCHAR"], ["wartosc_numeric", "DOUBLE"],
  ["precyzja", "INTEGER"], ["brak_wartosci_id", "INTEGER"], ["tajnosci_id", "INTEGER"],
  ["flaga_id", "INTEGER"], ["raw_archive_file", "VARCHAR"], ["source_row_number", "BIGINT"],
  ["processed_at_utc", "VARCHAR"],
] as const;
const DBW_DATASET_COLUMNS: Record<string, readonly (readonly string[])[]> = {
  observations: DBW_OBSERVATION_COLUMNS,
  dictionaries: [["indicator_id", "BIGINT"], ["column_name", "VARCHAR"], ["dictionary_name", "VARCHAR"],
    ["element_id", "BIGINT"], ["element_name", "VARCHAR"], ["processed_at_utc", "VARCHAR"]],
  metadata: [["indicator_id", "BIGINT"], ["metric_name", "VARCHAR"], ["metric_name_en", "VARCHAR"],
    ["description", "VARCHAR"], ["frequency", "VARCHAR"], ["measure_unit", "VARCHAR"],
    ["data_source", "VARCHAR"], ["legal_basis", "VARCHAR"], ["last_update", "VARCHAR"], ["processed_at_utc", "VARCHAR"]],
  taxonomy: [["indicator_id", "BIGINT"], ["indicator_name", "VARCHAR"], ["indicator_name_en", "VARCHAR"],
    ["thematic_area", "VARCHAR"], ["domain", "VARCHAR"], ["taxonomy_path", "VARCHAR"],
    ["node_id", "VARCHAR"], ["parent_id", "VARCHAR"], ["processed_at_utc", "VARCHAR"]],
};

const bronzeColumnsForSource = (sourceId: BronzeCampaignSourceId) => {
  if (sourceId === "opendata_org_locations_bronze") return OPENDATA_LOCATION_COLUMNS;
  if (sourceId === "opendata_org_people_bronze") return OPENDATA_PEOPLE_COLUMNS;
  return OPENDATA_ORGANIZATION_COLUMNS;
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA256_RE = /^[0-9a-f]{64}$/;
const CODE_SHA_RE = /^[0-9a-f]{40}$/;
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{1,255}$/;
const PARQUET_NAME_RE =
  /^(fragment-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|br_[A-Za-z0-9_-]+)\.parquet$/;
const LANDING_MANIFEST_MAX_BYTES = 1024 * 1024;
const LANDING_FILE_MAX_BYTES = 8 * 1024 * 1024;
const BRONZE_FILE_MAX_BYTES = 32 * 1024 * 1024;
const POINTER_FIELDS = new Set([
  "format_version",
  "source_id",
  "snapshot_id",
  "manifest_file_id",
  "manifest_file_name",
  "manifest_sha256",
  "manifest_size_bytes",
]);
const LANDING_MANIFEST_FIELDS = new Set([
  "format_version",
  "kind",
  "source_id",
  "snapshot_id",
  "created_at_utc",
  "code_sha",
  "status",
  "layer",
  "table_name",
  "row_count",
  "coverage_status",
  "files",
  "columns",
  "accepted_response_count",
  "published_response_count",
  "pending_publication_count",
  "receipt_checkpoint_sha256",
  "tests",
]);
const BULK_MANIFEST_FIELDS = new Set([
  "format_version",
  "kind",
  "source_id",
  "snapshot_id",
  "created_at_utc",
  "code_sha",
  "status",
  "layer",
  "table_name",
  "row_count",
  "coverage_status",
  "files",
  "columns",
  "accepted_distribution_count",
  "published_distribution_count",
  "pending_publication_count",
  "receipt_checkpoint_sha256",
  "tests",
]);
const BRONZE_MANIFEST_FIELDS = new Set([
  "format_version",
  "kind",
  "source_id",
  "snapshot_id",
  "created_at_utc",
  "code_sha",
  "status",
  "layer",
  "table_name",
  "row_count",
  "coverage_status",
  "files",
  "columns",
  "accepted_file_count",
  "published_file_count",
  "pending_publication_count",
  "receipt_checkpoint_sha256",
  "tests",
]);
const RETAINED_BRONZE_MANIFEST_FIELDS = new Set([
  "format_version", "kind", "source_id", "snapshot_id", "created_at_utc", "code_sha",
  "status", "layer", "coverage_status", "lineage_status", "inventory_sha256",
  "audit_report_sha256", "audit_run_id", "indicator_count", "published_indicator_count",
  "pending_indicator_count", "indicator_index", "datasets", "observation_schema", "tests",
]);

const isBulkSource = (
  sourceId: LandingSourceId
): sourceId is BulkLandingSourceId => sourceId.endsWith("_bulk");

const isBronzeSource = (
  sourceId: LandingSourceId
): sourceId is BronzeCampaignSourceId => sourceId.endsWith("_bronze");

const tableNameForSource = (sourceId: LandingSourceId): string => {
  if (sourceId === "gus_dbw_retained_bronze") return "br_dbw_observations";
  if (sourceId === "opendata_org_bronze") return "br_opendata_organizations";
  if (sourceId === "opendata_org_locations_bronze") return "br_opendata_locations";
  if (sourceId === "opendata_org_people_bronze") return "br_opendata_people";
  if (isBulkSource(sourceId)) return `${sourceId.slice(0, -"_bulk".length)}_distributions`;
  return `${sourceId}_responses`;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const hasExactFields = (value: Record<string, unknown>, fields: ReadonlySet<string>) =>
  Object.keys(value).length === fields.size && Object.keys(value).every((key) => fields.has(key));

const requiredString = (value: unknown, field: string): string => {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`Landing metadata has invalid ${field}.`);
  }
  return value;
};

const requiredInteger = (value: unknown, field: string, allowZero = true): number => {
  if (
    !Number.isSafeInteger(value) ||
    (allowZero ? (value as number) < 0 : (value as number) <= 0)
  ) {
    throw new Error(`Landing metadata has invalid ${field}.`);
  }
  return value as number;
};

const requiredSha256 = (value: unknown, field: string): string => {
  const digest = requiredString(value, field);
  if (!SHA256_RE.test(digest)) throw new Error(`Landing metadata has invalid ${field}.`);
  return digest;
};

const requiredDriveId = (value: unknown, field: string): string => {
  const id = requiredString(value, field);
  if (!DRIVE_ID_RE.test(id)) throw new Error(`Landing metadata has invalid ${field}.`);
  return id;
};

const requiredSnapshotId = (value: unknown): string => {
  const snapshotId = requiredString(value, "snapshot_id");
  if (!UUID_RE.test(snapshotId)) throw new Error("Landing metadata has invalid snapshot_id.");
  return snapshotId;
};

const requiredTimestamp = (value: unknown, field: string): string => {
  const timestamp = requiredString(value, field);
  if (Number.isNaN(Date.parse(timestamp))) {
    throw new Error(`Landing metadata has invalid ${field}.`);
  }
  return timestamp;
};

const decodeJson = (bytes: Uint8Array, label: string): unknown => {
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error(`${label} is not valid JSON.`);
  }
};

async function downloadExact(
  id: string,
  declaredSize: number | undefined,
  maximum: number,
  label: string,
  token: string,
  budget: DriveDownloadBudget
): Promise<Uint8Array> {
  if (
    declaredSize === undefined ||
    !Number.isSafeInteger(declaredSize) ||
    declaredSize <= 0 ||
    declaredSize > maximum
  ) {
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

function parsePointer(bytes: Uint8Array, sourceId: LandingSourceId): LandingSnapshotPointer {
  const raw = decodeJson(bytes, `${sourceId}/current-landing.json`);
  if (!isRecord(raw) || !hasExactFields(raw, POINTER_FIELDS) || raw.format_version !== 1) {
    throw new Error("current-landing.json has an unsupported format_version.");
  }
  if (raw.source_id !== sourceId) {
    throw new Error("current-landing.json does not match its source folder.");
  }
  const snapshotId = requiredSnapshotId(raw.snapshot_id);
  const manifestFileName = requiredString(raw.manifest_file_name, "manifest_file_name");
  if (manifestFileName !== `manifest-${snapshotId}.json`) {
    throw new Error("current-landing.json manifest name does not match its snapshot_id.");
  }
  const manifestSize = requiredInteger(raw.manifest_size_bytes, "manifest_size_bytes", false);
  if (manifestSize > LANDING_MANIFEST_MAX_BYTES) {
    throw new Error("Landing manifest exceeds the 1 MiB metadata limit.");
  }
  return {
    format_version: 1,
    source_id: sourceId,
    snapshot_id: snapshotId,
    manifest_file_id: requiredDriveId(raw.manifest_file_id, "manifest_file_id"),
    manifest_file_name: manifestFileName,
    manifest_sha256: requiredSha256(raw.manifest_sha256, "manifest_sha256"),
    manifest_size_bytes: manifestSize,
  };
}

function parseFile(
  raw: unknown,
  tableName: string,
  layer: "01_landing" | "02_bronze" = "01_landing",
  maxBytes: number = LANDING_FILE_MAX_BYTES
): LakehouseFile {
  if (!isRecord(raw) || !hasExactFields(raw, new Set(["id", "name", "size", "sha256"]))) {
    throw new Error("Landing manifest has an invalid file.");
  }
  const name = requiredString(raw.name, "file.name");
  if (!PARQUET_NAME_RE.test(name)) {
    throw new Error("Landing snapshot files must be named Parquet files.");
  }
  const size = requiredInteger(raw.size, "file.size", false);
  if (size > maxBytes) {
    throw new Error(`Landing file '${name}' exceeds the ${Math.round(maxBytes / (1024 * 1024))} MiB publication limit.`);
  }
  return {
    id: requiredDriveId(raw.id, "file.id"),
    name,
    size,
    sha256: requiredSha256(raw.sha256, "file.sha256"),
    tableName,
    layer,
    mimeType: "application/vnd.apache.parquet",
  };
}

function parseManifest(
  bytes: Uint8Array,
  pointer: LandingSnapshotPointer
): LandingSnapshotManifest {
  const raw = decodeJson(bytes, `${pointer.source_id} Landing manifest`);
  if (pointer.source_id === "gus_dbw_retained_bronze") {
    return parseRetainedBronzeManifest(raw, pointer);
  }
  const bulk = isBulkSource(pointer.source_id);
  const bronze = isBronzeSource(pointer.source_id);
  const expectedFields = bronze
    ? BRONZE_MANIFEST_FIELDS
    : bulk
      ? BULK_MANIFEST_FIELDS
      : LANDING_MANIFEST_FIELDS;
  if (
    !isRecord(raw) ||
    !hasExactFields(raw, expectedFields) ||
    raw.format_version !== (bulk ? 2 : 1) ||
    raw.kind !==
      (bronze ? "bronze_snapshot" : bulk ? "full_distribution_index" : "landing_snapshot") ||
    raw.source_id !== pointer.source_id ||
    raw.snapshot_id !== pointer.snapshot_id
  ) {
    throw new Error("Landing manifest does not match its source pointer.");
  }
  const expectedLayer = bronze ? "02_bronze" : "01_landing";
  if (raw.status !== "validated" || raw.layer !== expectedLayer) {
    throw new Error(`Landing manifest is not a validated ${expectedLayer} snapshot.`);
  }
  const expectedTableName = tableNameForSource(pointer.source_id);
  if (raw.table_name !== expectedTableName) {
    throw new Error("Landing manifest has an invalid table_name.");
  }
  if (
    (!bulk && !bronze && raw.coverage_status !== "incomplete") ||
    ((bulk || bronze) &&
      raw.coverage_status !== "incomplete" &&
      raw.coverage_status !== "complete_current_catalogue")
  ) {
    throw new Error("Landing manifest must identify its coverage as incomplete.");
  }
  const codeSha = requiredString(raw.code_sha, "code_sha");
  if (!CODE_SHA_RE.test(codeSha)) throw new Error("Landing metadata has invalid code_sha.");
  if (!isRecord(raw.tests) || raw.tests.passed !== true) {
    throw new Error("Landing snapshot does not have passing tests.");
  }
  const expectedColumns = bronze
    ? bronzeColumnsForSource(pointer.source_id as BronzeCampaignSourceId)
    : bulk
      ? BULK_INDEX_COLUMNS
      : LANDING_COLUMNS;
  if (!Array.isArray(raw.columns) || raw.columns.length !== expectedColumns.length) {
    throw new Error("Landing manifest has invalid published columns.");
  }
  const columns = raw.columns.map((column, index) => {
    if (!isRecord(column) || !hasExactFields(column, new Set(["name", "type"]))) {
      throw new Error("Landing manifest has invalid published columns.");
    }
    const [expectedName, expectedType] = expectedColumns[index];
    if (column.name !== expectedName || column.type !== expectedType) {
      throw new Error("Landing manifest has invalid published columns.");
    }
    return { name: expectedName, type: expectedType };
  });
  if (!Array.isArray(raw.files) || raw.files.length === 0) {
    throw new Error("Landing manifest has no Parquet files.");
  }
  const maxBytes = bronze ? BRONZE_FILE_MAX_BYTES : LANDING_FILE_MAX_BYTES;
  const files = raw.files.map((file) => parseFile(file, expectedTableName, expectedLayer, maxBytes));
  if (new Set(files.map((file) => file.id)).size !== files.length) {
    throw new Error("Landing manifest reuses a file ID.");
  }
  const rowCount = requiredInteger(raw.row_count, "row_count", false);
  const acceptedField = bronze
    ? "accepted_file_count"
    : bulk
      ? "accepted_distribution_count"
      : "accepted_response_count";
  const publishedField = bronze
    ? "published_file_count"
    : bulk
      ? "published_distribution_count"
      : "published_response_count";
  const accepted = requiredInteger(raw[acceptedField], acceptedField);
  const published = requiredInteger(raw[publishedField], publishedField);
  const pending = requiredInteger(raw.pending_publication_count, "pending_publication_count");
  if (published !== (bronze ? files.length : rowCount) || accepted !== published + pending) {
    throw new Error("Landing manifest publication counts are inconsistent.");
  }
  const common = {
    source_id: pointer.source_id,
    snapshot_id: pointer.snapshot_id,
    created_at_utc: requiredTimestamp(raw.created_at_utc, "created_at_utc"),
    code_sha: codeSha,
    status: "validated" as const,
    table_name: expectedTableName,
    row_count: rowCount,
    files,
    columns,
    pending_publication_count: pending,
    receipt_checkpoint_sha256: requiredSha256(
      raw.receipt_checkpoint_sha256,
      "receipt_checkpoint_sha256"
    ),
    tests: { passed: true as const },
  };
  if (bronze) {
    return {
      ...common,
      format_version: 1,
      kind: "bronze_snapshot",
      source_id: pointer.source_id as BronzeCampaignSourceId,
      layer: "02_bronze" as const,
      coverage_status: raw.coverage_status as "incomplete" | "complete_current_catalogue",
      accepted_file_count: accepted,
      published_file_count: published,
    };
  }
  if (bulk) {
    return {
      ...common,
      format_version: 2,
      kind: "full_distribution_index",
      source_id: pointer.source_id as BulkLandingSourceId,
      layer: "01_landing" as const,
      coverage_status: raw.coverage_status as "incomplete" | "complete_current_catalogue",
      accepted_distribution_count: accepted,
      published_distribution_count: published,
    };
  }
  return {
    ...common,
    format_version: 1,
    kind: "landing_snapshot",
    source_id: pointer.source_id as "world_bank_wdi" | "gus_bdl" | "eurostat",
    layer: "01_landing" as const,
    coverage_status: "incomplete",
    accepted_response_count: accepted,
    published_response_count: published,
  };
}

const DBW_DATASET_NAMES = ["observations", "dictionaries", "metadata", "taxonomy"] as const;

function parseColumns(raw: unknown, label: string): Array<{ name: string; type: string }> {
  if (!Array.isArray(raw) || raw.length === 0) throw new Error(`${label} has no columns.`);
  const columns = raw.map((column) => {
    if (!isRecord(column) || !hasExactFields(column, new Set(["name", "type"]))) {
      throw new Error(`${label} has invalid columns.`);
    }
    return { name: requiredString(column.name, "column.name"), type: requiredString(column.type, "column.type") };
  });
  if (new Set(columns.map(({ name }) => name)).size !== columns.length) {
    throw new Error(`${label} has duplicate columns.`);
  }
  return columns;
}

export function parseRetainedBronzeManifest(
  raw: unknown,
  pointer: LandingSnapshotPointer
): LandingSnapshotManifest {
  if (
    !isRecord(raw) || !hasExactFields(raw, RETAINED_BRONZE_MANIFEST_FIELDS) ||
    raw.format_version !== 1 || raw.kind !== "retained_bronze_snapshot" ||
    raw.source_id !== pointer.source_id || raw.snapshot_id !== pointer.snapshot_id ||
    raw.status !== "validated" || raw.layer !== "02_bronze" ||
    raw.coverage_status !== "incomplete_retained_inventory" ||
    raw.lineage_status !== "unresolved_native_to_bronze"
  ) throw new Error("Retained DBW manifest does not match its source pointer.");
  const indicatorCount = requiredInteger(raw.indicator_count, "indicator_count", false);
  const published = requiredInteger(raw.published_indicator_count, "published_indicator_count");
  const pending = requiredInteger(raw.pending_indicator_count, "pending_indicator_count");
  if (indicatorCount !== 1550 || published + pending !== indicatorCount) {
    throw new Error("Retained DBW indicator counts are inconsistent.");
  }
  if (!isRecord(raw.tests) || !hasExactFields(raw.tests, new Set(["passed", "rows_and_schemas_preserved"])) ||
      raw.tests.passed !== true || raw.tests.rows_and_schemas_preserved !== true) {
    throw new Error("Retained DBW snapshot lacks preservation tests.");
  }
  if (!isRecord(raw.indicator_index) || !hasExactFields(raw.indicator_index, new Set(["id", "name", "size", "sha256"]))) {
    throw new Error("Retained DBW indicator index descriptor is invalid.");
  }
  const indicatorIndex = {
    id: requiredDriveId(raw.indicator_index.id, "indicator_index.id"),
    name: requiredString(raw.indicator_index.name, "indicator_index.name"),
    size: requiredInteger(raw.indicator_index.size, "indicator_index.size", false),
    sha256: requiredSha256(raw.indicator_index.sha256, "indicator_index.sha256"),
  };
  if (!/^fragment-[0-9a-f-]{36}\.json$/.test(indicatorIndex.name) || indicatorIndex.size > LANDING_FILE_MAX_BYTES) {
    throw new Error("Retained DBW indicator index exceeds its bounded contract.");
  }
  if (!Array.isArray(raw.datasets) || raw.datasets.length !== 4) throw new Error("Retained DBW datasets are incomplete.");
  const datasets = raw.datasets.map((dataset) => {
    if (!isRecord(dataset) || !hasExactFields(dataset, new Set(["name", "table_name", "row_count", "columns", "files"]))) {
      throw new Error("Retained DBW dataset has invalid fields.");
    }
    const name = requiredString(dataset.name, "dataset.name");
    if (!(DBW_DATASET_NAMES as readonly string[]).includes(name)) throw new Error("Retained DBW dataset name is unsupported.");
    const expectedTable = `br_dbw_${name === "taxonomy" ? "indicators" : name}`;
    if (dataset.table_name !== expectedTable) throw new Error("Retained DBW dataset table name is invalid.");
    const files = Array.isArray(dataset.files)
      ? dataset.files.map((file) => {
          if (!isRecord(file) || !hasExactFields(file, new Set(["id", "name", "size", "sha256", "row_count"]))) {
            throw new Error("Retained DBW dataset fragment is malformed.");
          }
          return { ...parseFile({ id: file.id, name: file.name, size: file.size, sha256: file.sha256 },
            expectedTable, "02_bronze", LANDING_FILE_MAX_BYTES),
            rowCount: requiredInteger(file.row_count, "file.row_count", false) };
        })
      : (() => { throw new Error("Retained DBW dataset files are invalid."); })();
    if (name === "observations" ? files.length !== 0 : files.length === 0) {
      throw new Error("Retained DBW fixed files or observation selection contract is invalid.");
    }
    const columns = parseColumns(dataset.columns, name);
    if (JSON.stringify(columns.map(({ name, type }) => [name, type])) !== JSON.stringify(DBW_DATASET_COLUMNS[name])) {
      throw new Error(`Retained DBW ${name} schema differs from the audited contract.`);
    }
    const rowCount = requiredInteger(dataset.row_count, "dataset.row_count");
    if (name !== "observations" && files.reduce((sum, file) => sum + file.rowCount, 0) !== rowCount) {
      throw new Error(`Retained DBW ${name} fragment rows do not match its dataset.`);
    }
    return { dataset_id: name, layer: "02_bronze" as const, table_name: expectedTable,
      row_count: rowCount, columns, files };
  });
  if (new Set(datasets.map((item) => item.dataset_id)).size !== 4) throw new Error("Retained DBW dataset names are duplicated.");
  const observationSchema = parseColumns(raw.observation_schema, "observations");
  const observationDataset = datasets.find((item) => item.dataset_id === "observations");
  const taxonomyDataset = datasets.find((item) => item.dataset_id === "taxonomy");
  if (!observationDataset || JSON.stringify(observationDataset.columns) !== JSON.stringify(observationSchema)) {
    throw new Error("Retained DBW observation schemas are inconsistent.");
  }
  if (!taxonomyDataset || taxonomyDataset.row_count !== indicatorCount) {
    throw new Error("Retained DBW taxonomy rows do not match the indicator inventory.");
  }
  const fixedIds = datasets.flatMap((dataset) => dataset.files.map((file) => file.id));
  if (new Set(fixedIds).size !== fixedIds.length) throw new Error("Retained DBW fixed datasets reuse a file ID.");
  return {
    format_version: 1, kind: "retained_bronze_snapshot", source_id: "gus_dbw_retained_bronze",
    snapshot_id: pointer.snapshot_id, created_at_utc: requiredTimestamp(raw.created_at_utc, "created_at_utc"),
    code_sha: requiredString(raw.code_sha, "code_sha"), status: "validated", layer: "02_bronze",
    coverage_status: "incomplete_retained_inventory", lineage_status: "unresolved_native_to_bronze",
    inventory_sha256: requiredSha256(raw.inventory_sha256, "inventory_sha256"),
    audit_report_sha256: requiredSha256(raw.audit_report_sha256, "audit_report_sha256"),
    audit_run_id: requiredString(raw.audit_run_id, "audit_run_id"), indicator_count: indicatorCount,
    published_indicator_count: published, pending_indicator_count: pending, datasets,
    table_name: "br_dbw_observations", columns: observationSchema, files: [],
    row_count: observationDataset.row_count,
    observation_schema: observationSchema, indicator_index: indicatorIndex,
    indicators: [], tests: { passed: true, rows_and_schemas_preserved: true },
  };
}

export function parseRetainedIndicatorIndex(
  bytes: Uint8Array,
  manifest: Extract<LandingSnapshotManifest, { kind: "retained_bronze_snapshot" }>
): RetainedBronzeIndicator[] {
  const raw = decodeJson(bytes, "retained DBW indicator index");
  if (
    !isRecord(raw) ||
    !hasExactFields(raw, new Set(["format_version", "kind", "source_id", "inventory_sha256", "indicator_count", "published_indicator_count", "pending_indicator_count", "indicators"])) ||
    raw.format_version !== 1 || raw.kind !== "retained_bronze_indicator_index" ||
    raw.source_id !== manifest.source_id || raw.inventory_sha256 !== manifest.inventory_sha256 ||
    raw.indicator_count !== manifest.indicator_count ||
    raw.published_indicator_count !== manifest.published_indicator_count ||
    raw.pending_indicator_count !== manifest.pending_indicator_count || !Array.isArray(raw.indicators)
  ) throw new Error("Retained DBW indicator index does not match its manifest.");
  const ids = new Set<number>();
  const fileIds = new Set(manifest.datasets.flatMap((dataset) => dataset.files.map((file) => file.id)));
  const fileNames = new Set(manifest.datasets.flatMap((dataset) => dataset.files.map((file) => file.name)));
  fileIds.add(manifest.indicator_index.id);
  fileNames.add(manifest.indicator_index.name);
  const indicators = raw.indicators.map((item): RetainedBronzeIndicator => {
    if (!isRecord(item) || !hasExactFields(item, new Set([
      "indicator_id", "indicator_name", "indicator_name_en", "thematic_area", "domain",
      "taxonomy_path", "status", "row_count", "parts",
    ]))) throw new Error("Retained DBW indicator index entry is malformed.");
    const indicatorId = requiredInteger(item.indicator_id, "indicator_id", false);
    if (ids.has(indicatorId)) throw new Error("Retained DBW indicator index contains duplicate IDs.");
    ids.add(indicatorId);
    if (item.status !== "pending" && item.status !== "published") throw new Error("Retained DBW indicator status is invalid.");
    const parts = Array.isArray(item.parts) ? item.parts.map((part) => {
      if (!isRecord(part) || !hasExactFields(part, new Set(["id", "name", "size", "sha256", "row_count", "part"]))) {
        throw new Error("Retained DBW indicator part is malformed.");
      }
      const parsed = parseFile({ id: part.id, name: part.name, size: part.size, sha256: part.sha256 },
        `br_dbw_observations__indicator_${indicatorId}`, "02_bronze", LANDING_FILE_MAX_BYTES);
      if (fileIds.has(parsed.id) || fileNames.has(parsed.name)) {
        throw new Error("Retained DBW publication reuses a fragment identity.");
      }
      fileIds.add(parsed.id); fileNames.add(parsed.name);
      const partNumber = requiredInteger(part.part, "part", false);
      return { ...parsed, rowCount: requiredInteger(part.row_count, "part.row_count", false), part: partNumber };
    }) : (() => { throw new Error("Retained DBW indicator parts are invalid."); })();
    const rowCount = requiredInteger(item.row_count, "indicator.row_count");
    if ((item.status === "pending" && (parts.length !== 0 || rowCount !== 0)) ||
        (item.status === "published" && parts.length === 0) ||
        parts.reduce((total, part) => total + part.rowCount, 0) !== rowCount ||
        parts.some((part, index) => part.part !== index + 1)) {
      throw new Error("Retained DBW indicator publication state is inconsistent.");
    }
    return { indicator_id: indicatorId, indicator_name: requiredString(item.indicator_name, "indicator_name"),
      indicator_name_en: typeof item.indicator_name_en === "string" ? item.indicator_name_en : "",
      thematic_area: typeof item.thematic_area === "string" ? item.thematic_area : "",
      domain: typeof item.domain === "string" ? item.domain : "",
      taxonomy_path: typeof item.taxonomy_path === "string" ? item.taxonomy_path : "",
      status: item.status, row_count: rowCount, parts };
  });
  if (indicators.length !== 1550 || ids.size !== 1550) throw new Error("Retained DBW indicator index is incomplete.");
  const published = indicators.filter(({ status }) => status === "published").length;
  const rows = indicators.reduce((total, indicator) => total + indicator.row_count, 0);
  if (published !== manifest.published_indicator_count ||
      indicators.length - published !== manifest.pending_indicator_count || rows !== manifest.row_count) {
    throw new Error("Retained DBW indicator index totals do not match its manifest.");
  }
  return indicators;
}

async function resolveSource(
  sourceId: LandingSourceId,
  campaignsFolderId: string,
  token: string,
  budget: DriveDownloadBudget
): Promise<LandingSnapshotResolution | null> {
  const sourceFolders = await findFoldersByName(sourceId, campaignsFolderId, token);
  if (sourceFolders.length === 0) return null;
  if (sourceFolders.length !== 1) {
    throw new Error(`Landing source folder '${sourceId}' is ambiguous.`);
  }
  const pointerFiles = await findNamedFilesInFolder(
    "current-landing.json",
    sourceFolders[0].id,
    token
  );
  if (pointerFiles.length === 0) return null;
  if (
    pointerFiles.length !== 1 ||
    pointerFiles[0].mimeType === "application/vnd.google-apps.folder"
  ) {
    throw new Error("current-landing.json is ambiguous or not a file.");
  }
  const pointerFile = pointerFiles[0];
  const pointerBytes = await downloadExact(
    pointerFile.id,
    pointerFile.size,
    POINTER_MAX_BYTES,
    `${sourceId}/current-landing.json`,
    token,
    budget
  );
  const pointer = parsePointer(pointerBytes, sourceId);
  const manifestMetadata = await findNamedFilesInFolderById(pointer.manifest_file_id, token);
  if (
    !manifestMetadata ||
    manifestMetadata.name !== pointer.manifest_file_name ||
    manifestMetadata.size !== pointer.manifest_size_bytes
  ) {
    throw new Error("Landing manifest metadata does not match current-landing.json.");
  }
  const manifestBytes = await downloadExact(
    pointer.manifest_file_id,
    pointer.manifest_size_bytes,
    LANDING_MANIFEST_MAX_BYTES,
    `${sourceId} Landing manifest`,
    token,
    budget
  );
  if ((await sha256Hex(manifestBytes)) !== pointer.manifest_sha256) {
    throw new Error("Landing manifest does not match its pointer SHA-256.");
  }
  const manifest = parseManifest(manifestBytes, pointer);
  if (manifest.kind === "retained_bronze_snapshot") {
    const indexBytes = await downloadExact(
      manifest.indicator_index.id, manifest.indicator_index.size, LANDING_FILE_MAX_BYTES,
      "retained DBW indicator index", token, budget
    );
    if ((await sha256Hex(indexBytes)) !== manifest.indicator_index.sha256) {
      throw new Error("Retained DBW indicator index does not match its SHA-256.");
    }
    manifest.indicators = parseRetainedIndicatorIndex(indexBytes, manifest);
  }
  return {
    pointer,
    manifest,
    fingerprint: `${sourceId}:${pointer.snapshot_id}:${pointer.manifest_file_id}:${pointer.manifest_sha256}`,
  };
}

const issuesForAll = (message: string): LandingCatalogIssue[] =>
  LANDING_SOURCE_IDS.map((source_id) => ({ source_id, message }));

/**
 * Resolves only the bounded, supported source folders. Missing folders and
 * absent pointers mean that source has not published yet. Existing malformed
 * pointers are reported independently so another source and the NBP release
 * remain usable.
 */
export async function resolveLandingCatalog(
  token: string,
  budget: DriveDownloadBudget
): Promise<LandingCatalogResolution> {
  const roots = await findFoldersByName(DRIVE_ROOT, "root", token);
  if (roots.length === 0) {
    throw new Error(`Master Lakehouse folder '${DRIVE_ROOT}' not found in Google Drive root.`);
  }
  if (roots.length !== 1) {
    throw new Error(`Master Lakehouse folder '${DRIVE_ROOT}' is ambiguous in Google Drive root.`);
  }
  const controls = await findFoldersByName("06_control", roots[0].id, token);
  if (controls.length === 0) return { snapshots: [], issues: [], fingerprint: "none" };
  if (controls.length !== 1) {
    return {
      snapshots: [],
      issues: issuesForAll("Landing control folder '06_control' is ambiguous."),
      fingerprint: "none",
    };
  }
  const campaigns = await findFoldersByName("source_campaigns", controls[0].id, token);
  if (campaigns.length === 0) return { snapshots: [], issues: [], fingerprint: "none" };
  if (campaigns.length !== 1) {
    return {
      snapshots: [],
      issues: issuesForAll("Landing control folder 'source_campaigns' is ambiguous."),
      fingerprint: "none",
    };
  }

  const snapshots: LandingSnapshotResolution[] = [];
  const issues: LandingCatalogIssue[] = [];
  // Keep this sequential: all metadata and future table downloads share one
  // per-engine 512 MiB budget, and auth failure must stop immediately.
  for (const sourceId of LANDING_SOURCE_IDS) {
    try {
      const snapshot = await resolveSource(sourceId, campaigns[0].id, token, budget);
      if (snapshot) snapshots.push(snapshot);
    } catch (error) {
      if (isGoogleDriveAuthError(error)) throw error;
      issues.push({
        source_id: sourceId,
        message: error instanceof Error ? error.message : "Unknown Landing metadata error.",
      });
    }
  }
  return {
    snapshots,
    issues,
    fingerprint:
      snapshots
        .map((snapshot) => snapshot.fingerprint)
        .sort()
        .join("|") || "none",
  };
}
