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
  LandingCatalogIssue,
  LandingCatalogResolution,
  LandingSnapshotManifest,
  LandingSnapshotPointer,
  LandingSnapshotResolution,
  LandingSourceId,
  LakehouseFile,
} from "./types";

export const LANDING_SOURCE_IDS = [
  "world_bank_wdi",
  "gus_bdl",
  "eurostat",
  "world_bank_wdi_bulk",
  "eurostat_bulk",
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

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA256_RE = /^[0-9a-f]{64}$/;
const CODE_SHA_RE = /^[0-9a-f]{40}$/;
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{1,255}$/;
const PARQUET_NAME_RE =
  /^fragment-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.parquet$/;
const LANDING_MANIFEST_MAX_BYTES = 1024 * 1024;
const LANDING_FILE_MAX_BYTES = 8 * 1024 * 1024;
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

const isBulkSource = (
  sourceId: LandingSourceId
): sourceId is "world_bank_wdi_bulk" | "eurostat_bulk" => sourceId.endsWith("_bulk");

const tableNameForSource = (sourceId: LandingSourceId): string =>
  isBulkSource(sourceId)
    ? `${sourceId.slice(0, -"_bulk".length)}_distributions`
    : `${sourceId}_responses`;

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

function parseFile(raw: unknown, tableName: string): LakehouseFile {
  if (!isRecord(raw) || !hasExactFields(raw, new Set(["id", "name", "size", "sha256"]))) {
    throw new Error("Landing manifest has an invalid file.");
  }
  const name = requiredString(raw.name, "file.name");
  if (!PARQUET_NAME_RE.test(name)) {
    throw new Error("Landing snapshot files must be named Parquet files.");
  }
  const size = requiredInteger(raw.size, "file.size", false);
  if (size > LANDING_FILE_MAX_BYTES) {
    throw new Error(`Landing file '${name}' exceeds the 8 MiB publication limit.`);
  }
  return {
    id: requiredDriveId(raw.id, "file.id"),
    name,
    size,
    sha256: requiredSha256(raw.sha256, "file.sha256"),
    tableName,
    layer: "01_landing",
    mimeType: "application/vnd.apache.parquet",
  };
}

function parseManifest(
  bytes: Uint8Array,
  pointer: LandingSnapshotPointer
): LandingSnapshotManifest {
  const raw = decodeJson(bytes, `${pointer.source_id} Landing manifest`);
  const bulk = isBulkSource(pointer.source_id);
  const expectedFields = bulk ? BULK_MANIFEST_FIELDS : LANDING_MANIFEST_FIELDS;
  if (
    !isRecord(raw) ||
    !hasExactFields(raw, expectedFields) ||
    raw.format_version !== (bulk ? 2 : 1) ||
    raw.kind !== (bulk ? "full_distribution_index" : "landing_snapshot") ||
    raw.source_id !== pointer.source_id ||
    raw.snapshot_id !== pointer.snapshot_id
  ) {
    throw new Error("Landing manifest does not match its source pointer.");
  }
  if (raw.status !== "validated" || raw.layer !== "01_landing") {
    throw new Error("Landing manifest is not a validated 01_landing snapshot.");
  }
  const expectedTableName = tableNameForSource(pointer.source_id);
  if (raw.table_name !== expectedTableName) {
    throw new Error("Landing manifest has an invalid table_name.");
  }
  if (
    (!bulk && raw.coverage_status !== "incomplete") ||
    (bulk &&
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
  const expectedColumns = bulk ? BULK_INDEX_COLUMNS : LANDING_COLUMNS;
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
  const files = raw.files.map((file) => parseFile(file, expectedTableName));
  if (new Set(files.map((file) => file.id)).size !== files.length) {
    throw new Error("Landing manifest reuses a file ID.");
  }
  const rowCount = requiredInteger(raw.row_count, "row_count", false);
  const acceptedField = bulk ? "accepted_distribution_count" : "accepted_response_count";
  const publishedField = bulk ? "published_distribution_count" : "published_response_count";
  const accepted = requiredInteger(raw[acceptedField], acceptedField);
  const published = requiredInteger(raw[publishedField], publishedField);
  const pending = requiredInteger(raw.pending_publication_count, "pending_publication_count");
  if (published !== rowCount || accepted !== published + pending) {
    throw new Error("Landing manifest publication counts are inconsistent.");
  }
  const common = {
    source_id: pointer.source_id,
    snapshot_id: pointer.snapshot_id,
    created_at_utc: requiredTimestamp(raw.created_at_utc, "created_at_utc"),
    code_sha: codeSha,
    status: "validated" as const,
    layer: "01_landing" as const,
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
  if (bulk) {
    return {
      ...common,
      format_version: 2,
      kind: "full_distribution_index",
      source_id: pointer.source_id as "world_bank_wdi_bulk" | "eurostat_bulk",
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
    coverage_status: "incomplete",
    accepted_response_count: accepted,
    published_response_count: published,
  };
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
  // per-engine 64 MiB budget, and auth failure must stop immediately.
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
