/** Immutable NBP silver release resolution for browser readers. */
import { DRIVE_ROOT } from "./auth";
import {
  fetchDriveFileBuffer,
  findFoldersByName,
  findNamedFilesInFolder,
  findNamedFilesInFolderById,
} from "./driveApi";
import type {
  LakehouseFile,
  ReleaseCatalogResolution,
  ReleaseDataset,
  ReleaseManifest,
  ReleasePointer,
} from "./types";

export const DRIVE_DOWNLOAD_LIMIT_BYTES = 64 * 1024 * 1024;
export const POINTER_MAX_BYTES = 64 * 1024;
export const MANIFEST_MAX_BYTES = 4 * 1024 * 1024;
const REQUIRED_DATASET_IDS = [
  "nbp_exchange_rates_table_a",
  "nbp_exchange_rates_table_b",
  "nbp_exchange_rates_table_c",
  "nbp_gold_prices",
] as const;

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
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{1,255}$/;
const CODE_SHA_RE = /^[0-9a-f]{40}$/;
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

function parseFile(raw: unknown, datasetId: string): LakehouseFile {
  if (!isRecord(raw)) throw new Error(`Release manifest has invalid file for '${datasetId}'.`);
  return {
    id: asDriveId(raw.id, "file.id"),
    name: asNonEmptyString(raw.name, "file.name"),
    size: asPositiveInteger(raw.size, "file.size"),
    sha256: asSha256(raw.sha256, "file.sha256"),
    tableName: datasetId,
    layer: "03_silver",
  };
}

function parseDataset(raw: unknown): ReleaseDataset {
  if (!isRecord(raw)) throw new Error("Release manifest has an invalid dataset.");
  const dataset_id = asNonEmptyString(raw.dataset_id, "dataset_id");
  if (raw.layer !== "03_silver") {
    throw new Error(`Release dataset '${dataset_id}' is not in layer 03_silver.`);
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
  return {
    dataset_id,
    layer: "03_silver",
    table_name: asNonEmptyString(raw.table_name, "table_name"),
    row_count: asPositiveInteger(raw.row_count, "row_count"),
    min_date: asIsoDate(raw.min_date, "min_date"),
    max_date: asIsoDate(raw.max_date, "max_date"),
    columns: raw.columns.map((column) => ({
      name: column.name as string,
      type: column.type as string,
    })),
    files: raw.files.map((file) => parseFile(file, dataset_id)),
  };
}

function parseManifest(bytes: Uint8Array, pointer: ReleasePointer): ReleaseManifest {
  let raw: unknown;
  try {
    raw = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error("The selected release manifest is not valid JSON.");
  }
  if (!isRecord(raw) || raw.format_version !== 1) {
    throw new Error("The selected release manifest has an unsupported format_version.");
  }
  if (asCanonicalUuid(raw.release_id, "release_id") !== pointer.release_id) {
    throw new Error("The selected release manifest does not match current-release.json.");
  }
  if (raw.release_scope !== "nbp_silver" || raw.status !== "validated") {
    throw new Error("The selected release is not a validated NBP silver release.");
  }
  if (!isRecord(raw.tests) || raw.tests.passed !== true) {
    throw new Error("The selected release does not have passing tests.");
  }
  if (!Array.isArray(raw.datasets))
    throw new Error("The selected release manifest has no datasets.");
  const datasets = raw.datasets.map(parseDataset);
  for (const dataset of datasets) {
    if (dataset.min_date > dataset.max_date) {
      throw new Error(`Release dataset '${dataset.dataset_id}' has inverted date bounds.`);
    }
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
  const ids = new Set(datasets.map((dataset) => dataset.dataset_id));
  if (
    ids.size !== datasets.length ||
    ids.size !== REQUIRED_DATASET_IDS.length ||
    REQUIRED_DATASET_IDS.some((id) => !ids.has(id))
  ) {
    throw new Error(
      "The selected release must contain exactly the four required NBP silver datasets."
    );
  }
  if (!Array.isArray(raw.artifacts) || !Array.isArray(raw.inputs)) {
    throw new Error("The selected release manifest has invalid artifacts or inputs.");
  }
  return {
    format_version: 1,
    release_id: pointer.release_id,
    release_scope: "nbp_silver",
    status: "validated",
    code_sha: (() => {
      const codeSha = asNonEmptyString(raw.code_sha, "code_sha");
      if (!CODE_SHA_RE.test(codeSha)) throw new Error("Release metadata has invalid code_sha.");
      return codeSha;
    })(),
    created_at_utc: asNonEmptyString(raw.created_at_utc, "created_at_utc"),
    datasets,
    artifacts: raw.artifacts,
    inputs: raw.inputs,
    tests: { passed: true },
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
  const pointerFiles = await findNamedFilesInFolder("current-release.json", roots[0].id, token);
  if (pointerFiles.length === 0) return { kind: "legacy" };
  if (
    pointerFiles.length !== 1 ||
    pointerFiles[0].mimeType === "application/vnd.google-apps.folder"
  ) {
    throw new Error(
      "current-release.json is ambiguous or not a file; refusing to select a release."
    );
  }
  const pointerFile = pointerFiles[0];
  const pointerBytes = await downloadExact(
    pointerFile.id,
    pointerFile.size,
    POINTER_MAX_BYTES,
    "current-release.json",
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
  return {
    kind: "release",
    pointer,
    manifest,
    manifestFileId: pointer.manifest_file_id,
    fingerprint: `${pointer.release_id}:${pointer.manifest_file_id}:${pointer.manifest_sha256}`,
  };
}
