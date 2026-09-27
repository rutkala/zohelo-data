import { DRIVE_ROOT } from "./auth";
import {
  fetchDriveFileBuffer,
  findFoldersByName,
  findNamedFilesInFolder,
  findNamedFilesInFolderById,
  isGoogleDriveAuthError,
  listFolderChildrenMetadata,
} from "./driveApi";
import type { DriveFileMetadata } from "./driveApi";
import type {
  SourceInventoryEntry,
  SourceInventoryResolution,
  SourceInventoryStage,
} from "./types";
import { sha256Hex } from "./releaseCatalog";

const FOLDER = "application/vnd.google-apps.folder";
const MAX_API_PAGES = 120;
const MAX_BDL_CHECKPOINT_BYTES = 4 * 1024 * 1024;
const MAX_BDL_CHECKPOINT_ATTEMPTS = 2;
class BdlCheckpointUpdatingError extends Error {}
type StageSpec = {
  stage: "Landing" | "Bronze";
  path: string[];
  descend: number;
  suffixes: string[];
};
const PARQUET_SUFFIXES = [".parquet"];
const SOURCES: Array<{
  source_id: SourceInventoryEntry["source_id"];
  label: string;
  stages: StageSpec[];
}> = [
  {
    source_id: "gus_dbw",
    label: "GUS DBW",
    stages: [
      ...["bulk", "hvd"].map((leaf) => ({
        stage: "Landing" as const,
        path: ["01_landing", "gus_dbw", "native", leaf],
        descend: 0,
        suffixes: [".zip", ".csv", ".xlsx"],
      })),
      ...["metadata", "taxonomy"].map((leaf) => ({
        stage: "Landing" as const,
        path: ["01_landing", "gus_dbw", "native", leaf],
        descend: 0,
        suffixes: [".json"],
      })),
      ...["observations", "dictionaries", "taxonomy", "metadata"].map((leaf) => ({
        stage: "Bronze" as const,
        path: ["02_bronze", "gus_dbw", leaf],
        descend: 0,
        suffixes: PARQUET_SUFFIXES,
      })),
    ],
  },
  {
    source_id: "gus_teryt",
    label: "GUS TERYT",
    stages: [
      {
        stage: "Landing",
        path: ["01_landing", "gus_teryt", "native", "bulk"],
        descend: 0,
        suffixes: [".zip"],
      },
      { stage: "Bronze", path: ["02_bronze", "gus_teryt"], descend: 0, suffixes: PARQUET_SUFFIXES },
    ],
  },
  {
    source_id: "gugik_prg",
    label: "GUGiK PRG",
    stages: [
      {
        stage: "Landing",
        path: ["01_landing", "gugik_prg", "native", "bulk"],
        descend: 0,
        suffixes: [".zip", ".xlsx"],
      },
      { stage: "Bronze", path: ["02_bronze", "gugik_prg"], descend: 0, suffixes: PARQUET_SUFFIXES },
    ],
  },
  {
    source_id: "gleif",
    label: "GLEIF",
    stages: [
      {
        stage: "Landing",
        path: ["01_landing", "gleif", "native", "bulk"],
        descend: 1,
        suffixes: [".zip"],
      },
      { stage: "Bronze", path: ["02_bronze", "gleif"], descend: 0, suffixes: PARQUET_SUFFIXES },
    ],
  },
  {
    source_id: "mf_biala_lista",
    label: "MF VAT White List",
    stages: [
      {
        stage: "Landing",
        path: ["01_landing", "mf_biala_lista", "native", "bulk"],
        descend: 1,
        suffixes: [".7z"],
      },
      {
        stage: "Bronze",
        path: ["02_bronze", "mf_biala_lista"],
        descend: 0,
        suffixes: PARQUET_SUFFIXES,
      },
    ],
  },
  {
    source_id: "imgw_pib",
    label: "IMGW-PIB",
    stages: [
      {
        stage: "Landing",
        path: ["01_landing", "imgw_pib", "native", "bulk"],
        descend: 2,
        suffixes: [".zip", ".csv"],
      },
      { stage: "Bronze", path: ["02_bronze", "imgw_pib"], descend: 0, suffixes: PARQUET_SUFFIXES },
    ],
  },
];

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export async function resolveSourceInventory(
  token: string,
  isCurrent: () => boolean = () => true
): Promise<SourceInventoryResolution> {
  let pages = 0;
  const reserveRequest = () => {
    if (!isCurrent()) throw new Error("Files on Drive refresh was superseded.");
    pages += 1;
    if (pages > MAX_API_PAGES)
      throw new Error("Files on Drive refresh exceeded its API page budget.");
  };
  const roots = await findFoldersByName(DRIVE_ROOT, "root", token, reserveRequest);
  if (roots.length !== 1)
    throw new Error(`Master Lakehouse folder '${DRIVE_ROOT}' is missing or ambiguous.`);
  const rootId = roots[0].id;
  const pathCache = new Map<string, string | null>([["", rootId]]);
  const resolvePath = async (path: string[]) => {
    let parent = rootId;
    for (let index = 0; index < path.length; index += 1) {
      const key = path.slice(0, index + 1).join("/");
      if (pathCache.has(key)) {
        const cached = pathCache.get(key);
        if (!cached) return null;
        parent = cached;
        continue;
      }
      const matches = await findFoldersByName(path[index], parent, token, reserveRequest);
      if (matches.length > 1) throw new Error(`Folder '${key}' is ambiguous.`);
      const id = matches[0]?.id ?? null;
      pathCache.set(key, id);
      if (!id) return null;
      parent = id;
    }
    return parent;
  };
  const scan = async (folderId: string, depth: number): Promise<DriveFileMetadata[]> => {
    const children = await listFolderChildrenMetadata(folderId, token, reserveRequest);
    const files = children.filter((item) => item.mimeType !== FOLDER);
    if (depth <= 0) return files;
    for (const child of children.filter((item) => item.mimeType === FOLDER)) {
      files.push(...(await scan(child.id, depth - 1)));
    }
    return files;
  };
  const summarize = (
    stage: "Landing" | "Bronze",
    files: DriveFileMetadata[]
  ): SourceInventoryStage => ({
    stage,
    basis: "observed_drive_metadata",
    file_count: files.length,
    byte_count: files.reduce((sum, file) => {
      if (!Number.isSafeInteger(file.size) || (file.size as number) < 0)
        throw new Error("Drive file has no safe byte size.");
      const total = sum + (file.size as number);
      if (!Number.isSafeInteger(total)) throw new Error("Drive byte total exceeds safe precision.");
      return total;
    }, 0),
    latest_modified_time: files.reduce<string | null>((latest, file) => {
      if (!file.modifiedTime || Number.isNaN(Date.parse(file.modifiedTime))) {
        throw new Error("Drive file has no valid modification time.");
      }
      return !latest || file.modifiedTime > latest ? file.modifiedTime : latest;
    }, null),
  });
  const entries: SourceInventoryEntry[] = [];
  for (const source of SOURCES) {
    try {
      const byStage = new Map<"Landing" | "Bronze", DriveFileMetadata[]>();
      for (const spec of source.stages) {
        const folderId = await resolvePath(spec.path);
        const files = folderId ? await scan(folderId, spec.descend) : [];
        const payloads = files.filter(
          (file) =>
            !file.mimeType?.startsWith("application/vnd.google-apps.") &&
            spec.suffixes.some((suffix) => file.name.toLowerCase().endsWith(suffix))
        );
        byStage.set(spec.stage, [...(byStage.get(spec.stage) ?? []), ...payloads]);
      }
      const stages = [...byStage].map(([stage, files]) => summarize(stage, files));
      entries.push({
        source_id: source.source_id,
        label: source.label,
        state: stages.some((stage) => stage.file_count > 0) ? "retained" : "absent",
        fetched_at: new Date().toISOString(),
        stages,
      });
    } catch (error) {
      if (isGoogleDriveAuthError(error)) throw error;
      entries.push({
        source_id: source.source_id,
        label: source.label,
        state: "error",
        fetched_at: new Date().toISOString(),
        stages: [],
        error: error instanceof Error ? error.message : "Unknown inventory error.",
      });
    }
  }
  try {
    const controlId = await resolvePath(["01_landing", "gus_bdl", "web_bulk", "_control"]);
    if (!controlId) throw new Error("BDL Web control folder is absent.");
    let bytes: Uint8Array | null = null;
    let after: DriveFileMetadata | null = null;
    for (let attempt = 0; attempt < MAX_BDL_CHECKPOINT_ATTEMPTS; attempt += 1) {
      const candidates = await findNamedFilesInFolder(
        "web-queue-v1.json",
        controlId,
        token,
        reserveRequest
      );
      if (candidates.length !== 1) throw new Error("BDL Web checkpoint is missing or ambiguous.");
      const before = candidates[0];
      if (!before.size || before.size > MAX_BDL_CHECKPOINT_BYTES)
        throw new Error("BDL Web checkpoint exceeds its 4 MiB limit.");
      for (const identity of [
        before.version,
        before.sha256Checksum,
        before.md5Checksum,
        before.modifiedTime,
      ]) {
        if (!identity) throw new Error("BDL Web checkpoint lacks stable Drive identity metadata.");
      }
      reserveRequest();
      const candidateBytes = await fetchDriveFileBuffer(before.id, token, MAX_BDL_CHECKPOINT_BYTES);
      reserveRequest();
      const candidateAfter = await findNamedFilesInFolderById(before.id, token);
      if (
        candidateAfter &&
        before.size === candidateAfter.size &&
        before.version === candidateAfter.version &&
        before.sha256Checksum === candidateAfter.sha256Checksum &&
        before.md5Checksum === candidateAfter.md5Checksum &&
        before.modifiedTime === candidateAfter.modifiedTime
      ) {
        if (
          candidateBytes.byteLength !== before.size ||
          (await sha256Hex(candidateBytes)) !== before.sha256Checksum
        ) {
          throw new Error("BDL Web checkpoint bytes do not match stable Drive metadata.");
        }
        bytes = candidateBytes;
        after = candidateAfter;
        break;
      }
    }
    if (!bytes || !after) throw new BdlCheckpointUpdatingError();
    const raw: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (
      !isRecord(raw) ||
      raw.format_version !== 1 ||
      raw.source_id !== "gus_bdl" ||
      !isRecord(raw.selection_plans) ||
      !isRecord(raw.candidates)
    ) {
      throw new Error("BDL Web checkpoint has an unsupported schema.");
    }
    let files = 0,
      byteCount = 0,
      complete = 0;
    const candidateIds = new Set(Object.keys(raw.candidates));
    const plans = Object.entries(raw.selection_plans);
    for (const [planId, plan] of plans) {
      if (!candidateIds.has(planId))
        throw new Error("BDL Web plan is outside its candidate catalogue.");
      if (!isRecord(plan) || !isRecord(plan.summary))
        throw new Error("BDL Web selection plan is invalid.");
      const f = plan.summary.files,
        b = plan.summary.bytes;
      if (
        !Number.isSafeInteger(f) ||
        (f as number) < 0 ||
        !Number.isSafeInteger(b) ||
        (b as number) < 0
      )
        throw new Error("BDL Web selection totals are invalid.");
      files += f as number;
      byteCount += b as number;
      if (!Number.isSafeInteger(files) || !Number.isSafeInteger(byteCount)) {
        throw new Error("BDL Web selection totals exceed safe precision.");
      }
      if (plan.summary.complete === true) complete += 1;
    }
    entries.unshift({
      source_id: "gus_bdl_web",
      label: "GUS BDL Web",
      state: complete === candidateIds.size ? "retained" : "in_progress",
      fetched_at: new Date().toISOString(),
      stages: [
        {
          stage: "Landing",
          basis: "mutable_checkpoint",
          file_count: files,
          byte_count: byteCount,
          latest_modified_time: after.modifiedTime ?? null,
          selection_complete_count: complete,
          selection_total_count: candidateIds.size,
        },
      ],
    });
  } catch (error) {
    if (isGoogleDriveAuthError(error)) throw error;
    if (error instanceof BdlCheckpointUpdatingError) {
      entries.unshift({
        source_id: "gus_bdl_web",
        label: "GUS BDL Web",
        state: "updating",
        fetched_at: new Date().toISOString(),
        stages: [],
        message:
          "The collection checkpoint changed during refresh. Refresh again for stable totals.",
      });
      return { entries, drive_api_pages: pages };
    }
    entries.unshift({
      source_id: "gus_bdl_web",
      label: "GUS BDL Web",
      state: "error",
      fetched_at: new Date().toISOString(),
      stages: [],
      error: error instanceof Error ? error.message : "Unknown inventory error.",
    });
  }
  return { entries, drive_api_pages: pages };
}
