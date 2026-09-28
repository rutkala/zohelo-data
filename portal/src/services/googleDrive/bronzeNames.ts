import type { PublishedDataset } from "./types";

export type BronzeSourceKey =
  | "nbp" | "gus_bdl" | "gus_dbw" | "world_bank_wdi" | "eurostat" | "opendata_org";

const SOURCE_PREFIX: Record<BronzeSourceKey, string> = {
  nbp: "nbp_",
  gus_bdl: "bdl_",
  gus_dbw: "dbw_",
  world_bank_wdi: "wdi_",
  eurostat: "eurostat_",
  opendata_org: "opendata_",
};

/** The source key is pinned by a validated release or snapshot, never guessed from a file name. */
export const canonicalBronzeName = (name: string, source: BronzeSourceKey): string => {
  const sourcePrefix = SOURCE_PREFIX[source];
  if (name.startsWith(`${source}_`)) return name;
  const unprefixed = name.startsWith("br_") ? name.slice(3) : name;
  if (name.startsWith("br_") && !unprefixed.startsWith(sourcePrefix))
    throw new Error(`Bronze table '${name}' does not belong to source '${source}'.`);
  return `${source}_${unprefixed.startsWith(sourcePrefix)
    ? unprefixed.slice(sourcePrefix.length) : unprefixed}`;
};

export interface NamedBronzeDataset {
  dataset: PublishedDataset;
  legacyName: string | null;
}

export const nameBronzeDataset = (
  dataset: PublishedDataset,
  source: BronzeSourceKey
): NamedBronzeDataset => {
  if (dataset.layer !== "02_bronze") return { dataset, legacyName: null };
  const canonical = canonicalBronzeName(dataset.table_name, source);
  return {
    dataset: { ...dataset, table_name: canonical },
    legacyName: canonical === dataset.table_name ? null : dataset.table_name,
  };
};
