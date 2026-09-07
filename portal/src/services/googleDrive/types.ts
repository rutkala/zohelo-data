/**
 * Types for Google Drive Lakehouse Integration
 */

export interface LakehouseFile {
  id: string;
  name: string;
  mimeType?: string;
  size?: number;
  /** Immutable release file digest, when the catalog is release-backed. */
  sha256?: string;
  tableName: string;
  layer: string;
}

export interface LakehouseTable {
  type: "table";
  name: string;
  id: string | null;
  layer: string;
  expanded: boolean;
  loaded: boolean;
  children: LakehouseFile[];
}

export interface LakehouseLayer {
  type: "layer";
  name: string;
  id: string | null;
  expanded: boolean;
  loaded: boolean;
  children: LakehouseTable[];
}

export interface GoogleDriveAuthState {
  token: string | null;
  isAuthenticated: boolean;
  authSource: "google_identity" | "manual" | "none";
  error: string | null;
}

export interface GoogleOAuthTokenResponse {
  access_token: string;
  expires_in?: number;
  scope?: string;
  token_type?: string;
  error?: string;
}

export interface GoogleTokenClient {
  requestAccessToken: (options?: { prompt?: string }) => void;
}

declare global {
  interface Window {
    google?: {
      accounts: {
        oauth2: {
          initTokenClient: (config: {
            client_id: string;
            scope: string;
            callback: (response: GoogleOAuthTokenResponse) => void;
            error_callback?: (error: unknown) => void;
          }) => GoogleTokenClient;
        };
      };
    };
  }
}

export interface ReleasePointer {
  format_version: 1;
  release_id: string;
  manifest_file_id: string;
  manifest_sha256: string;
  updated_at_utc: string;
  previous_manifest_file_id?: string;
}

export type ReleaseLayer = "02_bronze" | "03_silver" | "04_gold";

export interface ReleaseDataset {
  dataset_id: string;
  layer: ReleaseLayer;
  table_name: string;
  row_count: number;
  min_date: string | null;
  max_date: string | null;
  columns: Array<{ name: string; type: string }>;
  files: LakehouseFile[];
}

export interface ReleaseArtifact {
  id: string;
  name: string;
  size: number;
  sha256: string;
}

export interface ReleaseManifestBase {
  release_id: string;
  status: "validated";
  code_sha: string;
  created_at_utc: string;
  datasets: ReleaseDataset[];
  artifacts: ReleaseArtifact[];
  inputs: unknown;
  tests: { passed: true };
}

/** The historic four-dataset immutable silver release. */
export interface SilverReleaseManifest extends ReleaseManifestBase {
  format_version: 1;
  release_scope: "nbp_silver";
}

/** The complete NBP platform release, including bronze, silver, and gold. */
export interface PlatformReleaseManifest extends ReleaseManifestBase {
  format_version: 2;
  release_scope: "nbp_platform";
}

export type ReleaseManifest = SilverReleaseManifest | PlatformReleaseManifest;

export interface BusinessCatalogueSource {
  source_id: string;
  name: string;
  description: string;
  status: string;
  checked_through: string | null;
  latest_observation_date: string | null;
  last_successful_ingestion_at: string | null;
  last_attempt_at: string | null;
  raw_response_count: number;
}

export interface BusinessCatalogueLineageNode {
  id: string;
  label: string;
  kind: string;
  layer: string;
  description: string;
}

export interface BusinessCatalogue {
  format_version: 1;
  code_sha: string;
  sources: BusinessCatalogueSource[];
  lineage: {
    nodes: BusinessCatalogueLineageNode[];
    edges: Array<{ from: string; to: string }>;
  };
  /** Metric definitions are governed separately and may initially be empty. */
  metrics: Array<Record<string, unknown>>;
}

export type ReleaseCatalogResolution =
  | { kind: "legacy" }
  | {
      kind: "release";
      pointer: ReleasePointer;
      manifest: ReleaseManifest;
      manifestFileId: string;
      fingerprint: string;
      /** Present only for a validated v2 platform release. */
      businessCatalogue?: BusinessCatalogue;
    };
