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

export type PublishedLayer = "01_landing" | ReleaseLayer;

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

/** The complete BDL platform release, including bronze, silver, and gold. */
export interface BdlPlatformReleaseManifest extends ReleaseManifestBase {
  format_version: 2;
  release_scope: "bdl_platform";
}

export type ReleaseManifest =
  | SilverReleaseManifest
  | PlatformReleaseManifest
  | BdlPlatformReleaseManifest;

export type LandingResponseSourceId = "world_bank_wdi" | "gus_bdl" | "eurostat";
export type BulkLandingSourceId = "world_bank_wdi_bulk" | "eurostat_bulk" | "opendata_org_bulk";
export type BronzeCampaignSourceId =
  | "opendata_org_bronze"
  | "opendata_org_locations_bronze"
  | "opendata_org_people_bronze";
export type LandingSourceId =
  | LandingResponseSourceId
  | BulkLandingSourceId
  | BronzeCampaignSourceId;

export interface LandingSnapshotPointer {
  format_version: 1;
  source_id: LandingSourceId;
  snapshot_id: string;
  manifest_file_id: string;
  manifest_file_name: string;
  manifest_sha256: string;
  manifest_size_bytes: number;
}

export interface LandingResponseSnapshotManifest {
  format_version: 1;
  kind: "landing_snapshot";
  source_id: LandingResponseSourceId;
  snapshot_id: string;
  created_at_utc: string;
  code_sha: string;
  status: "validated";
  layer: "01_landing";
  table_name: string;
  row_count: number;
  coverage_status: "incomplete";
  files: LakehouseFile[];
  columns: Array<{ name: string; type: string }>;
  accepted_response_count: number;
  published_response_count: number;
  pending_publication_count: number;
  receipt_checkpoint_sha256: string;
  tests: { passed: true };
}

/** A metadata-only index of full archives retained outside browser DuckDB. */
export interface BulkDistributionIndexManifest {
  format_version: 2;
  kind: "full_distribution_index";
  source_id: BulkLandingSourceId;
  snapshot_id: string;
  created_at_utc: string;
  code_sha: string;
  status: "validated";
  layer: "01_landing";
  table_name: string;
  row_count: number;
  coverage_status: "incomplete" | "complete_current_catalogue";
  files: LakehouseFile[];
  columns: Array<{ name: string; type: string }>;
  accepted_distribution_count: number;
  published_distribution_count: number;
  pending_publication_count: number;
  receipt_checkpoint_sha256: string;
  tests: { passed: true };
}

/** A verified Bronze campaign snapshot. */
export interface BronzeCampaignManifest {
  format_version: 1;
  kind: "bronze_snapshot";
  source_id: BronzeCampaignSourceId;
  snapshot_id: string;
  created_at_utc: string;
  code_sha: string;
  status: "validated";
  layer: "02_bronze";
  table_name: string;
  row_count: number;
  coverage_status: "incomplete" | "complete_current_catalogue";
  files: LakehouseFile[];
  columns: Array<{ name: string; type: string }>;
  accepted_file_count: number;
  published_file_count: number;
  pending_publication_count: number;
  receipt_checkpoint_sha256: string;
  tests: { passed: true };
}

export type LandingSnapshotManifest =
  | LandingResponseSnapshotManifest
  | BulkDistributionIndexManifest
  | BronzeCampaignManifest;

export interface LandingSnapshotResolution {
  pointer: LandingSnapshotPointer;
  manifest: LandingSnapshotManifest;
  fingerprint: string;
}

export interface LandingCatalogIssue {
  source_id: LandingSourceId;
  message: string;
}

export interface LandingCatalogResolution {
  snapshots: LandingSnapshotResolution[];
  issues: LandingCatalogIssue[];
  fingerprint: string;
}

/** The relation details shared by NBP releases and independent Landing snapshots. */
export interface PublishedDataset {
  dataset_id: string;
  layer: PublishedLayer;
  table_name: string;
  columns: Array<{ name: string; type: string }>;
  files: LakehouseFile[];
}

export interface BusinessCatalogueSource {
  provider_metadata?: Record<string, string>;
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

export interface SingleReleaseResolution {
  pointer: ReleasePointer;
  manifest: ReleaseManifest;
  manifestFileId: string;
  fingerprint: string;
  /** Present only for a validated v2 platform release. */
  businessCatalogue?: BusinessCatalogue;
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
      /** All validated releases participating in this resolution (e.g. NBP, BDL). */
      releases?: SingleReleaseResolution[];
    };
