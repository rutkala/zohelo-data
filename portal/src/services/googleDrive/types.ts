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

export interface ReleaseDataset {
  dataset_id: string;
  layer: "03_silver";
  table_name: string;
  row_count: number;
  min_date: string;
  max_date: string;
  columns: Array<{ name: string; type: string }>;
  files: LakehouseFile[];
}

export interface ReleaseManifest {
  format_version: 1;
  release_id: string;
  release_scope: "nbp_silver";
  status: "validated";
  code_sha: string;
  created_at_utc: string;
  datasets: ReleaseDataset[];
  artifacts: unknown;
  inputs: unknown;
  tests: { passed: true };
}

export type ReleaseCatalogResolution =
  | { kind: "legacy" }
  | {
      kind: "release";
      pointer: ReleasePointer;
      manifest: ReleaseManifest;
      manifestFileId: string;
      fingerprint: string;
    };
