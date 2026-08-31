/**
 * Types for Google Drive Lakehouse Integration
 */

export interface LakehouseFile {
  id: string;
  name: string;
  mimeType?: string;
  size?: number;
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
