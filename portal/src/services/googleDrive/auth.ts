/**
 * Google Identity Services (GIS) OAuth 2.0 Client for Google Drive
 */
import type { GoogleOAuthTokenResponse, GoogleTokenClient } from "./types";

export const DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly";
export const DRIVE_ROOT = "zohelo-data";
export const LAKEHOUSE_LAYERS = [
  "01_landing",
  "02_bronze",
  "03_silver",
  "04_gold",
  "05_archive",
] as const;

const STORAGE_KEY = "zohelo_gdrive_access_token";
const EXPIRY_STORAGE_KEY = "zohelo_gdrive_access_token_expires_at";

let tokenClientInstance: GoogleTokenClient | null = null;
let currentAccessToken: string | null = null;
let currentAccessTokenExpiresAt: number | null | undefined;

const readStoredExpiry = (): number | null => {
  try {
    const raw = sessionStorage.getItem(EXPIRY_STORAGE_KEY);
    if (!raw?.trim()) return null;
    const expiry = Number(raw);
    return Number.isFinite(expiry) && expiry >= 0 ? expiry : null;
  } catch {
    return null;
  }
};

export const resolveGoogleClientId = (
  runtimeEnv?: Partial<Window["env"]>,
  buildClientId?: string
): string => {
  const runtimeClientId = runtimeEnv?.DUCK_UI_GOOGLE_CLIENT_ID?.trim();
  const buildClient = buildClientId?.trim();
  return runtimeClientId || buildClient || "";
};

export const getGoogleClientId = (): string =>
  resolveGoogleClientId(window.env, import.meta.env.DUCK_UI_GOOGLE_CLIENT_ID);

export const getRequiredGoogleClientId = (): string => {
  const clientId = getGoogleClientId();
  if (clientId) {
    return clientId;
  }
  throw new Error(
    "Google OAuth client ID is missing. Set DUCK_UI_GOOGLE_CLIENT_ID (mapped from GOOGLE_OAUTH_CLIENT_ID in workflows) before attempting Google Drive sign-in."
  );
};

const toOAuthErrorMessage = (errorCode: string): string => {
  if (errorCode !== "invalid_client") {
    return `Google OAuth error: ${errorCode}`;
  }
  const origin = window.location.origin;
  return `Google OAuth error: invalid_client. Configure DUCK_UI_GOOGLE_CLIENT_ID for ${origin} and add that origin in Google Cloud OAuth Authorized JavaScript origins.`;
};

export const getStoredToken = (): string | null => {
  if (
    currentAccessToken &&
    currentAccessTokenExpiresAt !== undefined &&
    currentAccessTokenExpiresAt !== null &&
    Date.now() >= currentAccessTokenExpiresAt
  ) {
    clearStoredToken();
    return null;
  }
  if (currentAccessToken) return currentAccessToken;
  try {
    const stored = sessionStorage.getItem(STORAGE_KEY);
    if (stored) {
      currentAccessToken = stored;
      // Legacy raw-token entries have no expiry and remain usable until Drive
      // rejects them. Manual tokens likewise deliberately have unknown TTL.
      currentAccessTokenExpiresAt = readStoredExpiry();
      if (currentAccessTokenExpiresAt !== null && Date.now() >= currentAccessTokenExpiresAt) {
        clearStoredToken();
        return null;
      }
      return stored;
    }
  } catch (err) {
    console.warn("[GoogleAuth] sessionStorage unavailable:", err);
  }
  return null;
};

export const isStoredTokenExpired = (token: string): boolean => {
  if (!token) return false;
  let expiresAt = currentAccessToken === token ? currentAccessTokenExpiresAt : undefined;
  if (expiresAt === undefined) {
    try {
      if (sessionStorage.getItem(STORAGE_KEY) !== token) return false;
      expiresAt = readStoredExpiry();
    } catch {
      return false;
    }
  }
  return expiresAt !== null && expiresAt !== undefined && Date.now() >= expiresAt;
};

export const setStoredToken = (token: string | null, expiresAt?: number): void => {
  currentAccessToken = token;
  currentAccessTokenExpiresAt = token ? (expiresAt ?? null) : null;
  try {
    if (token) {
      sessionStorage.setItem(STORAGE_KEY, token);
      if (expiresAt === undefined) sessionStorage.removeItem(EXPIRY_STORAGE_KEY);
      else sessionStorage.setItem(EXPIRY_STORAGE_KEY, String(expiresAt));
    } else {
      sessionStorage.removeItem(STORAGE_KEY);
      sessionStorage.removeItem(EXPIRY_STORAGE_KEY);
    }
  } catch (err) {
    console.warn("[GoogleAuth] Failed to update sessionStorage:", err);
  }
};

export const clearStoredToken = (): void => {
  setStoredToken(null);
};

export const clearStoredTokenIfCurrent = (token: string): boolean => {
  if (!token || (currentAccessToken !== null && currentAccessToken !== token)) return false;
  try {
    const stored = sessionStorage.getItem(STORAGE_KEY);
    if (stored && stored !== token) return false;
  } catch {
    // Memory identity below still protects against clearing a newer token.
  }
  if (currentAccessToken !== token) return false;
  clearStoredToken();
  return true;
};

export const waitForGoogleIdentity = async (timeoutMs = 10000): Promise<boolean> => {
  if (window.google?.accounts?.oauth2) {
    return true;
  }

  const start = Date.now();
  return new Promise((resolve) => {
    const interval = setInterval(() => {
      if (window.google?.accounts?.oauth2) {
        clearInterval(interval);
        resolve(true);
      } else if (Date.now() - start > timeoutMs) {
        clearInterval(interval);
        console.warn("[GoogleAuth] Google Identity Services script load timed out.");
        resolve(false);
      }
    }, 100);
  });
};

/**
 * Initializes or reuses the Google Identity token client and requests an access token.
 */
export const requestGoogleAccessToken = async (options?: {
  promptConsent?: boolean;
}): Promise<string> => {
  const isLoaded = await waitForGoogleIdentity();
  const google = window.google;
  if (!isLoaded || !google?.accounts?.oauth2) {
    throw new Error(
      "Google Identity Services failed to load. Please check your network or disable ad-blockers."
    );
  }

  return new Promise((resolve, reject) => {
    try {
      const clientId = getRequiredGoogleClientId();
      tokenClientInstance = google.accounts.oauth2.initTokenClient({
        client_id: clientId,
        scope: DRIVE_SCOPE,
        callback: (response: GoogleOAuthTokenResponse) => {
          if (response.error) {
            reject(new Error(toOAuthErrorMessage(response.error)));
            return;
          }
          if (response.access_token) {
            const expiresAt =
              typeof response.expires_in === "number"
                ? Date.now() + response.expires_in * 1000
                : undefined;
            setStoredToken(response.access_token, expiresAt);
            resolve(response.access_token);
          } else {
            reject(new Error("No access token returned from Google Sign-In"));
          }
        },
        error_callback: (error: unknown) => {
          console.error("[GoogleAuth] Token client error callback:", error);
          reject(
            new Error(
              typeof error === "string"
                ? error
                : (error as Error)?.message || "Google Sign-In failed"
            )
          );
        },
      });

      tokenClientInstance.requestAccessToken({
        prompt: options?.promptConsent ? "consent" : "",
      });
    } catch (err) {
      reject(err);
    }
  });
};
