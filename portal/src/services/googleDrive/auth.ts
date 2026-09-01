/**
 * Google Identity Services (GIS) OAuth 2.0 Client for Google Drive
 */
import type { GoogleOAuthTokenResponse, GoogleTokenClient } from "./types";

export const DEFAULT_GOOGLE_CLIENT_ID =
  "196210210522-0q02hogqrtgl8frrr8ge8v22e8ot6ndi.apps.googleusercontent.com";
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

let tokenClientInstance: GoogleTokenClient | null = null;
let currentAccessToken: string | null = null;

export const resolveGoogleClientId = (
  runtimeEnv?: Partial<Window["env"]>,
  buildClientId?: string
): string => {
  const runtimeClientId = runtimeEnv?.DUCK_UI_GOOGLE_CLIENT_ID?.trim();
  const buildClient = buildClientId?.trim();
  return runtimeClientId || buildClient || DEFAULT_GOOGLE_CLIENT_ID;
};

export const getGoogleClientId = (): string =>
  resolveGoogleClientId(window.env, import.meta.env.DUCK_UI_GOOGLE_CLIENT_ID);

const toOAuthErrorMessage = (errorCode: string): string => {
  if (errorCode !== "invalid_client") {
    return `Google OAuth error: ${errorCode}`;
  }
  const origin = window.location.origin;
  return `Google OAuth error: invalid_client. Configure DUCK_UI_GOOGLE_CLIENT_ID for ${origin} and add that origin in Google Cloud OAuth Authorized JavaScript origins.`;
};

export const getStoredToken = (): string | null => {
  if (currentAccessToken) return currentAccessToken;
  try {
    const stored = sessionStorage.getItem(STORAGE_KEY);
    if (stored) {
      currentAccessToken = stored;
      return stored;
    }
  } catch (err) {
    console.warn("[GoogleAuth] sessionStorage unavailable:", err);
  }
  return null;
};

export const setStoredToken = (token: string | null): void => {
  currentAccessToken = token;
  try {
    if (token) {
      sessionStorage.setItem(STORAGE_KEY, token);
    } else {
      sessionStorage.removeItem(STORAGE_KEY);
    }
  } catch (err) {
    console.warn("[GoogleAuth] Failed to update sessionStorage:", err);
  }
};

export const clearStoredToken = (): void => {
  setStoredToken(null);
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
      tokenClientInstance = google.accounts.oauth2.initTokenClient({
        client_id: getGoogleClientId(),
        scope: DRIVE_SCOPE,
        callback: (response: GoogleOAuthTokenResponse) => {
          if (response.error) {
            reject(new Error(toOAuthErrorMessage(response.error)));
            return;
          }
          if (response.access_token) {
            setStoredToken(response.access_token);
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
