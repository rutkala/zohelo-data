import { afterEach, describe, expect, it, vi } from "vitest";
import {
  getStoredToken,
  clearStoredTokenIfCurrent,
  isStoredTokenExpired,
  resolveGoogleClientId,
  setStoredToken,
} from "../auth";

describe("resolveGoogleClientId", () => {
  it("prefers runtime DUCK_UI_GOOGLE_CLIENT_ID when present", () => {
    expect(
      resolveGoogleClientId(
        { DUCK_UI_GOOGLE_CLIENT_ID: "runtime-client-id" } as Partial<Window["env"]>,
        "build-client-id"
      )
    ).toBe("runtime-client-id");
  });

  it("falls back to build DUCK_UI_GOOGLE_CLIENT_ID when runtime is empty", () => {
    expect(
      resolveGoogleClientId({ DUCK_UI_GOOGLE_CLIENT_ID: " " } as Partial<Window["env"]>, "build-id")
    ).toBe("build-id");
  });

  it("returns empty when neither runtime nor build client ids are set", () => {
    expect(resolveGoogleClientId(undefined, "")).toBe("");
  });
});

describe("Google Drive access token lifetime", () => {
  afterEach(() => {
    setStoredToken(null);
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("does not reuse a GIS token after its expires_in deadline", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00Z"));
    setStoredToken("gis-token", Date.now() + 3600_000);
    expect(getStoredToken()).toBe("gis-token");
    vi.advanceTimersByTime(3600_001);
    expect(getStoredToken()).toBeNull();
  });

  it("keeps legacy and manual tokens with unknown TTL until Drive rejects them", () => {
    setStoredToken("manual-token");
    expect(getStoredToken()).toBe("manual-token");
  });

  it("treats a GIS token with expires_in zero as immediately expired", () => {
    setStoredToken("zero-lifetime-token", 0);
    expect(isStoredTokenExpired("zero-lifetime-token")).toBe(true);
    expect(getStoredToken()).toBeNull();
  });

  it("clears a matching in-memory token when session storage is unavailable", () => {
    setStoredToken("memory-token");
    vi.stubGlobal("sessionStorage", {
      getItem: () => {
        throw new Error("blocked");
      },
      removeItem: () => {
        throw new Error("blocked");
      },
    });
    expect(clearStoredTokenIfCurrent("memory-token")).toBe(true);
  });

  it("does not clear memory when storage contains a newer token", () => {
    const values = new Map<string, string>();
    vi.stubGlobal("sessionStorage", {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
      removeItem: (key: string) => values.delete(key),
    });
    setStoredToken("old-token");
    values.set("zohelo_gdrive_access_token", "new-token");
    expect(clearStoredTokenIfCurrent("old-token")).toBe(false);
    expect(getStoredToken()).toBe("old-token");
  });
});
