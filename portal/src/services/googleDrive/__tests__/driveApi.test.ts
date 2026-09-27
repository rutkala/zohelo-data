import { describe, expect, it, vi } from "vitest";
import { clearStoredToken, setStoredToken } from "../auth";
import { GoogleDriveAuthError, driveRequest } from "../driveApi";

describe("driveRequest authorization failures", () => {
  it("turns an expired/revoked token response into a sign-in error", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("expired", { status: 401 })));
    await expect(driveRequest("https://example.test/drive", "old-token")).rejects.toBeInstanceOf(
      GoogleDriveAuthError
    );
  });

  it("returns successful responses without retrying", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("ok", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    await expect(
      driveRequest("https://example.test/drive", "usable-token")
    ).resolves.toBeInstanceOf(Response);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("rejects a token that expires during an open session before making a request", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    setStoredToken("session-token", Date.now() + 1000);
    vi.advanceTimersByTime(1001);
    await expect(
      driveRequest("https://example.test/drive", "session-token")
    ).rejects.toBeInstanceOf(GoogleDriveAuthError);
    expect(fetchMock).not.toHaveBeenCalled();
    setStoredToken(null);
    vi.useRealTimers();
  });

  it("keeps a reloaded legacy token with no expiry metadata usable", async () => {
    const values = new Map<string, string>();
    vi.stubGlobal("sessionStorage", {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
      removeItem: (key: string) => values.delete(key),
    });
    const fetchMock = vi.fn().mockResolvedValue(new Response("ok", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    clearStoredToken();
    values.set("zohelo_gdrive_access_token", "legacy-token");
    values.delete("zohelo_gdrive_access_token_expires_at");
    await expect(
      driveRequest("https://example.test/drive", "legacy-token")
    ).resolves.toBeInstanceOf(Response);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    clearStoredToken();
  });
});
