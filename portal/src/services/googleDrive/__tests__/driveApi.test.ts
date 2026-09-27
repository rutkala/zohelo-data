import { describe, expect, it, vi } from "vitest";
import { clearStoredToken, setStoredToken } from "../auth";
import { GoogleDriveAuthError, driveRequest, listNativeMetadataBatchPage } from "../driveApi";

describe("batched native metadata listing", () => {
  it("groups bounded parents, filters MIME, requests complete pages and passes abort signal", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ files: [], nextPageToken: "next" })));
    vi.stubGlobal("fetch", fetchMock);
    const controller = new AbortController();
    expect(await listNativeMetadataBatchPage(["a'", "b"], "files", "token", "previous", controller.signal))
      .toEqual({ files: [], nextPageToken: "next" });
    const url = new URL(fetchMock.mock.calls[0][0]);
    expect(url.searchParams.get("q")).toBe("('a\\'' in parents or 'b' in parents) and mimeType != 'application/vnd.google-apps.folder' and trashed=false");
    expect(url.searchParams.get("pageSize")).toBe("1000");
    expect(url.searchParams.get("fields")).toContain("incompleteSearch");
    expect(url.searchParams.get("pageToken")).toBe("previous");
    expect(fetchMock.mock.calls[0][1].signal).toBe(controller.signal);
  });

  it("rejects incomplete searches and malformed pages without fabricating completeness", async () => {
    for (const payload of [{ files: [], incompleteSearch: true }, { files: {} },
      { files: [], nextPageToken: 23 }]) {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(payload))));
      await expect(listNativeMetadataBatchPage(["a"], "folders", "token")).rejects.toThrow("incomplete");
    }
    await expect(listNativeMetadataBatchPage(Array.from({ length: 26 }, (_, i) => String(i)),
      "files", "token")).rejects.toThrow("parent batch");
  });
});

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
