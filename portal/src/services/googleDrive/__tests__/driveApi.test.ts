import { describe, expect, it, vi } from "vitest";
import { clearStoredToken, setStoredToken } from "../auth";
import { buildNativeMetadataBatchUrl, GoogleDriveAuthError, driveRequest,
  listNativeMetadataBatchPage } from "../driveApi";

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
    const activeSignal = fetchMock.mock.calls[0][1].signal as AbortSignal;
    expect(activeSignal.aborted).toBe(false);
    controller.abort();
    expect(activeSignal.aborted).toBe(true);
  });

  it("rejects incomplete searches and malformed pages without fabricating completeness", async () => {
    for (const payload of [{ files: [], incompleteSearch: true }, { files: {} },
      { files: [], nextPageToken: 23 }]) {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(payload))));
      await expect(listNativeMetadataBatchPage(["a"], "folders", "token")).rejects.toThrow("incomplete");
    }
    await expect(listNativeMetadataBatchPage(Array.from({ length: 101 }, (_, i) => String(i)),
      "files", "token")).rejects.toThrow("parent batch");
  });

  it("uses minimal scanner fields and bounds the actual encoded URL including continuation token", () => {
    const ids = Array.from({ length: 100 }, (_, i) => `${i}`.padEnd(33, "x"));
    const folder = new URL(buildNativeMetadataBatchUrl(ids, "folders", "p".repeat(200)));
    const file = new URL(buildNativeMetadataBatchUrl(ids, "files", "p".repeat(200)));
    expect(file.toString().length).toBeLessThan(7800);
    expect(folder.searchParams.get("fields")).toContain("version,modifiedTime");
    expect(folder.searchParams.get("fields")).not.toContain("webViewLink");
    expect(file.searchParams.get("fields")).toContain("webViewLink,sha256Checksum");
    expect(file.searchParams.get("fields")).not.toContain("webContentLink");
    expect(file.searchParams.get("fields")).not.toContain("capabilities");
    expect(new URL(buildNativeMetadataBatchUrl(["a'\\b"], "folders")).searchParams.get("q"))
      .toContain("'a\\'\\\\b' in parents");
    expect(() => buildNativeMetadataBatchUrl(ids, "files", "p".repeat(2200)))
      .toThrow("7800-character limit");
  });

  it("retries rate-limited scanner pages and never retries ordinary permission denial", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    const rate = new Response(JSON.stringify({ error: { errors: [{ reason: "userRateLimitExceeded" }] } }),
      { status: 403 });
    const fetchMock = vi.fn().mockResolvedValueOnce(rate)
      .mockResolvedValueOnce(new Response(JSON.stringify({ files: [] })));
    vi.stubGlobal("fetch", fetchMock);
    await expect(listNativeMetadataBatchPage(["a"], "files", "token"))
      .resolves.toEqual({ files: [], nextPageToken: null });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    fetchMock.mockClear().mockResolvedValue(new Response(JSON.stringify({ error: {
      errors: [{ reason: "insufficientFilePermissions" }] } }), { status: 403 }));
    await expect(listNativeMetadataBatchPage(["a"], "files", "token"))
      .rejects.toThrow("403");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    vi.restoreAllMocks();
  });

  it("aborts immediately during a backoff without another Drive request", async () => {
    vi.spyOn(Math, "random").mockReturnValue(0);
    const controller = new AbortController();
    const fetchMock = vi.fn().mockResolvedValue(new Response("busy", { status: 429 }));
    vi.stubGlobal("fetch", fetchMock);
    const task = listNativeMetadataBatchPage(["a"], "files", "token", undefined,
      controller.signal);
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    controller.abort();
    await expect(task).rejects.toThrow();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    vi.restoreAllMocks();
  });

  it("stops after four attempts on persistent server errors", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0);
    const fetchMock = vi.fn().mockImplementation(async () => new Response("busy", { status: 503 }));
    vi.stubGlobal("fetch", fetchMock);
    try {
      const task = listNativeMetadataBatchPage(["a"], "folders", "token");
      const rejected = expect(task).rejects.toThrow("503");
      await vi.advanceTimersByTimeAsync(250 + 500 + 1000);
      await rejected;
      expect(fetchMock).toHaveBeenCalledTimes(4);
    } finally {
      vi.useRealTimers();
      vi.restoreAllMocks();
    }
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
