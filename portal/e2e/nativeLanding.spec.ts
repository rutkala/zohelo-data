import { test, expect } from "@playwright/test";

test("native Landing expands paginated nested folders without a publication and opens Drive-managed originals", async ({ page }) => {
  const folder = "application/vnd.google-apps.folder";
  const root = { id: "landing", name: "01_landing", mimeType: folder, parents: ["project"], version: "1", modifiedTime: "2026-09-26T00:00:00Z" };
  const nested = { id: "bdl", name: "GUS BDL", mimeType: folder, parents: ["landing"], version: "1", modifiedTime: "2026-09-26T00:00:00Z" };
  const leaf = { id: "deep", name: "_control", mimeType: folder, parents: ["bdl"], version: "1", modifiedTime: "2026-09-26T00:00:00Z" };
  const archive = { id: "huge-archive", name: "native.tar.gz", mimeType: "application/gzip", parents: ["deep"],
    version: "2", modifiedTime: "2026-09-26T01:00:00Z", size: "9000000000",
    webViewLink: "https://drive.google.com/file/d/huge-archive/view",
    webContentLink: "https://drive.google.com/uc?id=huge-archive&export=download",
    capabilities: { canDownload: true } };
  let mediaRequests = 0;
  await page.addInitScript(() => {
    sessionStorage.setItem("zohelo_gdrive_access_token", "synthetic-test-token");
    Object.defineProperty(window, "open", { configurable: true, value: () => ({
      close() {}, location: { replace(link: string) {
        (window as Window & { __nativeAction?: string }).__nativeAction = link;
      } }
    }) });
  });
  await page.route("https://www.googleapis.com/drive/v3/files**", async route => {
    const url = new URL(route.request().url());
    const headers = { "access-control-allow-origin": "*" };
    if (url.searchParams.get("alt") === "media") {
      mediaRequests++;
      return route.fulfill({ status: 403, headers });
    }
    const id = url.pathname.split("/").pop();
    if (id && id !== "files") {
      const item = ({ landing: root, bdl: nested, deep: leaf, "huge-archive": archive } as Record<string, object>)[id];
      return route.fulfill({ status: item ? 200 : 404, headers, json: item ?? {} });
    }
    const q = url.searchParams.get("q") ?? "";
    let files: object[] = [];
    let nextPageToken: string | undefined;
    if (q.includes("name='zohelo-data'")) files = [{ id: "project", name: "zohelo-data", mimeType: folder }];
    else if (q.includes("name='01_landing'")) files = [root];
    else if (q.includes("'landing' in parents")) files = [nested];
    else if (q.includes("'bdl' in parents")) files = [leaf];
    else if (q.includes("'deep' in parents")) {
      files = url.searchParams.has("pageToken") ? [archive] :
        Array.from({ length: 1000 }, (_, i) => ({ ...archive, id: `other-${i}`, name: "duplicate.unknown", size: "2", webViewLink: undefined, webContentLink: undefined, capabilities: { canDownload: false } }));
      if (!url.searchParams.has("pageToken")) nextPageToken = "continuation";
    }
    await route.fulfill({ headers, json: { files, nextPageToken } });
  });
  await page.goto("./");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.getByPlaceholder("Profile name").fill("Native fixture");
  await profile.getByRole("button", { name: "Create Profile", exact: true }).click();
  const section = page.getByRole("region", { name: "Native Landing files" });
  await section.getByText("01_landing", { exact: true }).waitFor();
  await section.getByRole("button", { name: "Expand GUS BDL" }).click();
  await section.getByRole("button", { name: "Expand _control" }).click();
  const archiveRow = section.locator('[data-native-id="huge-archive"]');
  await archiveRow.waitFor();
  await expect(section.getByText("duplicate.unknown")).toHaveCount(1000);
  await expect(archiveRow).toContainText("8.38 GiB");
  await archiveRow.getByRole("button", { name: "Open in Drive" }).click();
  await expect.poll(() => page.evaluate(() => (window as Window & { __nativeAction?: string }).__nativeAction)).toBe(archive.webViewLink);
  await archiveRow.getByRole("button", { name: "Download via Drive" }).click();
  await expect.poll(() => page.evaluate(() => (window as Window & { __nativeAction?: string }).__nativeAction)).toBe(archive.webContentLink);
  await expect(archiveRow.getByRole("button", { name: "Preview in SQL" })).toHaveCount(0);
  expect(mediaRequests).toBe(0);
  await page.getByTitle("Disconnect Google Drive").click();
  await expect(section).toHaveCount(0);
});
