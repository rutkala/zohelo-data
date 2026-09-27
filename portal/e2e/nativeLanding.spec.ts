import { test, expect } from "@playwright/test";
import { createHash } from "node:crypto";

for (const viewport of [
  { width: 1280, height: 900 },
  { width: 390, height: 844 },
]) {
  test(`Landing is a single compact browser with safe native actions at ${viewport.width}px`, async ({
    page,
  }, testInfo) => {
    await page.setViewportSize(viewport);
    const folder = "application/vnd.google-apps.folder";
    const root = {
      id: "landing",
      name: "01_landing",
      mimeType: folder,
      parents: ["project"],
      version: "1",
      modifiedTime: "2026-09-26T00:00:00Z",
    };
    const nested = {
      id: "bdl",
      name: "GUS BDL",
      mimeType: folder,
      parents: ["landing"],
      version: "1",
      modifiedTime: "2026-09-26T00:00:00Z",
    };
    const leaf = {
      id: "deep",
      name: "_control",
      mimeType: folder,
      parents: ["bdl"],
      version: "1",
      modifiedTime: "2026-09-26T00:00:00Z",
    };
    const archive = {
      id: "huge-archive",
      name: "native.tar.gz",
      mimeType: "application/gzip",
      parents: ["deep"],
      version: "2",
      modifiedTime: "2026-09-26T01:00:00Z",
      size: "9000000000",
      webViewLink: "https://drive.google.com/file/d/huge-archive/view",
      webContentLink: "https://drive.google.com/uc?id=huge-archive&export=download",
      capabilities: { canDownload: true },
    };
    const csv = "marker,value\nNATIVE_PREVIEW_OK,7\n";
    const small = {
      id: "small-csv",
      name: "sample.csv",
      mimeType: "text/csv",
      parents: ["deep"],
      version: "3",
      modifiedTime: "2026-09-26T02:00:00Z",
      size: String(Buffer.byteLength(csv)),
      sha256Checksum: createHash("sha256").update(csv).digest("hex"),
      webViewLink: "https://drive.google.com/file/d/small-csv/view",
      webContentLink: "https://drive.google.com/uc?id=small-csv&export=download",
      capabilities: { canDownload: true },
    };
    let mediaRequests = 0;
    await page.addInitScript(() => {
      sessionStorage.setItem("zohelo_gdrive_access_token", "synthetic-test-token");
      Object.defineProperty(window, "open", {
        configurable: true,
        value: () => ({
          close() {},
          location: {
            replace(link: string) {
              (window as Window & { __nativeAction?: string }).__nativeAction = link;
            },
          },
        }),
      });
    });
    await page.route("https://www.googleapis.com/drive/v3/files**", async (route) => {
      const url = new URL(route.request().url());
      const headers = { "access-control-allow-origin": "*" };
      if (url.searchParams.get("alt") === "media") {
        mediaRequests++;
        return url.pathname.endsWith("/small-csv")
          ? route.fulfill({ headers, contentType: "text/csv", body: csv })
          : route.fulfill({ status: 403, headers });
      }
      const id = url.pathname.split("/").pop();
      if (id && id !== "files") {
        const item = (
          {
            landing: root,
            bdl: nested,
            deep: leaf,
            "huge-archive": archive,
            "small-csv": small,
          } as Record<string, object>
        )[id];
        return route.fulfill({ status: item ? 200 : 404, headers, json: item ?? {} });
      }
      const q = url.searchParams.get("q") ?? "";
      let files: object[] = [];
      let nextPageToken: string | undefined;
      if (q.includes("name='zohelo-data'"))
        files = [{ id: "project", name: "zohelo-data", mimeType: folder }];
      else if (q.includes("name='01_landing'")) files = [root];
      else if (q.includes("'landing' in parents")) files = [nested];
      else if (q.includes("'bdl' in parents")) files = [leaf];
      else if (q.includes("'deep' in parents")) {
        files = url.searchParams.has("pageToken")
          ? [archive, small]
          : Array.from({ length: 1000 }, (_, i) => ({
              ...archive,
              id: `other-${i}`,
              name: "duplicate.unknown",
              size: "2",
              webViewLink: undefined,
              webContentLink: undefined,
              capabilities: { canDownload: false },
            }));
        if (!url.searchParams.has("pageToken")) nextPageToken = "continuation";
      }
      await route.fulfill({ headers, json: { files, nextPageToken } });
    });
    await page.goto("./");
    const profile = page.getByRole("dialog", { name: "Create Profile" });
    await profile.getByPlaceholder("Profile name").fill("Native fixture");
    await profile.getByRole("button", { name: "Create Profile", exact: true }).click();
    const section = page.getByRole("region", { name: "Landing", exact: true });
    if (viewport.width < 768)
      await page.getByRole("button", { name: "Tables", exact: true }).click();
    await section.getByRole("button", { name: "Expand GUS BDL" }).waitFor();
    await expect(section).toHaveCount(1);
    await expect(page.getByText("Native Landing files", { exact: true })).toHaveCount(0);
    await expect(page.getByText("Files on Drive", { exact: true })).toHaveCount(0);
    await expect(page.getByText("01_landing", { exact: true })).toHaveCount(0);
    await expect(section.getByText(folder, { exact: false })).toHaveCount(0);
    await expect(section.getByRole("button", { name: "Open in Drive" })).toHaveCount(0);
    await testInfo.attach(`landing-${viewport.width}`, {
      body: await page.screenshot(),
      contentType: "image/png",
    });
    const sourceRow = section.locator('[data-native-id="bdl"]');
    await sourceRow.getByRole("button", { name: "Actions for GUS BDL" }).click();
    await sourceRow.getByRole("button", { name: "Query file metadata" }).click();
    await expect(page.getByRole("tab", { name: "Landing/GUS BDL file metadata" })).toBeVisible();
    await expect(
      page.getByText("_control/duplicate.unknown", { exact: true }).first()
    ).toBeVisible();
    expect(mediaRequests).toBe(0);
    if (viewport.width < 768)
      await page.getByRole("button", { name: "Tables", exact: true }).click();
    await section.getByRole("button", { name: "Expand GUS BDL" }).click();
    await section.getByRole("button", { name: "Expand _control" }).click();
    const archiveRow = section.locator('[data-native-id="huge-archive"]');
    await archiveRow.waitFor();
    await expect(section.getByText("duplicate.unknown")).toHaveCount(1000);
    await expect(archiveRow).toContainText("8.38 GiB");
    await archiveRow
      .getByRole("button", { name: "Actions for native.tar.gz", exact: true })
      .click();
    await archiveRow.getByRole("button", { name: "Open in Drive" }).click();
    await expect
      .poll(() =>
        page.evaluate(() => (window as Window & { __nativeAction?: string }).__nativeAction)
      )
      .toBe(archive.webViewLink);
    await archiveRow.getByRole("button", { name: "Download via Drive" }).click();
    await expect
      .poll(() =>
        page.evaluate(() => (window as Window & { __nativeAction?: string }).__nativeAction)
      )
      .toBe(archive.webContentLink);
    await expect(archiveRow.getByRole("button", { name: "Preview in SQL" })).toHaveCount(0);
    expect(mediaRequests).toBe(0);
    const previewRow = section.locator('[data-native-id="small-csv"]');
    await previewRow.getByRole("button", { name: "File sample.csv", exact: true }).click();
    await expect(archiveRow.getByRole("button", { name: "Open in Drive" })).toHaveCount(0);
    const previewButton = previewRow.getByRole("button", { name: "Preview in SQL" });
    await previewButton.scrollIntoViewIfNeeded();
    const bounds = await previewButton.boundingBox();
    expect(bounds?.width).toBeGreaterThan(60);
    expect(bounds?.x).toBeGreaterThanOrEqual(0);
    expect((bounds?.x ?? 0) + (bounds?.width ?? 0)).toBeLessThanOrEqual(viewport.width);
    expect(await section.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(
      true
    );
    await testInfo.attach(`landing-file-${viewport.width}`, {
      body: await page.screenshot(),
      contentType: "image/png",
    });
    await previewButton.click();
    await expect(page.getByRole("tab", { name: "Landing/sample.csv" })).toBeVisible();
    // This runs on the editor's dedicated connection, not the catalogue
    // connection that registered the file. A cross-connection TEMP VIEW fails.
    await expect(page.getByText("NATIVE_PREVIEW_OK", { exact: true }).first()).toBeVisible();
    expect(mediaRequests).toBe(1);
    if (viewport.width < 768)
      await page.getByRole("button", { name: "Tables", exact: true }).click();
    await section.getByRole("button", { name: "Refresh Landing" }).click();
    await expect(section.getByRole("button", { name: "Expand GUS BDL" })).toBeVisible();
    await expect(section.locator('[data-native-id="small-csv"]')).toHaveCount(0);
    await page.getByRole("button", { name: "Disconnect Google Drive", exact: true }).click();
    await expect(section).toHaveCount(0);
  });
}
