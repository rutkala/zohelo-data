import { defineConfig } from "@playwright/test";

const configuredBase = process.env.DUCK_UI_BASEPATH ?? "./";
const basePath = configuredBase === "./" ? "/" : configuredBase;
const previewUrl = `http://localhost:4599${basePath}`;

/**
 * E2E smoke tests against the real production build through `vite preview`.
 * Run `npm run build` first; CI does.
 */
export default defineConfig({
  testDir: "./e2e",
  timeout: 90_000,
  expect: { timeout: 30_000 },
  retries: process.env.CI ? 2 : 0,
  workers: 1, // OPFS/IndexedDB state is per-origin; serialize to keep runs deterministic
  reporter: process.env.CI ? [["github"], ["list"]] : [["list"]],
  use: {
    baseURL: previewUrl,
    trace: "retain-on-failure",
  },
  webServer: {
    command: "npm exec -- vite preview --port 4599 --strictPort",
    url: previewUrl,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
