import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import LakehouseExplorer from "./LakehouseExplorer";

const state = {
  googleAuth: { token: "token", isAuthenticated: true, authSource: "manual", error: null },
  lakehouseCatalog: [],
  lakehouseRelease: null,
  lakehouseLanding: null,
  lakehouseSourceInventory: {
    drive_api_pages: 2,
    entries: [
      {
        source_id: "gus_teryt",
        label: "GUS TERYT",
        state: "retained",
        fetched_at: "2026-09-21T10:00:00Z",
        stages: [
          {
            stage: "Landing",
            basis: "observed_drive_metadata",
            file_count: 3,
            byte_count: 12,
            latest_modified_time: "2026-09-20T10:00:00Z",
          },
        ],
      },
    ],
  },
  isLakehouseLoading: false,
  isSourceInventoryLoading: false,
  lakehouseStatusMessage: "",
  activeLakehouseDataset: null,
  activeLakehouseLayer: null,
  signInWithGoogle: vi.fn(),
  setManualGoogleToken: vi.fn(),
  disconnectGoogleDrive: vi.fn(),
  refreshLakehouseCatalog: vi.fn(),
  toggleLakehouseLayer: vi.fn(),
  toggleLakehouseTable: vi.fn(),
  selectLakehouseDataset: vi.fn(),
  selectLakehouseFile: vi.fn(),
  createTab: vi.fn(),
  executeQuery: vi.fn(),
};

vi.mock("@/store", () => ({
  useDuckStore: Object.assign((selector: (value: typeof state) => unknown) => selector(state), {
    getState: () => state,
  }),
}));

describe("Files on Drive panel", () => {
  it("starts collapsed so the query catalogue remains visible", () => {
    const html = renderToStaticMarkup(createElement(LakehouseExplorer));

    expect(html).toContain("Files on Drive");
    expect(html).toContain('aria-expanded="false"');
    expect(html).not.toContain("GUS TERYT");
    expect(html).not.toContain("3 files");
  });
});
